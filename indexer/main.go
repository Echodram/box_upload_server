// indexer scans the folder shared with the Python upload server, indexes every
// .opus recording together with its companion JSON metadata, and upserts the
// results into MongoDB.  After the initial scan it optionally switches to
// watch mode (default) and keeps the database in sync as new files arrive.
//
// Usage:
//
//	indexer [flags]
//
// Flags / environment variables (env takes precedence over flag default):
//
//	-mongo  MONGO_URI   MongoDB connection string  (default: mongodb://localhost:27017)
//	-db     MONGO_DB    Database name              (default: boxuploader)
//	-dir    DATA_DIR    Path to shared devices/    (default: ./devices)
//	-scan               One-shot scan, no watch mode
//
// MongoDB schema
// ──────────────
//
// Collection "devices"
//   folder_name   string  – unique; matches the on-disk folder (BX…_12345 or legacy)
//   device_id     string  – BX… part for BX-derived IDs, folder_name for legacy
//   derived_imei  string  – full 15-digit IMEI when available, last-5 from name otherwise
//   id_format     string  – "BX-derived" | "legacy"
//   first_indexed time
//   last_indexed  time
//   recording_count int
//
// Collection "recordings"
//   audio_path     string  – unique relative path from DATA_DIR (forward slashes)
//   audio_filename string
//   file_size_bytes int64
//   device_id      string  – indexed; BX… part or folder_name
//   folder_name    string
//   derived_imei   string
//   recorded_at    time    – indexed (desc); from received_at in JSON or file mtime
//   received_at    time    – from JSON received_at
//   has_gps        bool    – indexed; true when lat+lon are non-null
//   latitude       float64 – omitted when null
//   longitude      float64 – omitted when null
//   bluetooth_count  int
//   bluetooth_devices []{ name string, mac string }
//   meta_path      string  – relative path to companion .json; empty when absent
//   indexed_at     time

package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/fsnotify/fsnotify"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// ── constants ─────────────────────────────────────────────────────────────────

const (
	defaultDB         = "boxuploader"
	colDevices        = "devices"
	colRecordings     = "recordings"
	connectTimeout    = 10 * time.Second
	operationTimeout  = 30 * time.Second
	jsonWaitDelay     = 600 * time.Millisecond // wait after .opus CREATE before reading JSON
)

// ── regexps ───────────────────────────────────────────────────────────────────

// bxFolderRe matches folder names produced by the Python server for BX-derived
// device IDs: e.g. "BX0A1B2C3D4E_12345"
var bxFolderRe = regexp.MustCompile(`^(BX[0-9A-Z]{10})_([0-9]{5})$`)

var btAddrRe = regexp.MustCompile(`([0-9A-Fa-f]{2}(?::[0-9A-Fa-f]{2}){5})`)

// ── on-disk structures ────────────────────────────────────────────────────────

// metaJSON mirrors the .json companion files written by the Python server
// (see process_metadata_txt in main.py).
type metaJSON struct {
	SourceFile       string     `json:"source_file"`
	SavedFile        string     `json:"saved_file"`
	DeviceID         string     `json:"device_id"`
	DeviceIDFormat   string     `json:"device_id_format"`
	DerivedIMEI      string     `json:"derived_imei"`
	ReceivedAt       string     `json:"received_at"`
	AudioFile        string     `json:"audio_file"`
	Latitude         *float64   `json:"latitude"`
	Longitude        *float64   `json:"longitude"`
	BluetoothCount   int        `json:"bluetooth_count"`
	BluetoothDevices []btDevice `json:"bluetooth_devices"`
}

type btDevice struct {
	Name string `bson:"name" json:"name"`
	MAC  string `bson:"mac"  json:"mac"`
}

type opusTagMeta struct {
	Tags            map[string][]string
	Latitude        *float64
	Longitude       *float64
	BluetoothCount  int
	BluetoothDevices []btDevice
}

// ── helpers ───────────────────────────────────────────────────────────────────

// parseFolderName splits a device folder name into its components.
//
//	BX-derived → deviceID="BX0A1B2C3D4E", imeiSuffix="12345", isBX=true
//	legacy     → deviceID=folderName,      imeiSuffix="",       isBX=false
func parseFolderName(folderName string) (deviceID, imeiSuffix string, isBX bool) {
	if m := bxFolderRe.FindStringSubmatch(folderName); m != nil {
		return m[1], m[2], true
	}
	return folderName, "", false
}

// readMeta opens and decodes a .json companion file.  Returns nil on any error.
func readMeta(path string) *metaJSON {
	f, err := os.Open(path)
	if err != nil {
		return nil
	}
	defer f.Close()
	var m metaJSON
	if err := json.NewDecoder(f).Decode(&m); err != nil {
		return nil
	}
	return &m
}

// parseTime tries several ISO 8601 layouts; returns zero time on failure.
func parseTime(s string) time.Time {
	for _, layout := range []string{
		time.RFC3339Nano,
		"2006-01-02T15:04:05.999999",
		"2006-01-02T15:04:05",
	} {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UTC()
		}
	}
	return time.Time{}
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func parseBluetoothDeviceTagValue(v string) btDevice {
	v = strings.TrimSpace(v)
	if v == "" {
		return btDevice{Name: "UNKNOWN", MAC: ""}
	}

	name := v
	mac := ""

	if i := strings.Index(v, "name="); i >= 0 {
		after := v[i+5:]
		if j := strings.Index(after, " address="); j >= 0 {
			name = strings.TrimSpace(after[:j])
		} else {
			name = strings.TrimSpace(after)
		}
	}

	if m := btAddrRe.FindStringSubmatch(v); len(m) > 1 {
		mac = strings.ToUpper(m[1])
	}

	if name == "" {
		name = "UNKNOWN"
	}

	return btDevice{Name: name, MAC: mac}
}

func parseOpusTagsPacket(packet []byte) (*opusTagMeta, error) {
	if len(packet) < 16 || string(packet[:8]) != "OpusTags" {
		return nil, fmt.Errorf("not an OpusTags packet")
	}

	offset := 8
	if offset+4 > len(packet) {
		return nil, fmt.Errorf("truncated vendor length")
	}
	vendorLen := int(binary.LittleEndian.Uint32(packet[offset : offset+4]))
	offset += 4
	if offset+vendorLen > len(packet) {
		return nil, fmt.Errorf("truncated vendor string")
	}
	offset += vendorLen

	if offset+4 > len(packet) {
		return nil, fmt.Errorf("truncated comment count")
	}
	commentCount := int(binary.LittleEndian.Uint32(packet[offset : offset+4]))
	offset += 4

	meta := &opusTagMeta{
		Tags:            map[string][]string{},
		BluetoothDevices: []btDevice{},
	}

	for i := 0; i < commentCount; i++ {
		if offset+4 > len(packet) {
			break
		}
		commentLen := int(binary.LittleEndian.Uint32(packet[offset : offset+4]))
		offset += 4
		if commentLen < 0 || offset+commentLen > len(packet) {
			break
		}
		comment := string(packet[offset : offset+commentLen])
		offset += commentLen

		eq := strings.IndexByte(comment, '=')
		if eq <= 0 {
			continue
		}
		key := strings.ToLower(strings.TrimSpace(comment[:eq]))
		val := strings.TrimSpace(comment[eq+1:])
		if key == "" {
			continue
		}
		meta.Tags[key] = append(meta.Tags[key], val)

		switch key {
		case "gps_latitude":
			if f, err := strconv.ParseFloat(val, 64); err == nil {
				v := f
				meta.Latitude = &v
			}
		case "gps_longitude":
			if f, err := strconv.ParseFloat(val, 64); err == nil {
				v := f
				meta.Longitude = &v
			}
		case "bluetooth_count":
			if n, err := strconv.Atoi(val); err == nil && n >= 0 {
				meta.BluetoothCount = n
			}
		case "bluetooth_device":
			meta.BluetoothDevices = append(meta.BluetoothDevices, parseBluetoothDeviceTagValue(val))
		}
	}

	if meta.BluetoothCount == 0 && len(meta.BluetoothDevices) > 0 {
		meta.BluetoothCount = len(meta.BluetoothDevices)
	}

	return meta, nil
}

func readNextOggPacket(r io.Reader) ([]byte, bool, error) {
	packet := make([]byte, 0, 4096)

	for {
		header := make([]byte, 27)
		if _, err := io.ReadFull(r, header); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return nil, false, io.EOF
			}
			return nil, false, err
		}

		if string(header[:4]) != "OggS" {
			return nil, false, fmt.Errorf("invalid ogg capture pattern")
		}

		segCount := int(header[26])
		lacing := make([]byte, segCount)
		if _, err := io.ReadFull(r, lacing); err != nil {
			return nil, false, err
		}

		payloadSize := 0
		for _, s := range lacing {
			payloadSize += int(s)
		}
		payload := make([]byte, payloadSize)
		if _, err := io.ReadFull(r, payload); err != nil {
			return nil, false, err
		}

		offset := 0
		for _, seg := range lacing {
			n := int(seg)
			if n > 0 {
				packet = append(packet, payload[offset:offset+n]...)
			}
			offset += n
			if seg < 255 {
				complete := true
				return packet, complete, nil
			}
		}
	}
}

func readOpusTags(path string) *opusTagMeta {
	f, err := os.Open(path)
	if err != nil {
		return nil
	}
	defer f.Close()

	// First packet should be OpusHead; second should be OpusTags.
	_, _, err = readNextOggPacket(f)
	if err != nil {
		return nil
	}
	tagsPacket, _, err := readNextOggPacket(f)
	if err != nil {
		return nil
	}

	meta, err := parseOpusTagsPacket(tagsPacket)
	if err != nil {
		return nil
	}
	return meta
}

// ── indexer ───────────────────────────────────────────────────────────────────

type indexer struct {
	dataDir string
	devices *mongo.Collection
	recs    *mongo.Collection
}

func (ix *indexer) syncDeviceRecordingCount(ctx context.Context, folderName string) error {
	if folderName == "" {
		return nil
	}

	count, err := ix.recs.CountDocuments(ctx, bson.M{"folder_name": folderName})
	if err != nil {
		return err
	}

	_, err = ix.devices.UpdateOne(
		ctx,
		bson.M{"folder_name": folderName},
		bson.M{"$set": bson.M{"recording_count": count, "last_indexed": time.Now().UTC()}},
	)
	return err
}

func (ix *indexer) removeRecordingByPath(ctx context.Context, audioPath string) error {
	audioPath = filepath.ToSlash(audioPath)
	if audioPath == "" {
		return nil
	}

	var deleted struct {
		FolderName string `bson:"folder_name"`
	}
	err := ix.recs.FindOneAndDelete(ctx, bson.M{"audio_path": audioPath}).Decode(&deleted)
	if err == mongo.ErrNoDocuments {
		return nil
	}
	if err != nil {
		return err
	}

	if e := ix.syncDeviceRecordingCount(ctx, deleted.FolderName); e != nil {
		log.Printf("WARN sync count %s: %v", deleted.FolderName, e)
	}

	log.Printf("DEL   %s", audioPath)
	return nil
}

func (ix *indexer) pruneStaleRecordings(ctx context.Context) error {
	cursor, err := ix.recs.Find(ctx, bson.M{}, options.Find().SetProjection(bson.M{"audio_path": 1}))
	if err != nil {
		return err
	}
	defer cursor.Close(ctx)

	for cursor.Next(ctx) {
		var row struct {
			AudioPath string `bson:"audio_path"`
		}
		if err := cursor.Decode(&row); err != nil {
			log.Printf("WARN decode stale row: %v", err)
			continue
		}
		if row.AudioPath == "" {
			continue
		}

		abs := filepath.Join(ix.dataDir, filepath.FromSlash(row.AudioPath))
		if _, err := os.Stat(abs); os.IsNotExist(err) {
			if e := ix.removeRecordingByPath(ctx, row.AudioPath); e != nil {
				log.Printf("WARN remove stale %s: %v", row.AudioPath, e)
			}
		}
	}

	return cursor.Err()
}

// ensureIndexes creates all MongoDB indexes idempotently (CreateMany is a no-op
// for indexes that already exist when the same name and key are supplied).
func (ix *indexer) ensureIndexes(ctx context.Context) error {
	_, err := ix.devices.Indexes().CreateMany(ctx, []mongo.IndexModel{
		{
			Keys:    bson.D{{Key: "folder_name", Value: 1}},
			Options: options.Index().SetUnique(true).SetName("folder_name_1"),
		},
		{
			Keys:    bson.D{{Key: "device_id", Value: 1}},
			Options: options.Index().SetName("device_id_1"),
		},
	})
	if err != nil {
		return err
	}

	_, err = ix.recs.Indexes().CreateMany(ctx, []mongo.IndexModel{
		// Primary lookup key – guarantees idempotent upserts.
		{
			Keys:    bson.D{{Key: "audio_path", Value: 1}},
			Options: options.Index().SetUnique(true).SetName("audio_path_1"),
		},
		// Per-device listing (most common webapp query).
		{
			Keys: bson.D{
				{Key: "device_id", Value: 1},
				{Key: "recorded_at", Value: -1},
			},
			Options: options.Index().SetName("device_id_1_recorded_at_-1"),
		},
		// Global timeline.
		{
			Keys:    bson.D{{Key: "recorded_at", Value: -1}},
			Options: options.Index().SetName("recorded_at_-1"),
		},
		// GPS-only filter.
		{
			Keys:    bson.D{{Key: "has_gps", Value: 1}},
			Options: options.Index().SetName("has_gps_1"),
		},
	})
	return err
}

// upsertDevice creates or updates the device document for folderName.
func (ix *indexer) upsertDevice(ctx context.Context, folderName, fullIMEI string) error {
	deviceID, imeiSuffix, isBX := parseFolderName(folderName)

	idFormat := "legacy"
	derivedIMEI := imeiSuffix // last-5 from folder name
	if isBX {
		idFormat = "BX-derived"
	}
	// Prefer the full IMEI recovered from the JSON metadata when available.
	if len(fullIMEI) >= 14 {
		derivedIMEI = fullIMEI
	}

	now := time.Now().UTC()
	filter := bson.M{"folder_name": folderName}
	update := bson.M{
		"$set": bson.M{
			"device_id":    deviceID,
			"id_format":    idFormat,
			"last_indexed": now,
		},
		// Only set these fields when the document is first created.
		"$setOnInsert": bson.M{
			"first_indexed":   now,
			"recording_count": 0,
		},
	}
	// Overwrite derived_imei only when we have a better (longer) value.
	if derivedIMEI != "" {
		update["$max"] = bson.M{"derived_imei_len": len(derivedIMEI)}
		setDoc := update["$set"].(bson.M)
		setDoc["derived_imei"] = derivedIMEI
	}

	_, err := ix.devices.UpdateOne(ctx, filter, update, options.Update().SetUpsert(true))
	return err
}

// indexOpusFile upserts a single recording document for the .opus file at
// absPath (absolute) / relPath (relative to ix.dataDir).
func (ix *indexer) indexOpusFile(ctx context.Context, absPath, relPath string) error {
	fi, err := os.Stat(absPath)
	if err != nil {
		return err
	}

	folderName := filepath.Base(filepath.Dir(absPath))
	deviceID, imeiSuffix, isBX := parseFolderName(folderName)
	derivedIMEI := imeiSuffix
	if !isBX {
		derivedIMEI = ""
	}

	// ── companion JSON ────────────────────────────────────────────────────────
	baseName := strings.TrimSuffix(absPath, filepath.Ext(absPath))
	jsonAbs := baseName + ".json"
	jsonRel := ""

	var meta *metaJSON
	if _, e := os.Stat(jsonAbs); e == nil {
		jsonRel, _ = filepath.Rel(ix.dataDir, jsonAbs)
		meta = readMeta(jsonAbs)
	}

	// ── OpusTags (embedded metadata) ─────────────────────────────────────────
	opusMeta := readOpusTags(absPath)

	// ── timestamps ────────────────────────────────────────────────────────────
	receivedAt := fi.ModTime().UTC()
	recordedAt := receivedAt

	if meta != nil {
		if t := parseTime(meta.ReceivedAt); !t.IsZero() {
			receivedAt = t
			recordedAt = t
		}
		// Full IMEI from metadata always beats the partial suffix.
		if len(meta.DerivedIMEI) >= 14 {
			derivedIMEI = meta.DerivedIMEI
		}
	}

	// ── GPS ───────────────────────────────────────────────────────────────────
	var lat, lon *float64
	if opusMeta != nil {
		lat = opusMeta.Latitude
		lon = opusMeta.Longitude
	}
	if (lat == nil || lon == nil) && meta != nil {
		lat = meta.Latitude
		lon = meta.Longitude
	}
	hasGPS := lat != nil && lon != nil

	// ── Bluetooth ─────────────────────────────────────────────────────────────
	btCount := 0
	btDevices := []btDevice{}
	if opusMeta != nil {
		btCount = opusMeta.BluetoothCount
		if opusMeta.BluetoothDevices != nil {
			btDevices = opusMeta.BluetoothDevices
		}
	}
	if btCount == 0 && len(btDevices) == 0 && meta != nil {
		btCount = meta.BluetoothCount
		if meta.BluetoothDevices != nil {
			btDevices = meta.BluetoothDevices
		}
	}

	// ── build document ────────────────────────────────────────────────────────
	now := time.Now().UTC()
	audioPath := filepath.ToSlash(relPath)
	doc := bson.M{
		"audio_path":        audioPath,
		"audio_filename":    fi.Name(),
		"file_size_bytes":   fi.Size(),
		"device_id":         deviceID,
		"folder_name":       folderName,
		"derived_imei":      derivedIMEI,
		"recorded_at":       recordedAt,
		"received_at":       receivedAt,
		"has_gps":           hasGPS,
		"bluetooth_count":   btCount,
		"bluetooth_devices": btDevices,
		"meta_path":         filepath.ToSlash(jsonRel),
		"indexed_at":        now,
	}
	if opusMeta != nil && len(opusMeta.Tags) > 0 {
		doc["opustags"] = opusMeta.Tags
	}
	if lat != nil {
		doc["latitude"] = *lat
	}
	if lon != nil {
		doc["longitude"] = *lon
	}

	filter := bson.M{"audio_path": audioPath}
	res, err := ix.recs.UpdateOne(ctx, filter, bson.M{"$set": doc}, options.Update().SetUpsert(true))
	if err != nil {
		return err
	}

	// Keep device document and recording_count in sync.
	if err := ix.upsertDevice(ctx, folderName, derivedIMEI); err != nil {
		log.Printf("WARN upsert device %s: %v", folderName, err)
	}
	if res.UpsertedCount > 0 {
		_, _ = ix.devices.UpdateOne(
			ctx,
			bson.M{"folder_name": folderName},
			bson.M{"$inc": bson.M{"recording_count": 1}},
		)
		log.Printf("NEW   %s", audioPath)
	} else {
		log.Printf("OK    %s", audioPath)
	}

	return nil
}

// scanDir walks ix.dataDir and indexes every .opus file found.
func (ix *indexer) scanDir(ctx context.Context) error {
	err := filepath.Walk(ix.dataDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			log.Printf("WARN walk %s: %v", path, err)
			return nil // continue
		}
		if info.IsDir() || strings.ToLower(filepath.Ext(path)) != ".opus" {
			return nil
		}
		rel, _ := filepath.Rel(ix.dataDir, path)
		if e := ix.indexOpusFile(ctx, path, rel); e != nil {
			log.Printf("ERROR index %s: %v", rel, e)
		}
		return nil
	})
	if err != nil {
		return err
	}

	// Remove DB rows whose files no longer exist on disk.
	if err := ix.pruneStaleRecordings(ctx); err != nil {
		log.Printf("WARN prune stale recordings: %v", err)
	}

	return nil
}

// watchDir uses fsnotify to watch ix.dataDir recursively for new or updated
// .opus/.json files and indexes them as they arrive.
func (ix *indexer) watchDir(ctx context.Context) error {
	w, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	}
	defer w.Close()

	// Register all existing directories so we catch files in device sub-folders.
	if err := filepath.Walk(ix.dataDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if info.IsDir() {
			return w.Add(path)
		}
		return nil
	}); err != nil {
		return err
	}

	log.Printf("Watching %s for new recordings…", ix.dataDir)

	for {
		select {
		case <-ctx.Done():
			return nil

		case event, ok := <-w.Events:
			if !ok {
				return nil
			}

			path := event.Name
			ext := strings.ToLower(filepath.Ext(path))

			if event.Op&(fsnotify.Remove|fsnotify.Rename) != 0 {
				switch ext {
				case ".opus":
					rel, e := filepath.Rel(ix.dataDir, path)
					if e == nil {
						opCtx, cancel := context.WithTimeout(context.Background(), operationTimeout)
						if e := ix.removeRecordingByPath(opCtx, rel); e != nil {
							log.Printf("WARN remove %s: %v", rel, e)
						}
						cancel()
					}
				case ".json":
					opusPath := strings.TrimSuffix(path, ".json") + ".opus"
					rel, e := filepath.Rel(ix.dataDir, opusPath)
					if e == nil {
						opCtx, cancel := context.WithTimeout(context.Background(), operationTimeout)
						if e := ix.removeRecordingByPath(opCtx, rel); e != nil {
							log.Printf("WARN remove %s: %v", rel, e)
						}
						cancel()
					}
				}
				continue
			}

			// Only act on Create or Write events.
			if event.Op&(fsnotify.Create|fsnotify.Write) == 0 {
				continue
			}

			switch ext {
			case ".opus":
				// Give the server a moment to finish writing the companion .json.
				time.AfterFunc(jsonWaitDelay, func() {
					rel, _ := filepath.Rel(ix.dataDir, path)
					opCtx, cancel := context.WithTimeout(context.Background(), operationTimeout)
					defer cancel()
					if e := ix.indexOpusFile(opCtx, path, rel); e != nil {
						log.Printf("ERROR index %s: %v", rel, e)
					}
				})

			case ".json":
				// When the metadata file lands, re-index the paired .opus (which
				// may already be in MongoDB without GPS/BT data).
				opusPath := strings.TrimSuffix(path, ".json") + ".opus"
				if _, e := os.Stat(opusPath); e == nil {
					rel, _ := filepath.Rel(ix.dataDir, opusPath)
					opCtx, cancel := context.WithTimeout(context.Background(), operationTimeout)
					if e := ix.indexOpusFile(opCtx, opusPath, rel); e != nil {
						log.Printf("ERROR index %s: %v", rel, e)
					}
					cancel()
				}

			default:
				// Register new sub-directories so we watch files inside them too.
				fi, e := os.Stat(path)
				if e == nil && fi.IsDir() {
					_ = w.Add(path)
					log.Printf("Watching new directory %s", path)
				}
			}

		case watchErr, ok := <-w.Errors:
			if !ok {
				return nil
			}
			log.Printf("WARN watcher: %v", watchErr)
		}
	}
}

// ── main ──────────────────────────────────────────────────────────────────────

func main() {
	mongoURI := flag.String("mongo", getenv("MONGO_URI", "mongodb://localhost:27017"),
		"MongoDB connection URI")
	dbName := flag.String("db", getenv("MONGO_DB", defaultDB),
		"MongoDB database name")
	dataDir := flag.String("dir", getenv("DATA_DIR", "./devices"),
		"Path to the shared devices folder")
	scanOnly := flag.Bool("scan", false,
		"Run initial scan only and exit (no watch mode)")
	flag.Parse()

	// ── MongoDB connection ────────────────────────────────────────────────────
	connCtx, connCancel := context.WithTimeout(context.Background(), connectTimeout)
	defer connCancel()

	client, err := mongo.Connect(connCtx, options.Client().ApplyURI(*mongoURI))
	if err != nil {
		log.Fatalf("mongo.Connect: %v", err)
	}
	defer func() {
		shutCtx, cancel := context.WithTimeout(context.Background(), connectTimeout)
		_ = client.Disconnect(shutCtx)
		cancel()
	}()

	pingCtx, pingCancel := context.WithTimeout(context.Background(), connectTimeout)
	defer pingCancel()
	if err := client.Ping(pingCtx, nil); err != nil {
		log.Fatalf("mongo ping: %v", err)
	}
	log.Printf("Connected to MongoDB: %s  db=%s", *mongoURI, *dbName)

	db := client.Database(*dbName)
	ix := &indexer{
		dataDir: *dataDir,
		devices: db.Collection(colDevices),
		recs:    db.Collection(colRecordings),
	}

	// ── ensure indexes ────────────────────────────────────────────────────────
	idxCtx, idxCancel := context.WithTimeout(context.Background(), operationTimeout)
	defer idxCancel()
	if err := ix.ensureIndexes(idxCtx); err != nil {
		log.Fatalf("ensureIndexes: %v", err)
	}
	log.Println("Indexes OK")

	// ── initial scan ──────────────────────────────────────────────────────────
	log.Printf("Scanning %s…", *dataDir)
	scanCtx, scanCancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer scanCancel()
	if err := ix.scanDir(scanCtx); err != nil {
		log.Printf("scan error: %v", err)
	}
	log.Println("Initial scan complete")

	if *scanOnly {
		return
	}

	// ── watch mode ────────────────────────────────────────────────────────────
	appCtx, appCancel := context.WithCancel(context.Background())
	defer appCancel()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		<-sig
		log.Println("Shutting down…")
		appCancel()
	}()

	if err := ix.watchDir(appCtx); err != nil {
		log.Fatalf("watchDir: %v", err)
	}
}
