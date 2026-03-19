import os
import mimetypes
from datetime import datetime, timezone
from pathlib import Path

from flask import (
    Flask,
    abort,
    jsonify,
    render_template,
    request,
    redirect,
    url_for,
    Response,
    stream_with_context,
)
from pymongo import MongoClient, DESCENDING

# ── configuration ─────────────────────────────────────────────────────────────
MONGO_URI  = os.environ.get("MONGO_URI",  "mongodb://localhost:27017")
MONGO_DB   = os.environ.get("MONGO_DB",   "boxuploader")
DATA_DIR   = Path(os.environ.get("DATA_DIR", "/data/devices"))
PAGE_SIZE  = int(os.environ.get("PAGE_SIZE", "20"))

app = Flask(__name__)
app.config["TEMPLATES_AUTO_RELOAD"] = True

_mongo  = MongoClient(MONGO_URI)
_db     = _mongo[MONGO_DB]
devices_col    = _db["devices"]
recordings_col = _db["recordings"]

# ── helpers ───────────────────────────────────────────────────────────────────

def _fmt_dt(v):
    """Return a human-readable UTC string from a datetime or None."""
    if not v:
        return "—"
    if isinstance(v, str):
        return v[:19].replace("T", " ")
    return v.strftime("%Y-%m-%d %H:%M")


def _doc_to_dict(doc):
    """Stringify ObjectId and datetime fields so jsonify / templates can use them."""
    out = {}
    for k, v in doc.items():
        if k == "_id":
            out[k] = str(v)
        elif isinstance(v, datetime):
            out[k] = _fmt_dt(v)
        elif isinstance(v, list):
            out[k] = [_doc_to_dict(i) if isinstance(i, dict) else i for i in v]
        else:
            out[k] = v
    return out


def _resolve_safe_path(rel_path: str) -> Path:
    """Resolve a path under DATA_DIR and block path traversal."""
    base = DATA_DIR.resolve()
    safe_path = (base / rel_path).resolve()
    try:
        safe_path.relative_to(base)
    except ValueError:
        abort(403)
    return safe_path


# ── pages ─────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    """Dashboard – list all known devices."""
    devs = [_doc_to_dict(d) for d in devices_col.find().sort("last_indexed", DESCENDING)]
    return render_template("index.html", devices=devs, total=len(devs))


@app.route("/device/<folder_name>")
def device(folder_name):
    """Per-device page with paginated recording list."""
    dev = devices_col.find_one({"folder_name": folder_name})
    if not dev:
        abort(404)

    page  = max(1, int(request.args.get("page", 1)))
    skip  = (page - 1) * PAGE_SIZE
    total = recordings_col.count_documents({"folder_name": folder_name})

    recs = [
        _doc_to_dict(r)
        for r in recordings_col.find({"folder_name": folder_name})
            .sort("recorded_at", DESCENDING)
            .skip(skip)
            .limit(PAGE_SIZE)
    ]

    return render_template(
        "device.html",
        device=_doc_to_dict(dev),
        recordings=recs,
        page=page,
        page_size=PAGE_SIZE,
        total=total,
        pages=max(1, -(-total // PAGE_SIZE)),  # ceil division
    )


@app.route("/recording/<path:audio_path>")
def recording(audio_path):
    """Individual recording detail page."""
    rec = recordings_col.find_one({"audio_path": audio_path})
    if not rec:
        abort(404)
    dev = devices_col.find_one({"folder_name": rec.get("folder_name")})
    return render_template(
        "recording.html",
        recording=_doc_to_dict(rec),
        device=_doc_to_dict(dev) if dev else {},
    )


# ── audio streaming ───────────────────────────────────────────────────────────

CHUNK = 64 * 1024  # 64 KB read chunks


@app.route("/audio/<path:audio_path>")
def serve_audio(audio_path):
    """
    Stream an .opus file from the shared DATA_DIR volume.
    Supports HTTP Range requests so the browser audio element can seek.
    """
    safe_path = _resolve_safe_path(audio_path)
    if not safe_path.exists():
        abort(404)

    file_size = safe_path.stat().st_size
    mime      = mimetypes.guess_type(str(safe_path))[0] or "audio/ogg"

    range_header = request.headers.get("Range")
    if range_header:
        # Parse "bytes=start-end"
        byte_range = range_header.strip().replace("bytes=", "")
        parts      = byte_range.split("-")
        start      = int(parts[0]) if parts[0] else 0
        end        = int(parts[1]) if len(parts) > 1 and parts[1] else file_size - 1
        end        = min(end, file_size - 1)
        length     = end - start + 1

        def generate():
            with open(safe_path, "rb") as fh:
                fh.seek(start)
                remaining = length
                while remaining > 0:
                    data = fh.read(min(CHUNK, remaining))
                    if not data:
                        break
                    remaining -= len(data)
                    yield data

        return Response(
            stream_with_context(generate()),
            status=206,
            mimetype=mime,
            headers={
                "Content-Range":  f"bytes {start}-{end}/{file_size}",
                "Accept-Ranges":  "bytes",
                "Content-Length": str(length),
            },
        )

    # Full file
    def generate_full():
        with open(safe_path, "rb") as fh:
            while True:
                data = fh.read(CHUNK)
                if not data:
                    break
                yield data

    return Response(
        stream_with_context(generate_full()),
        status=200,
        mimetype=mime,
        headers={
            "Accept-Ranges":  "bytes",
            "Content-Length": str(file_size),
        },
    )


@app.post("/recording/<path:audio_path>/delete")
def delete_recording(audio_path):
    """Delete recording file(s) from disk and remove its DB entry."""
    rec = recordings_col.find_one({"audio_path": audio_path})
    if not rec:
        abort(404)

    safe_audio_path = _resolve_safe_path(audio_path)
    folder_name = rec.get("folder_name")

    candidates = [
        safe_audio_path,
        safe_audio_path.with_suffix(".txt"),
        safe_audio_path.with_suffix(".json"),
    ]

    deleted_files = []
    failed_files = []

    for p in candidates:
        if not p.exists():
            continue
        try:
            p.unlink()
            deleted_files.append(str(p))
        except Exception as exc:
            failed_files.append({"path": str(p), "error": str(exc)})

    recordings_col.delete_one({"audio_path": audio_path})

    if folder_name:
        count = recordings_col.count_documents({"folder_name": folder_name})
        devices_col.update_one(
            {"folder_name": folder_name},
            {"$set": {"recording_count": count, "last_indexed": datetime.now(timezone.utc)}},
        )

    if request.accept_mimetypes.best == "application/json":
        status = 500 if failed_files else 200
        return jsonify({
            "ok": len(failed_files) == 0,
            "audio_path": audio_path,
            "deleted_files": deleted_files,
            "failed_files": failed_files,
        }), status

    target = url_for("device", folder_name=folder_name) if folder_name else url_for("index")
    return redirect(target, code=303)


# ── JSON API (used by JS fetch calls) ─────────────────────────────────────────

@app.route("/api/devices")
def api_devices():
    devs = [_doc_to_dict(d) for d in devices_col.find().sort("last_indexed", DESCENDING)]
    return jsonify(devs)


@app.route("/api/recordings")
def api_recordings():
    folder = request.args.get("folder_name")
    page   = max(1, int(request.args.get("page", 1)))
    filt   = {"folder_name": folder} if folder else {}
    total  = recordings_col.count_documents(filt)
    recs   = [
        _doc_to_dict(r)
        for r in recordings_col.find(filt)
            .sort("recorded_at", DESCENDING)
            .skip((page - 1) * PAGE_SIZE)
            .limit(PAGE_SIZE)
    ]
    return jsonify({"total": total, "page": page, "results": recs})


# ── entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
