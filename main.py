#!/usr/bin/env python3
"""
Simple high-performance server for ESP32 audio uploads
Handles thousands of concurrent uploads, parses headers, converts Opus to WAV
"""

import os
import socket
import threading
import queue
import datetime
import struct
import wave
import tempfile
from pathlib import Path
import logging
from concurrent.futures import ThreadPoolExecutor
import subprocess

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('audio_server.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

# ==================== CONFIGURATION ====================

HOST = '0.0.0.0'
PORT = 40500
BASE_DIR = 'audio_files'
MAX_WORKERS = 200  # Handle up to 200 concurrent connections
BUFFER_SIZE = 8192

# ==================== HEADER PARSER ====================

class HeaderParser:
    """Simple header parser for ESP32 audio files"""
    
    @staticmethod
    def parse(header_data):
        """Parse header from bytes"""
        header = {}
        try:
            lines = header_data.decode('utf-8').strip().split('\n')
            for line in lines:
                if ':' in line and not line.startswith('---'):
                    key, value = line.split(':', 1)
                    header[key.strip()] = value.strip()
            
            # Extract useful fields
            result = {
                'device_id': header.get('DEVICE', 'unknown'),
                'mac': header.get('MAC', ''),
                'timestamp': header.get('TIMESTAMP', ''),
                'audio_size': int(header.get('AUDIO_SIZE', 0)),
                'sample_rate': int(header.get('SAMPLE_RATE', 24000)),
                'gps': header.get('GPS', ''),
                'bluetooth': header.get('BLUETOOTH_DEVICES', ''),
            }
            
            # Parse GPS if available
            if result['gps'] and result['gps'] != '0.0,0.0':
                gps_parts = result['gps'].split(',')
                if len(gps_parts) >= 2:
                    result['gps_lat'] = float(gps_parts[0])
                    result['gps_lon'] = float(gps_parts[1])
            
            return result
            
        except Exception as e:
            logger.error(f"Header parse error: {e}")
            return {'device_id': 'unknown'}

# ==================== AUDIO CONVERTER ====================

class AudioConverter:
    """Simple Opus to WAV converter"""
    
    def __init__(self, output_dir='converted'):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        
        # Check if ffmpeg is available
        self.ffmpeg_available = self._check_ffmpeg()
    
    def _check_ffmpeg(self):
        """Check if ffmpeg is installed"""
        try:
            subprocess.run(['ffmpeg', '-version'], 
                         capture_output=True, check=True)
            return True
        except:
            logger.warning("ffmpeg not found. Install with: sudo apt install ffmpeg")
            return False
    
    def convert_to_wav(self, opus_data, sample_rate=24000, output_path=None):
        """Convert Opus bytes to WAV file"""
        if not self.ffmpeg_available:
            logger.error("ffmpeg required for conversion")
            return None
        
        if output_path is None:
            output_path = self.output_dir / f"audio_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.wav"
        
        try:
            # Save raw Opus data to temp file
            with tempfile.NamedTemporaryFile(suffix='.opus', delete=False) as f:
                f.write(opus_data)
                temp_opus = f.name
            
            # Convert using ffmpeg
            cmd = [
                'ffmpeg',
                '-f', 'opus',
                '-ar', str(sample_rate),
                '-ac', '1',
                '-i', temp_opus,
                '-ar', str(sample_rate),
                '-ac', '1',
                '-c:a', 'pcm_s16le',
                '-f', 'wav',
                '-y',
                str(output_path)
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True)
            os.unlink(temp_opus)
            
            if result.returncode == 0:
                logger.info(f"Converted to WAV: {output_path}")
                return str(output_path)
            else:
                logger.error(f"Conversion failed: {result.stderr}")
                return None
                
        except Exception as e:
            logger.error(f"Conversion error: {e}")
            return None

# ==================== FILE HANDLER ====================

class FileHandler:
    """Handles saving files and queueing for conversion"""
    
    def __init__(self, base_dir=BASE_DIR):
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(exist_ok=True)
        self.converter = AudioConverter()
        self.parser = HeaderParser()
        self.stats = {
            'total_files': 0,
            'total_bytes': 0,
            'devices': {}
        }
        self.lock = threading.Lock()
    
    def save_file(self, client_ip, data):
        """Save uploaded file and return info"""
        try:
            # Find header end
            header_end = data.find(b'---END HEADER---\n')
            if header_end == -1:
                logger.error("No header found")
                return None
            
            header_end += len(b'---END HEADER---\n')
            header_data = data[:header_end]
            audio_data = data[header_end:]
            
            # Parse header
            header = self.parser.parse(header_data)
            device_id = header['device_id']
            
            # Create device folder
            device_dir = self.base_dir / device_id
            device_dir.mkdir(exist_ok=True)
            
            # Generate filename with timestamp
            timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"{device_id}_{timestamp}.raw"
            filepath = device_dir / filename
            
            # Save raw file
            with open(filepath, 'wb') as f:
                f.write(data)
            
            file_size = len(data)
            
            # Update stats
            with self.lock:
                self.stats['total_files'] += 1
                self.stats['total_bytes'] += file_size
                if device_id not in self.stats['devices']:
                    self.stats['devices'][device_id] = 0
                self.stats['devices'][device_id] += 1
            
            logger.info(f"Saved: {filename} from {device_id} ({file_size} bytes)")
            
            # Queue for conversion (in background thread)
            threading.Thread(
                target=self._convert_in_background,
                args=(audio_data, header, device_id, timestamp),
                daemon=True
            ).start()
            
            return {
                'filename': filename,
                'device_id': device_id,
                'size': file_size,
                'header': header
            }
            
        except Exception as e:
            logger.error(f"Save error: {e}")
            return None
    
    def _convert_in_background(self, audio_data, header, device_id, timestamp):
        """Convert audio in background thread"""
        try:
            sample_rate = header.get('sample_rate', 24000)
            
            # Create device subfolder in converted directory
            conv_dir = self.converter.output_dir / device_id
            conv_dir.mkdir(exist_ok=True)
            
            # Generate output filename
            wav_filename = f"{device_id}_{timestamp}.wav"
            wav_path = conv_dir / wav_filename
            
            # Convert
            result = self.converter.convert_to_wav(
                audio_data, 
                sample_rate, 
                str(wav_path)
            )
            
            if result:
                logger.info(f"Background conversion complete: {wav_filename}")
            
        except Exception as e:
            logger.error(f"Background conversion error: {e}")

# ==================== CLIENT HANDLER ====================

def handle_client(client_socket, client_address, file_handler):
    """Handle single client connection"""
    client_ip = client_address[0]
    
    try:
        # Set timeout
        client_socket.settimeout(30)
        
        # Receive all data
        data = b''
        while True:
            chunk = client_socket.recv(BUFFER_SIZE)
            if not chunk:
                break
            data += chunk
            
            # Simple check for complete file (can be improved)
            if len(data) > 10 * 1024 * 1024:  # 10MB max
                logger.warning(f"File too large from {client_ip}")
                break
        
        if len(data) < 100:  # Too small
            logger.warning(f"Empty or too small file from {client_ip}")
            return
        
        # Process file
        result = file_handler.save_file(client_ip, data)
        
        if result:
            # Send success response
            response = f"ACK:File {result['filename']} received OK\n"
            client_socket.send(response.encode())
            logger.info(f"✓ {client_ip} - {result['device_id']} - {result['filename']}")
        else:
            # Send error response
            client_socket.send(b"ERROR:Processing failed\n")
            logger.warning(f"✗ {client_ip} - Processing failed")
            
    except socket.timeout:
        logger.error(f"Timeout from {client_ip}")
    except Exception as e:
        logger.error(f"Error with {client_ip}: {e}")
    finally:
        client_socket.close()

# ==================== MAIN SERVER ====================

def start_server():
    """Start the high-performance server"""
    
    # Create file handler
    file_handler = FileHandler()
    
    # Create socket
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 65536)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 65536)
    
    try:
        server.bind((HOST, PORT))
        server.listen(1000)
        
        logger.info("=" * 60)
        logger.info("SIMPLE AUDIO SERVER STARTED")
        logger.info("=" * 60)
        logger.info(f"Host: {HOST}:{PORT}")
        logger.info(f"Storage: {BASE_DIR}")
        logger.info(f"Max workers: {MAX_WORKERS}")
        logger.info("=" * 60)
        
        # Thread pool for handling clients
        executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)
        
        # Stats printing thread
        def print_stats():
            while True:
                import time
                time.sleep(60)
                stats = file_handler.stats
                logger.info("-" * 60)
                logger.info(f"STATS - Total files: {stats['total_files']}")
                logger.info(f"STATS - Total bytes: {stats['total_bytes'] / 1024 / 1024:.1f} MB")
                logger.info(f"STATS - Active devices: {len(stats['devices'])}")
                logger.info("-" * 60)
        
        threading.Thread(target=print_stats, daemon=True).start()
        
        # Main accept loop
        while True:
            client, address = server.accept()
            executor.submit(handle_client, client, address, file_handler)
            
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    except Exception as e:
        logger.error(f"Server error: {e}")
    finally:
        server.close()
        executor.shutdown(wait=True)
        logger.info("Server stopped")



# ==================== MAIN ====================

if __name__ == "__main__":
    start_server()