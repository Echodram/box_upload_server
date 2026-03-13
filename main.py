import socket
import os
import datetime
import threading
import queue
import time
from pathlib import Path
import select
import logging
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict
import hashlib
import json

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('opus_server.log'),
        logging.StreamHandler()
    ]
)

class DeviceManager:
    """Manages device folders and configurations"""
    def __init__(self, base_directory='received_files'):
        self.base_directory = Path(base_directory)
        self.devices_directory = self.base_directory / 'devices'
        self.device_folders = {}
        self.device_info_file = self.base_directory / 'devices.json'
        self.lock = threading.Lock()
        self.load_device_info()
        
    def load_device_info(self):
        """Load device information from JSON file"""
        try:
            if self.device_info_file.exists():
                with open(self.device_info_file, 'r') as f:
                    self.device_folders = json.load(f)
                logging.info(f"Loaded {len(self.device_folders)} devices from registry")
        except Exception as e:
            logging.error(f"Error loading device info: {e}")
            self.device_folders = {}
    
    def save_device_info(self):
        """Save device information to JSON file"""
        try:
            with open(self.device_info_file, 'w') as f:
                json.dump(self.device_folders, f, indent=2)
        except Exception as e:
            logging.error(f"Error saving device info: {e}")
    
    def get_device_folder(self, client_address, device_id=None):
        """Get or create folder for a specific device"""
        client_ip = client_address[0]
        
        # Use provided device ID or generate one from IP
        if device_id:
            device_key = device_id
        else:
            device_key = f"device_{client_ip.replace('.', '_')}"
        
        with self.lock:
            if device_key not in self.device_folders:
                # Create new device entry
                folder_name = device_key
                folder_path = self.devices_directory / folder_name
                folder_path.mkdir(parents=True, exist_ok=True)
                
                self.device_folders[device_key] = {
                    'folder_name': folder_name,
                    'folder_path': str(folder_path),
                    'ip_address': client_ip,
                    'first_seen': datetime.datetime.now().isoformat(),
                    'last_seen': datetime.datetime.now().isoformat(),
                    'files_received': 0
                }
                self.save_device_info()
                logging.info(f"Created new device folder: {folder_name} for IP {client_ip}")
            else:
                # Update last seen timestamp
                self.device_folders[device_key]['last_seen'] = datetime.datetime.now().isoformat()
                self.device_folders[device_key]['ip_address'] = client_ip
            
            return Path(self.device_folders[device_key]['folder_path'])
    
    def increment_file_count(self, device_key):
        """Increment file count for a device"""
        with self.lock:
            if device_key in self.device_folders:
                self.device_folders[device_key]['files_received'] += 1
                self.device_folders[device_key]['last_seen'] = datetime.datetime.now().isoformat()
                self.save_device_info()
    
    def get_device_stats(self):
        """Get statistics for all devices"""
        with self.lock:
            return {
                'total_devices': len(self.device_folders),
                'devices': self.device_folders
            }

class ConnectionManager:
    """Manages client connections and prevents overload"""
    def __init__(self, max_connections=10000, max_concurrent_per_ip=10):
        self.max_connections = max_connections
        self.max_concurrent_per_ip = max_concurrent_per_ip
        self.active_connections = 0
        self.connections_per_ip = defaultdict(int)
        self.lock = threading.Lock()
        
    def can_accept_connection(self, client_ip):
        """Check if we can accept a new connection from this IP"""
        with self.lock:
            if self.active_connections >= self.max_connections:
                logging.warning(f"Max connections reached ({self.max_connections})")
                return False
            
            if self.connections_per_ip[client_ip] >= self.max_concurrent_per_ip:
                logging.warning(f"Max concurrent connections reached for IP {client_ip}")
                return False
            
            self.active_connections += 1
            self.connections_per_ip[client_ip] += 1
            return True
    
    def connection_closed(self, client_ip):
        """Notify that a connection has closed"""
        with self.lock:
            self.active_connections -= 1
            self.connections_per_ip[client_ip] -= 1
            if self.connections_per_ip[client_ip] <= 0:
                del self.connections_per_ip[client_ip]

class FileProcessor:
    """Handles file writing with queue-based processing"""
    def __init__(self, device_manager, max_workers=50):
        self.device_manager = device_manager
        self.file_queue = queue.Queue(maxsize=10000)
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.running = True
        
        # Start file processing threads
        for i in range(max_workers):
            threading.Thread(target=self._process_files, daemon=True, name=f"FileProcessor-{i}").start()
    
    def queue_file(self, filename, file_data, client_address, device_id=None):
        """Queue a file for processing"""
        try:
            # Get device folder
            device_folder = self.device_manager.get_device_folder(client_address, device_id)
            device_key = device_folder.name
            
            # Add timestamp to avoid conflicts
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            
            name_parts = filename.rsplit('.', 1)
            if len(name_parts) == 2:
                unique_filename = f"{name_parts[0]}_{timestamp}.{name_parts[1]}"
            else:
                unique_filename = f"{filename}_{timestamp}"
            
            file_info = {
                'filename': unique_filename,
                'original_filename': filename,
                'data': file_data,
                'client_address': client_address,
                'device_folder': device_folder,
                'device_key': device_key,
                'timestamp': timestamp
            }
            
            self.file_queue.put(file_info, timeout=1)
            return unique_filename
            
        except queue.Full:
            logging.error("File queue is full, dropping file")
            return None
    
    def _process_files(self):
        """Process files from the queue"""
        while self.running:
            try:
                file_info = self.file_queue.get(timeout=1)
                if file_info is None:
                    break
                
                self._save_file(file_info)
                self.file_queue.task_done()
                
            except queue.Empty:
                continue
            except Exception as e:
                logging.error(f"Error processing file: {e}")
    
    def _save_file(self, file_info):
        """Save file to device-specific folder"""
        file_path = file_info['device_folder'] / file_info['filename']
        
        try:
            with open(file_path, 'wb') as f:
                f.write(file_info['data'])
            
            file_size = len(file_info['data'])
            
            # Update device statistics
            self.device_manager.increment_file_count(file_info['device_key'])
            
            logging.info(f"SUCCESS - Saved: {file_info['filename']} ({file_size} bytes) to device {file_info['device_key']} from {file_info['client_address']}")
            
            # Verify file integrity
            if self.verify_file(file_path, file_size):
                if file_path.suffix.lower() == '.txt':
                    self.process_metadata_txt(file_path, file_info)
                return True
            else:
                logging.error(f"File verification failed: {file_info['filename']}")
                if file_path.exists():
                    file_path.unlink()
                return False
                
        except Exception as e:
            logging.error(f"Error saving file {file_info['filename']}: {e}")
            if file_path.exists():
                file_path.unlink()
            return False

    def process_metadata_txt(self, file_path, file_info):
        """Parse ESP32 metadata txt and write a normalized JSON companion file."""
        try:
            parsed = {
                'source_file': file_info['original_filename'],
                'saved_file': file_info['filename'],
                'device_id': file_info['device_key'],
                'received_at': datetime.datetime.now().isoformat(),
                'audio_file': None,
                'latitude': None,
                'longitude': None,
                'bluetooth_count': 0,
                'bluetooth_devices': []
            }

            pending_ble_name = None

            with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
                for raw_line in f:
                    line = raw_line.strip()
                    if not line or '=' not in line:
                        continue

                    key, value = line.split('=', 1)
                    key = key.strip()
                    value = value.strip()

                    if key == 'audio_file':
                        parsed['audio_file'] = value
                    elif key == 'latitude':
                        parsed['latitude'] = None if value == 'NA' else float(value)
                    elif key == 'longitude':
                        parsed['longitude'] = None if value == 'NA' else float(value)
                    elif key == 'bluetooth_count':
                        try:
                            parsed['bluetooth_count'] = int(value)
                        except ValueError:
                            parsed['bluetooth_count'] = 0
                    elif key == 'ble_name':
                        pending_ble_name = value
                    elif key == 'ble_mac':
                        parsed['bluetooth_devices'].append({
                            'name': pending_ble_name or 'UNKNOWN',
                            'mac': value
                        })
                        pending_ble_name = None

            json_path = file_path.with_suffix('.json')
            with open(json_path, 'w', encoding='utf-8') as jf:
                json.dump(parsed, jf, indent=2)

            logging.info(
                "METADATA - Parsed %s: lat=%s lon=%s ble=%d",
                file_info['filename'],
                parsed['latitude'],
                parsed['longitude'],
                len(parsed['bluetooth_devices'])
            )
        except Exception as e:
            logging.error(f"Failed to parse metadata txt {file_info['filename']}: {e}")
    
    def verify_file(self, file_path, expected_size):
        """Verify file was written correctly"""
        try:
            actual_size = file_path.stat().st_size
            return actual_size == expected_size
        except Exception as e:
            logging.error(f"File verification error: {e}")
            return False
    
    def stop(self):
        """Stop file processing"""
        self.running = False
        self.executor.shutdown(wait=True)

class OpusFileServer:
    def __init__(self, host='0.0.0.0', port=8080, base_directory='received_files', 
                 max_connections=10000, max_workers=50):
        self.host = host
        self.port = port
        self.device_manager = DeviceManager(base_directory)
        self.connection_manager = ConnectionManager(max_connections)
        self.file_processor = FileProcessor(self.device_manager, max_workers)
        self.server_socket = None
        self.running = False
        self.stats = {
            'files_received': 0,
            'connections_handled': 0,
            'errors': 0,
            'start_time': None
        }
    
    def parse_header(self, data):
        """Parse the header data to extract filename, size, and device ID"""
        try:
            header_str = data.decode('utf-8', errors='ignore')
            lines = header_str.split('\n')
            header_line = lines[0]
            
            parts = header_line.strip().split()
            filename = None
            file_size = None
            device_id = None
            
            for part in parts:
                if part.startswith('FILE:'):
                    filename = part[5:]
                elif part.startswith('SIZE:'):
                    file_size = int(part[5:])
                elif part.startswith('DEVICE:'):
                    device_id = part[7:]
            
            return filename, file_size, device_id, len(header_line) + 1  # +1 for newline
            
        except Exception as e:
            logging.error(f"Error parsing header: {e}")
            return None, None, None, 0
    
    def receive_file_data(self, client_socket, file_size, header_size):
        """Receive file data with timeout and chunking"""
        try:
            client_socket.settimeout(30.0)  # 30 second timeout for data transfer
            
            bytes_received = 0
            file_data = bytearray()
            
            while bytes_received < file_size:
                chunk_size = min(8192, file_size - bytes_received)
                chunk = client_socket.recv(chunk_size)
                if not chunk:
                    break
                file_data.extend(chunk)
                bytes_received += len(chunk)
            
            return bytes(file_data)
            
        except socket.timeout:
            logging.error("Timeout while receiving file data")
            return None
        except Exception as e:
            logging.error(f"Error receiving file data: {e}")
            return None
    
    def handle_client(self, client_socket, client_address):
        """Handle a single client connection"""
        client_ip = client_address[0]
        
        # Check if we can accept this connection
        if not self.connection_manager.can_accept_connection(client_ip):
            client_socket.close()
            return
        
        try:
            logging.info(f"New connection from: {client_address}")
            self.stats['connections_handled'] += 1
            
            # Set initial timeout for header
            client_socket.settimeout(10.0)
            
            # Receive header
            header_data = b''
            while b'\n' not in header_data:
                chunk = client_socket.recv(1024)
                if not chunk:
                    break
                header_data += chunk
            
            if not header_data:
                logging.warning("No data received from client")
                return
            
            # Parse header
            filename, file_size, device_id, header_size = self.parse_header(header_data)
            
            if not filename or file_size is None:
                logging.error("Invalid header format")
                return
            
            # Validate file size (max 10MB)
            if file_size > 10 * 1024 * 1024:
                logging.error(f"File too large: {file_size} bytes from {client_address}")
                return
            
            logging.info(f"Expecting file: {filename} ({file_size} bytes) from device {device_id or client_ip}")
            
            # Extract file data from header buffer and receive remaining data
            file_data_from_header = header_data[header_size:]
            remaining_size = file_size - len(file_data_from_header)
            
            if remaining_size > 0:
                remaining_data = self.receive_file_data(client_socket, remaining_size, header_size)
                if remaining_data is None:
                    logging.error("Failed to receive file data")
                    return
                file_data = file_data_from_header + remaining_data
            else:
                file_data = file_data_from_header
            
            if len(file_data) != file_size:
                logging.error(f"Incomplete file: {filename} (expected {file_size}, got {len(file_data)})")
                return
            
            # Queue file for processing with device ID
            unique_filename = self.file_processor.queue_file(filename, file_data, client_address, device_id)
            
            if unique_filename:
                # Send acknowledgment
                ack_msg = f"ACK:File {filename} received successfully for device {device_id or client_ip}\n"
                client_socket.send(ack_msg.encode())
                self.stats['files_received'] += 1
                logging.info(f"SUCCESS - Queued: {filename} from device {device_id or client_ip}")
            else:
                error_msg = f"ERROR:Server busy, please retry\n"
                client_socket.send(error_msg.encode())
                
        except socket.timeout:
            logging.error(f"Timeout handling client {client_address}")
        except Exception as e:
            logging.error(f"Error handling client {client_address}: {e}")
            self.stats['errors'] += 1
        finally:
            client_socket.close()
            self.connection_manager.connection_closed(client_ip)
    
    def print_device_stats(self):
        """Print current device statistics"""
        stats = self.device_manager.get_device_stats()
        logging.info(f"DEVICE STATISTICS - Total devices: {stats['total_devices']}")
        
        for device_key, device_info in stats['devices'].items():
            logging.info(f"  {device_key}: {device_info['files_received']} files, last seen: {device_info['last_seen'][:19]}")
    
    def start_server(self):
        """Start the high-performance file server"""
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        
        # Increase buffer sizes
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 65536)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 65536)
        
        # Use thread pool for connection handling
        connection_pool = ThreadPoolExecutor(max_workers=200)
        
        try:
            self.server_socket.bind((self.host, self.port))
            self.server_socket.listen(1000)  # Large backlog
            self.running = True
            self.stats['start_time'] = datetime.datetime.now()
            
            logging.info(f"HIGH-PERFORMANCE OPUS FILE SERVER STARTED")
            logging.info(f"Host: {self.host}:{self.port}")
            logging.info(f"Base directory: {self.device_manager.base_directory.absolute()}")
            logging.info(f"Max connections: {self.connection_manager.max_connections}")
            logging.info(f"Max workers: {50}")  # File processor workers
            logging.info("Waiting for connections...")
            # Statistics thread
            def print_stats():
                while self.running:
                    time.sleep(300)  # Print stats every 5 minutes
                    uptime = datetime.datetime.now() - self.stats['start_time']
                    logging.info(
                        f"STATISTICS - Uptime: {uptime}, "
                        f"Files: {self.stats['files_received']}, "
                        f"Connections: {self.stats['connections_handled']}, "
                        f"Errors: {self.stats['errors']}, "
                        f"Queue: {self.file_processor.file_queue.qsize()}"
                    )
                    self.print_device_stats()
            
            threading.Thread(target=print_stats, daemon=True).start()
            
            while self.running:
                try:
                    client_socket, client_address = self.server_socket.accept()
                    # Submit connection handling to thread pool
                    connection_pool.submit(self.handle_client, client_socket, client_address)
                    
                except KeyboardInterrupt:
                    break
                except Exception as e:
                    logging.error(f"Accept error: {e}")
                    continue
        except Exception as e:
            logging.error(f"Server error: {e}")
        finally:
            self.running = False
            connection_pool.shutdown(wait=True)
            self.file_processor.stop()
            if self.server_socket:
                self.server_socket.close()
            logging.info("Server closed")

def main():
    # Configuration for high scalability
    HOST = '0.0.0.0'
    PORT = 40500
    BASE_DIR = 'devices'
    MAX_CONNECTIONS = 10000
    MAX_WORKERS = 100
    
    # Create and start server
    server = OpusFileServer(
        host=HOST, 
        port=PORT, 
        base_directory=BASE_DIR,
        max_connections=MAX_CONNECTIONS,
        max_workers=MAX_WORKERS
    )
    server.start_server()

if __name__ == "__main__":
    main()