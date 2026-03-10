#!/usr/bin/env python3
"""
Serveur simple pour recevoir les fichiers audio RAW de l'ESP32
Format attendu: EN-TÊTE puis données audio RAW
"""

import socket
import os
import datetime
import threading
import logging
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict
import json

# Configuration du logging
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
MAX_CONNECTIONS = 1000
BUFFER_SIZE = 8192

# ==================== GESTIONNAIRE DE FICHIERS ====================

class FileHandler:
    """Gère la réception et le stockage des fichiers RAW"""
    
    def __init__(self, base_dir=BASE_DIR):
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(exist_ok=True)
        
        # Statistiques
        self.stats = {
            'total_files': 0,
            'total_bytes': 0,
            'devices': defaultdict(int),
            'start_time': datetime.datetime.now().isoformat()
        }
        self.lock = threading.Lock()
        
        logger.info(f"Dossier de stockage: {self.base_dir.absolute()}")
    
    def parse_header(self, data):
        """Parse l'en-tête pour extraire les informations"""
        try:
            # Trouver la fin de l'en-tête
            header_end = data.find(b'---END HEADER---\n')
            if header_end == -1:
                return None, None, None, 0
            
            header_end += len(b'---END HEADER---\n')
            header_data = data[:header_end]
            
            # Parser l'en-tête
            header_text = header_data.decode('utf-8', errors='ignore')
            header_info = {}
            
            for line in header_text.split('\n'):
                if ':' in line and not line.startswith('---'):
                    key, value = line.split(':', 1)
                    header_info[key.strip()] = value.strip()
            
            # Extraire les informations importantes
            device_id = header_info.get('DEVICE', 'inconnu')
            filename = None
            timestamp = header_info.get('TIMESTAMP', '')
            
            # Le nom de fichier n'est pas dans l'en-tête, on le génère
            return device_id, header_info, header_end
            
        except Exception as e:
            logger.error(f"Erreur parsing en-tête: {e}")
            return None, None, 0
    
    def generate_filename(self, device_id, timestamp=None):
        """Génère un nom de fichier au format ISO8601"""
        if not timestamp or timestamp == '1970-01-01T00:00:00Z':
            # Utiliser l'heure actuelle si pas de timestamp valide
            now = datetime.datetime.now()
            timestamp = now.strftime("%Y%m%dT%H%M%SZ")
        else:
            # Nettoyer le timestamp ISO8601
            timestamp = timestamp.replace('-', '').replace(':', '').replace('Z', '')
        
        return f"{device_id}_{timestamp}.raw"
    
    def save_file(self, data, client_ip):
        """Sauvegarde un fichier reçu"""
        try:
            # Parser l'en-tête
            device_id, header_info, header_end = self.parse_header(data)
            
            if not device_id:
                logger.warning(f"En-tête invalide de {client_ip}")
                return None
            
            # Extraire les données audio
            audio_data = data[header_end:]
            
            # Générer le nom de fichier
            timestamp = header_info.get('TIMESTAMP', '') if header_info else ''
            filename = self.generate_filename(device_id, timestamp)
            
            # Créer le dossier pour ce device
            device_dir = self.base_dir / device_id
            device_dir.mkdir(exist_ok=True)
            
            # Chemin complet du fichier
            filepath = device_dir / filename
            
            # Sauvegarder le fichier
            with open(filepath, 'wb') as f:
                f.write(data)  # On sauvegarde tout (en-tête + audio)
            
            file_size = len(data)
            audio_size = len(audio_data)
            
            # Mettre à jour les statistiques
            with self.lock:
                self.stats['total_files'] += 1
                self.stats['total_bytes'] += file_size
                self.stats['devices'][device_id] += 1
            
            logger.info(f"✅ {device_id} - {filename} ({audio_size} bytes audio, {file_size} total)")
            
            return {
                'device_id': device_id,
                'filename': filename,
                'filepath': str(filepath),
                'size': file_size,
                'audio_size': audio_size,
                'timestamp': timestamp
            }
            
        except Exception as e:
            logger.error(f"Erreur sauvegarde fichier de {client_ip}: {e}")
            return None

# ==================== GESTIONNAIRE DE CONNEXIONS ====================

class ConnectionManager:
    """Gère les connexions entrantes"""
    
    def __init__(self, max_connections=MAX_CONNECTIONS):
        self.max_connections = max_connections
        self.active_connections = 0
        self.connections_per_ip = defaultdict(int)
        self.lock = threading.Lock()
    
    def can_accept(self, client_ip):
        """Vérifie si on peut accepter une nouvelle connexion"""
        with self.lock:
            if self.active_connections >= self.max_connections:
                logger.warning("Nombre max de connexions atteint")
                return False
            
            self.active_connections += 1
            self.connections_per_ip[client_ip] += 1
            return True
    
    def connection_closed(self, client_ip):
        """Notifie la fermeture d'une connexion"""
        with self.lock:
            self.active_connections -= 1
            self.connections_per_ip[client_ip] -= 1
            if self.connections_per_ip[client_ip] <= 0:
                del self.connections_per_ip[client_ip]

# ==================== GESTIONNAIRE CLIENT ====================

def handle_client(client_socket, client_address, file_handler, conn_manager):
    """Gère une connexion client"""
    client_ip = client_address[0]
    
    if not conn_manager.can_accept(client_ip):
        client_socket.close()
        return
    
    try:
        # Timeout de 30 secondes
        client_socket.settimeout(30)
        
        # Recevoir toutes les données
        data = b''
        while True:
            try:
                chunk = client_socket.recv(BUFFER_SIZE)
                if not chunk:
                    break
                data += chunk
                
                # Limite de taille (10MB max)
                if len(data) > 10 * 1024 * 1024:
                    logger.warning(f"Fichier trop grand de {client_ip}")
                    break
                    
            except socket.timeout:
                logger.debug(f"Timeout de {client_ip}, fin de réception")
                break
        
        if len(data) < 100:
            logger.warning(f"Données trop petites de {client_ip}")
            return
        
        # Sauvegarder le fichier
        result = file_handler.save_file(data, client_ip)
        
        if result:
            # Réponse de succès
            response = f"ACK:{result['filename']}\n"
            client_socket.send(response.encode())
            logger.info(f"✓ Réponse envoyée à {client_ip}")
        else:
            client_socket.send(b"ERROR:Invalid file\n")
            
    except Exception as e:
        logger.error(f"Erreur avec {client_ip}: {e}")
    finally:
        client_socket.close()
        conn_manager.connection_closed(client_ip)

# ==================== SERVEUR PRINCIPAL ====================

def print_stats(file_handler):
    """Affiche les statistiques périodiquement"""
    while True:
        time.sleep(60)  # Toutes les minutes
        stats = file_handler.stats
        uptime = datetime.datetime.now() - datetime.datetime.fromisoformat(stats['start_time'])
        
        logger.info("-" * 60)
        logger.info(f"STATISTIQUES - Uptime: {str(uptime).split('.')[0]}")
        logger.info(f"  Fichiers reçus: {stats['total_files']}")
        logger.info(f"  Données totales: {stats['total_bytes'] / 1024 / 1024:.1f} MB")
        logger.info(f"  Appareils actifs: {len(stats['devices'])}")
        
        # Top 5 appareils
        if stats['devices']:
            logger.info("  Top appareils:")
            top_devices = sorted(stats['devices'].items(), key=lambda x: x[1], reverse=True)[:5]
            for device, count in top_devices:
                logger.info(f"    {device}: {count} fichiers")
        logger.info("-" * 60)

def start_server():
    """Démarre le serveur"""
    
    # Initialiser les gestionnaires
    file_handler = FileHandler()
    conn_manager = ConnectionManager()
    
    # Créer le socket
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 65536)
    
    try:
        server.bind((HOST, PORT))
        server.listen(100)
        
        logger.info("=" * 60)
        logger.info("SERVEUR AUDIO RAW DÉMARRÉ")
        logger.info("=" * 60)
        logger.info(f"Hôte: {HOST}:{PORT}")
        logger.info(f"Stockage: {BASE_DIR}")
        logger.info(f"Connexions max: {MAX_CONNECTIONS}")
        logger.info("=" * 60)
        
        # Thread pool pour les connexions
        executor = ThreadPoolExecutor(max_workers=50)
        
        # Thread pour les statistiques
        threading.Thread(target=print_stats, args=(file_handler,), daemon=True).start()
        
        # Boucle principale
        while True:
            client, address = server.accept()
            executor.submit(handle_client, client, address, file_handler, conn_manager)
            
    except KeyboardInterrupt:
        logger.info("Arrêt demandé...")
    except Exception as e:
        logger.error(f"Erreur serveur: {e}")
    finally:
        server.close()
        executor.shutdown(wait=False)
        logger.info("Serveur arrêté")

# ==================== POINT D'ENTRÉE ====================

if __name__ == "__main__":
    import time
    start_server()