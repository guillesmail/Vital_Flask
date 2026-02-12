import sqlite3
import shutil
import os
import json
import time
from datetime import datetime

# === CONFIGURACIÓN ===
try:
    with open("config.json", "r", encoding="utf-8") as f:
        config = json.load(f)
    DB_PATH = config["db_path"] # Lee la ruta real (DDBB/vital...)
except:
    print("❌ Error: No se encuentra config.json")
    exit()

BACKUP_FOLDER = "backups"
RETENCION = 10 # Cuántos backups guardamos antes de borrar el más viejo

def realizar_backup():
    # 1. Crear carpeta si no existe
    if not os.path.exists(BACKUP_FOLDER):
        os.makedirs(BACKUP_FOLDER)

    # 2. Generar nombre con fecha (TIMESTAMP)
    # Ejemplo: vital_backup_2026-02-11_23-55.db
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    backup_name = f"vital_backup_{timestamp}.db"
    backup_path = os.path.join(BACKUP_FOLDER, backup_name)

    print(f"⏳ Iniciando backup de: {DB_PATH}...")

    try:
        # 3. CONEXIÓN Y COPIA SEGURA (API de Backup de SQLite)
        # Esto es mejor que un simple copy-paste porque espera a que terminen las escrituras
        src = sqlite3.connect(DB_PATH)
        dst = sqlite3.connect(backup_path)

        with dst:
            src.backup(dst)
        
        dst.close()
        src.close()
        
        print(f"✅ Backup creado exitosamente: {backup_path}")
        
        # 4. ROTACIÓN (Limpieza de viejos)
        limpiar_viejos()

    except Exception as e:
        print(f"❌ Error al crear backup: {e}")

def limpiar_viejos():
    # Listar todos los archivos en la carpeta backups
    archivos = [os.path.join(BACKUP_FOLDER, f) for f in os.listdir(BACKUP_FOLDER) if f.endswith(".db")]
    
    # Ordenarlos por fecha de creación (el más viejo primero)
    archivos.sort(key=os.path.getctime)

    # Si hay más archivos que la retención, borramos los sobrantes
    while len(archivos) > RETENCION:
        archivo_a_borrar = archivos.pop(0) # El primero de la lista (el más viejo)
        os.remove(archivo_a_borrar)
        print(f"🧹 Rotación: Se eliminó backup antiguo: {archivo_a_borrar}")

if __name__ == "__main__":
    realizar_backup()
    