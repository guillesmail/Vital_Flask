import shutil
from datetime import datetime
import os

DB_PATH = "DDBB/vital_ddbb_clientes.db"
BACKUP_DIR = "backups"

# Crear carpeta backups si no existe
os.makedirs(BACKUP_DIR, exist_ok=True)

# Timestamp para el nombre del archivo
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

backup_path = f"{BACKUP_DIR}/vital_ddbb_clientes_{timestamp}.db"

# Copiar la base
shutil.copy2(DB_PATH, backup_path)

print(f"✅ Backup creado correctamente: {backup_path}")
