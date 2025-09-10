import pandas as pd
import sqlite3
import os
import json

# === Cargar configuración ===
with open("config.json", "r", encoding="utf-8") as f:
    config = json.load(f)

db_path = config["db_path"]
import_folder = config["import_folder"]
archivos = config["archivos"]

# === Importar CSVs y crear tablas RAW ===
def importar():
    conn = sqlite3.connect(db_path)

    for origen, cfg in archivos.items():
        path_archivo = os.path.join(import_folder, cfg["archivo"])
        if not os.path.isfile(path_archivo):
            print(f"⚠ Archivo no encontrado: {path_archivo}")
            continue

        try:
            df = pd.read_csv(path_archivo, sep=cfg["sep"], encoding=cfg["encoding"])
            df.to_sql(f"raw_{origen}", conn, if_exists="replace", index=False)
            print(f"✓ Importado {origen} → tabla raw_{origen} ({len(df)} registros)")
        except Exception as e:
            print(f"❌ Error al importar {origen}: {e}")

    conn.close()

if __name__ == "__main__":
    importar()
