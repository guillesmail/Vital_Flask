import sqlite3
import pandas as pd
import json

# Leemos la config para saber la ruta de la DB
with open("config.json", "r", encoding="utf-8") as f:
    config = json.load(f)

conn = sqlite3.connect(config["db_path"])

try:
    # Pedimos solo la primera fila de TIENDA para ver los titulos
    df = pd.read_sql("SELECT * FROM raw_tienda LIMIT 1", conn)
    
    print("\n=== COLUMNAS REALES EN TIENDA ===")
    for col in df.columns:
        print(f"👉 '{col}'")
        
except Exception as e:
    print(f"Error: {e}")

conn.close()