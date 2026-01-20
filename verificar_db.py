import sqlite3
import json
import os

# Cargar ruta
with open("config.json", "r", encoding="utf-8") as f:
    config = json.load(f)

db_path = config["db_path"]

if not os.path.exists(db_path):
    print(f"🚨 ¡ERROR CRÍTICO! No encuentro el archivo en: {db_path}")
else:
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    print(f"📂 Conectado a: {db_path}\n")

    # 1. Contar Raw
    tablas = ["raw_mercately", "raw_tienda", "raw_nexion", "clientes_vital"]
    
    for t in tablas:
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {t}")
            cantidad = cursor.fetchone()[0]
            icono = "✅" if cantidad > 0 else "⚠️"
            print(f"{icono} Tabla {t.upper().ljust(15)} : {cantidad:,} filas".replace(",", "."))
        except Exception as e:
            print(f"❌ Tabla {t.upper().ljust(15)} : No existe o error ({e})")

    conn.close()