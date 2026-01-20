import sqlite3
import pandas as pd
import json

# Configuración
with open("config.json", "r", encoding="utf-8") as f:
    config = json.load(f)

db_path = config["db_path"]
conn = sqlite3.connect(db_path)

print("=== 1. LOS NOMBRES DE LAS COLUMNAS EN NEXION ===")
# Leemos 1 fila solo para ver encabezados
try:
    df_cols = pd.read_sql("SELECT * FROM raw_nexion LIMIT 1", conn)
    columnas = list(df_cols.columns)
    for col in columnas:
        if "tel" in col.lower() or "cel" in col.lower() or "movil" in col.lower() or "mail" in col.lower():
            print(f"👉 CANDIDATO: '{col}'")  # Resaltamos posibles columnas de contacto
        else:
            print(f"   '{col}'")
except Exception as e:
    print(f"Error leyendo nexion: {e}")

print("\n=== 2. MUESTRA DE LOS DESCARTADOS (Sin Mail ni Teléfono) ===")
# Usamos los nombres que configuraste en config.json para buscar los vacíos
mapa = config["archivos"]["nexion"]["map"]
col_nombre = mapa["nombre"]
col_mail = mapa["correo"]
col_tel = mapa["telefono"]

# Consulta SQL: Traeme gente donde el mail sea corto/nulo Y el telefono sea corto/nulo
query = f"""
SELECT "{col_nombre}", "{col_mail}", "{col_tel}"
FROM raw_nexion
WHERE ("{col_mail}" IS NULL OR LENGTH("{col_mail}") < 5)
  AND ("{col_tel}" IS NULL OR LENGTH("{col_tel}") < 5)
LIMIT 20
"""

try:
    df_basura = pd.read_sql(query, conn)
    if df_basura.empty:
        print("¡Sorpresa! No encontré descartados con ese criterio. Algo raro pasa.")
    else:
        print(df_basura)
except Exception as e:
    print(f"Error consultando descartados: {e}")

conn.close()
