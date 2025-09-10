import pandas as pd
import sqlite3
import json
from datetime import datetime

# === Cargar configuración ===
with open("config.json", "r", encoding="utf-8") as f:
    config = json.load(f)

db_path = config["db_path"]
archivos = config["archivos"]

# === Conectar a la base ===
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# === Crear tabla de clientes si no existe ===
cursor.execute("""
CREATE TABLE IF NOT EXISTS clientes_vital (
    id_cliente   INTEGER PRIMARY KEY AUTOINCREMENT,
    nombre       TEXT,
    apellido     TEXT,
    correo       TEXT UNIQUE,
    telefono     TEXT,
    localidad    TEXT,
    origen       TEXT,
    id_origen    TEXT,
    fecha_alta   TEXT
)
""")
conn.commit()

# === Función para insertar/actualizar ===
def insertar_o_actualizar(df, origen, mapeo):
    for _, row in df.iterrows():
        correo = str(row.get(mapeo.get("correo", ""), "")).strip().lower()
        if not correo or correo == "nan":
            continue

        cursor.execute("SELECT id_cliente, fecha_alta FROM clientes_vital WHERE correo = ?", (correo,))
        resultado = cursor.fetchone()

        fecha_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        fecha_alta = resultado[1] if resultado else fecha_now

        datos = (
            str(row.get(mapeo.get("nombre", ""), "")).strip(),
            str(row.get(mapeo.get("apellido", ""), "")).strip(),
            correo,
            str(row.get(mapeo.get("telefono", ""), "")).strip(),
            str(row.get(mapeo.get("localidad", ""), "")).strip(),
            origen,
            str(row.get(mapeo.get("id_origen", ""), "")).strip() if mapeo.get("id_origen") != "index" else str(row.name),
            fecha_alta
        )

        if resultado:
            cursor.execute("""
                UPDATE clientes_vital
                SET nombre=?, apellido=?, telefono=?, localidad=?, origen=?, id_origen=?, fecha_alta=?
                WHERE correo=?
            """, datos[0:6] + (datos[6], datos[2]))
        else:
            cursor.execute("""
                INSERT INTO clientes_vital
                (nombre, apellido, correo, telefono, localidad, origen, id_origen, fecha_alta)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, datos)

# === Consolidación dinámica ===
for origen, cfg in archivos.items():
    try:
        df = pd.read_sql(f"SELECT * FROM raw_{origen}", conn)
        mapeo = cfg.get("map", {})
        insertar_o_actualizar(df, origen, mapeo)
        print(f"✓ Consolidado: {origen}")
    except Exception as e:
        print(f"❌ Error consolidando {origen}: {e}")

conn.commit()
conn.close()
