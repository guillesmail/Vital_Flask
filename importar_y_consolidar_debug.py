
import pandas as pd
import sqlite3
import os
from datetime import datetime

db_path = "DDBB/vital_ddbb_clientes.db"
import_folder = "importaciones"

archivos = {
    "mercately": {
        "archivo": "mercately.csv",
        "sep": ",",
        "encoding": "utf-8-sig"
    },
    "tienda": {
        "archivo": "tienda.csv",
        "sep": ";",
        "encoding": "ISO-8859-1"
    },
    "nexion": {
        "archivo": "nexion.csv",
        "sep": ";",
        "encoding": "ANSI"
    }
}

conn = sqlite3.connect(db_path)

# PASO 1: Importar archivos como tablas RAW
for origen, config in archivos.items():
    path_archivo = os.path.join(import_folder, config["archivo"])
    print(f"🔍 Buscando archivo: {path_archivo}")
    
    if not os.path.isfile(path_archivo):
        print(f"❌ Archivo no encontrado: {path_archivo}")
        continue

    try:
        df_raw = pd.read_csv(path_archivo, sep=config["sep"], encoding=config["encoding"])
        df_raw.to_sql(f"raw_{origen}", conn, if_exists="replace", index=False)
        print(f"✅ Tabla raw_{origen} creada con {len(df_raw)} registros.")
    except Exception as e:
        print(f"❌ Error al importar {origen}: {e}")

# PASO 2: Consolidar datos normalizados a clientes_vital
cursor = conn.cursor()

def insertar_o_actualizar(df, origen):
    for _, row in df.iterrows():
        correo = row.get("correo")
        if not correo or pd.isna(correo):
            continue

        cursor.execute("SELECT id_cliente FROM clientes_vital WHERE correo = ?", (correo,))
        resultado = cursor.fetchone()

        datos = (
            row.get("nombre", ""),
            row.get("apellido", ""),
            correo,
            row.get("telefono", ""),
            row.get("localidad", ""),
            origen,
            row.get("id_origen", ""),
            datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        )

        if resultado:
            cursor.execute("""
                UPDATE clientes_vital
                SET nombre = ?, apellido = ?, telefono = ?, localidad = ?, origen = ?, id_origen = ?, fecha_alta = ?
                WHERE correo = ?
            """, datos[0:6] + (datos[6], datos[2]))
        else:
            cursor.execute("""
                INSERT INTO clientes_vital
                (nombre, apellido, correo, telefono, localidad, origen, id_origen, fecha_alta)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, datos)

# MERCATELY
try:
    df = pd.read_sql("SELECT * FROM raw_mercately", conn)
    df = df.rename(columns={
        "FirstName": "nombre",
        "Email": "correo",
        "Phone": "telefono",
        "Localidd": "localidad"
    })
    df["apellido"] = ""
    df["id_origen"] = df.index.astype(str)
    insertar_o_actualizar(df, "mercately")
    print("✅ Consolidado: mercately")
except Exception as e:
    print(f"❌ Error consolidando mercately: {e}")

# TIENDA
try:
    df = pd.read_sql("SELECT * FROM raw_tienda", conn)
    df = df.rename(columns={
        "nombre": "nombre",
        "correo": "correo",
        "tel": "telefono",
        "localidad": "localidad"
    })
    df["apellido"] = ""
    df["id_origen"] = df.index.astype(str)
    insertar_o_actualizar(df, "tienda")
    print("✅ Consolidado: tienda")
except Exception as e:
    print(f"❌ Error consolidando tienda: {e}")

conn.commit()
conn.close()
