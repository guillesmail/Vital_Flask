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
    print(f"📂 Conectado a base de datos en: {db_path}")

    for origen, cfg in archivos.items():
        path_archivo = os.path.join(import_folder, cfg["archivo"])
        
        # Verificar si el archivo existe
        if not os.path.isfile(path_archivo):
            # Solo mostramos advertencia si no es uno de los archivos clave que ya sabemos que falta
            # (O podés ignorar esta línea si preferís ver todos los avisos)
            print(f"⚠ Archivo no encontrado: {path_archivo}")
            continue

        try:
            print(f"⏳ Procesando {origen}...")
            # 1. Leer el CSV
            df = pd.read_csv(path_archivo, sep=cfg["sep"], encoding=cfg["encoding"], low_memory=False)

            # === BLOQUE ANTI-GEMELOS (Versión Robusta) ===
            # Soluciona problemas de "Casilla" vs "casilla" que SQLite odia.
            new_columns = []
            seen = {} # Registro de columnas vistas (en minúscula)
            
            for col in df.columns:
                col_str = str(col).strip()
                col_lower = col_str.lower() # Normalizamos a minúscula para comparar
                
                if col_lower in seen:
                    seen[col_lower] += 1
                    # Si ya existe, le agregamos un número: Casilla_1
                    new_columns.append(f"{col_str}_{seen[col_lower]}")
                else:
                    seen[col_lower] = 0
                    new_columns.append(col_str)
            
            df.columns = new_columns
            # ============================================

            # 2. Guardar en Base de Datos
            nombre_tabla = f"raw_{origen}"
            df.to_sql(nombre_tabla, conn, if_exists="replace", index=False)
            print(f"   ✅ ÉXITO: {origen} → tabla {nombre_tabla} ({len(df)} registros)")

        except Exception as e:
            print(f"   ❌ ERROR FATAL en {origen}: {e}")

    conn.close()

if __name__ == "__main__":
    importar()