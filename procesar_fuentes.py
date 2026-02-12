import sqlite3
import pandas as pd
import numpy as np
import json
import os
import sys

# === CARGA DE CONFIGURACIÓN ===
try:
    with open("config.json", "r", encoding="utf-8") as f:
        config = json.load(f)
except Exception as e:
    print(f"❌ Error crítico: No se pudo leer config.json ({e})")
    sys.exit()

db_path = config["db_path"]
conn = sqlite3.connect(db_path)

OPCIONES = {
    "1": "nexion",          "n": "nexion",
    "2": "mercately",       "me": "mercately",
    "3": "tienda",          "t": "tienda",
    "4": "monday_piscinas", "mo": "monday_piscinas"
}

def mostrar_menu():
    print("\n📊 === PROCESADOR DE FUENTES (SMART MERGE + CASCADA) ===")
    print("Seleccioná qué origen querés actualizar:")
    print("  1. [n]  Nexion")
    print("  2. [me] Mercately")
    print("  3. [t]  Tienda Nube")
    print("  4. [mo] Monday Piscinas")
    print("  0. [x]  Salir")
    
    seleccion = input("\n👉 Tu elección: ").strip().lower()
    return seleccion

def buscar_columna(df, nombre_config):
    """Busca una columna en el DF ignorando mayúsculas/minúsculas"""
    if not nombre_config: return None
    for col in df.columns:
        if col.strip().lower() == nombre_config.strip().lower():
            return col
    return None

def procesar_origen(origen_key):
    if origen_key not in config["archivos"]:
        print(f"❌ Error: La clave '{origen_key}' no está en config.json.")
        return

    cfg = config["archivos"][origen_key]
    tabla_raw = f"raw_{origen_key}"
    tabla_source = f"source_{origen_key}"
    
    # Mapeo de columnas clave
    mapa = cfg["map"]
    col_id_primaria = mapa.get("id_origen") # Plan A (CUIT)
    col_correo = mapa.get("correo")         # Plan B
    col_tel = mapa.get("telefono")          # Plan C

    print(f"\n🚀 PROCESANDO: {origen_key.upper()}")
    print("-" * 50)

    try:
        # 1. LEER RAW
        try:
            df_raw = pd.read_sql(f"SELECT * FROM {tabla_raw}", conn)
        except:
            print(f"⚠️  No existe la tabla {tabla_raw}. Ejecutá import_raw primero.")
            return

        if df_raw.empty:
            print("⚠️  RAW vacío. Nada que procesar.")
            return

        # 2. LIMPIEZA INICIAL
        # Convertimos vacíos "" a NaN
        df_raw = df_raw.replace(r'^\s*$', np.nan, regex=True)
        total_inicial = len(df_raw)

        # 3. LÓGICA DE CASCADA (CUIT -> Mail -> Telefono)
        # Buscamos los nombres reales de las columnas en el Excel
        col_id_real = buscar_columna(df_raw, col_id_primaria)
        col_mail_real = buscar_columna(df_raw, col_correo)
        col_tel_real = buscar_columna(df_raw, col_tel)

        if not col_id_real:
            print(f"❌ Error Crítico: No encuentro la columna principal '{col_id_primaria}'")
            return

        print("   🔍 Aplicando Cascada de Identidad:")
        print(f"      1. Prioridad: {col_id_real}")
        print(f"      2. Fallback:  {col_mail_real if col_mail_real else '(No configurado)'}")
        print(f"      3. Fallback:  {col_tel_real if col_tel_real else '(No configurado)'}")

        # Creemos una columna temporal 'ID_FINAL'
        df_raw['ID_FINAL'] = df_raw[col_id_real]

        # Si el ID_FINAL es nulo, intentamos rellenar con Mail
        if col_mail_real:
            df_raw['ID_FINAL'] = df_raw['ID_FINAL'].fillna(df_raw[col_mail_real])
        
        # Si sigue nulo, intentamos rellenar con Teléfono
        if col_tel_real:
            df_raw['ID_FINAL'] = df_raw['ID_FINAL'].fillna(df_raw[col_tel_real])

        # 4. FILTRADO FINAL
        # Los que quedaron sin ID_FINAL después de los 3 intentos, se descartan
        df_clean = df_raw.dropna(subset=['ID_FINAL'])
        
        # Copiamos el ID_FINAL a la columna original de ID para mantener consistencia
        df_clean[col_id_real] = df_clean['ID_FINAL']
        # Borramos la columna temporal
        df_clean = df_clean.drop(columns=['ID_FINAL'])

        # Eliminamos duplicados
        df_clean = df_clean.drop_duplicates(subset=[col_id_real], keep='last')

        descartados = total_inicial - len(df_clean)

        # 5. MERGE CON HISTORIAL
        try:
            df_source = pd.read_sql(f"SELECT * FROM {tabla_source}", conn)
            df_source = df_source.replace(r'^\s*$', np.nan, regex=True)
        except:
            df_source = pd.DataFrame()

        if not df_source.empty:
            df_clean.set_index(col_id_real, inplace=True)
            df_source.set_index(col_id_real, inplace=True)
            df_final = df_clean.combine_first(df_source)
            df_final.reset_index(inplace=True)
        else:
            df_final = df_clean

        # 6. GUARDAR
        df_final.to_sql(tabla_source, conn, if_exists="replace", index=False)
        
        print(f"\n📝 REPORTE DE {origen_key.upper()}:")
        print(f"   📥 Total Recibidos:       {total_inicial}")
        print(f"   🗑️  DESCARTADOS TOTALES:   {descartados}")
        print(f"       (No tenían ni {col_id_primaria}, ni mail, ni teléfono)")
        print(f"   ✅ Guardados en Base:     {len(df_final)}")

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

def main():
    while True:
        opcion = mostrar_menu()
        if opcion in ["0", "x"]: break
        if opcion in OPCIONES:
            procesar_origen(OPCIONES[opcion])
            if input("\n¿Otra? (s/n): ").lower() != "s": break
        else: print("⚠️ Opción inválida.")
    conn.close()

if __name__ == "__main__":
    main()