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
    print("\n📊 === PROCESADOR DE FUENTES (SMART MERGE) ===")
    print("Seleccioná qué origen querés actualizar:")
    print("  1. [n]  Nexion")
    print("  2. [me] Mercately")
    print("  3. [t]  Tienda Nube")
    print("  4. [mo] Monday Piscinas")
    print("  0. [x]  Salir")
    
    seleccion = input("\n👉 Tu elección: ").strip().lower()
    return seleccion

def procesar_origen(origen_key):
    if origen_key not in config["archivos"]:
        print(f"❌ Error: La clave '{origen_key}' no está en config.json.")
        return

    cfg = config["archivos"][origen_key]
    tabla_raw = f"raw_{origen_key}"
    tabla_source = f"source_{origen_key}"
    col_id_map = cfg["map"]["id_origen"]

    print(f"\n🚀 INICIANDO MERGE INTELIGENTE PARA: {origen_key.upper()}")
    print("-" * 50)

    try:
        # 1. LEER RAW (NUEVO)
        try:
            df_raw = pd.read_sql(f"SELECT * FROM {tabla_raw}", conn)
        except:
            print(f"⚠️  No existe la tabla {tabla_raw}. Ejecutá import_raw primero.")
            return

        if df_raw.empty:
            print("⚠️  RAW vacío. Nada que procesar.")
            return

        # Detectar columna ID
        col_id_real = None
        for col in df_raw.columns:
            if col.strip().lower() == col_id_map.strip().lower():
                col_id_real = col
                break
        
        if not col_id_real:
            if col_id_map in df_raw.columns: col_id_real = col_id_map
            else:
                print(f"❌ ERROR: No encuentro ID '{col_id_map}' en raw.")
                return

        # 2. LIMPIEZA DE RAW
        # Convertimos vacíos "" a NaN (Nulos reales) para que el merge funcione
        df_raw = df_raw.replace(r'^\s*$', np.nan, regex=True)
        
        # Eliminamos filas donde el ID principal sea Nulo (no sirven)
        df_clean = df_raw.dropna(subset=[col_id_real])
        
        # Eliminamos duplicados DENTRO del archivo nuevo (nos quedamos con el último)
        df_clean = df_clean.drop_duplicates(subset=[col_id_real], keep='last')
        
        registros_nuevos_validos = len(df_clean)

        # 3. LEER HISTORIAL (VIEJO)
        try:
            df_source = pd.read_sql(f"SELECT * FROM {tabla_source}", conn)
            # También aseguramos que el source tenga NaNs en los vacíos
            df_source = df_source.replace(r'^\s*$', np.nan, regex=True)
            total_historial_antes = len(df_source)
        except:
            df_source = pd.DataFrame()
            total_historial_antes = 0
            print("✨ Creando historial por primera vez.")

        # 4. FUSIÓN INTELIGENTE (SMART MERGE)
        if not df_source.empty:
            # Ponemos el ID como índice para poder comparar
            df_clean.set_index(col_id_real, inplace=True)
            df_source.set_index(col_id_real, inplace=True)
            
            # === LA MAGIA ===
            # combine_first: Prioriza df_clean. Pero si df_clean tiene NaN, usa df_source.
            # Esto evita borrar datos viejos si el nuevo viene vacío.
            df_final = df_clean.combine_first(df_source)
            
            # Reseteamos el índice para volver a tener el ID como columna normal
            df_final.reset_index(inplace=True)
        else:
            df_final = df_clean

        # 5. GUARDAR
        # Reemplazamos los NaN por None (NULL de SQL) o vacíos "" según prefieras
        # Para SQL suele ser mejor dejar NULL, pero para visualización a veces "" es mejor.
        # Vamos a dejarlo limpio.
        df_final.to_sql(tabla_source, conn, if_exists="replace", index=False)
        
        # 6. REPORTE
        total_ahora = len(df_final)
        nuevos_reales = total_ahora - total_historial_antes
        
        print(f"\n📝 REPORTE FINAL DE {origen_key.upper()}:")
        print(f"   📥 Registros en archivo nuevo: {registros_nuevos_validos}")
        print(f"   🏛️  Registros en historial previo: {total_historial_antes}")
        print("   ---------------------------")
        print(f"   🆕 Clientes 100% Nuevos:      {nuevos_reales}")
        print(f"   🔄 Clientes Actualizados/Mezclados: {registros_nuevos_validos - nuevos_reales}")
        print("   ---------------------------")
        print(f"   📚 TOTAL FINAL EN BASE:       {total_ahora}")
        print(f"   (Los datos vacíos del archivo nuevo NO borraron datos viejos)")

    except Exception as e:
        print(f"❌ Error inesperado: {e}")
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