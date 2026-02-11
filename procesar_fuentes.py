import sqlite3
import pandas as pd
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

# === MENÚ DE OPCIONES ===
# Mapeamos lo que escribe el usuario con la clave real del JSON
OPCIONES = {
    "1": "nexion",          "n": "nexion",
    "2": "mercately",       "me": "mercately",
    "3": "tienda",          "t": "tienda",
    "4": "monday_piscinas", "mo": "monday_piscinas"
}

def mostrar_menu():
    print("\n📊 === PROCESADOR DE FUENTES (CAPA PLATA) ===")
    print("Seleccioná qué origen querés actualizar hoy:")
    print("  1. [n]  Nexion")
    print("  2. [me] Mercately")
    print("  3. [t]  Tienda Nube")
    print("  4. [mo] Monday Piscinas")
    print("  0. [x]  Salir")
    
    seleccion = input("\n👉 Tu elección: ").strip().lower()
    return seleccion

def procesar_origen(origen_key):
    # Verificar que la clave exista en el JSON
    if origen_key not in config["archivos"]:
        print(f"❌ Error: La clave '{origen_key}' no está en config.json. Revisá el archivo.")
        return

    cfg = config["archivos"][origen_key]
    tabla_raw = f"raw_{origen_key}"
    tabla_source = f"source_{origen_key}"
    col_id_map = cfg["map"]["id_origen"]

    print(f"\n🚀 INICIANDO PROCESO PARA: {origen_key.upper()}")
    print("-" * 50)

    try:
        # 1. LEER RAW
        try:
            df_raw = pd.read_sql(f"SELECT * FROM {tabla_raw}", conn)
        except:
            print(f"⚠️  No existe la tabla {tabla_raw}. ¿Corriste import_raw.py primero?")
            return

        total_raw = len(df_raw)
        if total_raw == 0:
            print("⚠️  La tabla RAW está vacía. No hay nada que procesar.")
            return

        # 2. DETECTAR COLUMNA ID
        col_id_real = None
        # Buscamos coincidencia exacta o insensible a mayúsculas
        for col in df_raw.columns:
            if col.strip().lower() == col_id_map.strip().lower():
                col_id_real = col
                break
        
        if not col_id_real:
            # Fallback: confiamos en el config si no la encontramos
            if col_id_map in df_raw.columns:
                col_id_real = col_id_map
            else:
                print(f"❌ ERROR: No encuentro la columna ID '{col_id_map}' en {tabla_raw}.")
                print(f"   Columnas disponibles: {list(df_raw.columns)}")
                return

        # 3. FILTRO DE CALIDAD (Limpieza)
        # Eliminamos filas sin ID válido (sin mail en Tienda, sin tel en Mercately)
        df_clean = df_raw.dropna(subset=[col_id_real]) # Borra Nulos
        df_clean = df_clean[df_clean[col_id_real].astype(str).str.strip() != ""] # Borra vacíos ""
        
        registros_validos = len(df_clean)
        ignorados = total_raw - registros_validos

        # 4. LEER HISTORIAL (SOURCE)
        try:
            df_source = pd.read_sql(f"SELECT * FROM {tabla_source}", conn)
            total_historial_antes = len(df_source)
        except:
            df_source = pd.DataFrame()
            total_historial_antes = 0
            print("✨ Creando historial por primera vez.")

        # 5. FUSIÓN (UPSERT)
        # Concatenamos Historial + Nuevo Limpio
        if not df_source.empty:
            df_total = pd.concat([df_source, df_clean])
        else:
            df_total = df_clean

        # Eliminamos duplicados quedándonos con el ÚLTIMO (el que vino hoy)
        df_final = df_total.drop_duplicates(subset=[col_id_real], keep='last')
        
        # 6. GUARDAR
        df_final.to_sql(tabla_source, conn, if_exists="replace", index=False)
        
        # 7. REPORTE FINAL
        total_historial_ahora = len(df_final)
        nuevos_reales = total_historial_ahora - total_historial_antes
        # Si procesamos 100 válidos y solo 10 son nuevos reales, 90 eran actualizaciones de existentes
        actualizados = registros_validos - nuevos_reales 
        if actualizados < 0: actualizados = 0 # Por si es la primera carga

        print(f"\n📝 REPORTE FINAL DE {origen_key.upper()}:")
        print(f"   📥 Recibidos del RAW:       {total_raw}")
        print(f"   🗑️  Ignorados (Sin ID):      {ignorados}")
        print(f"   ✅ Procesados Válidos:      {registros_validos}")
        print("   ---------------------------")
        print(f"   🆕 NUEVOS AGREGADOS:        {nuevos_reales}")
        print(f"   🔄 EXISTENTES ACTUALIZADOS: {actualizados}")
        print("   ---------------------------")
        print(f"   📚 TOTAL EN BASE HISTÓRICA: {total_historial_ahora}")

    except Exception as e:
        print(f"❌ Ocurrió un error inesperado: {e}")

# === BUCLE PRINCIPAL ===
def main():
    while True:
        opcion = mostrar_menu()
        
        if opcion == "0" or opcion == "x":
            print("👋 Saliendo...")
            break
        
        if opcion in OPCIONES:
            key_seleccionada = OPCIONES[opcion]
            procesar_origen(key_seleccionada)
            
            # Preguntar si quiere seguir
            continuar = input("\n¿Querés procesar otra fuente? (s/n): ").lower()
            if continuar != "s":
                print("👋 Listo por hoy.")
                conn.close()
                break
        else:
            print("⚠️  Opción no válida. Probá con 1, 2, 3 o 't', 'n', etc.")

if __name__ == "__main__":
    main()