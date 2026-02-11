import sqlite3
import pandas as pd
import json
import re

# === CONFIGURACIÓN ===
with open("config.json", "r", encoding="utf-8") as f:
    config = json.load(f)
db_path = config["db_path"]

# Solo listamos las tablas y qué etiqueta queremos poner.
# El script buscará la columna de mail AUTOMÁTICAMENTE.
TABLAS_A_PROCESAR = [
    {"nombre": "raw_monday_PRESUPUESTOS_PISCINAS_2022_VENDIDAS", "etiqueta": "PISCINA"},
    {"nombre": "raw_monday_VENDIDAS_2020", "etiqueta": "PISCINA"},
    {"nombre": "raw_monday_FINSTALPIS2_2020_INSTALADAS", "etiqueta": "PISCINA"}
]

def limpiar_email(email):
    if pd.isna(email) or str(email).strip() == "": return None
    clean = str(email).strip().lower()
    if re.match(r'^[\w\.-]+@[\w\.-]+\.\w+$', clean): return clean
    return None

def buscar_columna_email(df_columns):
    """Busca la columna de email sin importar mayúsculas/minúsculas"""
    for col in df_columns:
        c = col.lower().strip()
        if "mail" in c or "correo" in c:
            return col
    return None

conn = sqlite3.connect(db_path)
cursor = conn.cursor()

print("--- INICIANDO ETIQUETADO INTELIGENTE ---")

# 1. Asegurarnos que la columna 'etiquetas' existe
try:
    cursor.execute("ALTER TABLE clientes_vital ADD COLUMN etiquetas TEXT DEFAULT ''")
    conn.commit()
except:
    pass # Ya existía

total_etiquetados = 0

for item in TABLAS_A_PROCESAR:
    tabla = item["nombre"]
    etiqueta_a_poner = item["etiqueta"]
    
    print(f"\n🔍 Analizando tabla: {tabla}...")
    
    try:
        # 1. Obtenemos solo 1 fila para ver los nombres REALES de las columnas
        df_cols = pd.read_sql(f'SELECT * FROM "{tabla}" LIMIT 1', conn)
        
        # 2. Detectamos cuál es la columna de mail
        col_real = buscar_columna_email(df_cols.columns)
        
        if not col_real:
            print(f"   ⚠️ SALTADO: No encontré columna de mail en {tabla}. Columnas: {list(df_cols.columns)}")
            continue
            
        print(f"   ✅ Columna detectada: '{col_real}'")
        
        # 3. Traemos los datos usando el nombre EXACTO
        df_origen = pd.read_sql(f'SELECT "{col_real}" FROM "{tabla}"', conn)
        
        count_local = 0
        
        for raw_mail in df_origen[col_real]:
            mail = limpiar_email(raw_mail)
            if not mail: continue
            
            # --- Lógica de Actualización ---
            cursor.execute("SELECT id_cliente, etiquetas FROM clientes_vital WHERE correo = ?", (mail,))
            resultado = cursor.fetchone()
            
            if resultado:
                id_cliente, etiquetas_actuales = resultado
                if etiquetas_actuales is None: etiquetas_actuales = ""
                
                tags_lista = [t.strip().upper() for t in etiquetas_actuales.split(",")]
                
                if etiqueta_a_poner.upper() not in tags_lista:
                    if etiquetas_actuales == "":
                        nueva_etiqueta = etiqueta_a_poner.upper()
                    else:
                        nueva_etiqueta = etiquetas_actuales + ", " + etiqueta_a_poner.upper()
                    
                    cursor.execute("UPDATE clientes_vital SET etiquetas = ? WHERE id_cliente = ?", (nueva_etiqueta, id_cliente))
                    count_local += 1
                    total_etiquetados += 1
        
        print(f"   💾 Se etiquetaron {count_local} clientes.")
        conn.commit()

    except Exception as e:
        print(f"   ❌ Error procesando {tabla}: {e}")
        print("      (Tip: Verificá que el nombre de la tabla raw sea exacto, a veces cambia raw_onday vs raw_monday)")

conn.close()
print(f"\n🎉 FIN: {total_etiquetados} clientes etiquetados correctamente.")