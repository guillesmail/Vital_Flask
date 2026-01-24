import sqlite3
import pandas as pd
import json
import re

# === CARGAR RUTA DE LA BASE DE DATOS ===
with open("config.json", "r", encoding="utf-8") as f:
    config = json.load(f)
db_path = config["db_path"]

# ==========================================
# ⚙️ CONFIGURACION MANUAL (EDITAR ACÁ)
# ==========================================
# Formato: "NOMBRE_TABLA": {"email": "NOMBRE_COLUMNA_MAIL", "nombre": "NOMBRE_COLUMNA_NOMBRE"}

FUENTES = {
    "raw_monday_PRESUPUESTOS_PISCINAS_2022_VENDIDAS": {
        "email": "EMAIL",        # <--- Poné acá el nombre exacto de la columna mail
        "nombre": "Name"         # <--- Poné acá el nombre exacto de la columna nombre
    },
    "raw_monday_VENDIDAS_2020": {
        "email": "eMail",       # <--- Ejemplo: Cambialo por el real
        "nombre": "Name"
    }
}
# ==========================================

def limpiar_email(email):
    """Valida y limpia el email"""
    if pd.isna(email) or str(email).strip() == "":
        return None
    clean = str(email).strip().lower()
    # Regex simple para validar formato
    if re.match(r'^[\w\.-]+@[\w\.-]+\.\w+$', clean):
        return clean
    return None

def limpiar_nombre(nombre):
    """Capitaliza el nombre (Juan Perez)"""
    if pd.isna(nombre) or str(nombre).strip() == "":
        return ""
    return str(nombre).strip().title()

# === PROCESO ===
conn = sqlite3.connect(db_path)
print("--- INICIANDO EXTRACCIÓN MANUAL ---")

# Usamos un diccionario para evitar duplicados. 
# Clave = Email (único), Valor = Nombre
clientes_unicos = {} 

for tabla, cols in FUENTES.items():
    col_email = cols["email"]
    col_nombre = cols["nombre"]
    
    print(f"📂 Procesando tabla: {tabla}...")
    
    try:
        # Traemos solo las 2 columnas que nos interesan
        query = f'SELECT "{col_nombre}", "{col_email}" FROM "{tabla}"'
        df = pd.read_sql(query, conn)
        
        count_local = 0
        for index, row in df.iterrows():
            mail_valido = limpiar_email(row[col_email])
            nombre_valido = limpiar_nombre(row[col_nombre])
            
            if mail_valido:
                # Si el mail no está en la lista, lo agregamos.
                # Si ya está, lo ignoramos (o podés decidir sobrescribirlo si preferís datos más nuevos)
                if mail_valido not in clientes_unicos:
                    clientes_unicos[mail_valido] = nombre_valido
                    count_local += 1
        
        print(f"   ✅ Se encontraron {count_local} mails válidos nuevos.")

    except Exception as e:
        print(f"   ❌ ERROR en {tabla}: {e}")
        print("      (Verificá que el nombre de la tabla y las columnas sean exactos)")

conn.close()

# === EXPORTAR A EXCEL ===
print("\n--- RESULTADOS FINALES ---")
total = len(clientes_unicos)

if total > 0:
    # Convertimos el diccionario en una lista para Pandas
    data_final = [{"Email": k, "Nombre": v} for k, v in clientes_unicos.items()]
    df_export = pd.DataFrame(data_final)
    
    nombre_archivo = "LISTA_MAILING_PISCINAS_FINAL.xlsx"
    df_export.to_excel(nombre_archivo, index=False)
    print(f"🎉 ÉXITO: Se generó '{nombre_archivo}' con {total} clientes únicos.")
else:
    print("⚠️ No se encontraron mails válidos. Revisá los nombres de las columnas.")