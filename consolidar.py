import pandas as pd
import sqlite3
import json
import re
from datetime import datetime

# === Cargar configuración ===
with open("config.json", "r", encoding="utf-8") as f:
    config = json.load(f)

db_path = config["db_path"]
archivos = config["archivos"]

# === Funciones de Limpieza ===

def limpiar_telefono(tel):
    """
    Normaliza teléfonos. Intenta llevar todo a formato 549...
    Retorna '' si el numero no es viable.
    """
    if pd.isna(tel) or tel == "":
        return ""
    
    # Dejar solo números
    tel_clean = re.sub(r'\D', '', str(tel))
    
    # Regla Argentina: Si tiene 10 dígitos (ej: 11 4444 5555), agregar 549
    if len(tel_clean) == 10:
        return f"549{tel_clean}"
    
    # Si tiene longitud decente (más de 8), lo dejamos pasar
    if len(tel_clean) >= 8: 
        return tel_clean
        
    return ""

def validar_email(email):
    """Retorna el email limpio o None"""
    if pd.isna(email) or email == "":
        return None
    email = str(email).strip().lower()
    # Debe tener al menos una letra, un arroba y un punto
    patron = r'^[\w\.-]+@[\w\.-]+\.\w+$'
    if re.match(patron, email):
        return email
    return None

def limpiar_texto(txt):
    if pd.isna(txt) or txt == "":
        return ""
    return str(txt).strip().title()

# === Conexión ===
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# === CREACIÓN DE TABLA (Adaptada para Híbrido) ===
# IMPORTANTE: Quitamos "UNIQUE" del correo y telefono para manejarlo por software
cursor.execute("""
CREATE TABLE IF NOT EXISTS clientes_vital (
    id_cliente   INTEGER PRIMARY KEY AUTOINCREMENT,
    nombre       TEXT,
    apellido     TEXT,
    correo       TEXT,
    telefono     TEXT,
    localidad    TEXT,
    origen       TEXT,
    id_origen    TEXT,
    fecha_alta   TEXT,
    prioridad    INTEGER DEFAULT 0
)
""")

# Índices para velocidad
cursor.execute("CREATE INDEX IF NOT EXISTS idx_correo ON clientes_vital(correo)")
cursor.execute("CREATE INDEX IF NOT EXISTS idx_telefono ON clientes_vital(telefono)")
conn.commit()

# === Lógica Central ===
def insertar_o_actualizar(df, origen, cfg_origen):
    mapeo = cfg_origen.get("map", {})
    prioridad_nueva = cfg_origen.get("prioridad", 0)
    
    # Contadores para el reporte
    c_nuevos = 0
    c_updates = 0
    c_ignorados = 0
    c_descartados = 0

    for _, row in df.iterrows():
        # 1. Extraer y limpiar datos clave
        correo_raw = row.get(mapeo.get("correo", ""), "")
        correo = validar_email(correo_raw)
        
        tel_raw = row.get(mapeo.get("telefono", ""), "")
        telefono = limpiar_telefono(tel_raw)

        # 🛑 FILTRO DE CALIDAD: Si no tiene ni mail ni teléfono, no sirve.
        if not correo and not telefono:
            c_descartados += 1
            continue

        # 2. BUSQUEDA DOBLE (El corazón del sistema)
        resultado = None
        
        # A) Primero buscamos por MAIL (es el identificador más fuerte)
        if correo:
            cursor.execute("SELECT id_cliente, fecha_alta, prioridad, correo, telefono FROM clientes_vital WHERE correo = ?", (correo,))
            resultado = cursor.fetchone()
        
        # B) Si no apareció, buscamos por TELEFONO (si tenemos uno válido)
        if not resultado and telefono:
            cursor.execute("SELECT id_cliente, fecha_alta, prioridad, correo, telefono FROM clientes_vital WHERE telefono = ?", (telefono,))
            resultado = cursor.fetchone()

        # 3. Preparar datos para guardar
        nombre = limpiar_texto(row.get(mapeo.get("nombre", ""), ""))
        apellido = limpiar_texto(row.get(mapeo.get("apellido", ""), ""))
        localidad = limpiar_texto(row.get(mapeo.get("localidad", ""), ""))
        
        if mapeo.get("id_origen") == "index":
            id_origen_val = str(row.name)
        else:
            id_origen_val = str(row.get(mapeo.get("id_origen", ""), "")).strip()

        fecha_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Tupla completa
        datos_nuevos = (
            nombre, apellido, correo, telefono, localidad, 
            origen, id_origen_val, fecha_now, prioridad_nueva
        )

        if resultado:
            # === CLIENTE EXISTENTE (Lo encontramos) ===
            id_cliente_db = resultado[0]
            prioridad_db = resultado[2] if resultado[2] is not None else 0
            
            # REGLA DE ACTUALIZACIÓN:
            # 1. Si mi fuente es MEJOR o IGUAL (Nexion vs Monday), sobrescribo.
            if prioridad_nueva >= prioridad_db:
                # Armamos UPDATE dinámico para no borrar datos útiles con vacíos
                # Ejemplo: Si Nexion viene sin mail, no quiero borrar el mail que ya tengo de Tienda.
                
                campos = []
                valores = []
                
                # Solo actualizamos campos si traen información real
                if nombre: 
                    campos.append("nombre=?")
                    valores.append(nombre)
                if apellido: 
                    campos.append("apellido=?")
                    valores.append(apellido)
                if correo: 
                    campos.append("correo=?")
                    valores.append(correo)
                if telefono: 
                    campos.append("telefono=?")
                    valores.append(telefono)
                if localidad: 
                    campos.append("localidad=?")
                    valores.append(localidad)
                
                # Origen siempre se actualiza si ganamos la prioridad
                campos.extend(["origen=?", "id_origen=?", "prioridad=?"])
                valores.extend([origen, id_origen_val, prioridad_nueva])
                
                # Condición WHERE
                valores.append(id_cliente_db)
                
                sql = f"UPDATE clientes_vital SET {', '.join(campos)} WHERE id_cliente=?"
                cursor.execute(sql, tuple(valores))
                c_updates += 1
            else:
                # Mi fuente es PEOR (ej: Monday queriendo pisar a Nexion). No toco nada.
                c_ignorados += 1
        else:
            # === CLIENTE NUEVO (Nadie lo conoce) ===
            cursor.execute("""
                INSERT INTO clientes_vital
                (nombre, apellido, correo, telefono, localidad, origen, id_origen, fecha_alta, prioridad)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, datos_nuevos)
            c_nuevos += 1

    print(f"   ↳ {origen.upper()}: {c_nuevos} Nuevos | {c_updates} Actualizados | {c_ignorados} Ignorados (baja prio) | {c_descartados} Descartados (vacíos)")

# === Ejecución ===
print("--- Iniciando Consolidación Híbrida (Mail + Tel) ---")
for origen, cfg in archivos.items():
    try:
        # Importante: Leemos de la tabla RAW
        df = pd.read_sql(f"SELECT * FROM raw_{origen}", conn)
        
        insertar_o_actualizar(df, origen, cfg)
        
        # CAMBIO CLAVE: Guardamos inmediatamente después de cada archivo exitoso
        conn.commit() 
        print(f"   ✅ Cambios de {origen} guardados correctamente.")
        
    except Exception as e:
        # Si falla uno (como Monday), hacemos rollback parcial y seguimos
        conn.rollback()
        print(f"⚠️  Saltando {origen}: {e}")

conn.close()
print("--- Fin del Proceso ---")
