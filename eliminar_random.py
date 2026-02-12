import sqlite3
import json
import sys

# === CARGA DE CONFIGURACIÓN ===
try:
    with open("config.json", "r", encoding="utf-8") as f:
        config = json.load(f)
    db_path = config["db_path"]
except Exception as e:
    print(f"❌ Error: No se pudo leer config.json ({e})")
    sys.exit()

conn = sqlite3.connect(db_path)
cursor = conn.cursor()

def listar_tablas():
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';")
    tablas = cursor.fetchall()
    return [t[0] for t in tablas]

print("\n🗑️ === ELIMINADOR DE REGISTROS AL AZAR (CHAOS MONKEY) ===")
print(f"📂 Base de datos: {db_path}\n")

# 1. MOSTRAR TABLAS DISPONIBLES
tablas_existentes = listar_tablas()

if not tablas_existentes:
    print("❌ No hay tablas en la base de datos.")
    sys.exit()

print("Tablas disponibles:")
for i, t in enumerate(tablas_existentes, 1):
    print(f"  {i}. {t}")

# 2. PREGUNTAR TABLA
seleccion = input("\n👉 Escribí el NOMBRE de la tabla (o el número) a podar: ").strip()

tabla_elegida = ""
if seleccion.isdigit() and 1 <= int(seleccion) <= len(tablas_existentes):
    tabla_elegida = tablas_existentes[int(seleccion) - 1]
else:
    if seleccion in tablas_existentes:
        tabla_elegida = seleccion
    else:
        print("❌ Tabla no válida.")
        sys.exit()

# 3. PREGUNTAR CANTIDAD
cantidad_str = input(f"👉 ¿Cuántos registros querés eliminar al azar de '{tabla_elegida}'? (ej: 100): ").strip()

if not cantidad_str.isdigit():
    print("❌ Debes ingresar un número entero.")
    sys.exit()

cantidad = int(cantidad_str)

# 4. CHEQUEO DE CANTIDAD ACTUAL
cursor.execute(f"SELECT COUNT(*) FROM {tabla_elegida}")
total_actual = cursor.fetchone()[0]

print(f"\n📊 Estado actual de '{tabla_elegida}': {total_actual} registros.")

if cantidad > total_actual:
    print(f"⚠️  Advertencia: Querés borrar {cantidad}, pero solo hay {total_actual}. Se borrará TODO.")
    cantidad = total_actual

# 5. CONFIRMACIÓN FINAL (Protocolo de Seguridad)
confirmacion = input(f"\n⚠️  ¿ESTÁS SEGURO que querés eliminar {cantidad} registros de '{tabla_elegida}'? (Escribí 'SI' para confirmar): ")

if confirmacion.upper() == "SI":
    try:
        # === LA SENTENCIA SQL MÁGICA ===
        # Usamos 'rowid' que es un ID interno oculto de SQLite para identificar filas
        query = f"""
            DELETE FROM {tabla_elegida} 
            WHERE rowid IN (
                SELECT rowid FROM {tabla_elegida} ORDER BY RANDOM() LIMIT {cantidad}
            )
        """
        cursor.execute(query)
        conn.commit()
        
        # Verificar resultado
        cursor.execute(f"SELECT COUNT(*) FROM {tabla_elegida}")
        total_nuevo = cursor.fetchone()[0]
        
        print(f"\n✅ LISTO. Se eliminaron {total_actual - total_nuevo} registros.")
        print(f"📉 La tabla '{tabla_elegida}' quedó con {total_nuevo} registros.")
        
    except Exception as e:
        print(f"❌ Error al intentar eliminar: {e}")
        conn.rollback()
else:
    print("🛡️ Operación cancelada. No se tocó nada.")

conn.close()