# Importamos los módulos necesarios
import pandas as pd               # Para leer y procesar archivos CSV
import sqlite3                    # Para interactuar con la base de datos SQLite
import os                         # Para trabajar con rutas de archivos
from datetime import datetime     # Para registrar la fecha y hora de importación

# Ruta a la base de datos SQLite
db_path = "DDBB/vital_ddbb_clientes.db"

# Carpeta donde se encuentran los archivos CSV a importar
import_folder = "importaciones"

# Diccionario con la configuración para cada origen de datos
archivos = {
    "mercately": {
        "archivo": "mercately.csv",              # Nombre del archivo CSV a importar
        "columnas": {                            # Mapeo de nombres de columnas originales → nombres estandarizados
            "FirstName": "nombre",
            "Email": "correo",
            "Phone": "telefono",
            "Localidd": "localidad"
        },
        "sep": ","                               # Separador usado en el CSV
    },
    "tienda": {
        "archivo": "tienda.csv",
        "columnas": {
            "nombre": "nombre",
            "correo": "correo",
            "tel": "telefono",
            "localidad": "localidad"
        },
        "sep": ";",                              # Separador usado en el CSV
        "encoding": "ISO-8859-1"                 # Codificación para manejar caracteres especiales
    }
}

# Conectamos con la base de datos SQLite
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Función para insertar o actualizar registros en la tabla final "clientes_vital"
def insertar_o_actualizar(df, origen):
    for _, row in df.iterrows():  # Iteramos por cada fila del DataFrame
        correo = row.get("correo")
        
        # Si no hay correo, se omite (es el identificador único)
        if not correo or pd.isna(correo):
            continue

        # Verificamos si ya existe ese cliente en la tabla
        cursor.execute("SELECT id_cliente FROM clientes_vital WHERE correo = ?", (correo,))
        resultado = cursor.fetchone()

        # Armamos la tupla con los datos a insertar o actualizar
        datos = (
            row.get("nombre", ""),
            row.get("apellido", ""),  # Agregamos campo de apellido (aunque no todos los orígenes lo tienen)
            correo,
            row.get("telefono", ""),
            row.get("localidad", ""),
            origen,                   # Se registra de qué origen vino el dato
            row.get("id_origen", ""),# Un identificador interno del archivo
            datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # Fecha y hora de la importación
        )

        if resultado:
            # Si ya existe el cliente (mismo correo), se actualizan sus datos
            cursor.execute("""
                UPDATE clientes_vital
                SET nombre = ?, apellido = ?, telefono = ?, localidad = ?, origen = ?, id_origen = ?, fecha_alta = ?
                WHERE correo = ?
            """, datos[0:6] + (datos[6], datos[2]))  # email usado para el WHERE
        else:
            # Si no existe, se inserta como nuevo cliente
            cursor.execute("""
                INSERT INTO clientes_vital
                (nombre, apellido, correo, telefono, localidad, origen, id_origen, fecha_alta)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, datos)

# Bucle que recorre cada origen configurado (mercately, tienda, etc.)
for origen, config in archivos.items():
    path_archivo = os.path.join(import_folder, config["archivo"])  # Ruta completa al archivo

    if not os.path.isfile(path_archivo):
        # Si el archivo no existe, se informa y se pasa al siguiente
        print(f"❌ Archivo no encontrado: {path_archivo}")
        continue

    try:
        # Se intenta leer el archivo CSV
        df = pd.read_csv(
            path_archivo,
            encoding=config.get("encoding", "utf-8-sig"),
            sep=config["sep"]
        )

        # Verificamos si faltan columnas importantes en el archivo
        columnas_en_archivo = df.columns.tolist()
        columnas_faltantes = [
            col for col in config["columnas"] if col not in columnas_en_archivo
        ]

        if columnas_faltantes:
            print(f"❌ Error: columnas faltantes en '{origen}': {', '.join(columnas_faltantes)}")
            print("⏭️  Se omite la importación de esta tabla.\n")
            continue

        # Renombramos las columnas según la configuración para que todas se llamen igual
        df = df.rename(columns=config["columnas"])

        # Agregamos columnas adicionales necesarias para la consolidación
        df["apellido"] = ""                       # Algunos orígenes no tienen apellido, se completa vacío
        df["id_origen"] = df.index.astype(str)    # Creamos un id interno por fila

        # Enviamos el DataFrame a la función para insertar o actualizar en la tabla final
        insertar_o_actualizar(df, origen)

        print(f"✅ Importación correcta: {origen}\n")

    except Exception as e:
        # Si ocurre un error general, lo mostramos por consola
        print(f"❌ Error al procesar '{origen}': {e}\n")

# Guardamos los cambios y cerramos la conexión con la base de datos
conn.commit()
conn.close()

