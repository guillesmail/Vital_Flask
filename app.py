from flask import Flask, render_template, request, send_file
import sqlite3
import pandas as pd
import os

app = Flask(__name__)
DB_PATH = os.path.join("DDBB", "vital_ddbb_clientes.db")

def obtener_datos(tabla, localidad=None):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    if tabla == "tienda":
        query = """
        SELECT rowid AS id,
            "nombre" AS nombre,
            "correo" AS correo,
            "tel" AS tel,
            "localidad" AS localidad
        FROM tienda
        """

    elif tabla == "mercately":
        query = """
        SELECT rowid AS id,
               "FirstName" AS nombre,
               "Email" AS correo,
               "Phone" AS tel,
               "Localidd" AS localidad
        FROM mercately
        """
    elif tabla == "clientes_vital":
        query = """
        SELECT id_cliente AS id,
            nombre,
            correo,
            telefono AS tel,
            localidad
        FROM clientes_vital
        """
    elif tabla == "raw_mercately":
        query = """
        SELECT rowid AS id,
            "FirstName" AS nombre,
            "Email" AS correo,
            "Phone" AS tel,
            "Localidd" AS localidad
        FROM raw_mercately
        """

    elif tabla == "raw_tienda":
        query = """
        SELECT rowid AS id,
            "Nombre y Apellido" AS nombre,
            "Email" AS correo,
            "Teléfono" AS tel,
            "Localidad" AS localidad
        FROM raw_tienda
        """

    else:
        # Si se pasa una tabla no esperada
        return []

    if localidad:
        query += " WHERE localidad LIKE ?"
        cursor.execute(query, ('%' + localidad + '%',))
    else:
        cursor.execute(query)

    datos = cursor.fetchall()
    conn.close()
    return datos

@app.route("/", methods=["GET"])
def index():
    tabla = request.args.get("tabla", "clientes_vital")
    localidad = request.args.get("localidad", "")
    datos = obtener_datos(tabla, localidad)
    return render_template("index.html", datos=datos, tabla=tabla, localidad=localidad)

@app.route("/exportar/<formato>")
def exportar(formato):
    tabla = request.args.get("tabla", "mercately")
    localidad = request.args.get("localidad", "")
    datos = obtener_datos(tabla, localidad)

    columnas = ["id", "nombre", "correo", "tel", "localidad"]
    df = pd.DataFrame(datos, columns=columnas)

    nombre_archivo = f"{tabla}_filtrado.{formato}"
    ruta = os.path.join("DDBB", nombre_archivo)

    if formato == "csv":
        df.to_csv(ruta, index=False)
    elif formato == "xlsx":
        df.to_excel(ruta, index=False)

    return send_file(ruta, as_attachment=True)

if __name__ == "__main__":
    app.run(debug=True, port=5001)

