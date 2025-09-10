import json

CONFIG_FILE = "config.json"

def validar_json():
    try:
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        print(f"✅ {CONFIG_FILE} es un JSON válido.")

        # Validar estructura esperada
        if "db_path" not in data or "import_folder" not in data or "archivos" not in data:
            print("⚠ El JSON es válido, pero falta alguna de las claves principales: db_path, import_folder o archivos.")
        else:
            print("✅ Estructura básica correcta.")

        # Validar cada origen
        for origen, cfg in data["archivos"].items():
            for campo in ["archivo", "sep", "encoding", "map"]:
                if campo not in cfg:
                    print(f"⚠ El origen '{origen}' no tiene el campo '{campo}' definido.")

            if "map" in cfg:
                for obligatorio in ["nombre", "apellido", "correo", "telefono", "localidad", "id_origen"]:
                    if obligatorio not in cfg["map"]:
                        print(f"⚠ El origen '{origen}' no tiene mapeo para '{obligatorio}'.")

        return True

    except json.JSONDecodeError as e:
        print(f"❌ Error de sintaxis en {CONFIG_FILE}: {e}")
        return False
    except FileNotFoundError:
        print(f"❌ No se encontró el archivo {CONFIG_FILE}")
        return False

if __name__ == "__main__":
    validar_json()
