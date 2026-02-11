# ver_archivo.py
archivo = "importaciones/tienda.csv"

try:
    with open(archivo, "r", encoding="latin-1") as f:
        print(f"--- PRIMERAS 3 LÍNEAS DE {archivo} ---")
        for i in range(3):
            print(f.readline().strip())
    print("------------------------------------------")
except Exception as e:
    print(f"Error leyendo el archivo: {e}")
    