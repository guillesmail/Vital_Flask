archivo = "importaciones/nexion.csv"

print(f"--- LEYENDO: {archivo} ---")

try:
    # Probamos con Latin-1 (típico de sistemas viejos/Windows)
    with open(archivo, "r", encoding="latin-1") as f:
        cabecera = f.readline().strip()
        fila1 = f.readline().strip()
        
    print("\nOpción A (Latin-1):")
    print(f"ENCABEZADO: {cabecera}")
    print(f"FILA 1:     {fila1}")

except Exception as e:
    print(f"Error leyendo como Latin-1: {e}")