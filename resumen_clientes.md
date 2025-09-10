# 📑 Resumen de uso — Sistema de clientes

## 🔹 Flujo general
1. **Importar CSVs**  
   ```bash
   python import_raw.py
   ```  
   - Lee cada archivo definido en `config.json`.  
   - Crea/reemplaza tablas `raw_xxxx` en SQLite (los datos quedan tal cual vienen).  

2. **Consolidar datos**  
   ```bash
   python consolidar.py
   ```  
   - Usa los `map` de `config.json`.  
   - Normaliza y guarda en la tabla unificada `clientes_vital`.  
   - Clave única = `correo`.  
     - Si existe → `UPDATE` (se conserva `id_cliente` y `fecha_alta`).  
     - Si no existe → `INSERT`.  

3. **Generar mailing** (únicamente correos únicos de todas las tablas raw)  
   - Se hace directo en DB Browser:  
   ```sql
   INSERT OR IGNORE INTO mailing (correo)
   SELECT DISTINCT LOWER(TRIM("Email")) FROM raw_mercately WHERE "Email" LIKE '%@%';
   -- Repetir para raw_tienda, raw_nexion, raw_monday...
   ```

---

## 🔹 Tablas principales
- **raw_xxxx** → datos originales de cada CSV (estructura distinta).  
- **clientes_vital** → todos los clientes unificados (estructura común).  
- **mailing** → solo correos únicos de todas las fuentes.  
- **vista_nexion_fechas** → vista para consultar `Fecha Alta` de `raw_nexion` ya normalizada a `YYYY-MM-DD`.  

---

## 🔹 Consultas útiles (SQL)

### Ver estructura de una tabla
```sql
PRAGMA table_info(raw_nexion);
```

### Ver últimos registros por fecha en raw_nexion
```sql
SELECT "Nombre", "E-mail", fecha_alta_norm
FROM vista_nexion_fechas
ORDER BY fecha_alta_norm DESC
LIMIT 10;
```

### Clientes creados desde 1 enero 2025
```sql
SELECT *
FROM vista_nexion_fechas
WHERE fecha_alta_norm >= '2025-01-01';
```

### Clientes entre dos fechas
```sql
SELECT *
FROM vista_nexion_fechas
WHERE fecha_alta_norm BETWEEN '2024-01-01' AND '2024-12-31';
```

### Correos únicos en mailing
```sql
SELECT COUNT(*) AS total_correos FROM mailing;
```

### Correos únicos por origen
```sql
SELECT 'mercately' AS origen, COUNT(DISTINCT LOWER(TRIM("Email")))
FROM raw_mercately
UNION ALL
SELECT 'tienda', COUNT(DISTINCT LOWER(TRIM("E-mail")))
FROM raw_tienda
UNION ALL
SELECT 'nexion', COUNT(DISTINCT LOWER(TRIM("E-mail")))
FROM raw_nexion
UNION ALL
SELECT 'monday', COUNT(DISTINCT LOWER(TRIM("EMAIL")))
FROM "raw_monday_CONTACTOS_CENTRALIZADO_1757518290.csv";
```

---

## 🔹 Checklist rápido
- ➡️ **Agregar nuevo origen** → editar `config.json`.  
- ➡️ **Importar** → `python import_raw.py`.  
- ➡️ **Consolidar** → `python consolidar.py`.  
- ➡️ **Mailing** → ejecutar SQL en DB Browser.  
- ➡️ **Consultas por fecha** → usar `vista_nexion_fechas`.  
