🧾 Guía operativa — Sistema de Clientes Vital
🎯 Objetivo del sistema

Este proyecto sirve para construir y mantener una base única de clientes de la empresa, a partir de archivos exportados desde distintas plataformas:

🟢 Mercately

🔵 Nexion

🟡 Monday.com

🟣 Tienda (en desarrollo / opcional)

El sistema permite:

📥 Importar datos sin perder información original

🧠 Identificar clientes por un dato lógico único (hoy: correo)

🆔 Asignar a cada cliente un ID interno permanente

🔁 Reimportar datos muchas veces sin duplicar clientes

🧠 Conceptos clave (leer antes de usar)
🧱 RAW (raw_xxxx)

Son tablas que contienen los datos tal cual vienen del archivo CSV.

Características:

❌ No se limpian

❌ No se normalizan

❌ No se unifican

🔁 Se reemplazan en cada importación

Ejemplos:

raw_mercately

raw_nexion

raw_monday_xxx

raw_tienda

👉 Pensalas como una foto del archivo importado.

🧩 CONSOLIDADO (clientes_vital)

Es la tabla final y unificada de clientes.

Características:

✅ 1 cliente = 1 registro

🔑 Clave lógica: correo

🆔 Clave técnica: id_cliente

🔒 El ID NO cambia nunca

👉 Esta es la tabla principal del sistema.

📂 Estructura del proyecto
VITAL_FLASK/
│
├── config.json              ⚙️ Configuración de orígenes
├── import_raw.py            📥 CSV → raw_xxxx
├── consolidar.py            🔄 raw_xxxx → clientes_vital
├── backup_db.py             💾 Backup automático
├── validar_config.py        ✅ Valida config.json (opcional)
├── README.md                📘 Documentación
│
├── DDBB/
│   └── vital_ddbb_clientes.db
│
├── importaciones/
│   ├── mercately.csv
│   ├── nexion.csv
│   ├── monday_xxx.csv
│   └── tienda.csv
│
└── backups/
    └── vital_ddbb_clientes_YYYYMMDD_HHMMSS.db

⚙️ Requisitos

🐍 Python 3.9 o superior

📦 Entorno virtual activado

📚 Librerías:

pip install pandas

🆕 Primer uso (base de datos vacía)
1️⃣ Configurar config.json

En este archivo se define:

📄 Nombre del archivo CSV

🔣 Separador

🧾 Encoding

🗺️ Mapeo de columnas

Ejemplo:

"mercately": {
  "archivo": "mercately.csv",
  "sep": ",",
  "encoding": "utf-8-sig",
  "map": {
    "correo": "Email",
    "nombre": "FirstName",
    "telefono": "Phone"
  }
}


⚠️ Importante sobre Monday

Monday no siempre exporta las mismas columnas

Cada exportación puede cambiar

Siempre verificar:

Que exista la columna de correo

Que el map coincida con el archivo actual

2️⃣ Validar la configuración (recomendado)
python validar_config.py


Evita errores de sintaxis o claves mal definidas.

3️⃣ Backup de la base (SIEMPRE)
python backup_db.py


💾 Se ejecuta antes de importar o consolidar, incluso si la base está vacía.

4️⃣ Importar archivos (CSV → RAW)
python import_raw.py


Qué hace:

📥 Lee todos los CSV definidos

🧱 Crea o reemplaza tablas raw_xxxx

❌ No toca clientes_vital

5️⃣ Consolidar clientes (RAW → FINAL)
python consolidar.py


Qué hace:

🔄 Lee todas las tablas raw_xxxx

🗺️ Usa el map de cada origen

🧠 Para cada correo:

Si existe → actualiza

Si no existe → crea

🔒 El id_cliente se conserva siempre

🔁 Usos posteriores (reimportaciones)

Cuando se vuelve a ejecutar el proceso:

🔁 Las tablas raw_xxxx se reemplazan

🔒 clientes_vital:

NO se borra

NO pierde IDs

Solo se actualiza o agrega información

👉 Un cliente mantiene siempre el mismo ID.

🔐 Seguridad y blindaje

La tabla clientes_vital está protegida por:

🔑 UNIQUE(correo)

🔠 Índice COLLATE NOCASE

🔽 Normalización a minúsculas

Esto evita:

❌ Duplicados por mayúsculas

❌ Cambios de ID

❌ Errores por reimportaciones

🧪 Consultas SQL típicas

Ver todos los clientes:

SELECT * FROM clientes_vital;


Buscar por correo:

SELECT *
FROM clientes_vital
WHERE correo = 'cliente@mail.com';


Cantidad total:

SELECT COUNT(*) FROM clientes_vital;

🚨 Errores comunes y solución
❌ UNIQUE constraint failed

👉 Hay correos duplicados por mayúsculas
✔️ Solución: normalizar y conservar el ID más antiguo

❌ JSONDecodeError

👉 config.json mal formado
✔️ Solución: ejecutar validar_config.py

❌ No aparecen datos nuevos

Revisar:

📄 Nombre del archivo

🔣 Separador

🧾 Encoding

📧 Columna de correo en el map

🧠 Reglas de oro

🛑 Nunca borrar clientes_vital
🛑 Nunca borrar registros manualmente
💾 Siempre hacer backup antes de consolidar

⚡ Resumen rápido (para operar sin pensar)
python backup_db.py
python import_raw.py
python consolidar.py


👉 Ese es el orden correcto y seguro.