🧾 Guía operativa — Sistema de Clientes Vital
🎯 Objetivo del sistema

Este proyecto sirve para construir y mantener una base única de clientes de la empresa, a partir de archivos exportados desde distintas plataformas.

📘 Sistema de Gestión de Clientes VITAL (Data Warehouse)
Este sistema implementa un Data Warehouse de Clientes que centraliza, limpia y unifica la información proveniente de múltiples fuentes (Tienda Nube, Mercately, Nexion, Monday) para crear una Agenda Maestra Única.

🏗️ Arquitectura del Sistema (Las 3 Capas)
El sistema procesa los datos en tres etapas para garantizar seguridad e integridad histórica:

1. Capa BRONCE (Raw)
Tablas: raw_tienda, raw_nexion, raw_mercately, etc.

Función: Es la "mesa de entrada". Recibe los archivos CSV tal cual vienen, sin tocar nada.

Comportamiento: Se borra y sobrescribe cada vez que importás un archivo nuevo. Es volátil.

2. Capa PLATA (Source / Histórico)
Tablas: source_tienda, source_nexion, source_mercately.

Función: Es la memoria a largo plazo de cada plataforma.

Comportamiento: SMART MERGE (Fusión Inteligente).

Si el archivo nuevo trae datos frescos → Actualiza.

Si el archivo nuevo viene vacío en un campo que antes tenía datos → Protege el dato viejo (No borra información histórica).

Si el cliente es nuevo → Lo crea.

Aquí se guardan TODAS las columnas originales (incluso las que no se usan en la agenda maestra).

3. Capa ORO (Consolidado)
Tabla: clientes_vital.

Función: Es la "Ficha Maestra" o "La Verdad" de la empresa.

Lógica: Unifica las fuentes basándose en Prioridades.

Nexion (Prioridad 100): La verdad absoluta para Nombres/Apellidos.

Mercately (Prioridad 50): La autoridad en Teléfonos.

Tienda (Prioridad 40): Aporta Emails y datos de facturación.

Monday (Prioridad 30): Aporta estados de obra/piscina.

Identidad: Une a las personas por Email (Tienda/Nexion) o Teléfono (Mercately), evitando duplicados.

🛠️ Herramientas y Scripts
🟢 import_raw.py (La Aspiradora)
Lee los archivos de la carpeta importaciones/ y los vuelca en las tablas raw_.

Uso: Ejecutar siempre que se traigan archivos nuevos.

🟡 procesar_fuentes.py (El Cerebro - Smart Merge)
Toma los datos de raw_, los limpia y los fusiona con source_.

Menú Interactivo: Permite elegir qué fuente procesar (ej: solo Tienda).

Protección: Aplica la lógica de "rellenar huecos" (combine_first) para no perder datos previos si el Excel nuevo viene incompleto.

🔴 consolidar.py (El Unificador)
Lee todas las tablas source_ y genera la tabla maestra clientes_vital respetando las prioridades configuradas en config.json.

🛡️ backup.py (Seguridad)
Crea una copia de seguridad completa de la base de datos en la carpeta backups/.

Rotación: Guarda los últimos 10 backups y borra los viejos automáticamente.

Recomendación: Ejecutar antes de cualquier proceso importante.

🧪 eliminar_random.py (Chaos Monkey - Solo Testing)
Herramienta peligrosa para eliminar registros al azar y probar la capacidad de recuperación del sistema.

⚙️ Configuración (config.json)
El archivo config.json es el centro de control. Define cómo se lee cada archivo.

Campos Clave:

prioridad: Número alto gana (100 le gana a 50).

id_origen: Qué columna se usa como DNI en esa plataforma (ej: "E-mail" en Tienda, "phone" en Mercately, "Codigo" en Nexion).

sep: Separador del CSV (, o ;).

encoding: Formato del archivo (utf-8 o latin-1 para Excel en español).

map: Diccionario que le dice al sistema qué columna del CSV corresponde a los datos maestros (nombre, correo, telefono).

🚀 Manual de Uso (Workflow Diario)
Paso 1: Preparación
Descargá los CSV de las plataformas (Tienda, Nexion, etc.).

Guardalos en la carpeta importaciones/ con el nombre correcto (ej: tienda.csv).

(Opcional pero recomendado) Ejecutá el backup:

Bash
python backup.py
Paso 2: Importación Cruda
Cargá los archivos nuevos al sistema:

Bash
python import_raw.py
Verificá que no haya errores de "File not found" de los archivos que te interesan.

Paso 3: Procesamiento Inteligente
Actualizá el historial de la fuente que acabas de subir:

Bash
python procesar_fuentes.py
Elegí la opción en el menú (ej: t para Tienda).

Revisá el reporte:

Nuevos: Clientes que nunca antes habías visto.

Actualizados: Clientes que ya tenías, pero que ahora tienen datos más frescos (o iguales).

Paso 4: Consolidación Final
Generá la agenda maestra unificada:

Bash
python consolidar.py
¡Listo! Tu tabla clientes_vital ahora tiene la información más reciente y limpia de todas las plataformas.

🚨 Solución de Problemas Comunes
❌ Error: utf-8 codec can't decode byte...

Causa: El archivo CSV tiene acentos y se guardó en formato Windows.

Solución: En config.json, cambiá "encoding": "utf-8" por "encoding": "latin-1".

❌ Error: Expected 1 fields in line X, saw Y

Causa: El separador configurado no coincide con el archivo.

Solución: Revisá si el archivo usa comas (,) o punto y coma (;) y actualizá el campo "sep" en config.json.

❌ No veo los datos nuevos en la tabla final

Causa: Quizás el id_origen (mail/teléfono) vino vacío en el Excel.

Solución: El sistema descarta automáticamente registros sin ID para no ensuciar la base. Revisá el reporte de procesar_fuentes.py (sección "Ignorados").