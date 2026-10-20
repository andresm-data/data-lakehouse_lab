# Lakehouse Lab: Delta Lake vs Apache Iceberg

Laboratorio práctico para explorar y medir las diferencias entre **Delta Lake** y **Apache Iceberg**, los dos formatos de tabla abiertos más usados en arquitecturas *lakehouse*. Con PySpark y los datos públicos de viajes de taxi de Nueva York (NYC TLC), el proyecto construye las mismas tablas en ambos formatos y compara su comportamiento en ingesta, consultas, actualizaciones, evolución de esquema, *time travel* y mantenimiento. El objetivo es obtener métricas reproducibles que sirvan para decidir qué formato conviene en cada caso.

## Tabla de contenido

- [Descripción](#descripción)
  - [Motivación](#motivación)
  - [Datos](#datos)
  - [Qué se compara](#qué-se-compara)
  - [Stack tecnológico](#stack-tecnológico)
- [Estructura](#estructura)
- [Uso](#uso)
  - [Comandos de ingesta](#comandos-de-ingesta)
  - [Benchmark de compresión](#benchmark-de-compresión)
- [Week 1](#week-1)
  - [Qué se construyó](#qué-se-construyó)
  - [OPTIMIZE: compactación de archivos](#optimize-compactación-de-archivos)
  - [VACUUM: limpieza de archivos retirados](#vacuum-limpieza-de-archivos-retirados)
  - [Aprendizajes](#aprendizajes)
  - [Problemas y soluciones](#problemas-y-soluciones)

## Descripción

### Motivación

Delta Lake y Apache Iceberg resuelven el mismo problema: añadir transacciones ACID, control de versiones y gestión de metadatos sobre archivos Parquet en un *data lake*. Sin embargo, difieren en su diseño interno (log de transacciones frente a árbol de metadatos y *snapshots*), en cómo manejan particiones y en las operaciones de mantenimiento que requieren. Este proyecto busca medir esas diferencias con datos reales en lugar de quedarse en la comparación teórica.

### Datos

Se usan los registros de viajes de **taxis amarillos** publicados por la [NYC Taxi & Limousine Commission](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page):

- `yellow_tripdata_2025-01.parquet` a `yellow_tripdata_2025-03.parquet`: viajes de enero a marzo de 2025.
- `taxi_zone_lookup.csv`: catálogo de zonas de taxi (borough, zona y zona de servicio).

Los archivos originales, tal como se descargan, se guardan en `data/landing/`. A partir de ellos, `lh-partition-date` genera en `data/raw/` un dataset unificado y particionado por fecha de recogida (`year=.../month=...`). Ninguno de los dos directorios se versiona en el repositorio.

### Qué se compara

Cada experimento se ejecuta sobre las mismas tablas escritas en ambos formatos:

| Dimensión | Qué se mide |
|---|---|
| Ingesta | Tiempo de escritura inicial y cargas incrementales por mes |
| Consultas | Latencia de consultas analíticas, *partition pruning* y *data skipping* |
| Operaciones DML | `UPDATE`, `DELETE` y `MERGE` (upserts) |
| Evolución de esquema | Agregar, renombrar y cambiar tipos de columnas |
| Particionamiento | Particionamiento explícito frente a *hidden partitioning* |
| *Time travel* | Consulta de versiones o *snapshots* anteriores |
| Mantenimiento | Compactación de archivos pequeños, `VACUUM` y expiración de *snapshots* |
| Almacenamiento | Tamaño en disco, número de archivos y volumen de metadatos |

### Stack tecnológico

- **Python** 3.11+
- **PySpark** 4.x como motor de procesamiento
- **PyArrow** para lectura y escritura de Parquet
- **Delta Lake** y **Apache Iceberg** como formatos de tabla
- **Poetry** para la gestión de dependencias
- **pytest** para las pruebas

## Estructura

```text
lakehouse-lab/
├── data/                     # No versionado
│   ├── landing/              # Archivos originales de NYC TLC, sin modificar
│   └── raw/                  # Dataset unificado y particionado por fecha
├── docs/                     # Documentación y resultados de los benchmarks
├── notebooks/                # Exploración y análisis interactivo
├── src/
│   └── lakehouse/
│       ├── ingest/           # Carga y limpieza de los datos crudos
│       ├── spark/            # Configuración de la sesión de Spark
│       ├── delta/            # Creación y operaciones sobre tablas Delta
│       ├── iceberg/          # Creación y operaciones sobre tablas Iceberg
│       └── benchmarks/       # Experimentos y medición de métricas
├── tests/                    # Pruebas unitarias
├── pyproject.toml            # Configuración del proyecto y dependencias
├── poetry.lock
└── README.md
```

## Uso

Instala las dependencias y los comandos del proyecto con:

```bash
poetry install
```

### Comandos de ingesta

El módulo `lakehouse.ingest` ofrece tres comandos para preparar los datos. Leen Parquet o CSV (`.csv`, `.csv.gz`, `.csv.bz2`, `.csv.lz4`, `.csv.zst`), procesan por lotes para mantener acotado el uso de memoria y comparten estas opciones:

| Opción | Descripción |
|---|---|
| `-f`, `--formato` | Formato de salida: `parquet` (por defecto) o `csv` |
| `-c`, `--compresion` | Parquet: `none`, `snappy`, `gzip`, `brotli`, `lz4`, `zstd` (por defecto). CSV: `none` (por defecto), `gzip`, `bz2`, `lz4`, `zstd` |
| `--sobrescribir` | Reemplaza el destino si ya existe |

**`lh-convert`**: convierte un archivo entre Parquet y CSV, o cambia su compresión. Con `-p` genera un dataset particionado por columnas existentes.

```bash
poetry run lh-convert data/landing/yellow_tripdata_2025-01.parquet data/interim/yellow_2025-01.csv -f csv
poetry run lh-convert data/interim/yellow_2025-01.csv data/interim/yellow_2025-01.parquet
poetry run lh-convert data/landing/yellow_tripdata_2025-01.parquet data/interim/por_vendor/ -p VendorID
```

**`lh-merge`**: une varios archivos del mismo formato y esquema en uno solo. Si el origen es un directorio particionado estilo Hive, las particiones (`year`, `month`, ...) se agregan como columnas.

```bash
poetry run lh-merge data/landing/yellow_tripdata_2025-0*.parquet data/interim/yellow_2025_q1.csv.gz -f csv -c gzip
poetry run lh-merge data/raw/yellow_tripdata.parquet data/interim/yellow_tripdata.csv -f csv
```

**`lh-partition-date`**: une varios archivos en un dataset particionado por una columna de fecha o timestamp. Cada fila se ubica según su propia fecha, sin importar el archivo del que provenga.

```bash
poetry run lh-partition-date data/landing/yellow_tripdata_2025-0*.parquet data/raw/yellow_tripdata.parquet \
    --columna tpep_pickup_datetime --granularidad month \
    --desde 2025-01-01 --hasta 2025-04-01
```

| Opción | Descripción |
|---|---|
| `--columna` | Columna de fecha o timestamp (obligatoria) |
| `--granularidad` | `year`, `month` (por defecto) o `day` |
| `--desde` / `--hasta` | Rango de fechas en formato ISO 8601; `--desde` es inclusiva y `--hasta` exclusiva |

Cada comando muestra todas sus opciones con `--help`.

### Benchmark de compresión

**`lh-bench-compression`**: convierte un archivo con cada códec usando `lh-convert` y mide el tiempo de escritura, el tamaño en disco y el tiempo de lectura. Muestra la tabla de resultados y la guarda en `docs/benchmarks/compresion_<formato>_<AAAAMMDD_HHMMSS>.md`.

```bash
poetry run lh-bench-compression data/landing/yellow_tripdata_2025-01.parquet -r 3
poetry run lh-bench-compression data/landing/yellow_tripdata_2025-01.parquet -f csv
poetry run lh-bench-compression data/landing/yellow_tripdata_2025-01.parquet -c none zstd brotli
```

| Opción | Descripción |
|---|---|
| `-f`, `--formato` | Formato de salida: `parquet` (por defecto) o `csv` |
| `-c`, `--compresiones` | Códecs a comparar. Por defecto, Parquet: `zstd gzip lz4 snappy`; CSV: `zstd gzip lz4 bz2` |
| `-r`, `--repeticiones` | Mediciones por códec; se reporta la mediana (por defecto: 1) |
| `--dir-trabajo` | Directorio de los archivos convertidos (por defecto: `data/benchmarks`) |
| `--dir-salida` | Directorio del reporte (por defecto: `docs/benchmarks`) |
| `--conservar` | Conserva los archivos convertidos, que por defecto se eliminan tras medirlos |

El tiempo de escritura incluye la lectura del origen; con un origen Parquet ese costo es bajo y parecido entre códecs. La lectura se hace justo después de escribir, así que el archivo suele estar en la caché del sistema operativo y lo que se mide es sobre todo la descompresión.

#### Con el dataset completo

El benchmark recibe un único archivo de origen, por lo que primero hay que unir el dataset particionado en un solo Parquet:

```bash
# 1. Particionar por fecha los archivos originales (si data/raw aún no existe)
poetry run lh-partition-date data/landing/yellow_tripdata_2025-0*.parquet data/raw/yellow_tripdata.parquet \
    --columna tpep_pickup_datetime --desde 2025-01-01 --hasta 2025-04-01

# 2. Unir las particiones en un Parquet consolidado
poetry run lh-merge data/raw/yellow_tripdata.parquet data/interim/yellow_tripdata.parquet

# 3. Ejecutar el benchmark para Parquet y para CSV
poetry run lh-bench-compression data/interim/yellow_tripdata.parquet -r 3
poetry run lh-bench-compression data/interim/yellow_tripdata.parquet -f csv
```

Conviene usar un origen Parquet y no el CSV consolidado: con un CSV, el tiempo de escritura incluye leer y analizar todo el archivo, y ese costo, igual para todos los códecs, oculta las diferencias entre ellos. Con unos 11 M de filas, gzip y bz2 en CSV pueden tardar varios minutos cada uno.

## Week 1

Notas de la primera semana (5 al 20 de octubre de 2026): ingesta de los datos, creación de la tabla Delta con `deltalake` (delta-rs, sin Spark) y prueba de sus operaciones principales. Cada operación tiene su comando y deja un informe con fecha en `docs/`.

### Qué se construyó

**Ingesta.** Los tres meses de taxis amarillos (enero a marzo de 2025) se unieron en un dataset particionado por fecha de recogida (`lh-partition-date`) y luego en un único Parquet de 11,198,001 filas y 195.6 MB (`lh-merge`). Antes se midió el efecto del formato y del códec ([informe](docs/benchmarks/compresion_consolidado_20261007_165331.md)): Parquet con zstd resultó el mejor equilibrio, y frente a CSV con el mismo códec lee entre 5.5 y 10 veces más rápido.

**Tabla Delta y operaciones.** Todas las operaciones se hacen sobre una copia de práctica, `data/delta_demo/yellow_tripdata`, para no tocar la tabla original.

| Versión | Operación | Comando | Resultado |
|---:|---|---|---|
| 0 | `WRITE` | `lh-delta-pyarrow` | Tabla particionada por `year`/`month`: 3 archivos, 11,198,001 filas, validadas por partición contra el origen |
| 1 | `MERGE` | `lh-delta-merge` | 300 filas corregidas y 50 insertadas; solo se reescribió el archivo de enero ([métricas](docs/delta/merge_20261008_152042.json)) |
| — | *Time travel* | `lh-delta-time-travel` | La versión 0 conserva los `tip_amount` originales de las 300 filas corregidas ([informe](docs/delta/time_travel_20261008_162016.md)) |
| 2 | `WRITE` (append) | `lh-delta-schema-evolution` | Columna nueva `trip_duration_min` con `schema_mode='merge'`; las filas anteriores la leen como `null` ([informe](docs/delta/schema_evolution_20261008_171329.md)) |
| 3 | `OPTIMIZE` | `lh-delta-optimize` | Compactación de los archivos de enero ([informe](docs/delta/optimize_20261020_154331.md)) |
| 4–5 | `VACUUM` | `lh-delta-vacuum` | Borrado de los archivos retirados ([informe](docs/delta/vacuum_20261020_154349.md)) |

```bash
poetry run lh-delta-optimize                      # --tamano-mb para cambiar el tamaño objetivo (100 MB)
poetry run lh-delta-vacuum                        # retención por defecto: 168 h
poetry run lh-delta-vacuum --retencion-horas 0    # solo en un entorno de práctica
```

### OPTIMIZE: compactación de archivos

`DeltaTable(ruta).optimize.compact()` une, dentro de cada partición, los archivos más pequeños que el tamaño objetivo (100 MB por defecto). Los números son los de los archivos activos de la versión actual, el equivalente a `numFiles` y `sizeInBytes` de `DESCRIBE DETAIL`:

| Medida | Antes (v2) | Después (v3) |
|---|---:|---:|
| Nº de archivos | 4 | 3 |
| Tamaño promedio por archivo | 56.66 MB | 69.39 MB |
| Tamaño total | 226.64 MB | 208.17 MB |
| Filas | 11,198,151 | 11,198,151 |

- Solo se compactó enero, la única partición con más de un archivo: el de 75.3 MB que dejó el MERGE y el de 11 KB con las 100 filas del append. Febrero y marzo ya tenían un único archivo y se omitieron (`totalFilesSkipped: 2`).
- La tabla tenía pocos archivos porque `write_deltalake` escribe un archivo por partición. El problema de los archivos pequeños aparece con muchas escrituras pequeñas (ingesta continua, MERGE frecuentes), y esta tabla solo tuvo una.
- El tamaño total bajó un 8 % porque OPTIMIZE reescribe en **zstd**, mientras que las escrituras anteriores usaron snappy, el códec por defecto de `write_deltalake`. Enero pasó de 75.3 MB a 56.9 MB con las mismas filas.
- OPTIMIZE no borra nada del disco: los 2 archivos reemplazados quedan retirados en el log y la versión 2 se sigue pudiendo leer. Después de OPTIMIZE había 6 Parquet en disco para 3 archivos activos.

### VACUUM: limpieza de archivos retirados

VACUUM borra los archivos que ninguna versión reciente usa: los que una versión retiró (`remove` en el log) hace más tiempo que la retención. Primero se hizo un *dry run* (`dry_run=True`) y luego el borrado, comprobando el disco antes y después.

| Archivo retirado | Tamaño | Retirado por | Hace | Con 168 h | Con 0 h |
|---|---:|---|---:|:---:|:---:|
| Enero original (versión 0) | 68.8 MB | v1, `MERGE` | 288 h | Se borra | Se borra |
| Enero después del MERGE | 75.3 MB | v3, `OPTIMIZE` | 0 h | Se conserva | Se borra |
| Lote del append (100 filas) | 0.01 MB | v3, `OPTIMIZE` | 0 h | Se conserva | Se borra |

- **Con la retención por defecto (168 h = 7 días)** el *dry run* encontró **1 archivo**: el que el MERGE retiró el 8 de octubre. Los dos que OPTIMIZE acababa de retirar eran demasiado recientes.
- **Para la demo se usó una retención de 0 h** (`enforce_retention_duration=False`), algo aceptable solo en esta copia de práctica. VACUUM borró **3 archivos y liberó 144.17 MB**: el disco pasó de 6 a 3 Parquet, que son justo los 3 archivos activos.
- La verificación salió bien: lo borrado coincide con el *dry run* y con lo que desapareció del disco, y la tabla sigue leyendo sus 11,198,151 filas.
- El costo: **la versión 0 ya no se puede leer**. El *time travel* depende de que los archivos antiguos sigan en disco, por eso el informe de *time travel* se guardó antes.
- En delta-rs, VACUUM también queda en el historial, como dos commits (`VACUUM START` y `VACUUM END`, versiones 4 y 5).

### Aprendizajes

- **El log manda.** Cada operación agrega un commit JSON a `_delta_log` con acciones `add` y `remove`, y la versión actual es el conjunto de archivos con `add` y sin `remove` posterior. Contar archivos en disco no da el estado de la tabla: después de OPTIMIZE había 6 Parquet y solo 3 activos.
- **Delta no modifica archivos, los reemplaza.** Para corregir 350 filas, el MERGE reescribió el archivo completo de enero (3,475,284 filas). Incluir la partición en la condición del MERGE (`t.year = ... AND t.month = ...`) evitó leer y reescribir los otros dos meses (`num_target_files_skipped_during_scan: 3`).
- **OPTIMIZE y VACUUM van juntos.** OPTIMIZE mejora la lectura, pero ocupa más disco hasta que VACUUM borra los archivos reemplazados. VACUUM libera espacio, pero recorta el *time travel* hasta la ventana de retención.
- **La retención es la ventana de *time travel*.** Con 168 h se puede volver hasta 7 días atrás; con 0 h, solo a la versión actual. Una retención corta en producción también puede romper lecturas o escrituras largas que aún usen archivos antiguos.
- **La evolución de esquema es barata.** Agregar una columna con `schema_mode='merge'` no reescribió ningún archivo: las filas anteriores la devuelven como `null` al leerlas.
- **El formato pesa más que el códec.** En el benchmark, pasar de CSV a Parquet cambió los tiempos de lectura en un orden de magnitud; cambiar de códec dentro de Parquet, mucho menos.

### Problemas y soluciones

| Problema | Solución |
|---|---|
| El MERGE falla si una fila destino coincide con varias filas de la fuente, y los datos de TLC tienen claves (`VendorID`, recogida, origen, destino) repetidas. | Validar la fuente con `check_unique_key` antes del MERGE y corregir solo filas cuya clave es única en la partición. La comparación de *time travel* también excluye las claves repetidas. |
| Filtrar con `is_null()` en el dataset de pyarrow daba resultados erróneos en los archivos escritos antes de agregar la columna. | Leer la columna y filtrar en memoria. |
| En el benchmark con un CSV de origen, el tiempo de escritura lo dominaba leer el CSV y ocultaba las diferencias entre códecs. | Usar siempre un Parquet como origen del benchmark. |
| `get_add_actions()` devuelve en delta-rs 1.x una tabla de `arro3`, no de pyarrow, y no tiene `to_pydict()`. | Convertirla con `pa.table(...)`. |
| Con la retención por defecto, VACUUM no podía borrar los archivos que OPTIMIZE acababa de retirar. | Retención de 0 h solo en la copia de práctica, documentada en el informe, con un *dry run* previo. |
| VACUUM es irreversible y rompe el *time travel*. | Copia de la tabla antes de ejecutarlo y conservar en `docs/` los resultados que dependían de versiones antiguas. |
