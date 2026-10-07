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
