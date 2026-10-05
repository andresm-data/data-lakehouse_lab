# Lakehouse Lab: Delta Lake vs Apache Iceberg

Laboratorio práctico para explorar y medir las diferencias entre **Delta Lake** y **Apache Iceberg**, los dos formatos de tabla abiertos más usados en arquitecturas *lakehouse*. Con PySpark y los datos públicos de viajes de taxi de Nueva York (NYC TLC), el proyecto construye las mismas tablas en ambos formatos y compara su comportamiento en ingesta, consultas, actualizaciones, evolución de esquema, *time travel* y mantenimiento. El objetivo es obtener métricas reproducibles que sirvan para decidir qué formato conviene en cada caso.

## Tabla de contenido

- [Descripción](#descripción)
  - [Motivación](#motivación)
  - [Datos](#datos)
  - [Qué se compara](#qué-se-compara)
  - [Stack tecnológico](#stack-tecnológico)
- [Estructura](#estructura)

## Descripción

### Motivación

Delta Lake y Apache Iceberg resuelven el mismo problema: añadir transacciones ACID, control de versiones y gestión de metadatos sobre archivos Parquet en un *data lake*. Sin embargo, difieren en su diseño interno (log de transacciones frente a árbol de metadatos y *snapshots*), en cómo manejan particiones y en las operaciones de mantenimiento que requieren. Este proyecto busca medir esas diferencias con datos reales en lugar de quedarse en la comparación teórica.

### Datos

Se usan los registros de viajes de **taxis amarillos** publicados por la [NYC Taxi & Limousine Commission](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page):

- `yellow_tripdata_2025-01.parquet` a `yellow_tripdata_2025-03.parquet`: viajes de enero a marzo de 2025.
- `taxi_zone_lookup.csv`: catálogo de zonas de taxi (borough, zona y zona de servicio).

Los archivos crudos se guardan en `data/raw/` y no se versionan en el repositorio.

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
├── data/
│   └── raw/                  # Datos crudos de NYC TLC (no versionados)
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
