# Versiones de Spark, Delta e Iceberg que funcionaron

Probado por primera vez el 2026-10-21 con `lh-spark-read-write` ([informe](read_write_20261021_155755.md)). El 2026-10-23 se fijaron las versiones exactas, se reconstruyó el entorno desde cero con `poetry install` y el mismo comando pasó con Parquet, Delta e Iceberg ([informe](read_write_20261023_172725.md)).

## Versiones finales

| Componente | Versión | Dónde se fija |
|---|---|---|
| Python | 3.14.7 | Sistema; `pyproject.toml` acepta `>3.11,<4.0` |
| Java (JDK) | OpenJDK 21.0.12 | Sistema; Spark 4.1 soporta 17 y 21 (`SUPPORTED_JAVA`) |
| Spark (PySpark) | 4.1.3 | `pyproject.toml`: `pyspark (==4.1.3)`; `versions.py`: `SPARK_VERSION` |
| Scala | 2.13.17 (binaria 2.13) | Viene con PySpark; `versions.py`: `SCALA_BINARY` |
| Delta (`delta-spark`) | 4.4.1 | `pyproject.toml`: `delta-spark (==4.4.1)`; `versions.py`: `DELTA_VERSION`; jar `io.delta:delta-spark_4.1_2.13:4.4.1` |
| Iceberg (runtime de Spark) | 1.12.0 | `versions.py`: `ICEBERG_VERSION`; jar `org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.12.0` |
| SQLite JDBC | 3.53.4.0 | `versions.py`: `SQLITE_JDBC_VERSION` |
| deltalake (delta-rs) | 1.6.6 | `pyproject.toml`: `deltalake (==1.6.6)` |
| PyIceberg | 0.12.0 (pyiceberg-core 0.10.1) | `pyproject.toml`: `pyiceberg[...] (==0.12.0)` |
| PyArrow | 25.0.1 | `pyproject.toml`: `pyarrow (==25.0.1)` |
| pytest / pytest-cov | 9.1.1 / 7.1.0 | `pyproject.toml`, grupo `dev` |
| Poetry | 2.4.1 | Sistema |
| DuckDB | — | No se usa todavía |

Las versiones de Spark, Delta e Iceberg viven en `src/lakehouse/versions.py`. `build_spark_session()` compara PySpark y delta-spark instalados con esas constantes antes de arrancar y falla con un mensaje claro si no coinciden; `tests/test_versions.py` comprueba que `pyproject.toml` fija las mismas versiones y que no tiene rangos.

## Por qué Spark 4.1 y no 4.2

- El proyecto tenía `pyspark 4.2.0`, pero Iceberg todavía no publica `iceberg-spark-runtime-4.2`: el más nuevo es `iceberg-spark-runtime-4.1_2.13:1.12.0`.
- El runtime de Iceberg se compila por versión de Spark (major.minor) y de Scala, así que hay que usar el que corresponde exactamente.
- `delta-spark 4.4.1` soporta Spark 4.0, 4.1 y 4.2 (publica un jar por versión: `delta-spark_4.1_2.13`, `delta-spark_4.2_2.13`). `configure_spark_with_delta_pip` elige el jar según el PySpark instalado.
- Spark 4.1 es, por tanto, la versión más nueva compatible con los dos formatos. Cuando salga el runtime de Iceberg para 4.2 se puede volver a subir: basta con cambiar `versions.py` y `pyproject.toml`, y los tests avisan si quedan distintos.

## Notas de configuración

- El jar de Iceberg no es un paquete de PyPI: se descarga de Maven con `spark.jars.packages`, igual que el de SQLite JDBC. Ninguno va en `pyproject.toml`.
- El catálogo Iceberg de Spark es `type=jdbc` sobre el mismo `data/iceberg/catalog.db` de PyIceberg, porque las tablas se crearon con un catálogo SQL. Con `type=hadoop` Spark no las encontraría.
- El catálogo de Spark se llama `lakehouse`, igual que en PyIceberg: el SQLite guarda las tablas por `catalog_name`. En Spark la tabla es `lakehouse.lakehouse.yellow_tripdata` (catálogo.namespace.tabla).
- PyIceberg crea el SQLite con la columna `iceberg_type` (esquema V1 de `JdbcCatalog`), por eso se configura `jdbc.schema-version=V1`.
