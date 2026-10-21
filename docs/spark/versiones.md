# Versiones de Spark, Delta e Iceberg que funcionaron

Probado el 2026-10-21 con `lh-spark-read-write` ([informe](read_write_20261021_155755.md)): Spark leyó las tablas Delta e Iceberg existentes y escribió y releyó una tabla de prueba en cada formato.

| Componente | Versión | Dónde se fija |
|---|---|---|
| Spark (PySpark) | 4.1.3 | `pyproject.toml`: `pyspark (>=4.1.1,<4.2.0)` |
| Scala | 2.13.17 (binaria 2.13) | Viene con PySpark; define el sufijo `_2.13` de los jars |
| Delta (`delta-spark`) | 4.4.1 | `pyproject.toml`: `delta-spark (>=4.4.1,<4.5.0)`; jar `io.delta:delta-spark_4.1_2.13:4.4.1` |
| Iceberg | 1.12.0 | `spark_session.py`: `org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.12.0` |
| SQLite JDBC | 3.53.4.0 | `spark_session.py`: `org.xerial:sqlite-jdbc:3.53.4.0` |
| Java | OpenJDK 21.0.12 | Sistema; en la semana 3 va al Dockerfile |
| Python | 3.14.7 | Sistema |

## Por qué Spark 4.1 y no 4.2

- El proyecto tenía `pyspark 4.2.0`, pero Iceberg todavía no publica `iceberg-spark-runtime-4.2`: el más nuevo es `iceberg-spark-runtime-4.1_2.13:1.12.0`.
- El runtime de Iceberg se compila por versión de Spark (major.minor) y de Scala, así que hay que usar el que corresponde exactamente.
- `delta-spark 4.4.1` soporta Spark 4.0, 4.1 y 4.2 (publica un jar por versión: `delta-spark_4.1_2.13`, `delta-spark_4.2_2.13`). `configure_spark_with_delta_pip` elige el jar según el PySpark instalado.
- Spark 4.1 es, por tanto, la versión más nueva compatible con los dos formatos. Cuando salga el runtime de Iceberg para 4.2 se puede volver a subir.

## Notas de configuración

- El jar de Iceberg no es un paquete de PyPI: se descarga de Maven con `spark.jars.packages`, igual que el de SQLite JDBC. Ninguno va en `pyproject.toml`.
- El catálogo Iceberg de Spark es `type=jdbc` sobre el mismo `data/iceberg/catalog.db` de PyIceberg, porque las tablas se crearon con un catálogo SQL. Con `type=hadoop` Spark no las encontraría.
- El catálogo de Spark se llama `lakehouse`, igual que en PyIceberg: el SQLite guarda las tablas por `catalog_name`. En Spark la tabla es `lakehouse.lakehouse.yellow_tripdata` (catálogo.namespace.tabla).
- PyIceberg crea el SQLite con la columna `iceberg_type` (esquema V1 de `JdbcCatalog`), por eso se configura `jdbc.schema-version=V1`.
