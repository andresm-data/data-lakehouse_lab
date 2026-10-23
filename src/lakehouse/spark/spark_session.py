"""Construye una SparkSession que lee y escribe tablas Delta e Iceberg."""
import platform
from importlib.metadata import version
from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from lakehouse.iceberg.catalog import CATALOG_DB, CATALOG_NAME, WAREHOUSE
from lakehouse.versions import (
    ICEBERG_PACKAGE, SQLITE_JDBC_PACKAGE, check_installed,
)


# =============================================================================
EXTENSIONS = ','.join([
    'io.delta.sql.DeltaSparkSessionExtension',
    'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions',
])


# =============================================================================
def iceberg_catalog_conf(name: str, db: Path, warehouse: Path) -> dict[str, str]:
    """Configuración de un catálogo Iceberg JDBC sobre el SQLite de PyIceberg.

    El catálogo de Spark debe ser del mismo tipo que el que creó las tablas:
    PyIceberg usa un catálogo SQL, que en Java es `JdbcCatalog`. Con un
    catálogo `hadoop`, Spark no encontraría las tablas. El nombre también
    debe coincidir, porque las filas del SQLite se guardan por `catalog_name`.

    Args:
        name (str): Nombre del catálogo en Spark (y en el SQLite).
        db (Path): Archivo SQLite del catálogo.
        warehouse (Path): Directorio de las tablas Iceberg.

    Returns:
        dict[str, str]: Claves `spark.sql.catalog.<name>*` y sus valores.
    """
    prefix = f'spark.sql.catalog.{name}'

    return {
        prefix: 'org.apache.iceberg.spark.SparkCatalog',
        f'{prefix}.type': 'jdbc',
        f'{prefix}.uri': f'jdbc:sqlite:{db.resolve()}',
        f'{prefix}.warehouse': warehouse.resolve().as_uri(),
        # PyIceberg crea las tablas del catálogo con la columna `iceberg_type`.
        f'{prefix}.jdbc.schema-version': 'V1',
    }


# =============================================================================
def build_spark_session(
    app_name: str = 'lakehouse',
    catalog_db: Path = CATALOG_DB,
    warehouse: Path = WAREHOUSE,
    master: str = 'local[*]',
    driver_memory: str = '4g',
) -> SparkSession:
    """Crea (o reutiliza) una SparkSession con Delta e Iceberg.

    - Activa las extensiones SQL de Delta e Iceberg.
    - `spark_catalog` pasa a ser `DeltaCatalog`, para leer y escribir Delta
      por ruta (`format('delta')`) o por nombre.
    - Registra el catálogo Iceberg `lakehouse`, el mismo que usa PyIceberg.

    Los jars se descargan de Maven la primera vez y quedan en `~/.ivy2*`.
    `configure_spark_with_delta_pip` agrega el jar de Delta que corresponde
    al paquete `delta-spark` instalado.

    Antes de arrancar compara PySpark y delta-spark con `lakehouse.versions`:
    un jar que no corresponde a la versión de Spark falla después con
    `ClassNotFoundException` o `NoSuchMethodError`, que es más difícil de
    rastrear.

    Args:
        app_name (str): Nombre de la aplicación.
        catalog_db (Path): SQLite del catálogo Iceberg; se crea si no existe.
        warehouse (Path): Directorio de las tablas Iceberg.
        master (str): URL del master de Spark.
        driver_memory (str): Memoria del driver.

    Returns:
        SparkSession: Sesión lista para usar.

    Raises:
        RuntimeError: Si las versiones instaladas no son las fijadas.
    """
    problems = check_installed()

    if problems:
        raise RuntimeError('Versiones incorrectas: ' + '; '.join(problems))

    catalog_db.parent.mkdir(parents=True, exist_ok=True)
    warehouse.mkdir(parents=True, exist_ok=True)

    builder = (
        SparkSession.builder
        .master(master)
        .appName(app_name)
        .config('spark.driver.memory', driver_memory)
        .config('spark.sql.session.timeZone', 'UTC')
        .config('spark.ui.showConsoleProgress', 'false')
        .config('spark.sql.extensions', EXTENSIONS)
        .config('spark.sql.catalog.spark_catalog',
                'org.apache.spark.sql.delta.catalog.DeltaCatalog')
    )

    for key, value in iceberg_catalog_conf(
        CATALOG_NAME, catalog_db, warehouse
    ).items():
        builder = builder.config(key, value)

    spark = configure_spark_with_delta_pip(
        builder, extra_packages=[ICEBERG_PACKAGE, SQLITE_JDBC_PACKAGE]
    ).getOrCreate()
    spark.sparkContext.setLogLevel('WARN')

    return spark


# =============================================================================
def versions(spark: SparkSession) -> dict[str, str]:
    """Versiones efectivas de la sesión: Spark, Scala, Java, Delta e Iceberg.

    Scala, Java e Iceberg se leen de la JVM, así que reflejan lo que
    realmente se cargó y no solo lo que se pidió.

    Returns:
        dict[str, str]: Componente -> versión.
    """
    jvm = spark.sparkContext._jvm

    return {
        'Spark': spark.version,
        'Scala': jvm.scala.util.Properties.versionNumberString(),
        'Java': jvm.java.lang.System.getProperty('java.version'),
        'Delta (delta-spark)': version('delta-spark'),
        'Iceberg (runtime)': jvm.org.apache.iceberg.IcebergBuild.version(),
        'Python': platform.python_version(),
    }
