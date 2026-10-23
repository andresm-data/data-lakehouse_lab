"""Versiones de Spark, Delta e Iceberg que usa el proyecto, en un solo lugar.

Las de PySpark y delta-spark también se fijan en `pyproject.toml`, porque
Poetry las instala. `check_installed()` falla si lo instalado no coincide
con estas constantes, así que las dos fuentes no pueden separarse sin que
se note. Los jars de Iceberg y SQLite JDBC no vienen de PyPI: se descargan
de Maven con las coordenadas que se arman aquí.
"""
from importlib.metadata import PackageNotFoundError, version


# =============================================================================
SPARK_VERSION = '4.1.3'
SCALA_BINARY = '2.13'
DELTA_VERSION = '4.4.1'
ICEBERG_VERSION = '1.12.0'
SQLITE_JDBC_VERSION = '3.53.4.0'

# Versiones mayores del JDK que soporta Spark 4.1.
SUPPORTED_JAVA = ('17', '21')

# El runtime de Iceberg se publica por versión de Spark (major.minor) y de
# Scala. Spark 4.2 todavía no tiene runtime, por eso el proyecto usa 4.1.
SPARK_MINOR = '.'.join(SPARK_VERSION.split('.')[:2])

ICEBERG_PACKAGE = (
    f'org.apache.iceberg:iceberg-spark-runtime-{SPARK_MINOR}_{SCALA_BINARY}:'
    f'{ICEBERG_VERSION}'
)
SQLITE_JDBC_PACKAGE = f'org.xerial:sqlite-jdbc:{SQLITE_JDBC_VERSION}'

PYTHON_PACKAGES = {
    'pyspark': SPARK_VERSION,
    'delta-spark': DELTA_VERSION,
}


# =============================================================================
def java_major(java_version: str) -> str:
    """Versión mayor del JDK: `'21.0.12'` -> `'21'`, `'1.8.0_402'` -> `'8'`."""
    parts = java_version.split('.')

    return parts[1] if parts[0] == '1' else parts[0]


# =============================================================================
def check_installed() -> list[str]:
    """Paquetes de Python instalados que no coinciden con las constantes.

    Returns:
        list[str]: Un mensaje por paquete distinto o ausente; vacía si todo
            coincide.
    """
    problems = []

    for package, expected in PYTHON_PACKAGES.items():
        try:
            installed = version(package)

        except PackageNotFoundError:
            problems.append(f'{package} no está instalado (se espera {expected})')
            continue

        if installed != expected:
            problems.append(
                f'{package} {installed} instalado, se espera {expected}'
            )

    return problems
