"""Lee y escribe tablas Parquet, Delta e Iceberg con Spark y valida filas y
esquema."""
import argparse
import sys
from datetime import date, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.dataset as ds
from deltalake import DeltaTable
from pyiceberg.catalog import Catalog
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.pandas.types import from_arrow_schema
from pyspark.sql.types import (
    DateType, DoubleType, LongType, StringType, StructField, StructType,
)

from lakehouse.iceberg.catalog import (
    CATALOG_DB, CATALOG_NAME, NAMESPACE, WAREHOUSE, get_catalog,
)
from lakehouse.iceberg.create_table import IDENTIFIER
from lakehouse.spark.spark_session import build_spark_session, versions


# =============================================================================
PARQUET_SOURCE = Path('data/interim/yellow_tripdata.parquet')
PARQUET_TARGET = Path('data/parquet/spark_smoke')
DELTA_SOURCE = Path('data/delta/yellow_tripdata_pyarrow')
DELTA_TARGET = Path('data/delta/spark_smoke')
ICEBERG_TARGET = f'{NAMESPACE}.spark_smoke'
REPORT_DIR = Path('docs/spark')

SAMPLE_SCHEMA = StructType([
    StructField('id', LongType(), False),
    StructField('zona', StringType(), True),
    StructField('monto', DoubleType(), True),
    StructField('fecha', DateType(), True),
])
SAMPLE_ROWS = [
    (1, 'Manhattan', 12.5, date(2025, 1, 15)),
    (2, 'Queens', 30.0, date(2025, 2, 3)),
    (3, 'Brooklyn', 18.75, date(2025, 3, 21)),
]


# =============================================================================
def spark_name(identifier: str) -> str:
    """Nombre de una tabla de PyIceberg (`ns.tabla`) en Spark
    (`catalogo.ns.tabla`)."""
    return f'{CATALOG_NAME}.{identifier}'


# =============================================================================
def _fields(schema: StructType) -> list[tuple[str, str]]:
    """Nombre y tipo de cada columna, sin la nulabilidad."""
    return [(f.name, f.dataType.simpleString()) for f in schema.fields]


# =============================================================================
def check(df: DataFrame, expected_rows: int, expected_schema: pa.Schema) -> dict:
    """Compara filas y esquema de un DataFrame con los de otro motor.

    El esquema esperado viene de `deltalake` o `pyiceberg` (Arrow) y se
    traduce a tipos de Spark; los timestamps sin zona se leen como
    `timestamp_ntz`. Se comparan nombres, orden y tipos, no la nulabilidad.

    Args:
        df (DataFrame): Tabla leída con Spark.
        expected_rows (int): Filas según el otro motor.
        expected_schema (pa.Schema): Esquema según el otro motor.

    Returns:
        dict: `filas`, `filas_esperadas`, `columnas`, `filas_ok`,
            `esquema_ok` y `diferencias` (columnas que no coinciden).
    """
    actual = _fields(df.schema)
    expected = _fields(from_arrow_schema(expected_schema,
                                         prefer_timestamp_ntz=True))
    rows = df.count()

    return {
        'filas': rows,
        'filas_esperadas': expected_rows,
        'columnas': actual,
        'filas_ok': rows == expected_rows,
        'esquema_ok': actual == expected,
        'diferencias': sorted(set(actual) ^ set(expected)),
    }


# =============================================================================
def read_parquet(spark: SparkSession, path: Path) -> dict:
    """Lee un archivo o directorio Parquet con Spark y lo valida contra
    `pyarrow`."""
    df = spark.read.parquet(str(path.resolve()))
    dataset = ds.dataset(path, format='parquet')

    return check(df, dataset.count_rows(), dataset.schema)


# =============================================================================
def read_delta(spark: SparkSession, path: Path) -> dict:
    """Lee una tabla Delta con Spark y la valida contra `deltalake`."""
    df = spark.read.format('delta').load(str(path.resolve()))
    table = DeltaTable(path)
    expected = pa.schema(table.schema().to_arrow())

    return check(df, table.to_pyarrow_dataset().count_rows(), expected)


# =============================================================================
def read_iceberg(spark: SparkSession, catalog: Catalog, identifier: str) -> dict:
    """Lee una tabla Iceberg con Spark y la valida contra `pyiceberg`.

    Las filas esperadas salen de `total-records` del snapshot actual, que
    PyIceberg mantiene sin leer los datos.
    """
    df = spark.table(spark_name(identifier))
    table = catalog.load_table(identifier)
    snapshot = table.current_snapshot()
    rows = int(snapshot.summary['total-records']) if snapshot else 0

    return check(df, rows, table.schema().as_arrow())


# =============================================================================
def sample(spark: SparkSession) -> DataFrame:
    """DataFrame pequeño de prueba para las escrituras."""
    return spark.createDataFrame(SAMPLE_ROWS, SAMPLE_SCHEMA)


# =============================================================================
def write_parquet(df: DataFrame, path: Path) -> None:
    """Escribe (sobrescribe) el DataFrame como Parquet en el directorio
    `path`."""
    df.write.mode('overwrite').parquet(str(path.resolve()))


# =============================================================================
def write_delta(df: DataFrame, path: Path) -> None:
    """Escribe (sobrescribe) el DataFrame como tabla Delta en `path`."""
    df.write.format('delta').mode('overwrite').save(str(path.resolve()))


# =============================================================================
def write_iceberg(spark: SparkSession, df: DataFrame, identifier: str) -> None:
    """Crea o reemplaza la tabla Iceberg `identifier` con el DataFrame."""
    namespace = identifier.rsplit('.', 1)[0]
    spark.sql(f'CREATE NAMESPACE IF NOT EXISTS {spark_name(namespace)}')
    df.writeTo(spark_name(identifier)).using('iceberg').createOrReplace()


# =============================================================================
def _sorted_rows(rows) -> list[tuple]:
    """Filas como tuplas, ordenadas por la primera columna."""
    return sorted((tuple(r) for r in rows), key=lambda r: r[0])


# =============================================================================
def verify_write(written: DataFrame, spark_rows: list, other_rows: list) -> dict:
    """Compara lo escrito con lo releído por Spark y por el otro motor.

    Args:
        written (DataFrame): DataFrame que se escribió.
        spark_rows (list): Filas releídas con Spark.
        other_rows (list): Filas releídas con `deltalake` o `pyiceberg`.

    Returns:
        dict: `filas` escritas, `spark_ok` y `otro_motor_ok`.
    """
    expected = _sorted_rows(written.collect())

    return {
        'filas': len(expected),
        'spark_ok': _sorted_rows(spark_rows) == expected,
        'otro_motor_ok': _sorted_rows(other_rows) == expected,
    }


# =============================================================================
def roundtrip_parquet(spark: SparkSession, path: Path) -> dict:
    """Escribe la muestra en Parquet y la relee con Spark y con `pyarrow`."""
    df = sample(spark)
    write_parquet(df, path)
    spark_rows = spark.read.parquet(str(path.resolve())).collect()
    other = ds.dataset(path, format='parquet').to_table().to_pylist()

    return verify_write(df, spark_rows, [tuple(r.values()) for r in other])


# =============================================================================
def roundtrip_delta(spark: SparkSession, path: Path) -> dict:
    """Escribe la muestra en Delta y la relee con Spark y con `deltalake`."""
    df = sample(spark)
    write_delta(df, path)
    spark_rows = spark.read.format('delta').load(str(path.resolve())).collect()
    other = DeltaTable(path).to_pyarrow_table().to_pylist()

    return verify_write(df, spark_rows, [tuple(r.values()) for r in other])


# =============================================================================
def roundtrip_iceberg(spark: SparkSession, catalog: Catalog,
                      identifier: str) -> dict:
    """Escribe la muestra en Iceberg y la relee con Spark y con `pyiceberg`."""
    df = sample(spark)
    write_iceberg(spark, df, identifier)
    spark_rows = spark.table(spark_name(identifier)).collect()
    other = catalog.load_table(identifier).scan().to_arrow().to_pylist()

    return verify_write(df, spark_rows, [tuple(r.values()) for r in other])


# =============================================================================
def all_ok(results: dict[str, dict]) -> bool:
    """`True` si todas las comprobaciones (`*_ok`) de todos los pasos pasan."""
    return all(
        v for r in results.values() for k, v in r.items() if k.endswith('_ok')
    )


# =============================================================================
def _mark(ok: bool) -> str:
    return 'OK' if ok else 'FALLA'


# =============================================================================
READS = [('parquet_lectura', 'Parquet'), ('delta_lectura', 'Delta'),
         ('iceberg_lectura', 'Iceberg')]
WRITES = [('parquet_escritura', 'Parquet'), ('delta_escritura', 'Delta'),
          ('iceberg_escritura', 'Iceberg')]


# =============================================================================
def render(vers: dict[str, str], results: dict[str, dict],
           names: dict[str, str]) -> str:
    """Arma el informe en Markdown.

    Args:
        vers (dict[str, str]): Versiones de `versions()`.
        results (dict[str, dict]): Resultados por paso (`<formato>_lectura`
            y `<formato>_escritura`, con `parquet`, `delta` e `iceberg`).
        names (dict[str, str]): Tabla o ruta de cada paso.

    Returns:
        str: Contenido del informe.
    """
    lines = [
        '# Spark con Parquet, Delta e Iceberg: lectura y escritura',
        '',
        f'- Fecha: {datetime.now():%Y-%m-%d %H:%M:%S}',
        f'- Resultado: {_mark(all_ok(results))}',
        '',
        '## Versiones',
        '',
        '| Componente | Versión |',
        '|---|---|',
        *(f'| {k} | {v} |' for k, v in vers.items()),
        '',
        '## Lectura',
        '',
        '| Formato | Tabla | Filas Spark | Filas esperadas | Filas | Esquema |',
        '|---|---|---:|---:|:---:|:---:|',
    ]

    for key, label in READS:
        r = results[key]
        lines.append(
            f'| {label} | `{names[key]}` | {r["filas"]:,} | '
            f'{r["filas_esperadas"]:,} | {_mark(r["filas_ok"])} | '
            f'{_mark(r["esquema_ok"])} |'
        )

    lines += [
        '',
        'Las filas y el esquema esperados vienen de otro motor: `pyarrow` '
        'para Parquet, `deltalake` para Delta y `pyiceberg` (snapshot actual) '
        'para Iceberg.',
        '',
        '## Escritura',
        '',
        '| Formato | Tabla | Filas | Relectura Spark | Relectura otro motor |',
        '|---|---|---:|:---:|:---:|',
    ]

    for key, label in WRITES:
        r = results[key]
        lines.append(
            f'| {label} | `{names[key]}` | {r["filas"]} | '
            f'{_mark(r["spark_ok"])} | {_mark(r["otro_motor_ok"])} |'
        )

    lines += ['', '## Esquema leído', '']

    for key, label in READS:
        r = results[key]
        cols = ', '.join(f'`{n}: {t}`' for n, t in r['columnas'])
        lines.append(f'- **{label}**: {cols}')

        if r['diferencias']:
            lines.append(f'  - Diferencias: {r["diferencias"]}')

    lines.append('')

    return '\n'.join(lines)


# =============================================================================
def save_report(text: str, out_dir: Path) -> Path:
    """Guarda el informe como `read_write_<AAAAMMDD_HHMMSS>.md`."""
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f'read_write_{datetime.now():%Y%m%d_%H%M%S}.md'
    path.write_text(text)

    return path


# =============================================================================
def main() -> int:
    """Lee las tablas existentes, escribe las de prueba y guarda el informe.

    Returns:
        int: `0` si todas las comprobaciones pasan, `1` si alguna falla.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--catalogo', type=Path, default=CATALOG_DB)
    parser.add_argument('--warehouse', type=Path, default=WAREHOUSE)
    parser.add_argument('--parquet', type=Path, default=PARQUET_SOURCE)
    parser.add_argument('--delta', type=Path, default=DELTA_SOURCE)
    parser.add_argument('--iceberg', default=IDENTIFIER)
    parser.add_argument('--parquet-destino', type=Path,
                        default=PARQUET_TARGET)
    parser.add_argument('--delta-destino', type=Path, default=DELTA_TARGET)
    parser.add_argument('--iceberg-destino', default=ICEBERG_TARGET)
    parser.add_argument('--salida', type=Path, default=REPORT_DIR)
    args = parser.parse_args()

    spark = build_spark_session('lh-spark-read-write', args.catalogo,
                                args.warehouse)
    catalog = get_catalog(args.catalogo, args.warehouse)

    try:
        vers = versions(spark)
        results = {
            'parquet_lectura': read_parquet(spark, args.parquet),
            'delta_lectura': read_delta(spark, args.delta),
            'iceberg_lectura': read_iceberg(spark, catalog, args.iceberg),
            'parquet_escritura': roundtrip_parquet(spark,
                                                   args.parquet_destino),
            'delta_escritura': roundtrip_delta(spark, args.delta_destino),
            'iceberg_escritura': roundtrip_iceberg(
                spark, catalog, args.iceberg_destino
            ),
        }

    finally:
        spark.stop()

    names = {
        'parquet_lectura': str(args.parquet),
        'delta_lectura': str(args.delta),
        'iceberg_lectura': spark_name(args.iceberg),
        'parquet_escritura': str(args.parquet_destino),
        'delta_escritura': str(args.delta_destino),
        'iceberg_escritura': spark_name(args.iceberg_destino),
    }

    print('Versiones:')

    for k, v in vers.items():
        print(f'  {k}: {v}')

    for key, r in results.items():
        checks = ', '.join(f'{k}={_mark(v)}' for k, v in r.items()
                           if k.endswith('_ok'))
        print(f'{key} ({names[key]}): {checks}')

    path = save_report(render(vers, results, names), args.salida)
    print(f'Informe -> {path}')

    if not all_ok(results):
        print('Error: alguna comprobación falló', file=sys.stderr)

        return 1

    return 0


# =============================================================================
if __name__ == "__main__":
    raise SystemExit(main())
