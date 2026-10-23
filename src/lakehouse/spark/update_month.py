"""Actualiza un mes completo de taxis en Delta e Iceberg con Spark y lo mide."""
import argparse
import json
import math
import sys
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

import pyarrow as pa
import pyarrow.parquet as pq
from deltalake import DeltaTable
from pyiceberg.catalog import Catalog
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from lakehouse.iceberg.catalog import CATALOG_DB, WAREHOUSE, get_catalog
from lakehouse.iceberg.create_table import IDENTIFIER, PARTITION_NAME
from lakehouse.spark.read_write import spark_name
from lakehouse.spark.spark_session import build_spark_session, versions


# =============================================================================
DELTA_TABLE = Path('data/delta/yellow_tripdata_pyarrow')
ICEBERG_TABLE = IDENTIFIER
MONTH = '2025-02'
REPORT_DIR = Path('docs/spark')
DATE_COLUMN = 'tpep_pickup_datetime'

# En los viajes sin tarifa registrada (payment_type 0) estos cargos vienen
# nulos. La actualización los deja en 0.0, así que es idempotente.
FEE_COLUMNS = ['congestion_surcharge', 'Airport_fee']


# =============================================================================
def parse_month(text: str) -> tuple[int, int]:
    """Convierte `AAAA-MM` en (`year`, `month`).

    Raises:
        ValueError: Si el texto no es un mes válido.
    """
    parsed = datetime.strptime(text, '%Y-%m')

    return parsed.year, parsed.month


# =============================================================================
def month_bounds(year: int, month: int) -> tuple[datetime, datetime]:
    """Inicio del mes y del mes siguiente, para filtrar `[inicio, fin)`."""
    start = datetime(year, month, 1)
    end = datetime(year + month // 12, month % 12 + 1, 1)

    return start, end


# =============================================================================
def fill_fees(df: DataFrame) -> DataFrame:
    """Pone en 0.0 los cargos nulos de `FEE_COLUMNS`; el resto no cambia."""
    return df.fillna(0.0, subset=FEE_COLUMNS)


# =============================================================================
def count_null_fees(df: DataFrame) -> int:
    """Filas con algún cargo de `FEE_COLUMNS` nulo."""
    condition = F.lit(False)

    for column in FEE_COLUMNS:
        condition = condition | F.col(column).isNull()

    return df.filter(condition).count()


# =============================================================================
def dir_size(path: Path) -> dict[str, int]:
    """Archivos y bytes en disco bajo `path`, incluidos los ya retirados."""
    files = [p for p in path.rglob('*') if p.is_file()] if path.exists() else []

    return {'archivos': len(files), 'bytes': sum(p.stat().st_size for p in files)}


# =============================================================================
def summarize(files: list[dict], year: int, month: int) -> dict:
    """Resume los archivos de datos activos de una tabla.

    Args:
        files (list[dict]): Un dict por archivo con `path`, `bytes`, `filas`
            y `mes` (`(year, month)`).
        year (int): Año del mes actualizado.
        month (int): Mes actualizado.

    Returns:
        dict: `rutas` del mes, `archivos_mes`, `bytes_mes`, `filas_mes`,
            `filas_por_mes` (`'AAAA-MM'` -> filas), `archivos_total` y
            `bytes_total`.
    """
    target = [f for f in files if f['mes'] == (year, month)]
    per_month: dict[str, int] = {}

    for f in files:
        key = f'{f["mes"][0]}-{f["mes"][1]:02d}'
        per_month[key] = per_month.get(key, 0) + f['filas']

    return {
        'rutas': sorted(f['path'] for f in target),
        'archivos_mes': len(target),
        'bytes_mes': sum(f['bytes'] for f in target),
        'filas_mes': sum(f['filas'] for f in target),
        'filas_por_mes': dict(sorted(per_month.items())),
        'archivos_total': len(files),
        'bytes_total': sum(f['bytes'] for f in files),
    }


# =============================================================================
def measure(before: dict, after: dict, year: int, month: int) -> dict:
    """Compara el estado antes y después de reescribir el mes.

    Args:
        before (dict): Estado previo (`*_state`).
        after (dict): Estado posterior (`*_state`).
        year (int): Año del mes actualizado.
        month (int): Mes actualizado.

    Returns:
        dict: Archivos y bytes agregados y retirados, tamaño del mes y de la
            tabla, crecimiento en disco y `otros_meses_ok`.
    """
    old, new = set(before['rutas']), set(after['rutas'])
    key = f'{year}-{month:02d}'
    others_before = {k: v for k, v in before['filas_por_mes'].items() if k != key}
    others_after = {k: v for k, v in after['filas_por_mes'].items() if k != key}

    return {
        'archivos_mes_antes': before['archivos_mes'],
        'archivos_mes_despues': after['archivos_mes'],
        'bytes_mes_antes': before['bytes_mes'],
        'bytes_mes_despues': after['bytes_mes'],
        'archivos_agregados': len(new - old),
        'archivos_retirados': len(old - new),
        'filas_mes_antes': before['filas_mes'],
        'filas_mes_despues': after['filas_mes'],
        'bytes_activos_antes': before['bytes_total'],
        'bytes_activos_despues': after['bytes_total'],
        'disco_antes': before['disco']['bytes'],
        'disco_despues': after['disco']['bytes'],
        'metadatos_nuevos': (after['metadatos']['archivos']
                             - before['metadatos']['archivos']),
        'bytes_metadatos_nuevos': (after['metadatos']['bytes']
                                   - before['metadatos']['bytes']),
        'otros_meses_ok': others_before == others_after,
    }


# =============================================================================
def parquet_codec(path: Path) -> str:
    """Compresión de la primera columna del primer row group de un Parquet."""
    return pq.ParquetFile(path).metadata.row_group(0).column(0).compression


# =============================================================================
def delta_state(path: Path, year: int, month: int) -> dict:
    """Versión, archivos activos y tamaño en disco de una tabla Delta.

    Los archivos activos salen del log con `deltalake`, no del disco: los
    retirados siguen en el directorio hasta un VACUUM.
    """
    table = DeltaTable(path)
    # `deltalake` devuelve una tabla de arro3; pyarrow la lee sin copiar.
    actions = pa.table(table.get_add_actions(flatten=True)).to_pylist()
    files = [
        {'path': a['path'], 'bytes': a['size_bytes'], 'filas': a['num_records'],
         'mes': (int(a['partition.year']), int(a['partition.month']))}
        for a in actions
    ]

    return {
        'version': table.version(),
        **summarize(files, year, month),
        'disco': dir_size(path),
        'metadatos': dir_size(path / '_delta_log'),
    }


# =============================================================================
def read_delta_month(spark: SparkSession, path: Path, year: int,
                     month: int) -> DataFrame:
    """Lee un mes de la tabla Delta; el filtro poda las particiones."""
    return (
        spark.read.format('delta').load(str(path.resolve()))
        .where((F.col('year') == year) & (F.col('month') == month))
    )


# =============================================================================
def update_delta(spark: SparkSession, path: Path, year: int,
                 month: int) -> dict:
    """Reescribe un mes de la tabla Delta con `replaceWhere`.

    Lee el mes, rellena los cargos nulos y reemplaza solo esa partición.
    Delta comprueba que todas las filas escritas cumplan el predicado de
    `replaceWhere`, así que no puede tocar otros meses por error.

    Args:
        spark (SparkSession): Sesión con Delta.
        path (Path): Directorio de la tabla Delta.
        year (int): Año del mes.
        month (int): Mes.

    Returns:
        dict: Tiempo, versiones, operación, nulos antes y después y las
            medidas de `measure`.
    """
    before = delta_state(path, year, month)
    df = read_delta_month(spark, path, year, month)
    nulls_before = count_null_fees(df)

    start = time.perf_counter()
    (
        fill_fees(df).write.format('delta').mode('overwrite')
        .option('replaceWhere', f'year = {year} AND month = {month}')
        .save(str(path.resolve()))
    )
    seconds = time.perf_counter() - start

    after = delta_state(path, year, month)
    commit = DeltaTable(path).history(1)[0]
    codec = parquet_codec(path / after['rutas'][0]) if after['rutas'] else None

    return {
        'formato': 'Delta',
        'tabla': str(path),
        'segundos': round(seconds, 2),
        'version_antes': before['version'],
        'version_despues': after['version'],
        'operacion': commit['operation'],
        'compresion': codec,
        'nulos_antes': nulls_before,
        'nulos_despues': count_null_fees(read_delta_month(spark, path, year, month)),
        **measure(before, after, year, month),
    }


# =============================================================================
def month_from_ordinal(ordinal: int) -> tuple[int, int]:
    """(`year`, `month`) de una partición `month()` de Iceberg.

    Iceberg guarda el mes como meses transcurridos desde 1970-01.
    """
    return 1970 + ordinal // 12, ordinal % 12 + 1


# =============================================================================
def iceberg_state(catalog: Catalog, identifier: str, year: int,
                  month: int) -> dict:
    """Snapshot, archivos activos y tamaño en disco de una tabla Iceberg.

    Los archivos activos salen de los manifiestos del snapshot actual
    (`inspect.files`); los del snapshot anterior siguen en disco hasta
    expirar los snapshots.
    """
    table = catalog.load_table(identifier)
    snapshot = table.current_snapshot()
    rows = table.inspect.files().to_pylist() if snapshot else []
    files = [
        {'path': r['file_path'], 'bytes': r['file_size_in_bytes'],
         'filas': r['record_count'],
         'mes': month_from_ordinal(r['partition'][PARTITION_NAME])}
        for r in rows if r['content'] == 0
    ]
    location = Path(urlparse(table.location()).path)

    return {
        'version': snapshot.snapshot_id if snapshot else None,
        'resumen': dict(snapshot.summary.additional_properties) if snapshot else {},
        'operacion': snapshot.summary.operation.value if snapshot else None,
        **summarize(files, year, month),
        'disco': dir_size(location),
        'metadatos': dir_size(location / 'metadata'),
    }


# =============================================================================
def month_condition(year: int, month: int) -> str:
    """Filtro SQL `[inicio, fin)` sobre la fecha de recogida.

    Se usan literales `TIMESTAMP_NTZ` porque la columna no tiene zona; así
    Iceberg traduce el filtro a la partición `month()` sin conversiones.
    """
    start, end = month_bounds(year, month)

    return (f"{DATE_COLUMN} >= TIMESTAMP_NTZ '{start:%Y-%m-%d %H:%M:%S}' "
            f"AND {DATE_COLUMN} < TIMESTAMP_NTZ '{end:%Y-%m-%d %H:%M:%S}'")


# =============================================================================
def read_iceberg_month(spark: SparkSession, identifier: str, year: int,
                       month: int) -> DataFrame:
    """Lee un mes de la tabla Iceberg; el filtro poda por la partición."""
    return spark.table(spark_name(identifier)).where(month_condition(year, month))


# =============================================================================
def update_iceberg(spark: SparkSession, catalog: Catalog, identifier: str,
                   year: int, month: int) -> dict:
    """Reescribe un mes de la tabla Iceberg con `overwrite(condición)`.

    Lee el mes, rellena los cargos nulos y reemplaza los archivos que cumplen
    la condición. Como la condición cubre meses completos, Iceberg la
    resuelve con la partición `month()` y retira archivos enteros
    (copy-on-write), sin archivos de borrado.

    Args:
        spark (SparkSession): Sesión con el catálogo Iceberg.
        catalog (Catalog): Catálogo de PyIceberg (el mismo SQLite).
        identifier (str): Tabla `namespace.tabla`.
        year (int): Año del mes.
        month (int): Mes.

    Returns:
        dict: Tiempo, snapshots, operación, nulos antes y después y las
            medidas de `measure`.
    """
    before = iceberg_state(catalog, identifier, year, month)
    df = read_iceberg_month(spark, identifier, year, month)
    nulls_before = count_null_fees(df)

    start = time.perf_counter()
    fill_fees(df).writeTo(spark_name(identifier)).overwrite(
        F.expr(month_condition(year, month))
    )
    seconds = time.perf_counter() - start

    after = iceberg_state(catalog, identifier, year, month)
    codec = (parquet_codec(Path(urlparse(after['rutas'][0]).path))
             if after['rutas'] else None)

    return {
        'formato': 'Iceberg',
        'tabla': spark_name(identifier),
        'segundos': round(seconds, 2),
        'version_antes': before['version'],
        'version_despues': after['version'],
        'operacion': after['operacion'],
        'compresion': codec,
        'nulos_antes': nulls_before,
        'nulos_despues': count_null_fees(
            read_iceberg_month(spark, identifier, year, month)
        ),
        **measure(before, after, year, month),
    }


# =============================================================================
def month_profile(df: DataFrame) -> dict:
    """Filas, sumas de importes y nulos de un mes, para comparar formatos."""
    nulls = [F.sum(F.col(c).isNull().cast('long')).alias(f'nulos_{c}')
             for c in FEE_COLUMNS]
    sums = [F.sum(c).alias(f'suma_{c}') for c in ['total_amount', *FEE_COLUMNS]]
    row = df.agg(F.count('*').alias('filas'), *sums, *nulls).first()

    return {k: (v if v is not None else 0) for k, v in row.asDict().items()}


# =============================================================================
def same_profile(a: dict, b: dict) -> bool:
    """`True` si dos perfiles coinciden; las sumas, con tolerancia relativa.

    Sumar millones de `double` en otro orden cambia los últimos decimales.
    """
    return a.keys() == b.keys() and all(
        math.isclose(a[k], b[k], rel_tol=1e-9) for k in a
    )


# =============================================================================
def compare(spark: SparkSession, path: Path, identifier: str, year: int,
            month: int) -> dict:
    """Perfila el mes actualizado en los dos formatos y los compara."""
    delta = month_profile(read_delta_month(spark, path, year, month))
    iceberg = month_profile(read_iceberg_month(spark, identifier, year, month))

    return {'Delta': delta, 'Iceberg': iceberg,
            'iguales': same_profile(delta, iceberg)}


# =============================================================================
def all_ok(results: list[dict], comparison: dict) -> bool:
    """Comprobaciones: mismas filas, sin nulos, otros meses intactos e iguales."""
    return comparison['iguales'] and all(
        r['filas_mes_antes'] == r['filas_mes_despues']
        and r['nulos_despues'] == 0 and r['otros_meses_ok']
        for r in results
    )


# =============================================================================
def _mb(n: int) -> str:
    return f'{n / 1024 ** 2:,.1f} MB'


def _mark(ok: bool) -> str:
    return 'OK' if ok else 'FALLA'


ROWS = [
    ('Tabla', lambda r: f'`{r["tabla"]}`'),
    ('Operación', lambda r: r['operacion']),
    ('Versión / snapshot antes', lambda r: r['version_antes']),
    ('Versión / snapshot después', lambda r: r['version_despues']),
    ('Tiempo de escritura', lambda r: f'{r["segundos"]:.2f} s'),
    ('Filas del mes', lambda r: f'{r["filas_mes_antes"]:,} → '
                                f'{r["filas_mes_despues"]:,}'),
    ('Filas con cargos nulos', lambda r: f'{r["nulos_antes"]:,} → '
                                         f'{r["nulos_despues"]:,}'),
    ('Archivos del mes', lambda r: f'{r["archivos_mes_antes"]} → '
                                   f'{r["archivos_mes_despues"]}'),
    ('Tamaño del mes', lambda r: f'{_mb(r["bytes_mes_antes"])} → '
                                 f'{_mb(r["bytes_mes_despues"])}'),
    ('Archivos de datos agregados / retirados',
     lambda r: f'{r["archivos_agregados"]} / {r["archivos_retirados"]}'),
    ('Compresión de los archivos nuevos', lambda r: r['compresion']),
    ('Archivos de metadatos nuevos',
     lambda r: f'{r["metadatos_nuevos"]} ({r["bytes_metadatos_nuevos"] / 1024:,.1f} KB)'),
    ('Datos activos de la tabla', lambda r: f'{_mb(r["bytes_activos_antes"])} → '
                                            f'{_mb(r["bytes_activos_despues"])}'),
    ('Tabla en disco (con archivos retirados)',
     lambda r: f'{_mb(r["disco_antes"])} → {_mb(r["disco_despues"])}'),
    ('Otros meses sin cambios', lambda r: _mark(r['otros_meses_ok'])),
]


# =============================================================================
def render(month: str, vers: dict[str, str], results: list[dict],
           comparison: dict) -> str:
    """Arma el informe en Markdown.

    Args:
        month (str): Mes actualizado (`AAAA-MM`).
        vers (dict[str, str]): Versiones de `versions()`.
        results (list[dict]): Resultado de `update_delta` y `update_iceberg`.
        comparison (dict): Resultado de `compare`.

    Returns:
        str: Contenido del informe.
    """
    labels = [r['formato'] for r in results]
    lines = [
        '# Actualización de un mes completo con Spark: Delta vs Iceberg',
        '',
        f'- Fecha: {datetime.now():%Y-%m-%d %H:%M:%S}',
        f'- Mes: {month}',
        f'- Cambio: cargos nulos (`{"`, `".join(FEE_COLUMNS)}`) en 0.0 y '
        'reescritura de todo el mes',
        f'- Resultado: {_mark(all_ok(results, comparison))}',
        '',
        '## Versiones',
        '',
        '| Componente | Versión |',
        '|---|---|',
        *(f'| {k} | {v} |' for k, v in vers.items()),
        '',
        '## Resultado por formato',
        '',
        f'| Medida | {" | ".join(labels)} |',
        '|---|' + '---|' * len(labels),
        *(f'| {name} | ' + ' | '.join(str(get(r)) for r in results) + ' |'
          for name, get in ROWS),
        '',
        'Los archivos activos salen del log de Delta y de los manifiestos de '
        'Iceberg. El tamaño en disco incluye los archivos retirados, que '
        'siguen ahí hasta un `VACUUM` (Delta) o hasta expirar los snapshots '
        '(Iceberg).',
        '',
        '## Contenido del mes después de actualizar',
        '',
        '| Medida | Delta | Iceberg |',
        '|---|---:|---:|',
    ]

    for key, value in comparison['Delta'].items():
        other = comparison['Iceberg'].get(key, 0)
        fmt = '{:,}' if isinstance(value, int) else '{:,.2f}'
        lines.append(f'| `{key}` | {fmt.format(value)} | {fmt.format(other)} |')

    lines += ['', f'- Mismo contenido en los dos formatos: '
                  f'{_mark(comparison["iguales"])}', '']

    return '\n'.join(lines)


# =============================================================================
def save_report(text: str, data: dict, out_dir: Path) -> Path:
    """Guarda el informe (`.md`) y los datos (`.json`) para el dashboard."""
    out_dir.mkdir(parents=True, exist_ok=True)
    stem = out_dir / f'update_month_{datetime.now():%Y%m%d_%H%M%S}'
    path = stem.with_suffix('.md')
    path.write_text(text)
    stem.with_suffix('.json').write_text(
        json.dumps(data, indent=2, ensure_ascii=False, default=str) + '\n'
    )

    return path


# =============================================================================
def main() -> int:
    """Actualiza el mes en Delta y en Iceberg, compara y guarda el informe.

    Returns:
        int: `0` si todas las comprobaciones pasan, `1` si alguna falla.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--mes', default=MONTH, help='Mes AAAA-MM')
    parser.add_argument('--delta', type=Path, default=DELTA_TABLE)
    parser.add_argument('--iceberg', default=ICEBERG_TABLE)
    parser.add_argument('--catalogo', type=Path, default=CATALOG_DB)
    parser.add_argument('--warehouse', type=Path, default=WAREHOUSE)
    parser.add_argument('--salida', type=Path, default=REPORT_DIR)
    args = parser.parse_args()
    year, month = parse_month(args.mes)

    spark = build_spark_session('lh-spark-update-month', args.catalogo,
                                args.warehouse)
    catalog = get_catalog(args.catalogo, args.warehouse)

    try:
        vers = versions(spark)
        results = [
            update_delta(spark, args.delta, year, month),
            update_iceberg(spark, catalog, args.iceberg, year, month),
        ]
        comparison = compare(spark, args.delta, args.iceberg, year, month)

    finally:
        spark.stop()

    for r in results:
        print(f'{r["formato"]}: {r["segundos"]:.2f} s, '
              f'{r["version_antes"]} -> {r["version_despues"]}, '
              f'{r["filas_mes_despues"]:,} filas, '
              f'nulos {r["nulos_antes"]:,} -> {r["nulos_despues"]:,}')

    print(f'Mismo contenido: {_mark(comparison["iguales"])}')

    data = {'mes': args.mes, 'versiones': vers, 'resultados': results,
            'comparacion': comparison}
    path = save_report(render(args.mes, vers, results, comparison), data,
                       args.salida)
    print(f'Informe -> {path}')

    if not all_ok(results, comparison):
        print('Error: alguna comprobación falló', file=sys.stderr)

        return 1

    return 0


# =============================================================================
if __name__ == "__main__":
    raise SystemExit(main())
