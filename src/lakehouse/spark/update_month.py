"""Actualiza un mes completo de taxis en Delta e Iceberg con Spark y lo mide."""
import time
from datetime import datetime
from pathlib import Path

import pyarrow as pa
from deltalake import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


# =============================================================================
DELTA_TABLE = Path('data/delta/yellow_tripdata_pyarrow')
MONTH = '2025-02'
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

    return {
        'formato': 'Delta',
        'tabla': str(path),
        'segundos': round(seconds, 2),
        'version_antes': before['version'],
        'version_despues': after['version'],
        'operacion': commit['operation'],
        'nulos_antes': nulls_before,
        'nulos_despues': count_null_fees(read_delta_month(spark, path, year, month)),
        **measure(before, after, year, month),
    }
