"""Versiones de Delta y snapshots de Iceberg para la pestaña de time travel.

Las dos tablas se describen con la misma forma: una fila por versión con
su número (Delta) o `snapshot_id` (Iceberg), la fecha del commit en UTC,
la operación y el total de filas según los metadatos. Así la pestaña no
necesita distinguir formatos.

Para consultar una versión anterior solo se leen `year`, `month`,
`total_amount` y `congestion_surcharge`, y se resumen por mes. Los nulos de
`congestion_surcharge` muestran la corrección de febrero que se hizo con
Spark en la semana 2: siguen en la versión anterior y no en la actual. En Delta, si VACUUM ya borró los
archivos de esa versión, la lectura falla y se devuelve `None`.
"""
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
from deltalake import DeltaTable
from pyiceberg.table import Table
from pyiceberg.table.snapshots import ancestors_of

from lakehouse.benchmarks.formats import DELTA, ICEBERG_TABLE
from lakehouse.delta.merge_table import TARGET as DELTA_DEMO
from lakehouse.iceberg.catalog import CATALOG_DB, WAREHOUSE, get_catalog
from lakehouse.iceberg.ops_demo import IDENTIFIER as ICEBERG_OPS


# =============================================================================
COLUMNS = ['year', 'month', 'total_amount', 'congestion_surcharge']
VERSION_COLUMNS = ['version', 'fecha', 'operacion', 'filas', 'actual', 'en_linea_actual']


# =============================================================================
@dataclass(frozen=True)
class Source:
    """Una tabla Delta (ruta) o Iceberg (identificador en el catálogo)."""
    name: str
    format_: str
    location: str
    note: str = ''
    catalog_db: Path = CATALOG_DB
    warehouse: Path = WAREHOUSE


TABLES = [
    Source('Delta · yellow_tripdata_pyarrow', 'delta', str(DELTA),
           'Tabla del benchmark. Febrero se reescribió con Spark en la semana 2.'),
    Source('Delta · delta_demo', 'delta', str(DELTA_DEMO),
           'Copia de práctica: MERGE, append, OPTIMIZE y VACUUM con retención de 0 h.'),
    Source('Iceberg · yellow_tripdata', 'iceberg', ICEBERG_TABLE,
           'Tabla del benchmark: un append por mes y el overwrite de febrero con Spark.'),
    Source('Iceberg · yellow_tripdata_ops', 'iceberg', ICEBERG_OPS,
           'Evolución de partición y rollback: el append duplicado quedó fuera de la rama.'),
]

# Comando que crea cada tabla, para el aviso cuando no existe.
COMMANDS = {
    str(DELTA): 'poetry run lh-delta-pyarrow',
    str(DELTA_DEMO): 'poetry run lh-delta-merge --reset',
    ICEBERG_TABLE: 'poetry run lh-iceberg-create',
    ICEBERG_OPS: 'poetry run lh-iceberg-ops',
}


# =============================================================================
def _utc(ms: int) -> datetime:
    """Milisegundos desde 1970 a fecha UTC sin zona horaria."""
    return datetime.fromtimestamp(ms / 1000, UTC).replace(tzinfo=None)


def _iceberg(source: Source) -> Table:
    return get_catalog(source.catalog_db, source.warehouse).load_table(source.location)


# =============================================================================
def exists(source: Source) -> bool:
    """Si la tabla existe, sin crear el catálogo ni directorios."""
    if source.format_ == 'delta':
        return (Path(source.location) / '_delta_log').is_dir()

    if not source.catalog_db.exists():
        return False

    return get_catalog(source.catalog_db, source.warehouse).table_exists(source.location)


# =============================================================================
def _delta_versions(path: str) -> pd.DataFrame:
    current = DeltaTable(path).version()
    rows = []

    for entry in sorted(DeltaTable(path).history(), key=lambda h: h['version']):
        mode = entry.get('operationParameters', {}).get('mode')
        actions = pa.table(DeltaTable(path, version=entry['version']).get_add_actions())
        rows.append({
            'version': entry['version'],
            'fecha': _utc(entry['timestamp']),
            'operacion': f'{entry["operation"]} ({mode})' if mode else entry['operation'],
            'filas': pc.sum(actions['num_records']).as_py() or 0,
            'actual': entry['version'] == current,
            'en_linea_actual': True,
        })

    return pd.DataFrame(rows, columns=VERSION_COLUMNS)


def _iceberg_versions(table: Table) -> pd.DataFrame:
    current = table.current_snapshot()
    line = {s.snapshot_id for s in ancestors_of(current, table.metadata)} if current else set()
    rows = [{
        'version': s.snapshot_id,
        'fecha': _utc(s.timestamp_ms),
        'operacion': s.summary.operation.value if s.summary else '',
        'filas': int(s.summary.get('total-records', 0)) if s.summary else 0,
        'actual': current is not None and s.snapshot_id == current.snapshot_id,
        'en_linea_actual': s.snapshot_id in line,
    } for s in sorted(table.snapshots(), key=lambda s: s.timestamp_ms)]

    return pd.DataFrame(rows, columns=VERSION_COLUMNS)


def versions(source: Source) -> pd.DataFrame:
    """Versiones (Delta) o snapshots (Iceberg), del más antiguo al actual.

    Returns:
        pd.DataFrame: Columnas `version` (número o `snapshot_id`), `fecha`
            (UTC), `operacion`, `filas` (total de la tabla en esa versión,
            según los metadatos), `actual` y `en_linea_actual` (si es la
            actual o una de sus antecesoras; en Iceberg, un snapshot
            deshecho con rollback sigue existiendo pero queda fuera).
    """
    if source.format_ == 'delta':
        return _delta_versions(source.location)

    return _iceberg_versions(_iceberg(source))


# =============================================================================
def as_of(source: Source, when: datetime) -> int | None:
    """Versión vigente en una fecha y hora (UTC), o `None` si es anterior a todas.

    En Delta es la última versión con commit hasta `when`. En Iceberg se usa
    el `snapshot_log`, que registra cuándo pasó a ser actual cada snapshot,
    así que respeta los rollbacks.
    """
    if source.format_ == 'delta':
        log = [(v, f) for v, f in zip(*(versions(source)[c] for c in ('version', 'fecha')))]
    else:
        log = [(e.snapshot_id, _utc(e.timestamp_ms)) for e in _iceberg(source).metadata.snapshot_log]

    valid = [v for v, f in log if f <= when]

    return int(valid[-1]) if valid else None


# =============================================================================
def _read(source: Source, version: int) -> pa.Table:
    if source.format_ == 'delta':
        dataset = DeltaTable(source.location, version=version).to_pyarrow_dataset()
        return dataset.to_table(columns=COLUMNS)

    return _iceberg(source).scan(snapshot_id=version, selected_fields=tuple(COLUMNS)).to_arrow()


def month_summary(source: Source, version: int) -> pd.DataFrame | None:
    """Viajes, `total_amount` y nulos de `congestion_surcharge` por mes.

    Returns:
        pd.DataFrame | None: Columnas `mes` (`AAAA-MM`), `viajes`, `total` y
            `nulos_congestion`,
            ordenadas por mes; `None` si los archivos de esa versión ya no
            están en disco (VACUUM en Delta).
    """
    try:
        table = _read(source, version)

    except FileNotFoundError:
        return None

    grouped = table.group_by(['year', 'month']).aggregate([
        ([], 'count_all'), ('total_amount', 'sum'),
        ('congestion_surcharge', 'count', pc.CountOptions(mode='only_null')),
    ]).to_pandas()
    df = pd.DataFrame({
        'mes': [f'{y}-{m:02d}' for y, m in zip(grouped['year'], grouped['month'])],
        'viajes': grouped['count_all'].astype('int64'),
        'total': grouped['total_amount_sum'],
        'nulos_congestion': grouped['congestion_surcharge_count'].astype('int64'),
    })

    return df.sort_values('mes', ignore_index=True)


# =============================================================================
def compare(before: pd.DataFrame, after: pd.DataFrame) -> pd.DataFrame:
    """Une el resumen de una versión con el de la actual, mes a mes.

    Un mes que solo está en una de las dos versiones aparece con 0 en la
    otra.

    Returns:
        pd.DataFrame: `mes` y, para `viajes`, `total` y `nulos_congestion`,
            una columna `_version` y otra `_actual`; además `diferencia`
            (viajes actual − versión).
    """
    df = before.merge(after, on='mes', how='outer', suffixes=('_version', '_actual'))
    counts = [f'{c}_{s}' for c in ('viajes', 'nulos_congestion') for s in ('version', 'actual')]
    df = df.fillna({c: 0 for c in counts} | {'total_version': 0.0, 'total_actual': 0.0})
    df[counts] = df[counts].astype('int64')
    df['diferencia'] = df['viajes_actual'] - df['viajes_version']

    return df[['mes', 'viajes_version', 'viajes_actual', 'diferencia',
               'total_version', 'total_actual',
               'nulos_congestion_version', 'nulos_congestion_actual']].sort_values('mes', ignore_index=True)
