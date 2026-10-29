"""Fixtures del dashboard: una base de resultados con dos ejecuciones y
tablas Delta e Iceberg pequeñas con varias versiones."""
from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pytest
from deltalake import DeltaTable, write_deltalake

from lakehouse.benchmarks.formats import Measurement
from lakehouse.benchmarks.results_db import TableSize, save
from lakehouse.dashboard.time_travel import Source
from lakehouse.iceberg.catalog import ensure_namespace, get_catalog


# =============================================================================
OLD_RUN = datetime(2026, 10, 25, 9, 0)
RUN = datetime(2026, 10, 26, 10, 34, 47)
MEASUREMENTS = [
    Measurement('iceberg', 'q2', (0.150, 0.140, 0.160), 3),
    Measurement('parquet', 'q2', (0.060, 0.050, 0.070), 3),
    Measurement('delta', 'q2', (0.040, 0.030, 0.050), 3),
    Measurement('iceberg', 'q1', (0.002, 0.001, 0.003), 1),
    Measurement('parquet', 'q1', (0.018, 0.017, 0.019), 1),
    Measurement('delta', 'q1', (0.007, 0.006, 0.008), 1),
]
SIZES = [
    TableSize('iceberg', 5, 3 * 2**20, 4 * 2**20),
    TableSize('parquet', 1, 2 * 2**20, 2 * 2**20),
    TableSize('delta', 6, 2**20, 5 * 2**20),
]


# =============================================================================
@pytest.fixture
def db(tmp_path: Path) -> Path:
    """Base con una ejecución vieja (solo q1) y la actual completa."""
    path = tmp_path / 'resultados.duckdb'
    save(path, OLD_RUN, MEASUREMENTS[3:], SIZES)
    save(path, RUN, MEASUREMENTS, SIZES)

    return path


@pytest.fixture
def saved_runs() -> tuple:
    """Ejecuciones guardadas en `db`, de la más reciente a la más antigua."""
    return RUN, OLD_RUN


# =============================================================================
# Tablas pequeñas para la pestaña de time travel.
def trips(rows: list[tuple[int, int, float, float | None]]) -> pa.Table:
    """Tabla con `year`, `month`, `total_amount` y `congestion_surcharge`."""
    return pa.table({
        'year': pa.array([r[0] for r in rows], pa.int32()),
        'month': pa.array([r[1] for r in rows], pa.int32()),
        'total_amount': pa.array([r[2] for r in rows], pa.float64()),
        'congestion_surcharge': pa.array([r[3] for r in rows], pa.float64()),
    })


JANUARY = [(2025, 1, 10.0, 2.5), (2025, 1, 20.0, None)]
FEBRUARY = [(2025, 2, 5.0, None), (2025, 2, 15.0, None), (2025, 2, 30.0, 2.5)]
FEBRUARY_FIXED = [(2025, 2, 5.0, 0.0), (2025, 2, 15.0, 0.0), (2025, 2, 30.0, 2.5)]
MARCH = [(2025, 3, 40.0, 2.5)]


@pytest.fixture
def delta_source(tmp_path: Path) -> Source:
    """Delta con tres versiones: enero y febrero, marzo, y febrero corregido."""
    path = tmp_path / 'delta'
    write_deltalake(path, trips(JANUARY + FEBRUARY), partition_by=['year', 'month'])
    write_deltalake(path, trips(MARCH), mode='append')
    write_deltalake(path, trips(FEBRUARY_FIXED), mode='overwrite',
                    predicate='year = 2025 AND month = 2')

    return Source('Delta · prueba', 'delta', str(path))


@pytest.fixture
def vacuumed_source(delta_source: Source) -> Source:
    """La misma tabla después de VACUUM con retención de 0 h."""
    DeltaTable(delta_source.location).vacuum(
        retention_hours=0, enforce_retention_duration=False, dry_run=False
    )

    return delta_source


@pytest.fixture
def iceberg_source(tmp_path: Path) -> tuple[Source, list[int]]:
    """Iceberg con enero, febrero y un marzo duplicado deshecho con rollback.

    Returns:
        tuple: La tabla y los `snapshot_id` en orden (el último es el
            deshecho).
    """
    source = Source('Iceberg · prueba', 'iceberg', 'lakehouse.prueba',
                    catalog_db=tmp_path / 'catalog.db', warehouse=tmp_path / 'warehouse')
    catalog = get_catalog(source.catalog_db, source.warehouse)
    ensure_namespace(catalog)
    table = catalog.create_table(source.location, schema=trips(JANUARY).schema)

    for rows in (JANUARY, FEBRUARY, MARCH):
        table.append(trips(rows))

    ids = [s.snapshot_id for s in table.snapshots()]
    table.manage_snapshots().rollback_to_snapshot(ids[1]).commit()

    return source, ids
