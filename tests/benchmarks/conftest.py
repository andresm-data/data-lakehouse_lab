"""Fixtures comunes de los benchmarks: la misma tabla en los tres formatos."""
from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from deltalake import write_deltalake

from lakehouse.iceberg.catalog import ensure_namespace, get_catalog


# =============================================================================
def make_trips(n: int = 90) -> pa.Table:
    """Viajes de enero a marzo con las columnas que usan las consultas."""
    return pa.table({
        'tpep_pickup_datetime': pa.array(
            [datetime(2025, i % 3 + 1, i % 28 + 1, i % 24) for i in range(n)],
            pa.timestamp('us')
        ),
        'PULocationID': pa.array([i % 7 for i in range(n)], pa.int32()),
        'payment_type': pa.array([i % 3 for i in range(n)], pa.int64()),
        'trip_distance': [i * 0.5 for i in range(n)],
        'fare_amount': [float(i % 10) for i in range(n)],
        'tip_amount': [i * 0.1 for i in range(n)],
        'total_amount': [i * 1.25 for i in range(n)],
        'year': pa.array([2025] * n, pa.int32()),
        'month': pa.array([i % 3 + 1 for i in range(n)], pa.int32()),
    })


# =============================================================================
@pytest.fixture
def trips() -> pa.Table:
    """Tabla de viajes de prueba."""
    return make_trips()


# =============================================================================
@pytest.fixture
def locations(tmp_path: Path, trips: pa.Table) -> dict[str, str]:
    """La tabla escrita en Parquet, Delta e Iceberg, con sus ubicaciones."""
    parquet = tmp_path / 'trips.parquet'
    pq.write_table(trips, parquet, row_group_size=30)

    delta = tmp_path / 'delta'
    write_deltalake(delta, trips, partition_by=['year', 'month'])

    catalog = get_catalog(tmp_path / 'catalog.db', tmp_path / 'warehouse')
    ensure_namespace(catalog)
    table = catalog.create_table('lakehouse.trips', schema=trips.schema)
    table.append(trips)

    return {
        'parquet': str(parquet),
        'delta': str(delta),
        'iceberg': table.metadata_location,
    }
