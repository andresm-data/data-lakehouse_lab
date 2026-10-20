"""Fixtures comunes de las pruebas de Iceberg."""
from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from lakehouse.iceberg.catalog import ensure_namespace, get_catalog


# =============================================================================
def make_table(rows: list[tuple[int, int, int, int]]) -> pa.Table:
    """Tabla con `id`, fecha de recogida, `year` y `month` a partir de
    tuplas (`id`, año, mes, día)."""
    return pa.table({
        'id': pa.array([r[0] for r in rows], pa.int64()),
        'tpep_pickup_datetime': pa.array(
            [datetime(y, m, d, 12) for _, y, m, d in rows], pa.timestamp('us')
        ),
        'nombre': pa.array([f'viaje-{r[0]}' for r in rows], pa.large_string()),
        'year': pa.array([r[1] for r in rows], pa.int32()),
        'month': pa.array([r[2] for r in rows], pa.int32()),
    })


ROWS = [
    (1, 2025, 1, 5), (2, 2025, 1, 31), (3, 2025, 2, 1),
    (4, 2025, 2, 28), (5, 2025, 3, 1), (6, 2025, 3, 15), (7, 2025, 3, 31),
]


@pytest.fixture
def rows() -> list[tuple[int, int, int, int]]:
    """Filas del Parquet de origen."""
    return ROWS


@pytest.fixture(name='make_table')
def make_table_fixture():
    """Expone `make_table` a las pruebas."""
    return make_table


@pytest.fixture
def origin(tmp_path: Path) -> Path:
    """Parquet con varios row groups y tres meses."""
    path = tmp_path / 'yellow_tripdata.parquet'
    pq.write_table(make_table(ROWS), path, row_group_size=2)
    return path


@pytest.fixture
def catalog(tmp_path: Path):
    """Catálogo SQLite temporal con el namespace `lakehouse`."""
    cat = get_catalog(tmp_path / 'catalog.db', tmp_path / 'warehouse')
    ensure_namespace(cat)
    return cat
