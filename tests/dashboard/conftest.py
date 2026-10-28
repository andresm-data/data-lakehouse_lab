"""Fixtures del dashboard: una base de resultados con dos ejecuciones."""
from datetime import datetime
from pathlib import Path

import pytest

from lakehouse.benchmarks.formats import Measurement
from lakehouse.benchmarks.results_db import TableSize, save


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
