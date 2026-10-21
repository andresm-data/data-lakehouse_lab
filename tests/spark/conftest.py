"""Fixtures comunes de las pruebas de Spark.

Arrancar Spark tarda varios segundos, así que todas las pruebas comparten
una sola sesión, con su catálogo Iceberg y su warehouse temporales.
"""
from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pytest
from deltalake import write_deltalake

from lakehouse.iceberg.catalog import ensure_namespace, get_catalog
from lakehouse.spark.spark_session import build_spark_session


# =============================================================================
def make_table() -> pa.Table:
    """Tabla pequeña con un timestamp sin zona, como la de taxis."""
    return pa.table({
        'id': pa.array([1, 2, 3, 4], pa.int64()),
        'tpep_pickup_datetime': pa.array(
            [datetime(2025, m, 10, 8) for m in (1, 1, 2, 3)], pa.timestamp('us')
        ),
        'monto': pa.array([10.0, 20.5, 7.25, 3.0], pa.float64()),
        'zona': pa.array(['a', 'b', 'c', None], pa.large_string()),
    })


# =============================================================================
@pytest.fixture(scope='session')
def lake(tmp_path_factory) -> dict[str, Path]:
    """Rutas temporales del catálogo, el warehouse y la tabla Delta."""
    root = tmp_path_factory.mktemp('lake')

    return {
        'root': root,
        'db': root / 'catalog.db',
        'warehouse': root / 'warehouse',
        'delta': root / 'delta' / 'origen',
    }


# =============================================================================
@pytest.fixture(scope='session')
def catalog(lake):
    """Catálogo SQL de PyIceberg con el namespace `lakehouse`."""
    cat = get_catalog(lake['db'], lake['warehouse'])
    ensure_namespace(cat)

    return cat


# =============================================================================
@pytest.fixture(scope='session')
def spark(lake, catalog):
    """SparkSession apuntando al mismo catálogo que PyIceberg."""
    session = build_spark_session(
        'tests', lake['db'], lake['warehouse'],
        master='local[2]', driver_memory='1g',
    )
    yield session
    session.stop()


# =============================================================================
@pytest.fixture(scope='session')
def delta_source(lake) -> Path:
    """Tabla Delta escrita con `deltalake`, sin Spark."""
    write_deltalake(lake['delta'], make_table())

    return lake['delta']


# =============================================================================
@pytest.fixture(scope='session')
def iceberg_source(catalog) -> str:
    """Tabla Iceberg creada y cargada con PyIceberg, sin Spark."""
    identifier = 'lakehouse.origen'
    data = make_table()
    table = catalog.create_table(identifier, schema=data.schema)
    table.append(data)

    return identifier
