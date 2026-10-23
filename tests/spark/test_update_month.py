"""Pruebas unitarias de lakehouse.spark.update_month."""
from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pytest
from deltalake import DeltaTable, write_deltalake

from lakehouse.iceberg.create_table import create_table
from lakehouse.spark.update_month import (
    count_null_fees, delta_state, dir_size, fill_fees, iceberg_state, measure,
    month_bounds, month_condition, month_from_ordinal, parse_month,
    read_delta_month, read_iceberg_month, summarize, update_delta,
    update_iceberg,
)


# =============================================================================
def make_trips() -> pa.Table:
    """Viajes de tres meses; en febrero hay cargos nulos."""
    months = [1, 2, 2, 2, 3]
    pickups = [datetime(2025, m, 10 + i, 8) for i, m in enumerate(months)]

    return pa.table({
        'VendorID': pa.array([1, 2, 1, 2, 1], pa.int32()),
        'tpep_pickup_datetime': pa.array(pickups, pa.timestamp('us')),
        'total_amount': pa.array([10.0, 20.5, 7.25, 3.0, 9.0], pa.float64()),
        'congestion_surcharge': pa.array([2.5, None, 2.5, None, 0.0],
                                         pa.float64()),
        'Airport_fee': pa.array([0.0, None, 1.75, 0.0, None], pa.float64()),
        'year': pa.array([2025] * 5, pa.int32()),
        'month': pa.array(months, pa.int32()),
    })


# =============================================================================
@pytest.fixture
def delta_trips(lake, request) -> Path:
    """Tabla Delta particionada por año y mes, escrita con `deltalake`."""
    path = lake['root'] / 'delta_update' / request.node.name
    write_deltalake(path, make_trips(), partition_by=['year', 'month'])

    return path


# =============================================================================
@pytest.fixture
def iceberg_trips(catalog, request) -> str:
    """Tabla Iceberg con partición `month()`, creada y cargada con PyIceberg."""
    identifier = f'lakehouse.upd_{request.node.name}'
    data = make_trips()
    create_table(catalog, identifier, data.schema).append(data)

    return identifier


# =============================================================================
class TestHelpers:

    def test_parse_month(self):
        assert parse_month('2025-02') == (2025, 2)

    def test_parse_month_invalido(self):
        with pytest.raises(ValueError):
            parse_month('2025-13')

    def test_month_bounds(self):
        assert month_bounds(2025, 2) == (datetime(2025, 2, 1),
                                         datetime(2025, 3, 1))

    def test_month_bounds_diciembre(self):
        assert month_bounds(2024, 12) == (datetime(2024, 12, 1),
                                          datetime(2025, 1, 1))

    def test_fill_y_count(self, spark):
        df = spark.createDataFrame(make_trips())

        assert count_null_fees(df) == 3
        assert count_null_fees(fill_fees(df)) == 0
        assert fill_fees(df).count() == 5

    def test_dir_size(self, tmp_path):
        (tmp_path / 'a').mkdir()
        (tmp_path / 'a' / 'x.bin').write_bytes(b'123')
        (tmp_path / 'y.bin').write_bytes(b'45')

        assert dir_size(tmp_path) == {'archivos': 2, 'bytes': 5}
        assert dir_size(tmp_path / 'no_existe') == {'archivos': 0, 'bytes': 0}


# =============================================================================
def _state(paths: list[str], per_month: dict, disk: int, meta: int) -> dict:
    files = [{'path': p, 'bytes': 10, 'filas': 2, 'mes': (2025, 2)}
             for p in paths]
    state = summarize(files, 2025, 2)
    state['filas_por_mes'] = per_month
    state['disco'] = {'archivos': 0, 'bytes': disk}
    state['metadatos'] = {'archivos': meta, 'bytes': meta * 100}

    return state


class TestSummarizeMeasure:

    def test_summarize(self):
        files = [
            {'path': 'b', 'bytes': 5, 'filas': 2, 'mes': (2025, 2)},
            {'path': 'a', 'bytes': 7, 'filas': 3, 'mes': (2025, 2)},
            {'path': 'c', 'bytes': 1, 'filas': 4, 'mes': (2025, 1)},
        ]

        s = summarize(files, 2025, 2)

        assert s['rutas'] == ['a', 'b']
        assert (s['archivos_mes'], s['bytes_mes'], s['filas_mes']) == (2, 12, 5)
        assert s['filas_por_mes'] == {'2025-01': 4, '2025-02': 5}
        assert (s['archivos_total'], s['bytes_total']) == (3, 13)

    def test_measure(self):
        before = _state(['a', 'b'], {'2025-01': 4, '2025-02': 4}, 100, 1)
        after = _state(['c'], {'2025-01': 4, '2025-02': 2}, 130, 3)

        m = measure(before, after, 2025, 2)

        assert (m['archivos_agregados'], m['archivos_retirados']) == (1, 2)
        assert (m['disco_antes'], m['disco_despues']) == (100, 130)
        assert m['metadatos_nuevos'] == 2
        assert m['bytes_metadatos_nuevos'] == 200
        assert m['otros_meses_ok']

    def test_measure_detecta_otro_mes(self):
        before = _state(['a'], {'2025-01': 4, '2025-02': 2}, 0, 0)
        after = _state(['b'], {'2025-01': 3, '2025-02': 2}, 0, 0)

        assert not measure(before, after, 2025, 2)['otros_meses_ok']


# =============================================================================
class TestDelta:

    def test_state(self, delta_trips):
        s = delta_state(delta_trips, 2025, 2)

        assert s['version'] == 0
        assert s['filas_mes'] == 3
        assert s['filas_por_mes'] == {'2025-01': 1, '2025-02': 3, '2025-03': 1}
        assert s['disco']['bytes'] > s['bytes_total'] > 0
        assert s['metadatos']['archivos'] == 1

    def test_read_month(self, spark, delta_trips):
        assert read_delta_month(spark, delta_trips, 2025, 2).count() == 3

    def test_update(self, spark, delta_trips):
        r = update_delta(spark, delta_trips, 2025, 2)

        assert (r['version_antes'], r['version_despues']) == (0, 1)
        assert r['operacion'] == 'WRITE'
        assert (r['nulos_antes'], r['nulos_despues']) == (2, 0)
        assert r['filas_mes_antes'] == r['filas_mes_despues'] == 3
        assert r['archivos_retirados'] == 1 and r['archivos_agregados'] >= 1
        assert r['otros_meses_ok']
        assert r['disco_despues'] > r['disco_antes']
        assert r['segundos'] >= 0

        # El mes 3 conserva su nulo: solo cambió febrero.
        data = DeltaTable(delta_trips).to_pyarrow_table().to_pylist()
        march = [row for row in data if row['month'] == 3]
        assert march[0]['Airport_fee'] is None

    def test_update_idempotente(self, spark, delta_trips):
        update_delta(spark, delta_trips, 2025, 2)
        r = update_delta(spark, delta_trips, 2025, 2)

        assert r['version_despues'] == 2
        assert r['nulos_antes'] == 0
        assert r['filas_mes_despues'] == 3


# =============================================================================
class TestIceberg:

    def test_month_from_ordinal(self):
        assert month_from_ordinal(661) == (2025, 2)
        assert month_from_ordinal(0) == (1970, 1)

    def test_month_condition(self):
        text = month_condition(2025, 12)

        assert "TIMESTAMP_NTZ '2025-12-01 00:00:00'" in text
        assert "< TIMESTAMP_NTZ '2026-01-01 00:00:00'" in text

    def test_state(self, catalog, iceberg_trips):
        s = iceberg_state(catalog, iceberg_trips, 2025, 2)

        assert s['version'] is not None
        assert s['operacion'] == 'append'
        assert s['filas_por_mes'] == {'2025-01': 1, '2025-02': 3, '2025-03': 1}
        assert s['archivos_mes'] == 1
        assert s['disco']['bytes'] > s['bytes_total'] > 0

    def test_state_sin_snapshots(self, catalog):
        identifier = 'lakehouse.upd_vacia'
        create_table(catalog, identifier, make_trips().schema)

        s = iceberg_state(catalog, identifier, 2025, 2)

        assert s['version'] is None and s['archivos_total'] == 0

    def test_read_month(self, spark, iceberg_trips):
        assert read_iceberg_month(spark, iceberg_trips, 2025, 2).count() == 3

    def test_update(self, spark, catalog, iceberg_trips):
        r = update_iceberg(spark, catalog, iceberg_trips, 2025, 2)

        assert r['version_antes'] != r['version_despues']
        assert r['operacion'] == 'overwrite'
        assert r['tabla'] == f'lakehouse.{iceberg_trips}'
        assert (r['nulos_antes'], r['nulos_despues']) == (2, 0)
        assert r['filas_mes_antes'] == r['filas_mes_despues'] == 3
        assert r['archivos_retirados'] == 1 and r['archivos_agregados'] >= 1
        assert r['otros_meses_ok']
        assert r['metadatos_nuevos'] > 0

        rows = catalog.load_table(iceberg_trips).scan().to_arrow().to_pylist()
        march = [row for row in rows if row['month'] == 3]
        assert march[0]['Airport_fee'] is None

    def test_update_idempotente(self, spark, catalog, iceberg_trips):
        update_iceberg(spark, catalog, iceberg_trips, 2025, 2)
        r = update_iceberg(spark, catalog, iceberg_trips, 2025, 2)

        assert r['nulos_antes'] == 0
        assert r['filas_mes_despues'] == 3
        assert len(catalog.load_table(iceberg_trips).snapshots()) == 3
