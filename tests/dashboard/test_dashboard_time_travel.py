"""Pruebas unitarias de lakehouse.dashboard.time_travel."""
from datetime import datetime, timedelta

import pytest

from lakehouse.dashboard import time_travel
from lakehouse.dashboard.time_travel import (
    Source, as_of, compare, exists, month_summary, versions,
)


# =============================================================================
def test_tablas_del_proyecto():
    formats = [s.format_ for s in time_travel.TABLES]

    assert formats == ['delta', 'delta', 'iceberg', 'iceberg']
    assert set(time_travel.COMMANDS) == {s.location for s in time_travel.TABLES}


# =============================================================================
class TestExists:

    def test_delta(self, delta_source, tmp_path):
        assert exists(delta_source)
        assert not exists(Source('x', 'delta', str(tmp_path / 'no_existe')))

    def test_iceberg(self, iceberg_source, tmp_path):
        source, _ = iceberg_source

        assert exists(source)
        assert not exists(Source('x', 'iceberg', 'lakehouse.otra',
                                 catalog_db=source.catalog_db, warehouse=source.warehouse))

    def test_iceberg_sin_catalogo_no_lo_crea(self, tmp_path):
        source = Source('x', 'iceberg', 'lakehouse.t', catalog_db=tmp_path / 'c.db',
                        warehouse=tmp_path / 'w')

        assert not exists(source)
        assert not source.catalog_db.exists()


# =============================================================================
class TestVersions:

    def test_delta(self, delta_source):
        df = versions(delta_source)

        assert list(df['version']) == [0, 1, 2]
        assert list(df['operacion']) == ['WRITE (ErrorIfExists)', 'WRITE (Append)', 'WRITE (Overwrite)']
        assert list(df['filas']) == [5, 6, 6]
        assert list(df['actual']) == [False, False, True]
        assert df['en_linea_actual'].all()
        assert df['fecha'].is_monotonic_increasing

    def test_iceberg_con_rollback(self, iceberg_source):
        source, ids = iceberg_source
        df = versions(source)

        assert list(df['version']) == ids
        assert list(df['operacion']) == ['append'] * 3
        assert list(df['filas']) == [2, 5, 6]
        # Después del rollback, el actual es el segundo y el tercero queda fuera de la rama.
        assert list(df['actual']) == [False, True, False]
        assert list(df['en_linea_actual']) == [True, True, False]

    def test_fechas_en_utc(self, delta_source):
        fecha = versions(delta_source)['fecha'].iloc[-1]
        now = datetime.now(time_travel.UTC).replace(tzinfo=None)

        assert abs(now - fecha) < timedelta(minutes=5)


# =============================================================================
class TestAsOf:

    def test_antes_de_la_primera_version(self, delta_source):
        assert as_of(delta_source, datetime(2000, 1, 1)) is None

    def test_delta(self, delta_source):
        fechas = versions(delta_source)['fecha']

        assert as_of(delta_source, fechas[0]) == 0
        assert as_of(delta_source, fechas[1] - timedelta(microseconds=1)) == 0
        assert as_of(delta_source, fechas[2] + timedelta(days=1)) == 2

    def test_iceberg_respeta_el_rollback(self, iceberg_source):
        source, ids = iceberg_source

        # Hoy el vigente es el segundo, no el tercero que se creó después.
        assert as_of(source, datetime(2100, 1, 1)) == ids[1]
        assert as_of(source, datetime(2000, 1, 1)) is None


# =============================================================================
class TestMonthSummary:

    def test_version_anterior_de_delta(self, delta_source):
        df = month_summary(delta_source, 0)

        assert list(df['mes']) == ['2025-01', '2025-02']
        assert list(df['viajes']) == [2, 3]
        assert list(df['total']) == pytest.approx([30, 50])
        assert list(df['nulos_congestion']) == [1, 2]

    def test_version_actual_de_delta(self, delta_source):
        df = month_summary(delta_source, 2)

        assert list(df['mes']) == ['2025-01', '2025-02', '2025-03']
        assert list(df['nulos_congestion']) == [1, 0, 0]  # febrero corregido

    def test_iceberg_lee_el_snapshot_pedido(self, iceberg_source):
        source, ids = iceberg_source

        assert list(month_summary(source, ids[0])['mes']) == ['2025-01']
        # El snapshot deshecho con rollback se sigue pudiendo leer.
        assert list(month_summary(source, ids[2])['viajes']) == [2, 3, 1]

    def test_delta_despues_de_vacuum(self, vacuumed_source):
        assert month_summary(vacuumed_source, 0) is None
        assert month_summary(vacuumed_source, 2) is not None


# =============================================================================
def test_compare(delta_source):
    df = compare(month_summary(delta_source, 0), month_summary(delta_source, 2))

    assert list(df['mes']) == ['2025-01', '2025-02', '2025-03']
    assert list(df['viajes_version']) == [2, 3, 0]
    assert list(df['viajes_actual']) == [2, 3, 1]
    assert list(df['diferencia']) == [0, 0, 1]
    assert list(df['total_version']) == pytest.approx([30, 50, 0])
    assert list(df['nulos_congestion_version']) == [1, 2, 0]
    assert list(df['nulos_congestion_actual']) == [1, 0, 0]
