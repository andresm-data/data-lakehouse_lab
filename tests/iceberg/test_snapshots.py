"""Pruebas unitarias de lakehouse.iceberg.snapshots."""
import sys
from datetime import date

import pyarrow.parquet as pq
import pytest

from lakehouse.iceberg.create_table import IDENTIFIER, create_table, load
from lakehouse.iceberg.snapshots import (
    main, month_scan, partition_rows, render, snapshot_rows,
)


# =============================================================================
@pytest.fixture
def table(catalog, origin):
    """Tabla Iceberg cargada con un snapshot por mes."""
    t = create_table(catalog, IDENTIFIER, pq.read_schema(origin))
    load(t, origin)
    return t


# =============================================================================
class TestSnapshotRows:

    def test_cadena_de_snapshots(self, table):
        rows = snapshot_rows(table)

        assert len(rows) == 3
        assert rows[0]['parent_id'] is None
        assert rows[1]['parent_id'] == rows[0]['snapshot_id']
        assert [r['actual'] for r in rows] == [False, False, True]
        assert rows[-1]['resumen']['total-records'] == '7'
        assert {r['operacion'] for r in rows} == {'append'}

    def test_tabla_sin_snapshots(self, catalog, origin):
        empty = create_table(catalog, IDENTIFIER, pq.read_schema(origin))

        assert snapshot_rows(empty) == []


# =============================================================================
class TestPartitionRows:

    def test_filas_por_mes(self, table):
        assert partition_rows(table) == [
            {'particion': '2025-01', 'filas': 2, 'archivos': 1},
            {'particion': '2025-02', 'filas': 2, 'archivos': 1},
            {'particion': '2025-03', 'filas': 3, 'archivos': 1},
        ]


# =============================================================================
class TestMonthScan:

    def test_poda_por_columna_de_fecha(self, table):
        result = month_scan(table, date(2025, 2, 1), date(2025, 3, 1))

        assert result['archivos_leidos'] == 1
        assert result['archivos_totales'] == 3
        assert result['filas'] == 2

    def test_rango_de_dos_meses(self, table):
        result = month_scan(table, date(2025, 1, 15), date(2025, 3, 1))

        assert result['archivos_leidos'] == 2
        assert result['filas'] == 3


# =============================================================================
class TestRenderAndMain:

    def test_render(self, table):
        text = render(
            IDENTIFIER, table, snapshot_rows(table), partition_rows(table),
            month_scan(table, date(2025, 2, 1), date(2025, 3, 1)),
        )

        assert '# Snapshots y hidden partitioning en Iceberg' in text
        assert 'month(tpep_pickup_datetime) -> pickup_month' in text
        assert str(table.current_snapshot().snapshot_id) in text
        assert 'Archivos planificados: 1 de 3' in text

    def test_ubicacion_relativa(self, table, monkeypatch, tmp_path):
        from lakehouse.iceberg.snapshots import _location

        assert _location(table).startswith('/')
        monkeypatch.chdir(tmp_path)
        assert _location(table) == 'warehouse/lakehouse/yellow_tripdata'

    def test_main_guarda_informe(self, monkeypatch, capsys, tmp_path, table):
        out_dir = tmp_path / 'informes'
        monkeypatch.setattr(sys, 'argv', [
            'lh-iceberg-snapshots', '--catalogo', str(tmp_path / 'catalog.db'),
            '--warehouse', str(tmp_path / 'warehouse'), '--salida', str(out_dir),
        ])

        assert main() == 0

        out = capsys.readouterr().out
        assert '(actual)' in out
        assert '2 filas, 1 de 3 archivos' in out
        assert len(list(out_dir.glob('snapshots_*.md'))) == 1
