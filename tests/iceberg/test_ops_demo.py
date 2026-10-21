"""Pruebas del script de demostración lakehouse.iceberg.ops_demo."""
import sys
from datetime import date

import pyarrow.parquet as pq
import pytest

from lakehouse.iceberg.create_table import create_table
from lakehouse.iceberg.ops_demo import IDENTIFIER, main, render, run


# =============================================================================
@pytest.fixture
def result(catalog, origin):
    """Tabla de la demo y el resultado de `run`."""
    table = create_table(catalog, IDENTIFIER, pq.read_schema(origin))
    return table, run(table, origin, date(2025, 2, 15), date(2025, 3, 15))


# =============================================================================
class TestRun:

    def test_antes_una_spec(self, result):
        _, r = result

        assert [s['spec_id'] for s in r['antes']['specs']] == [0]
        assert r['antes']['archivos'] == [{'spec_id': 0, 'archivos': 2, 'filas': 4}]

    def test_despues_dos_specs(self, result):
        _, r = result

        assert [(s['spec_id'], s['actual']) for s in r['despues']['specs']] == [
            (0, False), (1, True),
        ]
        assert r['despues']['archivos'] == [
            {'spec_id': 0, 'archivos': 2, 'filas': 4},
            {'spec_id': 1, 'archivos': 3, 'filas': 3},
        ]
        assert r['despues']['particiones'] == [
            {'spec_id': 0, 'particiones': 2, 'archivos': 2, 'filas': 4},
            {'spec_id': 1, 'particiones': 3, 'archivos': 3, 'filas': 3},
        ]

    def test_consulta_cruza_specs_y_coincide(self, result):
        _, r = result

        # 2025-02-28 (spec 0) y 2025-03-01 (spec 1).
        assert r['consulta']['filas'] == r['consulta']['esperadas'] == 2
        assert r['consulta']['archivos_por_spec'] == {0: 1, 1: 1}

    def test_rollback(self, result, rows):
        table, r = result
        rb = r['rollback']

        assert rb['filas_bueno'] == rb['filas_despues'] == len(rows)
        assert rb['filas_malo'] == rb['time_travel_malo'] == len(rows) + 3
        assert rb['actual'] == rb['bueno'] == table.current_snapshot().snapshot_id
        assert 'not an ancestor' in rb['error_adelante']
        malo = [s for s in rb['historial'] if s['snapshot_id'] == rb['malo']]
        assert malo[0]['en_linea_actual'] is False


# =============================================================================
class TestRenderAndMain:

    def test_render(self, result):
        table, r = result
        text = render(IDENTIFIER, table, r)

        assert '# Evolución de partición y rollback en Iceberg' in text
        assert '`day(tpep_pickup_datetime) -> pickup_day`' in text
        assert 'Filas en el Parquet de origen: 2 (coincide)' in text
        assert 'spec 0: 1, spec 1: 1' in text
        assert f'| {r["rollback"]["malo"]} |' in text

    def test_render_detecta_diferencia(self, result):
        table, r = result
        r['consulta']['esperadas'] = 99

        assert 'NO coincide' in render(IDENTIFIER, table, r)

    def test_main_guarda_informe(self, monkeypatch, capsys, tmp_path, origin):
        out_dir = tmp_path / 'informes'
        monkeypatch.setattr(sys, 'argv', [
            'lh-iceberg-ops', str(origin),
            '--catalogo', str(tmp_path / 'm.db'),
            '--warehouse', str(tmp_path / 'mwh'), '--salida', str(out_dir),
        ])

        assert main() == 0

        out = capsys.readouterr().out
        assert 'Despues: specs=[0, 1]' in out
        assert '2 filas (esperadas 2)' in out
        assert len(list(out_dir.glob('ops_*.md'))) == 1
