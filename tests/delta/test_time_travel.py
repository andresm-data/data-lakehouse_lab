"""Pruebas unitarias de lakehouse.delta.time_travel."""
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pytest
from deltalake import write_deltalake

from lakehouse.delta.merge_table import build_source, merge
from lakehouse.delta.time_travel import (
    compare, history_rows, main, render, save_report
)


# =============================================================================
START = datetime(2025, 1, 1)


def _table(n: int, month: int = 1) -> pa.Table:
    """`n` viajes con clave única; cada uno empieza un minuto después."""
    pickups = [START + timedelta(minutes=i) for i in range(n)]
    return pa.table({
        'VendorID': pa.array([1] * n, pa.int32()),
        'tpep_pickup_datetime': pa.array(pickups, pa.timestamp('us')),
        'tpep_dropoff_datetime': pa.array(
            [p + timedelta(minutes=10) for p in pickups], pa.timestamp('us')
        ),
        'PULocationID': pa.array([10] * n, pa.int32()),
        'DOLocationID': pa.array([20] * n, pa.int32()),
        'tip_amount': pa.array([2.1] * n, pa.float64()),
        'total_amount': pa.array([12.0] * n, pa.float64()),
        'year': pa.array([2025] * n, pa.int32()),
        'month': pa.array([month] * n, pa.int32()),
    })


@pytest.fixture
def table(tmp_path: Path) -> Path:
    """Tabla con versión 0 (20 viajes en enero, 5 en febrero) y un MERGE."""
    path = tmp_path / 'demo'
    write_deltalake(path, pa.concat_tables([_table(20), _table(5, month=2)]),
                    partition_by=['year', 'month'])
    merge(path, build_source(path, 2025, 1, 3, 2), 2025, 1)
    return path


# =============================================================================
# history_rows
# =============================================================================
class TestHistoryRows:

    def test_una_fila_por_version_en_orden(self, table):
        rows = history_rows(table)

        assert [r['version'] for r in rows] == [0, 1]
        assert [r['operacion'] for r in rows] == ['WRITE', 'MERGE']

    def test_solo_conserva_metricas_relevantes(self, table):
        merge_row = history_rows(table)[1]

        assert merge_row['metricas']['num_target_rows_updated'] == 3
        assert merge_row['metricas']['num_target_rows_inserted'] == 2
        assert 'execution_time_ms' not in merge_row['metricas']


# =============================================================================
# compare
# =============================================================================
class TestCompare:

    def test_detecta_filas_corregidas_e_insertadas(self, table):
        result = compare(table, 2025, 1)

        assert result['version'] == 1
        assert result['filas_v0'] == 25
        assert result['filas_actual'] == 27
        assert result['cambiadas']['tip_v0'].to_pylist() == [2.1] * 3
        assert result['insertadas'] == 2

    def test_otra_particion_sin_cambios(self, table):
        result = compare(table, 2025, 2)

        assert result['cambiadas'].num_rows == 0
        assert result['insertadas'] == 0

    def test_excluye_claves_repetidas(self, tmp_path):
        path = tmp_path / 'dup'
        write_deltalake(path, pa.concat_tables([_table(2), _table(2)]),
                        partition_by=['year', 'month'])
        write_deltalake(path, _table(2), mode='overwrite')

        assert compare(path, 2025, 1)['cambiadas'].num_rows == 0


# =============================================================================
# render / save_report
# =============================================================================
class TestReport:

    def test_render_incluye_historial_y_prueba(self, table):
        text = render(table, history_rows(table), compare(table, 2025, 1), 2025, 1)

        assert '| 1 |' in text and 'MERGE' in text
        assert '| Filas en la tabla | 25 | 27 |' in text
        assert 'Filas con `tip_amount` distinto: 3' in text
        assert '(actual − v0): [1.0]' in text
        assert '| 2.1 | 3.1 |' in text

    def test_save_report(self, tmp_path):
        path = save_report('# hola\n', tmp_path / 'out')

        assert path.read_text() == '# hola\n'
        assert path.name.startswith('time_travel_')


# =============================================================================
# main
# =============================================================================
class TestMain:

    def test_extremo_a_extremo(self, monkeypatch, capsys, table, tmp_path):
        monkeypatch.setattr(sys, 'argv', [
            'lh-delta-time-travel', '--tabla', str(table),
            '--salida', str(tmp_path / 'out'),
        ])

        assert main() == 0
        out = capsys.readouterr().out
        assert 'Versiones en el historial: 2' in out
        assert 'Filas con tip_amount distinto: 3' in out
        assert len(list((tmp_path / 'out').glob('time_travel_*.md'))) == 1
