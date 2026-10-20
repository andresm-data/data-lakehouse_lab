"""Pruebas unitarias de lakehouse.delta.optimize."""
import sys
from pathlib import Path

import pyarrow as pa
import pytest
from deltalake import DeltaTable, write_deltalake

from lakehouse.delta.optimize import describe, main, optimize, render, save_report


# =============================================================================
def _batch(n: int, month: int) -> pa.Table:
    """`n` filas en la partición `year=2025/month=<month>`."""
    return pa.table({
        'id': pa.array(range(n), pa.int64()),
        'year': pa.array([2025] * n, pa.int32()),
        'month': pa.array([month] * n, pa.int32()),
    })


@pytest.fixture
def table(tmp_path: Path) -> Path:
    """Tabla con 3 archivos pequeños en enero y 1 en febrero."""
    path = tmp_path / 'demo'

    for _ in range(3):
        write_deltalake(path, _batch(10, 1), mode='append',
                        partition_by=['year', 'month'])

    write_deltalake(path, _batch(5, 2), mode='append',
                    partition_by=['year', 'month'])
    return path


# =============================================================================
# describe
# =============================================================================
class TestDescribe:

    def test_cuenta_archivos_activos(self, table):
        stats = describe(table)

        assert stats['version'] == 3
        assert stats['archivos'] == 4
        assert stats['filas'] == 35
        assert stats['promedio'] == stats['bytes'] / 4
        assert stats['particiones']['year=2025/month=1'][0] == 3
        assert stats['particiones']['year=2025/month=2'][0] == 1

    def test_tabla_vacia(self, tmp_path):
        path = tmp_path / 'vacia'
        write_deltalake(path, _batch(0, 1), partition_by=['year', 'month'])

        stats = describe(path)

        assert stats['archivos'] == 0
        assert stats['promedio'] == 0


# =============================================================================
# optimize
# =============================================================================
class TestOptimize:

    def test_compacta_por_particion(self, table):
        metrics = optimize(table)
        stats = describe(table)

        assert metrics['numFilesRemoved'] == 3
        assert metrics['numFilesAdded'] == 1
        assert stats['archivos'] == 2
        assert stats['filas'] == 35
        assert DeltaTable(table).history(1)[0]['operation'] == 'OPTIMIZE'

    def test_no_borra_archivos_del_disco(self, table):
        optimize(table)

        assert len(list(table.rglob('*.parquet'))) == 5


# =============================================================================
# render / save_report
# =============================================================================
class TestReport:

    def test_render_incluye_antes_y_despues(self, table):
        before = describe(table)
        metrics = optimize(table)
        text = render(table, before, describe(table), metrics)

        assert '| Nº de archivos | 4 | 2 |' in text
        assert '| Filas | 35 | 35 |' in text
        assert '| `numFilesRemoved` | 3 |' in text
        assert '`year=2025/month=1` | 3 (' in text

    def test_save_report(self, tmp_path):
        path = save_report('# hola\n', tmp_path / 'out')

        assert path.read_text() == '# hola\n'
        assert path.name.startswith('optimize_')


# =============================================================================
# main
# =============================================================================
class TestMain:

    def test_extremo_a_extremo(self, monkeypatch, capsys, table, tmp_path):
        monkeypatch.setattr(sys, 'argv', [
            'lh-delta-optimize', '--tabla', str(table),
            '--salida', str(tmp_path / 'out'), '--tamano-mb', '1',
        ])

        assert main() == 0
        assert 'Archivos: 4 -> 2' in capsys.readouterr().out
        assert len(list((tmp_path / 'out').glob('optimize_*.md'))) == 1
