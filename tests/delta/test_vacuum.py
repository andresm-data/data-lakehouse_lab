"""Pruebas unitarias de lakehouse.delta.vacuum."""
import sys
from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pytest
from deltalake import DeltaTable, write_deltalake

from lakehouse.delta.vacuum import (
    can_read_version, files_on_disk, main, removed_files, render,
    save_report, vacuum
)


# =============================================================================
def _batch(n: int) -> pa.Table:
    """`n` filas en la partición `year=2025`."""
    return pa.table({
        'id': pa.array(range(n), pa.int64()),
        'year': pa.array([2025] * n, pa.int32()),
    })


@pytest.fixture
def table(tmp_path: Path) -> Path:
    """Tabla con 3 appends compactados: 1 archivo activo y 3 retirados."""
    path = tmp_path / 'demo'

    for _ in range(3):
        write_deltalake(path, _batch(10), mode='append', partition_by=['year'])

    DeltaTable(path).optimize.compact()
    return path


# =============================================================================
# files_on_disk / removed_files
# =============================================================================
class TestLog:

    def test_archivos_en_disco_sin_log(self, table):
        files = files_on_disk(table)

        assert len(files) == 4
        assert all(f.startswith('year=2025/') for f in files)

    def test_retirados_por_optimize(self, table):
        removed = removed_files(table)

        assert len(removed) == 3
        assert {r['version'] for r in removed.values()} == {3}
        assert {r['operacion'] for r in removed.values()} == {'OPTIMIZE'}
        assert all(isinstance(r['fecha'], datetime) for r in removed.values())


# =============================================================================
# vacuum
# =============================================================================
class TestVacuum:

    def test_retencion_por_defecto_no_borra_recientes(self, table):
        assert vacuum(table, 168, dry_run=True) == []

    def test_dry_run_no_borra(self, table):
        planned = vacuum(table, 0, dry_run=True)

        assert len(planned) == 3
        assert len(files_on_disk(table)) == 4

    def test_borra_solo_retirados(self, table):
        active = set(pa.table(DeltaTable(table).get_add_actions(flatten=True))
                     .to_pydict()['path'])

        deleted = vacuum(table, 0, dry_run=False)

        assert set(deleted) == set(removed_files(table))
        assert files_on_disk(table) == active
        assert DeltaTable(table).to_pyarrow_dataset().count_rows() == 30

    def test_rompe_time_travel(self, table):
        assert can_read_version(table, 0)

        vacuum(table, 0, dry_run=False)

        assert not can_read_version(table, 0)


# =============================================================================
# render / save_report
# =============================================================================
class TestReport:

    def test_render_con_retencion_corta(self, table):
        removed = removed_files(table)
        paths = sorted(removed)
        result = {
            'fecha': datetime.now(), 'retirados': removed, 'activos': 1,
            'disco_antes': 4, 'disco_despues': 1, 'dry_run_defecto': [],
            'dry_run': paths, 'borrados': paths, 'bytes_liberados': 3 * 1024,
            'coincide_disco': True, 'activos_intactos': True, 'lee_v0': False,
        }

        text = render(table, 0, result)

        assert '**Aviso:**' in text
        assert '| Borrados por VACUUM | 3 |' in text
        assert '| ¿Se puede leer la versión 0? | No |' in text
        assert 'v3 (OPTIMIZE)' in text
        assert 'Lo borrado coincide con el dry run: Sí' in text

    def test_render_sin_aviso_con_retencion_por_defecto(self, table):
        result = {
            'fecha': datetime.now(), 'retirados': {}, 'activos': 1,
            'disco_antes': 4, 'disco_despues': 4, 'dry_run_defecto': [],
            'dry_run': [], 'borrados': [], 'bytes_liberados': 0,
            'coincide_disco': True, 'activos_intactos': True, 'lee_v0': True,
        }

        assert '**Aviso:**' not in render(table, 168, result)

    def test_save_report(self, tmp_path):
        path = save_report('# hola\n', tmp_path / 'out')

        assert path.read_text() == '# hola\n'
        assert path.name.startswith('vacuum_')


# =============================================================================
# main
# =============================================================================
class TestMain:

    def test_extremo_a_extremo(self, monkeypatch, capsys, table, tmp_path):
        monkeypatch.setattr(sys, 'argv', [
            'lh-delta-vacuum', '--tabla', str(table),
            '--salida', str(tmp_path / 'out'), '--retencion-horas', '0',
        ])

        assert main() == 0
        out = capsys.readouterr().out
        assert 'Dry run con 168 h: 0 archivos' in out
        assert 'Borrados: 3' in out
        assert 'Parquet en disco: 4 -> 1' in out
        assert len(list((tmp_path / 'out').glob('vacuum_*.md'))) == 1
