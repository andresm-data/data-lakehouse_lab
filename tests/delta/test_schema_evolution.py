"""Pruebas unitarias de lakehouse.delta.schema_evolution."""
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pytest
from deltalake import DeltaTable, write_deltalake

from lakehouse.delta import schema_evolution as mod
from lakehouse.delta.schema_evolution import (
    COLUMN, append_batch, build_batch, main, render, save_report, verify
)


# =============================================================================
START = datetime(2025, 1, 1)


def _table(n: int, month: int = 1) -> pa.Table:
    """`n` viajes de 10 minutos con clave única; uno por minuto."""
    pickups = [START + timedelta(minutes=i) for i in range(n)]
    return pa.table({
        'VendorID': pa.array([1] * n, pa.int32()),
        'tpep_pickup_datetime': pa.array(pickups, pa.timestamp('us')),
        'tpep_dropoff_datetime': pa.array(
            [p + timedelta(minutes=10) for p in pickups], pa.timestamp('us')
        ),
        'PULocationID': pa.array([10] * n, pa.int32()),
        'DOLocationID': pa.array([20] * n, pa.int32()),
        'tip_amount': pa.array([2.0] * n, pa.float64()),
        'year': pa.array([2025] * n, pa.int32()),
        'month': pa.array([month] * n, pa.int32()),
    })


@pytest.fixture
def table(tmp_path: Path) -> Path:
    """Tabla Delta con 20 viajes en enero y 5 en febrero, sin `COLUMN`."""
    path = tmp_path / 'demo'
    write_deltalake(path, pa.concat_tables([_table(20), _table(5, month=2)]),
                    partition_by=['year', 'month'])
    return path


# =============================================================================
# build_batch
# =============================================================================
class TestBuildBatch:

    def test_agrega_duracion_en_minutos(self, table):
        batch = build_batch(table, 2025, 1, 4)

        assert batch.num_rows == 4
        assert batch.schema.names[-1] == COLUMN
        assert batch[COLUMN].to_pylist() == [10.0] * 4

    def test_solo_viajes_nuevos(self, table):
        batch = build_batch(table, 2025, 1, 4)

        existing = DeltaTable(table).to_pyarrow_table(
            columns=['tpep_pickup_datetime']
        )['tpep_pickup_datetime'].to_pylist()
        assert not set(batch['tpep_pickup_datetime'].to_pylist()) & set(existing)

    def test_solo_usa_la_particion_pedida(self, table):
        batch = build_batch(table, 2025, 2, 10)

        assert batch.num_rows == 5
        assert set(batch['month'].to_pylist()) == {2}


# =============================================================================
# append_batch / verify
# =============================================================================
class TestAppendAndVerify:

    def test_crea_version_y_deja_nulos_en_filas_anteriores(self, table):
        append_batch(table, build_batch(table, 2025, 1, 4))

        result = verify(table, 0, 2025, 1)

        assert result['version_antes'] == 0
        assert result['version_despues'] == 1
        assert result['operacion'] == 'WRITE (Append)'
        assert not result['columna_antes']
        assert result['columna_despues']
        assert result['filas_antes'] == 25
        assert result['nulos'] == 25
        assert result['no_nulos'] == 4

    def test_ejemplos_de_cada_tipo(self, table):
        append_batch(table, build_batch(table, 2025, 1, 4))

        result = verify(table, 0, 2025, 1)

        assert result['ejemplo_antiguas'][COLUMN].null_count == 3
        assert result['ejemplo_nuevas'][COLUMN].to_pylist() == [10.0] * 3


# =============================================================================
# render / save_report
# =============================================================================
class TestReport:

    def test_render(self, table):
        append_batch(table, build_batch(table, 2025, 1, 4))

        text = render(table, 4, verify(table, 0, 2025, 1))

        assert f'| `{COLUMN}` en el esquema | No | Sí |' in text
        assert '| Filas en la tabla | 25 | 29 |' in text
        assert 'Versión nueva: 0 → 1, operación `WRITE (Append)`' in text
        assert '| 10 | 20 | null |' in text
        assert '| 10 | 20 | 10.0 |' in text

    def test_save_report(self, tmp_path):
        path = save_report('# hola\n', tmp_path / 'out')

        assert path.read_text() == '# hola\n'
        assert path.name.startswith('schema_evolution_')


# =============================================================================
# main
# =============================================================================
class TestMain:

    def _run(self, monkeypatch, table: Path, out: Path) -> int:
        monkeypatch.setattr(sys, 'argv', [
            'lh-delta-schema-evolution', '--tabla', str(table),
            '--salida', str(out), '--filas', '4',
        ])
        return main()

    def test_extremo_a_extremo(self, monkeypatch, capsys, table, tmp_path):
        code = self._run(monkeypatch, table, tmp_path / 'out')

        assert code == 0
        out = capsys.readouterr().out
        assert 'Versión: 0 -> 1' in out
        assert f'Filas con {COLUMN} nulo:    25' in out
        assert len(list((tmp_path / 'out').glob('schema_evolution_*.md'))) == 1

    def test_falla_si_la_columna_ya_tenia_valores(
        self, monkeypatch, table, tmp_path
    ):
        self._run(monkeypatch, table, tmp_path / 'out')

        assert self._run(monkeypatch, table, tmp_path / 'out') == 1
