"""Pruebas unitarias de lakehouse.delta.merge_table."""
import json
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pytest
from deltalake import DeltaTable, write_deltalake

from lakehouse.delta.merge_table import (
    build_source, check_unique_key, copy_table, main, merge, save_metrics
)


# =============================================================================
START = datetime(2025, 1, 1)


def _table(n: int, month: int = 1, offset: int = 0) -> pa.Table:
    """`n` viajes con clave única; cada uno empieza un minuto después."""
    pickups = [START + timedelta(minutes=offset + i) for i in range(n)]
    return pa.table({
        'VendorID': pa.array([1] * n, pa.int32()),
        'tpep_pickup_datetime': pa.array(pickups, pa.timestamp('us')),
        'tpep_dropoff_datetime': pa.array(
            [p + timedelta(minutes=10) for p in pickups], pa.timestamp('us')
        ),
        'PULocationID': pa.array([10] * n, pa.int32()),
        'DOLocationID': pa.array([20] * n, pa.int32()),
        'tip_amount': pa.array([2.0] * n, pa.float64()),
        'total_amount': pa.array([12.0] * n, pa.float64()),
        'year': pa.array([2025] * n, pa.int32()),
        'month': pa.array([month] * n, pa.int32()),
    })


@pytest.fixture
def origin(tmp_path: Path) -> Path:
    """Tabla Delta con 20 viajes en enero y 5 en febrero."""
    path = tmp_path / 'origin'
    data = pa.concat_tables([_table(20), _table(5, month=2)])
    write_deltalake(path, data, partition_by=['year', 'month'])
    return path


@pytest.fixture
def duplicated(tmp_path: Path) -> Path:
    """Tabla Delta con 4 viajes, cada uno repetido dos veces."""
    path = tmp_path / 'duplicated'
    write_deltalake(path, pa.concat_tables([_table(4), _table(4)]),
                    partition_by=['year', 'month'])
    return path


@pytest.fixture
def target(origin: Path, tmp_path: Path) -> Path:
    path = tmp_path / 'demo'
    copy_table(origin, path)
    return path


# =============================================================================
# copy_table
# =============================================================================
class TestCopyTable:

    def test_copia_sin_tocar_la_original(self, origin, target):
        merge(target, build_source(target, 2025, 1, 3, 2), 2025, 1)

        assert DeltaTable(origin).version() == 0
        assert DeltaTable(target).version() == 1

    def test_no_sobrescribe_copia_existente(self, origin, target):
        merge(target, build_source(target, 2025, 1, 3, 2), 2025, 1)

        copy_table(origin, target)

        assert DeltaTable(target).version() == 1

    def test_reset_vuelve_a_copiar(self, origin, target):
        merge(target, build_source(target, 2025, 1, 3, 2), 2025, 1)

        copy_table(origin, target, reset=True)

        assert DeltaTable(target).version() == 0


# =============================================================================
# check_unique_key
# =============================================================================
class TestCheckUniqueKey:

    def test_clave_unica(self):
        check_unique_key(_table(5))

    def test_clave_repetida(self):
        with pytest.raises(ValueError, match='2 claves repetidas'):
            check_unique_key(pa.concat_tables([_table(3), _table(2)]))


# =============================================================================
# build_source
# =============================================================================
class TestBuildSource:

    def test_corrige_propina_y_agrega_filas_nuevas(self, target):
        source = build_source(target, 2025, 1, 3, 2)

        assert source.num_rows == 5
        assert source['tip_amount'].to_pylist() == [3.0] * 3 + [2.0] * 2
        assert source['total_amount'].to_pylist() == [13.0] * 3 + [12.0] * 2

    def test_solo_usa_la_particion_pedida(self, target):
        source = build_source(target, 2025, 2, 2, 1)

        assert pc.unique(source['month']).to_pylist() == [2]

    def test_omite_claves_repetidas_en_la_tabla(self, target):
        write_deltalake(target, _table(1), mode='append')

        source = build_source(target, 2025, 1, 3, 0)

        assert START not in source['tpep_pickup_datetime'].to_pylist()

    def test_filas_nuevas_no_existen_en_la_tabla(self, target):
        source = build_source(target, 2025, 1, 0, 3)

        existing = DeltaTable(target).to_pyarrow_table(
            columns=['tpep_pickup_datetime']
        )['tpep_pickup_datetime'].to_pylist()
        assert not set(source['tpep_pickup_datetime'].to_pylist()) & set(existing)

    def test_falla_si_la_fuente_repite_claves(self, duplicated):
        with pytest.raises(ValueError, match='4 claves repetidas'):
            build_source(duplicated, 2025, 1, 0, 8)


# =============================================================================
# merge
# =============================================================================
class TestMerge:

    def test_actualiza_e_inserta(self, target):
        metrics = merge(target, build_source(target, 2025, 1, 3, 2), 2025, 1)

        assert metrics['num_target_rows_updated'] == 3
        assert metrics['num_target_rows_inserted'] == 2
        assert metrics['num_target_files_removed'] == 1

        result = DeltaTable(target).to_pyarrow_table()
        assert result.num_rows == 27
        assert pc.sum(result['tip_amount']).as_py() == 25 * 2.0 + 2 * 2.0 + 3.0

    def test_no_reescribe_otras_particiones(self, target):
        metrics = merge(target, build_source(target, 2025, 1, 3, 2), 2025, 1)

        assert metrics['num_target_files_added'] == 1
        assert metrics['num_target_files_removed'] == 1


# =============================================================================
# save_metrics
# =============================================================================
class TestSaveMetrics:

    def test_guarda_json(self, target, tmp_path):
        path = save_metrics({'num_target_rows_updated': 3}, target, tmp_path / 'm')

        data = json.loads(path.read_text())
        assert data['version'] == 0
        assert data['metricas'] == {'num_target_rows_updated': 3}
        assert path.name.startswith('merge_')


# =============================================================================
# main
# =============================================================================
class TestMain:

    def _run(self, monkeypatch, *args: str) -> int:
        monkeypatch.setattr(sys, 'argv', ['lh-delta-merge', *args])
        return main()

    def test_extremo_a_extremo(self, monkeypatch, capsys, origin, tmp_path):
        code = self._run(
            monkeypatch, '--origen', str(origin),
            '--destino', str(tmp_path / 'demo'),
            '--metricas', str(tmp_path / 'm'),
            '--actualizar', '4', '--insertar', '3',
        )

        assert code == 0
        out = capsys.readouterr().out
        assert 'Filas actualizadas:   4' in out
        assert 'Filas insertadas:     3' in out
        assert len(list((tmp_path / 'm').glob('merge_*.json'))) == 1

    def test_error_por_claves_repetidas(
        self, monkeypatch, capsys, duplicated, tmp_path
    ):
        code = self._run(
            monkeypatch, '--origen', str(duplicated),
            '--destino', str(tmp_path / 'demo'),
            '--metricas', str(tmp_path / 'm'),
            '--actualizar', '0', '--insertar', '8',
        )

        assert code == 1
        assert 'claves repetidas' in capsys.readouterr().err
        assert not (tmp_path / 'm').exists()
