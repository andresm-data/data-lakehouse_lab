"""Pruebas unitarias de lakehouse.delta.create_table_pyarrow."""
import sys
from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from deltalake import DeltaTable, write_deltalake

from lakehouse.delta import create_table_pyarrow as mod
from lakehouse.delta.create_table_pyarrow import create_table, main, validate_rows


# =============================================================================
def _table(rows: list[tuple[int, int, int]]) -> pa.Table:
    """Tabla con columnas `id`, fecha, `year` y `month` a partir de tuplas."""
    return pa.table({
        'id': pa.array([r[0] for r in rows], pa.int64()),
        'tpep_pickup_datetime': pa.array(
            [datetime(y, m, 1) for _, y, m in rows], pa.timestamp('us')
        ),
        'nombre': pa.array([f'viaje-{r[0]}' for r in rows], pa.large_string()),
        'year': pa.array([r[1] for r in rows], pa.int32()),
        'month': pa.array([r[2] for r in rows], pa.int32()),
    })


ROWS = [
    (1, 2024, 12), (2, 2025, 1), (3, 2025, 1),
    (4, 2025, 2), (5, 2025, 3), (6, 2025, 3), (7, 2025, 3),
]


@pytest.fixture
def origin(tmp_path: Path) -> Path:
    """Parquet con varios row groups y meses de dos años distintos."""
    path = tmp_path / 'yellow_tripdata.parquet'
    pq.write_table(_table(ROWS), path, row_group_size=2)
    return path


# =============================================================================
# create_table
# =============================================================================
class TestCreateTable:

    def test_conserva_todas_las_filas(self, origin, tmp_path):
        target = tmp_path / 'delta'

        create_table(origin, target)

        result = DeltaTable(target).to_pyarrow_table().sort_by('id')
        assert result['id'].to_pylist() == [r[0] for r in ROWS]

    def test_particiona_por_year_y_month(self, origin, tmp_path):
        target = tmp_path / 'delta'

        create_table(origin, target)

        assert DeltaTable(target).metadata().partition_columns == ['year', 'month']
        folders = {p.relative_to(target).as_posix() for p in target.glob('year=*/*')}
        assert folders == {
            'year=2024/month=12', 'year=2025/month=1',
            'year=2025/month=2', 'year=2025/month=3',
        }

    def test_sobrescribe_tabla_existente(self, origin, tmp_path):
        target = tmp_path / 'delta'

        create_table(origin, target)
        create_table(origin, target)

        table = DeltaTable(target)
        assert table.version() == 1
        assert table.to_pyarrow_table().num_rows == len(ROWS)

    def test_sobrescribe_aunque_cambie_el_esquema(self, origin, tmp_path):
        target = tmp_path / 'delta'
        write_deltalake(target, pa.table({'otra': [1], 'year': [1], 'month': [1]}))

        create_table(origin, target)

        assert 'otra' not in DeltaTable(target).schema().to_arrow().names


# =============================================================================
# validate_rows
# =============================================================================
class TestValidateRows:

    def test_filas_coinciden(self, origin, tmp_path, capsys):
        target = tmp_path / 'delta'
        create_table(origin, target)

        validate_rows(origin, target)

        out = capsys.readouterr().out
        assert 'Filas en Parquet: 7' in out
        assert 'Filas en Delta:   7' in out
        assert 'year=2025 month=3: 3 vs 3 [OK]' in out
        assert 'DIFERENTE' not in out

    def test_filas_de_mas_en_delta(self, origin, tmp_path, capsys):
        target = tmp_path / 'delta'
        create_table(origin, target)
        write_deltalake(target, _table([(8, 2025, 2)]), mode='append')

        with pytest.raises(ValueError, match='no coinciden'):
            validate_rows(origin, target)

        assert 'year=2025 month=2: 1 vs 2 [DIFERENTE]' in capsys.readouterr().out

    def test_particion_faltante_en_delta(self, origin, tmp_path, capsys):
        target = tmp_path / 'delta'
        create_table(origin, target)
        DeltaTable(target).delete('year = 2024')

        with pytest.raises(ValueError, match='no coinciden'):
            validate_rows(origin, target)

        assert 'year=2024 month=12: 1 vs 0 [DIFERENTE]' in capsys.readouterr().out


# =============================================================================
# main
# =============================================================================
class TestMain:

    def _run(self, monkeypatch, *args: str) -> int:
        monkeypatch.setattr(sys, 'argv', ['lh-delta-pyarrow', *args])
        return main()

    def test_extremo_a_extremo(self, monkeypatch, capsys, origin, tmp_path):
        target = tmp_path / 'delta'

        code = self._run(monkeypatch, str(origin), str(target))

        assert code == 0
        assert f'Listo -> {target}' in capsys.readouterr().out
        assert DeltaTable(target).to_pyarrow_table().num_rows == len(ROWS)

    def test_rutas_por_defecto(self, monkeypatch):
        calls = []
        monkeypatch.setattr(mod, 'create_table', lambda *a: calls.append(a))
        monkeypatch.setattr(mod, 'validate_rows', lambda *a: None)

        self._run(monkeypatch)

        assert calls == [(mod.ORIGIN, mod.TARGET)]

    def test_validacion_fallida_devuelve_uno(self, monkeypatch, capsys, origin, tmp_path):
        def fail(*_):
            raise ValueError('Las filas no coinciden')

        monkeypatch.setattr(mod, 'validate_rows', fail)

        code = self._run(monkeypatch, str(origin), str(tmp_path / 'delta'))

        captured = capsys.readouterr()
        assert code == 1
        assert 'Error: Las filas no coinciden' in captured.err
        assert 'Listo' not in captured.out
