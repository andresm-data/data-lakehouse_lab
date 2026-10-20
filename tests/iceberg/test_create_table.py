"""Pruebas unitarias de lakehouse.iceberg.create_table."""
import sys
from pathlib import Path

import pyarrow.parquet as pq
import pytest
from deltalake import write_deltalake

from lakehouse.iceberg.create_table import (
    IDENTIFIER, create_table, describe_spec, load, main, validate_rows,
)


# =============================================================================
class TestCreateTable:

    def test_particion_oculta_por_mes(self, catalog, origin):
        table = create_table(catalog, IDENTIFIER, pq.read_schema(origin))

        assert describe_spec(table) == 'month(tpep_pickup_datetime) -> pickup_month'
        assert table.schema().column_names == pq.read_schema(origin).names
        assert table.current_snapshot() is None

    def test_recrea_tabla_existente(self, catalog, origin):
        first = create_table(catalog, IDENTIFIER, pq.read_schema(origin))
        load(first, origin)

        table = create_table(catalog, IDENTIFIER, pq.read_schema(origin))

        assert table.current_snapshot() is None
        assert table.scan().to_arrow().num_rows == 0

    def test_tabla_sin_particion(self, catalog, origin):
        table = catalog.create_table('lakehouse.plana', schema=pq.read_schema(origin))

        assert describe_spec(table) == 'sin partición'


# =============================================================================
class TestLoad:

    def test_un_snapshot_por_mes(self, catalog, origin, rows):
        table = create_table(catalog, IDENTIFIER, pq.read_schema(origin))

        months = load(table, origin)

        assert months == [(2025, 1), (2025, 2), (2025, 3)]
        snapshots = table.snapshots()
        assert len(snapshots) == 3
        assert [s.summary['source-month'] for s in snapshots] == [
            '2025-01', '2025-02', '2025-03',
        ]
        result = table.scan().to_arrow().sort_by('id')
        assert result['id'].to_pylist() == [r[0] for r in rows]

    def test_archivos_por_particion(self, catalog, origin):
        table = create_table(catalog, IDENTIFIER, pq.read_schema(origin))
        load(table, origin)

        folders = {
            p.name for p in Path(table.location().removeprefix('file://'))
            .glob('data/*')
        }
        assert folders == {
            'pickup_month=2025-01', 'pickup_month=2025-02', 'pickup_month=2025-03',
        }


# =============================================================================
class TestValidateRows:

    def _loaded(self, catalog, origin):
        table = create_table(catalog, IDENTIFIER, pq.read_schema(origin))
        load(table, origin)
        return table

    def test_coincide_con_parquet_y_delta(self, catalog, origin, tmp_path, capsys):
        delta = tmp_path / 'delta'
        write_deltalake(delta, pq.read_table(origin), partition_by=['year', 'month'])

        validate_rows(self._loaded(catalog, origin), origin, delta)

        out = capsys.readouterr().out
        assert 'Filas en Delta:' in out
        assert 'year=2025 month=3: 3 vs 3 vs 3 [OK]' in out

    def test_sin_delta_solo_compara_parquet(self, catalog, origin, tmp_path, capsys):
        validate_rows(self._loaded(catalog, origin), origin, tmp_path / 'no-existe')

        out = capsys.readouterr().out
        assert 'Delta' not in out
        assert 'year=2025 month=1: 2 vs 2 [OK]' in out

    def test_filas_de_mas_en_iceberg(self, catalog, origin, capsys, make_table):
        table = self._loaded(catalog, origin)
        table.append(make_table([(8, 2025, 2, 10)]))

        with pytest.raises(ValueError, match='no coinciden'):
            validate_rows(table, origin, None)

        assert 'year=2025 month=2: 2 vs 3 [DIFERENTE]' in capsys.readouterr().out


# =============================================================================
class TestMain:

    def _run(self, monkeypatch, tmp_path, origin, *extra: str) -> int:
        monkeypatch.setattr(sys, 'argv', [
            'lh-iceberg-create', str(origin), '--delta', str(tmp_path / 'nada'),
            '--catalogo', str(tmp_path / 'm.db'),
            '--warehouse', str(tmp_path / 'mwh'), *extra,
        ])
        return main()

    def test_extremo_a_extremo(self, monkeypatch, capsys, tmp_path, origin):
        code = self._run(monkeypatch, tmp_path, origin)

        out = capsys.readouterr().out
        assert code == 0
        assert 'append 2025-03: 3 filas' in out
        assert 'Listo -> file://' in out

    def test_validacion_fallida_devuelve_uno(self, monkeypatch, capsys, tmp_path, origin):
        import lakehouse.iceberg.create_table as mod

        def fail(*_):
            raise ValueError('Las filas no coinciden')

        monkeypatch.setattr(mod, 'validate_rows', fail)

        code = self._run(monkeypatch, tmp_path, origin)

        captured = capsys.readouterr()
        assert code == 1
        assert 'Error: Las filas no coinciden' in captured.err
        assert 'Listo' not in captured.out
