"""Pruebas unitarias de lakehouse.ingest.merge."""
import sys
from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import pytest

from lakehouse.ingest import merge as mod
from lakehouse.ingest._io import AVAILABLE_COMPRESSIONS
from lakehouse.ingest.merge import main, merge as convert_many
from lakehouse.ingest.partition_date import partition_date as convert_by_date

COL = 'tpep_pickup_datetime'


# =============================================================================
def _table(ids: list[int], stamps: list[datetime]) -> pa.Table:
    return pa.table({
        'id': pa.array(ids, pa.int64()),
        COL: pa.array(stamps, pa.timestamp('us')),
        'nombre': [f'viaje-{i}' for i in ids],
    })


@pytest.fixture
def origins(tmp_path: Path) -> list[Path]:
    """Tres archivos con el mismo esquema y varios row groups."""
    raw = tmp_path / 'raw'
    raw.mkdir()
    files = []

    for month, ids in [(1, [1, 2, 3]), (2, [4, 5]), (3, [6, 7, 8, 9])]:
        path = raw / f'yellow_2025-0{month}.parquet'
        stamps = [datetime(2025, month, d) for d in range(1, len(ids) + 1)]
        pq.write_table(_table(ids, stamps), path, row_group_size=2)
        files.append(path)

    return files


@pytest.fixture
def hive_dir(origins: list[Path], tmp_path: Path) -> Path:
    """Dataset particionado por year/month, como lo genera convert_by_date."""
    target = tmp_path / 'yellow.parquet'
    convert_by_date(origins, target, COL)
    return target


def _read_csv(path: Path, compression: str = 'none') -> pa.Table:
    if compression == 'none':
        return pacsv.read_csv(path)

    with pa.CompressedInputStream(pa.OSFile(str(path)), compression) as f:
        return pacsv.read_csv(f)


# =============================================================================
# convert_many: lista de archivos
# =============================================================================
class TestConvertManyArchivos:

    def test_csv_unico_sin_compresion_por_defecto(self, origins, tmp_path):
        target = tmp_path / 'out.csv'

        convert_many(origins, target, 'csv')

        assert target.read_text(encoding='utf-8').startswith('"id",')
        result = _read_csv(target)
        assert result['id'].to_pylist() == [1, 2, 3, 4, 5, 6, 7, 8, 9]
        assert result.column_names == ['id', COL, 'nombre']

    @pytest.mark.parametrize(
        'compression',
        [c for c in AVAILABLE_COMPRESSIONS['csv'] if c != 'none']
    )
    def test_csv_unico_comprimido(self, origins, tmp_path, compression):
        if not pa.Codec.is_available(compression):
            pytest.skip(f'Códec {compression} no disponible')
        target = tmp_path / f'out.csv.{compression}'

        convert_many(origins, target, 'csv', compression)

        assert _read_csv(target, compression).num_rows == 9

    def test_respeta_el_orden_de_los_origenes(self, origins, tmp_path):
        target = tmp_path / 'out.csv'

        convert_many(origins[::-1], target, 'csv')

        assert _read_csv(target)['id'].to_pylist() == [6, 7, 8, 9, 4, 5, 1, 2, 3]

    def test_parquet_unico(self, origins, tmp_path):
        target = tmp_path / 'out.parquet'

        convert_many(origins, target)

        result = pq.read_table(target)
        expected = pa.concat_tables(pq.read_table(o) for o in origins)
        assert result.equals(expected)
        meta = pq.ParquetFile(target).metadata
        assert meta.row_group(0).column(0).compression == 'ZSTD'

    def test_esquemas_distintos(self, origins, tmp_path):
        otro = tmp_path / 'raw' / 'otro.parquet'
        pq.write_table(pa.table({'id': [10]}), otro)

        with pytest.raises(ValueError, match='Esquema distinto'):
            convert_many([*origins, otro], tmp_path / 'out.csv', 'csv')

    def test_origen_inexistente(self, origins, tmp_path):
        with pytest.raises(FileNotFoundError):
            convert_many(
                [*origins, tmp_path / 'no_existe.parquet'],
                tmp_path / 'out.csv', 'csv'
            )

    def test_compresion_no_valida(self, origins, tmp_path):
        with pytest.raises(ValueError, match='no válida'):
            convert_many(origins, tmp_path / 'out.csv', 'csv', 'snappy')

    def test_destino_existente_sin_sobrescribir(self, origins, tmp_path):
        target = tmp_path / 'out.csv'
        target.write_text('previo')

        with pytest.raises(FileExistsError):
            convert_many(origins, target, 'csv')

        assert target.read_text() == 'previo'

    def test_sobrescribir_destino_existente(self, origins, tmp_path):
        target = tmp_path / 'out.csv'
        target.write_text('previo')

        convert_many(origins, target, 'csv', overwrite=True)

        assert _read_csv(target).num_rows == 9

    def test_destino_igual_a_un_origen(self, origins):
        with pytest.raises(ValueError, match='contiene o coincide'):
            convert_many(origins, origins[1], overwrite=True)

        assert all(o.is_file() for o in origins)

    def test_crea_directorios_intermedios(self, origins, tmp_path):
        target = tmp_path / 'a' / 'b' / 'out.csv'

        convert_many(origins, target, 'csv')

        assert target.is_file()


# =============================================================================
# convert_many: directorio (dataset particionado)
# =============================================================================
class TestConvertManyDirectorio:

    def test_dataset_hive_a_csv_con_columnas_de_particion(self, hive_dir, tmp_path):
        target = tmp_path / 'out.csv'

        convert_many([hive_dir], target, 'csv')

        result = _read_csv(target)
        assert result.num_rows == 9
        assert result.column_names[-2:] == ['year', 'month']
        rows = {r['id']: (r['year'], r['month']) for r in result.to_pylist()}
        assert rows[1] == (2025, 1)
        assert rows[5] == (2025, 2)
        assert rows[9] == (2025, 3)

    def test_dataset_hive_a_csv_comprimido(self, hive_dir, tmp_path):
        target = tmp_path / 'out.csv.gz'

        convert_many([hive_dir], target, 'csv', 'gzip')

        assert _read_csv(target, 'gzip').num_rows == 9

    def test_dataset_hive_a_parquet(self, hive_dir, tmp_path):
        target = tmp_path / 'out.parquet'

        convert_many([hive_dir], target)

        result = pq.read_table(target)
        assert result.num_rows == 9
        assert result.schema.field('year').type == pa.int32()

    def test_directorio_plano_sin_particiones(self, origins, tmp_path):
        target = tmp_path / 'out.csv'

        convert_many([tmp_path / 'raw'], target, 'csv')

        result = _read_csv(target)
        assert result.column_names == ['id', COL, 'nombre']
        assert result['id'].to_pylist() == [1, 2, 3, 4, 5, 6, 7, 8, 9]

    def test_directorio_sin_parquet(self, tmp_path):
        vacio = tmp_path / 'vacio'
        vacio.mkdir()

        with pytest.raises(FileNotFoundError, match='No se encontraron'):
            convert_many([vacio], tmp_path / 'out.csv', 'csv')

    def test_directorio_combinado_con_otros_origenes(self, origins, hive_dir, tmp_path):
        with pytest.raises(ValueError, match='no puede combinarse'):
            convert_many([hive_dir, origins[0]], tmp_path / 'out.csv', 'csv')

    def test_destino_dentro_del_directorio_no_se_lee(self, hive_dir):
        target = hive_dir / 'todo.csv'

        convert_many([hive_dir], target, 'csv')

        assert _read_csv(target).num_rows == 9


# =============================================================================
# main
# =============================================================================
class TestMainVariosOrigenes:

    def _run(self, monkeypatch, *args: str) -> int:
        monkeypatch.setattr(sys, 'argv', ['lh-convert', *args])
        return main()

    def test_directorio_a_csv_extremo_a_extremo(
        self, monkeypatch, capsys, hive_dir, tmp_path
    ):
        target = tmp_path / 'yellow.csv'

        code = self._run(monkeypatch, str(hive_dir), str(target), '-f', 'csv')

        assert code == 0
        assert f'Listo -> {target}' in capsys.readouterr().out
        assert _read_csv(target).num_rows == 9

    def test_varios_archivos_pasan_a_convert_many(
        self, monkeypatch, origins, tmp_path
    ):
        calls = []
        monkeypatch.setattr(mod, 'merge', lambda *a: calls.append(a))
        target = tmp_path / 'out.csv'

        self._run(
            monkeypatch, *map(str, origins), str(target),
            '-f', 'csv', '-c', 'gzip', '--sobrescribir'
        )

        assert calls == [(origins, target, 'csv', 'gzip', True)]

    def test_directorio_pasa_a_convert_many(self, monkeypatch, hive_dir, tmp_path):
        calls = []
        monkeypatch.setattr(mod, 'merge', lambda *a: calls.append(a))

        self._run(monkeypatch, str(hive_dir), str(tmp_path / 'out.csv'), '-f', 'csv')

        assert calls[0][0] == [hive_dir]
        assert calls[0][3] is None

    def test_error_devuelve_uno(self, monkeypatch, capsys, origins, tmp_path):
        target = tmp_path / 'out.csv'
        target.write_text('previo')

        code = self._run(monkeypatch, *map(str, origins), str(target), '-f', 'csv')

        assert code == 1
        assert 'ya existe' in capsys.readouterr().err


# =============================================================================
# merge con CSV de entrada
# =============================================================================
class TestMergeCsv:

    @pytest.fixture
    def csv_origins(self, origins, tmp_path) -> list[Path]:
        """Los mismos datos como CSV: uno plano, uno gzip y uno zstd."""
        files = []

        for origin, suffix in zip(origins, ['csv', 'csv.gz', 'csv.zst']):
            path = tmp_path / 'csv' / origin.name.replace('parquet', suffix)
            path.parent.mkdir(exist_ok=True)
            codec = {'csv': None, 'csv.gz': 'gzip', 'csv.zst': 'zstd'}[suffix]
            table = pq.read_table(origin)

            if codec:
                with pa.CompressedOutputStream(str(path), codec) as out:
                    pacsv.write_csv(table, out)

            else:
                pacsv.write_csv(table, path)

            files.append(path)

        return files

    def test_csv_a_csv_sin_compresion_por_defecto(self, csv_origins, tmp_path):
        target = tmp_path / 'out.csv'

        convert_many(csv_origins, target, 'csv')

        assert target.read_text(encoding='utf-8').startswith('"id",')
        assert _read_csv(target)['id'].to_pylist() == list(range(1, 10))

    def test_csv_a_csv_comprimido(self, csv_origins, tmp_path):
        target = tmp_path / 'out.csv.bz2'

        convert_many(csv_origins, target, 'csv', 'bz2')

        assert _read_csv(target, 'bz2')['id'].to_pylist() == list(range(1, 10))

    def test_csv_a_parquet(self, csv_origins, origins, tmp_path):
        target = tmp_path / 'out.parquet'

        convert_many(csv_origins, target)

        result = pq.read_table(target)
        expected = pa.concat_tables(pq.read_table(o) for o in origins)
        assert result.select(['id', 'nombre']).equals(expected.select(['id', 'nombre']))
        assert result[COL].cast(pa.timestamp('us')).equals(expected[COL])

    def test_no_mezcla_csv_y_parquet(self, csv_origins, origins, tmp_path):
        with pytest.raises(ValueError, match='mezclar'):
            convert_many([csv_origins[0], origins[1]], tmp_path / 'out.csv', 'csv')

    def test_dataset_csv_particionado_a_csv(self, origins, tmp_path):
        hive_csv = tmp_path / 'yellow_csv'
        convert_by_date(origins, hive_csv, COL, format_='csv')
        target = tmp_path / 'out.csv.gz'

        convert_many([hive_csv], target, 'csv', 'gzip')

        result = _read_csv(target, 'gzip')
        assert sorted(result['id'].to_pylist()) == list(range(1, 10))
        assert set(zip(result['year'].to_pylist(), result['month'].to_pylist())) == {
            (2025, 1), (2025, 2), (2025, 3)
        }

    def test_extremo_a_extremo(self, monkeypatch, capsys, csv_origins, tmp_path):
        target = tmp_path / 'out.csv'
        monkeypatch.setattr(
            sys, 'argv', ['lh-merge', *map(str, csv_origins), str(target), '-f', 'csv']
        )

        assert main() == 0
        assert _read_csv(target).num_rows == 9
