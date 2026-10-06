"""Pruebas unitarias del módulo lakehouse.ingest.convert."""
import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import pytest

from lakehouse.ingest import convert as mod
from lakehouse.ingest.convert import AVAILABLE_COMPRESSIONS, _validate, convert, main


# =============================================================================
@pytest.fixture
def sample_table() -> pa.Table:
    """Tabla de ejemplo con columnas aptas para particionar."""
    return pa.table({
        'id': pa.array([1, 2, 3, 4, 5, 6], pa.int64()),
        'nombre': ['ana', 'luis', 'maría', 'josé', 'eva', 'iván'],
        'anio': pa.array([2024, 2024, 2024, 2025, 2025, 2025], pa.int64()),
        'mes': pa.array([1, 1, 2, 1, 2, 2], pa.int64()),
        'monto': [10.5, 20.0, 30.25, 40.0, 50.75, 60.0],
    })


@pytest.fixture
def origin(tmp_path: Path, sample_table: pa.Table) -> Path:
    """Archivo Parquet de origen con varios row groups."""
    path = tmp_path / 'origen.parquet'
    pq.write_table(sample_table, path, row_group_size=2)
    return path


def _rows_by_id(table: pa.Table) -> list[dict]:
    return sorted(table.to_pylist(), key=lambda r: r['id'])


# =============================================================================
# _validate
# =============================================================================
class TestValidate:

    def test_parametros_validos_no_lanzan(self, origin, tmp_path):
        _validate(origin, tmp_path / 'out.parquet', 'parquet', 'zstd', None, False)

    @pytest.mark.parametrize('format_, compression', [
        ('parquet', 'bz2'),
        ('csv', 'snappy'),
        ('csv', 'brotli'),
        ('parquet', 'inexistente'),
    ])
    def test_compresion_no_valida_para_formato(
        self, origin, tmp_path, format_, compression
    ):
        with pytest.raises(ValueError, match='no válida'):
            _validate(origin, tmp_path / 'out', format_, compression, None, False)

    def test_codec_no_disponible_en_pyarrow(self, origin, tmp_path, monkeypatch):
        class CodecSinSoporte:
            @staticmethod
            def is_available(_):
                return False

        monkeypatch.setattr(mod.pa, 'Codec', CodecSinSoporte)

        with pytest.raises(ValueError, match='no incluye el códec'):
            _validate(origin, tmp_path / 'out', 'parquet', 'zstd', None, False)

    def test_csv_particionado_con_compresion(self, origin, tmp_path):
        with pytest.raises(ValueError, match='CSV particionado'):
            _validate(origin, tmp_path / 'out', 'csv', 'gzip', ['anio'], False)

    def test_origen_inexistente(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            _validate(
                tmp_path / 'no_existe.parquet', tmp_path / 'out',
                'parquet', 'zstd', None, False
            )

    def test_origen_es_directorio(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            _validate(tmp_path, tmp_path / 'out', 'parquet', 'zstd', None, False)

    def test_destino_existente_sin_sobrescribir(self, origin, tmp_path):
        target = tmp_path / 'out.parquet'
        target.touch()

        with pytest.raises(FileExistsError):
            _validate(origin, target, 'parquet', 'zstd', None, False)

    def test_destino_existente_con_sobrescribir(self, origin, tmp_path):
        target = tmp_path / 'out.parquet'
        target.touch()

        _validate(origin, target, 'parquet', 'zstd', None, True)

    @pytest.mark.parametrize('relative', ['origen.parquet', '.'])
    def test_destino_coincide_o_contiene_al_origen(
        self, origin, tmp_path, relative
    ):
        with pytest.raises(ValueError, match='contiene o coincide'):
            _validate(
                origin, tmp_path / relative, 'parquet', 'zstd', None, True
            )

        assert origin.is_file()

    def test_columnas_de_particion_inexistentes(self, origin, tmp_path):
        with pytest.raises(ValueError, match='Columnas inexistentes'):
            _validate(
                origin, tmp_path / 'out', 'parquet', 'zstd',
                ['anio', 'pais'], False
            )


# =============================================================================
# convert: archivo único
# =============================================================================
class TestConvertArchivoUnico:

    def test_parquet_usa_zstd_por_defecto(self, origin, tmp_path, sample_table):
        target = tmp_path / 'out.parquet'

        convert(origin, target)

        result = pq.read_table(target)
        assert result.equals(sample_table)
        meta = pq.ParquetFile(target).metadata
        assert meta.row_group(0).column(0).compression == 'ZSTD'

    @pytest.mark.parametrize('compression', AVAILABLE_COMPRESSIONS['parquet'])
    def test_parquet_con_cada_compresion(
        self, origin, tmp_path, sample_table, compression
    ):
        if compression != 'none' and not pa.Codec.is_available(compression):
            pytest.skip(f'Códec {compression} no disponible')
        target = tmp_path / 'out.parquet'

        convert(origin, target, 'parquet', compression)

        assert pq.read_table(target).equals(sample_table)

    def test_csv_sin_compresion_por_defecto(self, origin, tmp_path, sample_table):
        target = tmp_path / 'out.csv'

        convert(origin, target, 'csv')

        assert target.read_text(encoding='utf-8').startswith('"id","nombre"')
        result = pacsv.read_csv(target)
        assert _rows_by_id(result) == _rows_by_id(sample_table)

    @pytest.mark.parametrize('compression, extension', [
        ('gzip', 'gz'),
        ('bz2', 'bz2'),
        ('lz4', 'lz4'),
        ('zstd', 'zst'),
    ])
    def test_csv_comprimido(
        self, origin, tmp_path, sample_table, compression, extension
    ):
        if not pa.Codec.is_available(compression):
            pytest.skip(f'Códec {compression} no disponible')
        target = tmp_path / f'out.csv.{extension}'

        convert(origin, target, 'csv', compression)

        with pa.CompressedInputStream(pa.OSFile(str(target)), compression) as f:
            result = pacsv.read_csv(f)
        assert _rows_by_id(result) == _rows_by_id(sample_table)

    def test_crea_directorios_intermedios(self, origin, tmp_path):
        target = tmp_path / 'a' / 'b' / 'out.parquet'

        convert(origin, target)

        assert target.is_file()

    def test_sobrescribe_destino_existente(self, origin, tmp_path, sample_table):
        target = tmp_path / 'out.parquet'
        target.write_text('contenido previo')

        convert(origin, target, overwrite=True)

        assert pq.read_table(target).equals(sample_table)

    def test_no_sobrescribe_sin_permiso(self, origin, tmp_path):
        target = tmp_path / 'out.parquet'
        target.write_text('contenido previo')

        with pytest.raises(FileExistsError):
            convert(origin, target)

        assert target.read_text() == 'contenido previo'


# =============================================================================
# convert: particionado
# =============================================================================
class TestConvertParticionado:

    def test_parquet_particionado_hive(self, origin, tmp_path, sample_table):
        target = tmp_path / 'dataset'

        convert(origin, target, 'parquet', partition=['anio', 'mes'])

        dirs = {
            p.relative_to(target).as_posix()
            for p in target.glob('*/*') if p.is_dir()
        }
        assert dirs == {
            'anio=2024/mes=1', 'anio=2024/mes=2',
            'anio=2025/mes=1', 'anio=2025/mes=2',
        }

        result = ds.dataset(target, format='parquet', partitioning='hive').to_table()
        assert result.num_rows == sample_table.num_rows
        assert sorted(result['id'].to_pylist()) == sample_table['id'].to_pylist()

    def test_parquet_particionado_aplica_compresion(self, origin, tmp_path):
        target = tmp_path / 'dataset'

        convert(origin, target, 'parquet', 'snappy', ['anio'])

        files = list(target.rglob('*.parquet'))
        assert files
        for f in files:
            meta = pq.ParquetFile(f).metadata
            assert meta.row_group(0).column(0).compression == 'SNAPPY'

    def test_csv_particionado(self, origin, tmp_path, sample_table):
        target = tmp_path / 'dataset'

        convert(origin, target, 'csv', partition=['anio'])

        assert {p.name for p in target.iterdir()} == {'anio=2024', 'anio=2025'}
        result = ds.dataset(target, format='csv', partitioning='hive').to_table()
        assert result.num_rows == sample_table.num_rows

    def test_sobrescribir_reemplaza_dataset_existente(
        self, origin, tmp_path, sample_table
    ):
        target = tmp_path / 'dataset'
        obsoleto = target / 'anio=1999' / 'viejo.parquet'
        obsoleto.parent.mkdir(parents=True)
        pq.write_table(sample_table.slice(0, 1), obsoleto)

        convert(origin, target, 'parquet', partition=['anio'], overwrite=True)

        assert not obsoleto.exists()
        assert {p.name for p in target.iterdir()} == {'anio=2024', 'anio=2025'}
        result = ds.dataset(target, format='parquet', partitioning='hive').to_table()
        assert result.num_rows == sample_table.num_rows

    def test_sobrescribir_reemplaza_directorio_con_archivo(
        self, origin, tmp_path, sample_table
    ):
        target = tmp_path / 'out.parquet'
        target.mkdir()
        (target / 'basura.txt').write_text('x')

        convert(origin, target, overwrite=True)

        assert target.is_file()
        assert pq.read_table(target).equals(sample_table)

    def test_csv_particionado_con_compresion_falla(self, origin, tmp_path):
        target = tmp_path / 'dataset'

        with pytest.raises(ValueError, match='CSV particionado'):
            convert(origin, target, 'csv', 'gzip', ['anio'])

        assert not target.exists()


# =============================================================================
# main
# =============================================================================
class TestMain:

    def _run(self, monkeypatch, *args: str) -> int:
        monkeypatch.setattr(sys, 'argv', ['convert', *args])
        return main()

    def test_exito_devuelve_cero(self, monkeypatch, capsys, origin, tmp_path):
        target = tmp_path / 'out.csv'

        code = self._run(monkeypatch, str(origin), str(target), '-f', 'csv')

        assert code == 0
        assert target.is_file()
        assert f'Listo -> {target}' in capsys.readouterr().out

    def test_pasa_argumentos_a_convert(self, monkeypatch, origin, tmp_path):
        calls = []
        monkeypatch.setattr(mod, 'convert', lambda *a: calls.append(a))
        target = tmp_path / 'dataset'

        self._run(
            monkeypatch, str(origin), str(target),
            '-f', 'csv', '-c', 'none', '-p', 'anio', 'mes', '--sobrescribir'
        )

        assert calls == [(origin, target, 'csv', 'none', ['anio', 'mes'], True)]

    def test_error_devuelve_uno(self, monkeypatch, capsys, tmp_path):
        code = self._run(
            monkeypatch, str(tmp_path / 'no_existe.parquet'),
            str(tmp_path / 'out.parquet')
        )

        assert code == 1
        assert 'Error: No existe el archivo de origen' in capsys.readouterr().err

    def test_formato_invalido_termina_argparse(self, monkeypatch, origin, tmp_path):
        with pytest.raises(SystemExit) as exc:
            self._run(
                monkeypatch, str(origin), str(tmp_path / 'out'), '-f', 'json'
            )

        assert exc.value.code == 2
