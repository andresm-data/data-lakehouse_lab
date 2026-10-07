"""Pruebas unitarias de lakehouse.ingest._io."""
from pathlib import Path

import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq
import pytest

from lakehouse.ingest._io import (
    CSV_SUFFIXES,
    common_schema,
    expand_origins,
    input_format,
    read_schema,
    scan,
)

CODECS = {'.csv': 'none', '.csv.gz': 'gzip', '.csv.bz2': 'bz2',
          '.csv.lz4': 'lz4', '.csv.zst': 'zstd'}


# =============================================================================
def write_csv(table: pa.Table, path: Path) -> Path:
    """Escribe un CSV comprimido según su extensión."""
    codec = next(c for s, c in CODECS.items() if path.name.endswith(s))
    path.parent.mkdir(parents=True, exist_ok=True)

    if codec == 'none':
        pacsv.write_csv(table, path)

    else:
        with pa.CompressedOutputStream(str(path), codec) as out:
            pacsv.write_csv(table, out)

    return path


@pytest.fixture
def table() -> pa.Table:
    return pa.table({'id': [1, 2, 3], 'monto': [1.5, 2.0, 3.25], 'nombre': ['a', 'b', 'c']})


# =============================================================================
class TestInputFormat:

    def test_parquet(self):
        assert input_format(Path('x.parquet')) == 'parquet'

    @pytest.mark.parametrize('suffix', [*CSV_SUFFIXES, '.CSV', '.CSV.GZ'])
    def test_csv(self, suffix):
        assert input_format(Path(f'x{suffix}')) == 'csv'

    @pytest.mark.parametrize('name', ['x.txt', 'x.json', 'x.csv.7z', 'parquet'])
    def test_no_soportado(self, name):
        with pytest.raises(ValueError, match='no soportado'):
            input_format(Path(name))


# =============================================================================
class TestReadSchema:

    @pytest.mark.parametrize('suffix', CSV_SUFFIXES)
    def test_csv_comprimido_o_no(self, tmp_path, table, suffix):
        if CODECS[suffix] != 'none' and not pa.Codec.is_available(CODECS[suffix]):
            pytest.skip('Códec no disponible')
        path = write_csv(table, tmp_path / f'x{suffix}')

        assert read_schema(path) == table.schema

    def test_parquet(self, tmp_path, table):
        path = tmp_path / 'x.parquet'
        pq.write_table(table, path)

        assert read_schema(path).equals(table.schema, check_metadata=False)


# =============================================================================
class TestExpandOrigins:

    def test_archivos_se_mantienen_en_orden(self, tmp_path):
        files = [tmp_path / 'b.csv', tmp_path / 'a.csv']

        assert expand_origins(files) == (files, None)

    def test_directorio_recursivo_ordenado(self, tmp_path, table):
        base = tmp_path / 'ds'
        a = write_csv(table, base / 'year=2025' / 'month=2' / 'part-0.csv')
        b = write_csv(table, base / 'year=2025' / 'month=1' / 'part-0.csv.gz')
        (base / 'year=2025' / '_SUCCESS').write_text('')
        (base / 'notas.txt').write_text('x')

        assert expand_origins([base]) == ([b, a], base)

    def test_directorio_combinado_con_otros_origenes(self, tmp_path):
        (tmp_path / 'ds').mkdir()

        with pytest.raises(ValueError, match='no puede combinarse'):
            expand_origins([tmp_path / 'ds', tmp_path / 'x.csv'])

    def test_directorio_sin_archivos_soportados(self, tmp_path):
        (tmp_path / 'notas.txt').write_text('x')

        with pytest.raises(FileNotFoundError, match='No se encontraron'):
            expand_origins([tmp_path])


# =============================================================================
class TestCommonSchema:

    def test_no_mezcla_parquet_y_csv(self, tmp_path, table):
        p = tmp_path / 'a.parquet'
        pq.write_table(table, p)
        c = write_csv(table, tmp_path / 'b.csv')

        with pytest.raises(ValueError, match='mezclar'):
            common_schema([p, c])

    def test_csv_con_columnas_distintas(self, tmp_path, table):
        a = write_csv(table, tmp_path / 'a.csv')
        b = write_csv(table.rename_columns(['id', 'monto', 'otro']), tmp_path / 'b.csv')

        with pytest.raises(ValueError, match='Esquema distinto'):
            common_schema([a, b])

    def test_csv_con_tipos_inferidos_distintos_se_acepta(self, tmp_path, table):
        a = write_csv(table, tmp_path / 'a.csv')
        b = write_csv(table.set_column(1, 'monto', pa.array([1, 2, 3])), tmp_path / 'b.csv')

        assert common_schema([a, b]) == table.schema

    def test_particiones_numericas_y_de_texto(self, tmp_path, table):
        base = tmp_path / 'ds'
        files = [
            write_csv(table, base / 'year=2024' / 'zona=norte' / 'p.csv'),
            write_csv(table, base / 'year=2025' / 'zona=sur' / 'p.csv'),
        ]

        schema = common_schema(files, base)

        assert schema.names[-2:] == ['year', 'zona']
        assert schema.field('year').type == pa.int32()
        assert schema.field('zona').type == pa.string()

    def test_particion_que_ya_es_columna(self, tmp_path, table):
        base = tmp_path / 'ds'
        f = write_csv(table, base / 'id=1' / 'p.csv')

        with pytest.raises(ValueError, match='ya existen como columnas'):
            common_schema([f], base)


# =============================================================================
class TestScan:

    def test_csv_usa_los_tipos_del_primer_archivo(self, tmp_path, table):
        a = write_csv(table, tmp_path / 'a.csv')
        b = write_csv(table.set_column(1, 'monto', pa.array([4, 5, 6])), tmp_path / 'b.csv.gz')
        schema = common_schema([a, b])

        result = pa.Table.from_batches(scan([a, b], schema))

        assert result.schema.field('monto').type == pa.float64()
        assert result['monto'].to_pylist() == [1.5, 2.0, 3.25, 4.0, 5.0, 6.0]

    def test_agrega_particiones_y_nulos_si_faltan(self, tmp_path, table):
        base = tmp_path / 'ds'
        files = [
            write_csv(table, base / 'year=2024' / 'zona=norte' / 'p.csv'),
            write_csv(table, base / 'year=2025' / 'p.csv'),
        ]
        schema = common_schema(files, base)

        result = pa.Table.from_batches(scan(files, schema, base))

        assert result['year'].to_pylist() == [2024] * 3 + [2025] * 3
        assert result['zona'].to_pylist() == ['norte'] * 3 + [None] * 3


# =============================================================================
class TestWidenCsvTypes:
    """El tipo se infiere con el primer bloque (1 MB) y se amplía si hace falta."""

    ROWS = 300_000

    def _write(self, path: Path, head: str, tail: str) -> Path:
        lines = ['id,valor'] + [f'{i},{head}' for i in range(self.ROWS)]
        lines.append(f'{self.ROWS},{tail}')
        path.write_text('\n'.join(lines) + '\n')
        return path

    @pytest.mark.parametrize('head, tail, expected', [
        ('0', '0.75', pa.float64()),
        ('0', 'abc', pa.string()),
        ('', '7', pa.int64()),
        ('', '2.5', pa.float64()),
        ('', 'abc', pa.string()),
        ('', '', pa.null()),
        ('1', '2', pa.int64()),
    ])
    def test_amplia_segun_valores_posteriores(self, tmp_path, head, tail, expected):
        path = self._write(tmp_path / 'x.csv', head, tail)

        schema = common_schema([path])

        assert schema.field('valor').type == expected
        result = pa.Table.from_batches(scan([path], schema))
        assert result.num_rows == self.ROWS + 1

    def test_amplia_con_valores_de_otro_archivo(self, tmp_path):
        a = write_csv(pa.table({'id': [1], 'valor': [1]}), tmp_path / 'a.csv')
        b = write_csv(pa.table({'id': [2], 'valor': [0.5]}), tmp_path / 'b.csv.gz')

        schema = common_schema([a, b])

        assert schema.field('valor').type == pa.float64()
        result = pa.Table.from_batches(scan([a, b], schema))
        assert result['valor'].to_pylist() == [1.0, 0.5]

    def test_columnas_no_sospechosas_no_cambian(self, tmp_path, table):
        path = write_csv(table, tmp_path / 'x.csv')

        assert common_schema([path]) == table.schema

    def test_sin_columnas_enteras_ni_vacias(self, tmp_path):
        table = pa.table({'monto': [1.5], 'nombre': ['a']})
        path = write_csv(table, tmp_path / 'x.csv')

        assert common_schema([path]) == table.schema
