"""Pruebas unitarias del particionado por fecha de lakehouse.ingest.convert."""
import sys
from datetime import date, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import pytest

from lakehouse.ingest import convert as mod
from lakehouse.ingest.convert import (
    _expand_origins,
    _validate_by_date,
    convert_by_date,
    main,
)

COL = 'tpep_pickup_datetime'


# =============================================================================
def _month_table(ids: list[int], stamps: list[datetime]) -> pa.Table:
    return pa.table({
        'id': pa.array(ids, pa.int64()),
        COL: pa.array(stamps, pa.timestamp('us')),
        'monto': pa.array([float(i) for i in ids], pa.float64()),
    })


@pytest.fixture
def origins(tmp_path: Path) -> list[Path]:
    """Dos archivos mensuales con filas cruzadas entre meses y un atípico."""
    raw = tmp_path / 'raw'
    raw.mkdir()

    enero = raw / 'yellow_2025-01.parquet'
    pq.write_table(_month_table(
        [1, 2, 3, 4],
        [
            datetime(2024, 12, 31, 23, 50),   # fuera de rango, mes anterior
            datetime(2025, 1, 1, 0, 0),
            datetime(2025, 1, 15, 12, 0),
            datetime(2025, 2, 1, 0, 5),       # pertenece a febrero
        ]
    ), enero, row_group_size=2)

    febrero = raw / 'yellow_2025-02.parquet'
    pq.write_table(_month_table(
        [5, 6, 7, 8],
        [
            datetime(2025, 1, 31, 23, 59),    # pertenece a enero
            datetime(2025, 2, 10, 8, 0),
            datetime(2025, 2, 28, 23, 59),
            datetime(2007, 12, 5, 18, 45),    # atípico
        ]
    ), febrero, row_group_size=2)

    return [enero, febrero]


def _read(target: Path, format_: str = 'parquet') -> pa.Table:
    return ds.dataset(target, format=format_, partitioning='hive').to_table()


def _ids_by_partition(table: pa.Table, keys: list[str]) -> dict[tuple, list[int]]:
    result: dict[tuple, list[int]] = {}

    for row in table.to_pylist():
        result.setdefault(tuple(row[k] for k in keys), []).append(row['id'])

    return {k: sorted(v) for k, v in result.items()}


def _partition_dirs(target: Path, depth: int) -> set[str]:
    pattern = '/'.join(['*'] * depth)
    return {
        p.relative_to(target).as_posix()
        for p in target.glob(pattern) if p.is_dir()
    }


# =============================================================================
# _expand_origins
# =============================================================================
class TestExpandOrigins:

    def test_archivos_se_mantienen_en_orden(self, origins):
        assert _expand_origins(origins[::-1]) == origins[::-1]

    def test_directorio_se_expande_ordenado(self, origins, tmp_path):
        (tmp_path / 'raw' / 'notas.txt').write_text('x')

        assert _expand_origins([tmp_path / 'raw']) == origins

    def test_directorio_sin_parquet(self, tmp_path):
        vacio = tmp_path / 'vacio'
        vacio.mkdir()

        with pytest.raises(FileNotFoundError, match='No se encontraron'):
            _expand_origins([vacio])


# =============================================================================
# _validate_by_date
# =============================================================================
class TestValidateByDate:

    def _call(self, origins, target, **kwargs):
        params = dict(
            date_column=COL, granularity='month', start=None, end=None,
            format_='parquet', compression='zstd', overwrite=False
        )
        params.update(kwargs)
        _validate_by_date(origins, target, **params)

    def test_parametros_validos_no_lanzan(self, origins, tmp_path):
        self._call(origins, tmp_path / 'out')

    def test_granularidad_no_valida(self, origins, tmp_path):
        with pytest.raises(ValueError, match='Granularidad'):
            self._call(origins, tmp_path / 'out', granularity='hora')

    @pytest.mark.parametrize('start, end', [
        (datetime(2025, 2, 1), datetime(2025, 1, 1)),
        (datetime(2025, 1, 1), datetime(2025, 1, 1)),
    ])
    def test_inicio_no_anterior_al_fin(self, origins, tmp_path, start, end):
        with pytest.raises(ValueError, match='debe ser anterior'):
            self._call(origins, tmp_path / 'out', start=start, end=end)

    def test_columna_de_fecha_inexistente(self, origins, tmp_path):
        with pytest.raises(ValueError, match='Columnas inexistentes'):
            self._call(origins, tmp_path / 'out', date_column='fecha')

    def test_origen_inexistente(self, origins, tmp_path):
        with pytest.raises(FileNotFoundError):
            self._call(
                [*origins, tmp_path / 'no_existe.parquet'], tmp_path / 'out'
            )

    def test_destino_existente_sin_sobrescribir(self, origins, tmp_path):
        target = tmp_path / 'out'
        target.mkdir()

        with pytest.raises(FileExistsError):
            self._call(origins, target)

    def test_csv_con_compresion(self, origins, tmp_path):
        with pytest.raises(ValueError, match='CSV particionado'):
            self._call(
                origins, tmp_path / 'out', format_='csv', compression='gzip'
            )

    def test_esquemas_distintos(self, origins, tmp_path):
        otro = tmp_path / 'raw' / 'yellow_2025-03.parquet'
        tabla = _month_table([9], [datetime(2025, 3, 1)])
        pq.write_table(tabla.append_column('extra', pa.array([1])), otro)

        with pytest.raises(ValueError, match='Esquema distinto'):
            self._call([*origins, otro], tmp_path / 'out')

    def test_metadatos_distintos_no_cuentan_como_esquema_distinto(
        self, origins, tmp_path
    ):
        otro = tmp_path / 'raw' / 'yellow_2025-03.parquet'
        tabla = _month_table([9], [datetime(2025, 3, 1)])
        pq.write_table(tabla.replace_schema_metadata({'origen': 'x'}), otro)

        self._call([*origins, otro], tmp_path / 'out')

    def test_columna_no_temporal(self, origins, tmp_path):
        with pytest.raises(ValueError, match='se requiere fecha o timestamp'):
            self._call(origins, tmp_path / 'out', date_column='monto')

    @pytest.mark.parametrize('granularity, clash', [
        ('year', 'year'),
        ('month', 'month'),
        ('day', 'day'),
    ])
    def test_columna_derivada_ya_existe(self, tmp_path, granularity, clash):
        origin = tmp_path / 'origen.parquet'
        tabla = _month_table([1], [datetime(2025, 1, 1)])
        pq.write_table(tabla.append_column(clash, pa.array([1])), origin)

        with pytest.raises(ValueError, match='ya contiene las columnas'):
            self._call([origin], tmp_path / 'out', granularity=granularity)


# =============================================================================
# convert_by_date
# =============================================================================
class TestConvertByDate:

    def test_une_archivos_y_reubica_filas_por_mes(self, origins, tmp_path):
        target = tmp_path / 'yellow'

        convert_by_date(origins, target, COL)

        assert _partition_dirs(target, 2) == {
            'year=2007/month=12', 'year=2024/month=12',
            'year=2025/month=1', 'year=2025/month=2',
        }
        assert _ids_by_partition(_read(target), ['year', 'month']) == {
            (2007, 12): [8],
            (2024, 12): [1],
            (2025, 1): [2, 3, 5],
            (2025, 2): [4, 6, 7],
        }

    def test_conserva_columnas_y_valores(self, origins, tmp_path):
        target = tmp_path / 'yellow'

        convert_by_date(origins, target, COL)

        result = _read(target).select(['id', COL, 'monto']).sort_by('id')
        expected = pa.concat_tables(pq.read_table(o) for o in origins)
        assert result.equals(expected.sort_by('id'))

    def test_un_archivo_por_particion(self, origins, tmp_path):
        target = tmp_path / 'yellow'

        convert_by_date(origins, target, COL)

        for partition in _partition_dirs(target, 2):
            assert len(list((target / partition).iterdir())) == 1

    def test_granularidad_year(self, origins, tmp_path):
        target = tmp_path / 'yellow'

        convert_by_date(origins, target, COL, 'year')

        assert _partition_dirs(target, 1) == {'year=2007', 'year=2024', 'year=2025'}
        assert _partition_dirs(target, 2) == set()

    def test_granularidad_dia(self, origins, tmp_path):
        target = tmp_path / 'yellow'

        convert_by_date(origins, target, COL, 'day')

        result = _ids_by_partition(_read(target), ['year', 'month', 'day'])
        assert result[(2025, 1, 31)] == [5]
        assert result[(2025, 2, 1)] == [4]
        assert len(result) == 8

    def test_filtro_desde_y_hasta(self, origins, tmp_path):
        target = tmp_path / 'yellow'

        convert_by_date(
            origins, target, COL,
            start=datetime(2025, 1, 1), end=datetime(2025, 2, 28, 23, 59)
        )

        assert _ids_by_partition(_read(target), ['year', 'month']) == {
            (2025, 1): [2, 3, 5],
            (2025, 2): [4, 6],
        }

    def test_filtro_solo_desde(self, origins, tmp_path):
        target = tmp_path / 'yellow'

        convert_by_date(origins, target, COL, start=datetime(2025, 2, 1))

        assert sorted(_read(target)['id'].to_pylist()) == [4, 6, 7]

    def test_filtro_solo_hasta(self, origins, tmp_path):
        target = tmp_path / 'yellow'

        convert_by_date(origins, target, COL, end=datetime(2025, 1, 1))

        assert sorted(_read(target)['id'].to_pylist()) == [1, 8]

    def test_columna_de_tipo_date(self, tmp_path):
        origin = tmp_path / 'origen.parquet'
        pq.write_table(pa.table({
            'id': [1, 2, 3],
            'fecha': pa.array(
                [date(2025, 1, 5), date(2025, 2, 5), date(2025, 2, 6)],
                pa.date32()
            ),
        }), origin)
        target = tmp_path / 'out'

        convert_by_date([origin], target, 'fecha', start=datetime(2025, 1, 1))

        assert _ids_by_partition(_read(target), ['year', 'month']) == {
            (2025, 1): [1],
            (2025, 2): [2, 3],
        }

    def test_origen_como_directorio(self, origins, tmp_path):
        target = tmp_path / 'yellow'

        convert_by_date([tmp_path / 'raw'], target, COL)

        assert _read(target).num_rows == 8

    def test_compresion_por_defecto_y_explicita(self, origins, tmp_path):
        por_defecto = tmp_path / 'zstd'
        snappy = tmp_path / 'snappy'

        convert_by_date(origins, por_defecto, COL)
        convert_by_date(origins, snappy, COL, compression='snappy')

        for target, codec in [(por_defecto, 'ZSTD'), (snappy, 'SNAPPY')]:
            for f in target.rglob('*.parquet'):
                meta = pq.ParquetFile(f).metadata
                assert meta.row_group(0).column(0).compression == codec

    def test_salida_csv(self, origins, tmp_path):
        target = tmp_path / 'yellow'

        convert_by_date(origins, target, COL, format_='csv')

        files = list(target.rglob('*.csv'))
        assert len(files) == 4
        total = sum(pacsv.read_csv(f).num_rows for f in files)
        assert total == 8

    def test_sobrescribir_reemplaza_dataset(self, origins, tmp_path):
        target = tmp_path / 'yellow'
        obsoleto = target / 'year=1999' / 'month=1' / 'viejo.parquet'
        obsoleto.parent.mkdir(parents=True)
        pq.write_table(_month_table([99], [datetime(1999, 1, 1)]), obsoleto)

        convert_by_date(origins, target, COL, overwrite=True)

        assert not obsoleto.exists()
        assert 99 not in _read(target)['id'].to_pylist()

    def test_sobrescribir_sin_destino_previo(self, origins, tmp_path):
        target = tmp_path / 'yellow'

        convert_by_date(origins, target, COL, overwrite=True)

        assert _read(target).num_rows == 8

    def test_validacion_fallida_no_escribe(self, origins, tmp_path):
        target = tmp_path / 'yellow'

        with pytest.raises(ValueError):
            convert_by_date(origins, target, COL, granularity='hora')

        assert not target.exists()


# =============================================================================
# main con --particion-fecha
# =============================================================================
class TestMainByDate:

    def _run(self, monkeypatch, *args: str) -> int:
        monkeypatch.setattr(sys, 'argv', ['lh-convert', *args])
        return main()

    def test_exito_extremo_a_extremo(self, monkeypatch, capsys, origins, tmp_path):
        target = tmp_path / 'yellow'

        code = self._run(
            monkeypatch, *map(str, origins), str(target),
            '--particion-fecha', COL,
            '--desde', '2025-01-01', '--hasta', '2025-03-01'
        )

        assert code == 0
        assert f'Listo -> {target}' in capsys.readouterr().out
        assert sorted(_read(target)['id'].to_pylist()) == [2, 3, 4, 5, 6, 7]

    def test_pasa_argumentos_a_convert_by_date(
        self, monkeypatch, origins, tmp_path
    ):
        calls = []
        monkeypatch.setattr(mod, 'convert_by_date', lambda *a: calls.append(a))
        target = tmp_path / 'yellow'

        self._run(
            monkeypatch, *map(str, origins), str(target),
            '-f', 'csv', '-c', 'none', '--particion-fecha', COL,
            '--granularidad', 'day', '--desde', '2025-01-01',
            '--hasta', '2025-02-01T12:00', '--sobrescribir'
        )

        assert calls == [(
            origins, target, COL, 'day',
            datetime(2025, 1, 1), datetime(2025, 2, 1, 12, 0),
            'csv', 'none', True
        )]

    def test_granularidad_por_defecto_es_mes(self, monkeypatch, origins, tmp_path):
        calls = []
        monkeypatch.setattr(mod, 'convert_by_date', lambda *a: calls.append(a))

        self._run(
            monkeypatch, str(origins[0]), str(tmp_path / 'out'),
            '--particion-fecha', COL
        )

        assert calls[0][3] == 'month'

    def test_error_devuelve_uno(self, monkeypatch, capsys, origins, tmp_path):
        code = self._run(
            monkeypatch, *map(str, origins), str(tmp_path / 'out'),
            '--particion-fecha', 'monto'
        )

        assert code == 1
        assert 'se requiere fecha o timestamp' in capsys.readouterr().err

    @pytest.mark.parametrize('extra', [
        ['--desde', '2025-01-01'],
        ['--hasta', '2025-01-01'],
        ['--granularidad', 'day'],
    ])
    def test_opciones_de_fecha_requieren_particion_fecha(
        self, monkeypatch, capsys, origins, tmp_path, extra
    ):
        with pytest.raises(SystemExit) as exc:
            self._run(monkeypatch, str(origins[0]), str(tmp_path / 'out'), *extra)

        assert exc.value.code == 2
        assert 'requieren --particion-fecha' in capsys.readouterr().err

    def test_varios_origenes_requieren_particion_fecha(
        self, monkeypatch, capsys, origins, tmp_path
    ):
        with pytest.raises(SystemExit) as exc:
            self._run(monkeypatch, *map(str, origins), str(tmp_path / 'out'))

        assert exc.value.code == 2
        assert 'requieren --particion-fecha' in capsys.readouterr().err

    def test_directorio_requiere_particion_fecha(
        self, monkeypatch, capsys, origins, tmp_path
    ):
        with pytest.raises(SystemExit) as exc:
            self._run(monkeypatch, str(tmp_path / 'raw'), str(tmp_path / 'out'))

        assert exc.value.code == 2

    def test_particiones_y_particion_fecha_son_excluyentes(
        self, monkeypatch, origins, tmp_path
    ):
        with pytest.raises(SystemExit) as exc:
            self._run(
                monkeypatch, str(origins[0]), str(tmp_path / 'out'),
                '-p', 'id', '--particion-fecha', COL
            )

        assert exc.value.code == 2

    def test_fecha_con_formato_invalido(self, monkeypatch, origins, tmp_path):
        with pytest.raises(SystemExit) as exc:
            self._run(
                monkeypatch, str(origins[0]), str(tmp_path / 'out'),
                '--particion-fecha', COL, '--desde', '01/01/2025'
            )

        assert exc.value.code == 2
