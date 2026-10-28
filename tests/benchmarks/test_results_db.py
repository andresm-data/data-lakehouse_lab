"""Pruebas unitarias de lakehouse.benchmarks.results_db."""
import sys
from datetime import datetime
from pathlib import Path

import duckdb
import pytest

from lakehouse.benchmarks.formats import Measurement, write_csv
from lakehouse.benchmarks.results_db import (
    TableSize,
    csv_started,
    latest_csv,
    load_csv,
    main,
    save,
    table_size,
    table_sizes,
)


# =============================================================================
STARTED = datetime(2026, 10, 26, 10, 34, 47)
MEASUREMENTS = [
    Measurement('parquet', 'q1', (0.010, 0.012, 0.011), 1),
    Measurement('delta', 'q1', (0.005, 0.006, 0.004), 1),
    Measurement('iceberg', 'q1', (0.020, 0.020, 0.030), 1),
]
SIZES = [
    TableSize('parquet', 1, 100, 100),
    TableSize('delta', 2, 120, 150),
    TableSize('iceberg', 3, 90, 130),
]


def _rows(db: Path, sql: str) -> list[tuple]:
    with duckdb.connect(str(db), read_only=True) as con:
        return con.sql(sql).fetchall()


# =============================================================================
# table_size
# =============================================================================
class TestTableSize:

    def test_parquet_es_un_solo_archivo(self, locations):
        size = table_size('parquet', locations['parquet'])

        assert size.files == 1
        assert size.current_bytes == size.disk_bytes == Path(locations['parquet']).stat().st_size

    def test_parquet_en_directorio(self, tmp_path):
        (tmp_path / 'a.parquet').write_bytes(b'x' * 10)
        (tmp_path / 'b.parquet').write_bytes(b'x' * 5)

        assert table_size('parquet', str(tmp_path)) == TableSize('parquet', 2, 15, 15)

    def test_parquet_inexistente(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            table_size('parquet', str(tmp_path / 'no_existe.parquet'))

    @pytest.mark.parametrize('format_', ['delta', 'iceberg'])
    def test_datos_actuales_caben_en_disco(self, locations, format_):
        size = table_size(format_, locations[format_])

        assert size.files > 0
        assert 0 < size.current_bytes < size.disk_bytes  # el disco suma log o metadatos

    def test_delta_no_cuenta_archivos_reemplazados(self, locations, trips):
        from deltalake import write_deltalake

        before = table_size('delta', locations['delta'])
        write_deltalake(locations['delta'], trips, mode='overwrite', partition_by=['year', 'month'])
        after = table_size('delta', locations['delta'])

        assert after.files == before.files
        assert after.disk_bytes > before.disk_bytes + before.current_bytes * 0.9

    def test_formato_invalido(self):
        with pytest.raises(ValueError, match='Formato no válido'):
            table_size('orc', 'x')

    def test_table_sizes_respeta_el_orden(self, locations):
        assert [s.format_ for s in table_sizes(locations)] == list(locations)


# =============================================================================
# save
# =============================================================================
class TestSave:

    def test_crea_las_tablas(self, tmp_path):
        db = tmp_path / 'sub/resultados.duckdb'
        save(db, STARTED, MEASUREMENTS, SIZES)

        assert _rows(db, 'SELECT formato, mediana_s, min_s, max_s, tiempos_s FROM tiempos') == [
            ('parquet', 0.011, 0.010, 0.012, [0.010, 0.012, 0.011]),
            ('delta', 0.005, 0.004, 0.006, [0.005, 0.006, 0.004]),
            ('iceberg', 0.020, 0.020, 0.030, [0.020, 0.020, 0.030]),
        ]
        assert _rows(db, 'SELECT ejecucion, formato, archivos, bytes_actuales, bytes_disco '
                         'FROM tamanos') == [
            (STARTED, s.format_, s.files, s.current_bytes, s.disk_bytes) for s in SIZES
        ]

    def test_reemplaza_la_misma_ejecucion(self, tmp_path):
        db = tmp_path / 'resultados.duckdb'
        save(db, STARTED, MEASUREMENTS, SIZES)
        save(db, STARTED, MEASUREMENTS[:1], SIZES[:1])

        assert _rows(db, 'SELECT count(*) FROM tiempos') == [(1,)]
        assert _rows(db, 'SELECT count(*) FROM tamanos') == [(1,)]

    def test_acumula_ejecuciones(self, tmp_path):
        db = tmp_path / 'resultados.duckdb'
        save(db, STARTED, MEASUREMENTS, SIZES)
        save(db, datetime(2026, 10, 27), MEASUREMENTS, [])

        assert _rows(db, 'SELECT count(DISTINCT ejecucion) FROM tiempos') == [(2,)]
        assert _rows(db, 'SELECT count(*) FROM tamanos') == [(3,)]


# =============================================================================
# CSV
# =============================================================================
def test_load_csv_lee_lo_que_escribe_write_csv(tmp_path):
    path = tmp_path / 'r.csv'
    write_csv(MEASUREMENTS, path)

    assert load_csv(path) == MEASUREMENTS


def test_csv_started():
    assert csv_started(Path('docs/duckdb_formatos_20261026_103447.csv')) == STARTED


def test_latest_csv(tmp_path):
    assert latest_csv(tmp_path) is None

    for stamp in ('20261026_103447', '20261101_080000', '20261027_235959'):
        (tmp_path / f'duckdb_formatos_{stamp}.csv').touch()
    (tmp_path / 'compresion_parquet_20261231_000000.csv').touch()

    assert latest_csv(tmp_path).name == 'duckdb_formatos_20261101_080000.csv'


# =============================================================================
# main
# =============================================================================
class TestMain:

    def _argv(self, monkeypatch, locations, tmp_path, *extra):
        monkeypatch.setattr(sys, 'argv', [
            'lh-bench-db',
            '--db', str(tmp_path / 'resultados.duckdb'),
            '--parquet', locations['parquet'],
            '--delta', locations['delta'],
            '--iceberg', 'lakehouse.trips',
            '--catalogo', str(tmp_path / 'catalog.db'),
            '--warehouse', str(tmp_path / 'warehouse'),
            *extra,
        ])

    def test_carga_el_csv_y_mide_las_tablas(self, locations, tmp_path, monkeypatch, capsys):
        path = tmp_path / 'duckdb_formatos_20261026_103447.csv'
        write_csv(MEASUREMENTS, path)
        self._argv(monkeypatch, locations, tmp_path, '--csv', str(path))

        assert main() == 0

        db = tmp_path / 'resultados.duckdb'
        assert _rows(db, 'SELECT DISTINCT ejecucion FROM tiempos') == [(STARTED,)]
        assert [r[0] for r in _rows(db, 'SELECT formato FROM tamanos')] == [
            'parquet', 'delta', 'iceberg'
        ]
        assert '3 mediciones y 3 tamaños' in capsys.readouterr().out

    def test_usa_el_csv_mas_reciente(self, locations, tmp_path, monkeypatch):
        monkeypatch.setattr('lakehouse.benchmarks.results_db.DOCS_DIR', tmp_path)
        write_csv(MEASUREMENTS, tmp_path / 'benchmarks/duckdb_formatos_20261026_103447.csv')
        self._argv(monkeypatch, locations, tmp_path)

        assert main() == 0

    def test_sin_csv(self, locations, tmp_path, monkeypatch, capsys):
        monkeypatch.setattr('lakehouse.benchmarks.results_db.DOCS_DIR', tmp_path)
        self._argv(monkeypatch, locations, tmp_path)

        assert main() == 1
        assert 'no hay CSV' in capsys.readouterr().err

    def test_error_si_falta_la_tabla(self, locations, tmp_path, monkeypatch, capsys):
        path = tmp_path / 'duckdb_formatos_20261026_103447.csv'
        write_csv(MEASUREMENTS, path)
        self._argv(monkeypatch, locations, tmp_path, '--csv', str(path))
        sys.argv[sys.argv.index('lakehouse.trips')] = 'lakehouse.no_existe'

        assert main() == 1
        assert 'Error:' in capsys.readouterr().err
