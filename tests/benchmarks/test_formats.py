"""Pruebas unitarias de lakehouse.benchmarks.formats."""
import csv
import sys
from datetime import datetime
from pathlib import Path

import pytest

from lakehouse.benchmarks import formats
from lakehouse.benchmarks.formats import (
    Measurement,
    _display,
    compare_results,
    iceberg_location,
    main,
    report,
    run,
    write_csv,
)
from lakehouse.benchmarks.queries import FORMATS, QUERIES


# =============================================================================
STARTED = datetime(2026, 10, 26, 9, 30)


def fake_measurements() -> list[Measurement]:
    """Dos consultas en los tres formatos, con tiempos conocidos."""
    times = {
        ('parquet', 'q1'): (0.010, 0.012, 0.011),
        ('delta', 'q1'): (0.005, 0.006, 0.004),
        ('iceberg', 'q1'): (0.020, 0.020, 0.030),
        ('parquet', 'q2'): (0.100, 0.100, 0.100),
        ('delta', 'q2'): (0.200, 0.200, 0.200),
        ('iceberg', 'q2'): (0.050, 0.050, 0.050),
    }
    return [Measurement(f, q, t, 1) for (f, q), t in times.items()]


# =============================================================================
# Measurement
# =============================================================================
def test_measurement_estadisticas():
    m = Measurement('delta', 'q1', (0.3, 0.1, 0.2), 4)

    assert (m.median_s, m.min_s, m.max_s) == (0.2, 0.1, 0.3)


# =============================================================================
# run
# =============================================================================
class TestRun:

    def test_mide_cada_consulta_en_cada_formato(self, locations):
        measurements, results = run(locations, repetitions=2, warmup=0)

        assert [(m.query, m.format_) for m in measurements] == [
            (q, f) for q in QUERIES for f in FORMATS
        ]
        assert all(len(m.times) == 2 and min(m.times) > 0 for m in measurements)
        assert all(m.rows == len(results[m.format_, m.query]) for m in measurements)
        assert compare_results(results) == []

    def test_consultas_propias(self, locations):
        queries = {'conteo': 'SELECT count(*) FROM trips'}

        measurements, results = run(
            {'delta': locations['delta']}, queries, repetitions=1
        )

        assert [m.query for m in measurements] == ['conteo']
        assert results['delta', 'conteo'] == [(90,)]

    @pytest.mark.parametrize('kwargs, match', [
        ({'repetitions': 0}, 'repeticiones'),
        ({'warmup': -1}, 'calentamiento'),
    ])
    def test_parametros_invalidos(self, locations, kwargs, match):
        with pytest.raises(ValueError, match=match):
            run(locations, **kwargs)

    def test_formato_invalido_falla_antes_de_medir(self, locations):
        with pytest.raises(ValueError, match='no válido'):
            run({'parquet': locations['parquet'], 'csv': 'x.csv'})


# =============================================================================
# compare_results
# =============================================================================
class TestCompareResults:

    def test_iguales_con_tolerancia(self):
        results = {
            ('parquet', 'q'): [(1, 10.0), (2, 20.0)],
            ('delta', 'q'): [(1, 10.0 + 1e-12), (2, 20.0)],
        }

        assert compare_results(results) == []

    def test_distinto_numero_de_filas(self):
        results = {('parquet', 'q'): [(1,)], ('iceberg', 'q'): [(1,), (2,)]}

        assert compare_results(results) == [
            'q: iceberg devuelve 2 filas y parquet 1'
        ]

    def test_distinto_valor(self):
        results = {
            ('parquet', 'q'): [(1, 'a'), (2, 1.0)],
            ('delta', 'q'): [(1, 'a'), (2, 1.1)],
        }

        assert compare_results(results) == ['q: delta difiere de parquet en la fila 2']

    def test_sin_base_no_compara(self):
        assert compare_results({('delta', 'q'): [(1,)]}) == []


# =============================================================================
# report
# =============================================================================
class TestReport:

    def test_tablas_y_mas_rapido(self, locations):
        text = report(fake_measurements(), locations, 3, 1, STARTED, [],
                      queries={'q1': 'SELECT 1', 'q2': 'SELECT 2'})

        assert '- Fecha: 2026-10-26 09:30:00' in text
        assert '- Resultados iguales en los tres formatos: sí' in text
        assert '| q1 | 11.0 | **5.0** | 20.0 | delta |' in text
        assert '| q2 | 100.0 | 200.0 | **50.0** | iceberg |' in text
        assert '| **Total** | 111.0 | 205.0 | **70.0** | iceberg |' in text
        assert '| q2 | 1.00x | 2.00x | 0.50x |' in text
        assert '| q1 | 10.0 – 12.0 | 4.0 – 6.0 | 20.0 – 30.0 |' in text
        assert 'SELECT 2' in text
        assert 'Diferencias' not in text and 'Gráficas' not in text

    def test_diferencias_y_graficas(self, locations):
        text = report(fake_measurements(), locations, 3, 1, STARTED, ['q1: distinto'],
                      [Path('docs/x_tiempos.png')], {'q1': 'SELECT 1', 'q2': 'SELECT 2'})

        assert '- Resultados iguales en los tres formatos: no' in text
        assert '- q1: distinto' in text
        assert '![x_tiempos](x_tiempos.png)' in text

    def test_sin_parquet_omite_relativo(self, locations):
        measurements = [m for m in fake_measurements() if m.format_ != 'parquet']

        text = report(measurements, locations, 3, 1, STARTED, [],
                      queries={'q1': 'SELECT 1', 'q2': 'SELECT 2'})

        assert 'relativo' not in text
        assert '| Consulta | delta | iceberg | Más rápido |' in text


# =============================================================================
# _display
# =============================================================================
class TestDisplay:

    def test_uri_dentro_del_directorio_actual(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)

        assert _display((tmp_path / 'm' / 'v1.json').as_uri().replace('file://', 'file:')) == (
            str(Path('m/v1.json'))
        )

    def test_otras_ubicaciones_no_cambian(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path / '..')
        other = '/otra/ruta/v1.json'

        assert _display('data/x.parquet') == 'data/x.parquet'
        assert _display(other) == other


# =============================================================================
# write_csv
# =============================================================================
def test_write_csv(tmp_path):
    path = tmp_path / 'sub' / 'r.csv'

    write_csv(fake_measurements()[:1], path)

    with path.open(encoding='utf-8') as f:
        rows = list(csv.reader(f))

    assert rows[0] == ['consulta', 'formato', 'filas', 'mediana_s', 'min_s', 'max_s', 'tiempos_s']
    assert rows[1] == ['q1', 'parquet', '1', '0.011000', '0.010000', '0.012000',
                       '0.010000 0.012000 0.011000']


# =============================================================================
# iceberg_location y main
# =============================================================================
def test_iceberg_location(locations, tmp_path):
    location = iceberg_location('lakehouse.trips', tmp_path / 'catalog.db', tmp_path / 'warehouse')

    assert location == locations['iceberg']


class TestMain:

    def _argv(self, monkeypatch, locations, tmp_path, *extra):
        monkeypatch.setattr(sys, 'argv', [
            'lh-bench-formats',
            '--parquet', locations['parquet'],
            '--delta', locations['delta'],
            '--iceberg', 'lakehouse.trips',
            '--catalogo', str(tmp_path / 'catalog.db'),
            '--warehouse', str(tmp_path / 'warehouse'),
            '-r', '1', '--calentamiento', '0',
            '--dir-salida', str(tmp_path / 'salida'),
            *extra,
        ])

    def test_genera_informe_y_csv(self, locations, tmp_path, monkeypatch, capsys):
        self._argv(monkeypatch, locations, tmp_path)

        assert main() == 0

        report_md = next((tmp_path / 'salida').glob('*.md'))
        stem = report_md.stem
        assert sorted(p.name for p in (tmp_path / 'salida').iterdir()) == sorted([
            f'{stem}.csv', f'{stem}.md', f'{stem}_relativo.png', f'{stem}_tiempos.png'
        ])
        text = report_md.read_text(encoding='utf-8')
        assert f'({stem}_tiempos.png)' in text and f'({stem}_relativo.png)' in text
        assert 'Listo ->' in capsys.readouterr().out

    def test_error_si_falta_la_tabla(self, locations, tmp_path, monkeypatch, capsys):
        self._argv(monkeypatch, locations, tmp_path)
        sys.argv[sys.argv.index('lakehouse.trips')] = 'lakehouse.no_existe'

        assert main() == 1
        assert 'Error:' in capsys.readouterr().err

    def test_falla_si_los_resultados_difieren(self, locations, tmp_path, monkeypatch, capsys):
        self._argv(monkeypatch, locations, tmp_path)
        monkeypatch.setattr(formats, 'compare_results', lambda results: ['q1: distinto'])

        assert main() == 1
        assert 'Diferencia: q1: distinto' in capsys.readouterr().err
