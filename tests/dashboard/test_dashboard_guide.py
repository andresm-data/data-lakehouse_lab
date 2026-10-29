"""Pruebas unitarias de lakehouse.dashboard.guide."""
import pandas as pd
import pytest

from lakehouse.benchmarks.queries import FORMATS
from lakehouse.dashboard.data import load_sizes, load_times
from lakehouse.dashboard.guide import (
    CAPABILITIES, COUNT_QUERY, DECISIONS, GUIDE, NAMES, evidence, measured, to_markdown,
)


# =============================================================================
@pytest.fixture
def ev(db, saved_runs) -> pd.DataFrame:
    return evidence(load_times(db, saved_runs[0]), load_sizes(db, saved_runs[0]))


# =============================================================================
def test_contenido_de_los_tres_formatos():
    assert list(GUIDE) == list(NAMES) == list(FORMATS)
    assert all(set(g) == {'usar', 'evitar', 'proyecto'} for g in GUIDE.values())
    assert list(CAPABILITIES.columns) == ['Capacidad', 'Parquet', 'Delta Lake', 'Iceberg']
    assert set(DECISIONS['Usa']) <= {'Parquet', 'Delta Lake', 'Iceberg', 'Iceberg o Delta Lake'}


# =============================================================================
class TestEvidence:

    def test_totales_y_relativo(self, ev):
        assert list(ev['formato']) == ['parquet', 'delta', 'iceberg']
        assert list(ev['total_ms']) == pytest.approx([78, 47, 152])
        assert list(ev['relativo']) == pytest.approx([1, 47 / 78, 152 / 78])

    def test_consultas_ganadas(self, ev):
        # q1: iceberg 2 ms; q2: delta 40 ms.
        assert list(ev['ganadas']) == [0, 1, 1]
        assert set(ev['consultas']) == {2}

    def test_tamanos(self, ev):
        assert list(ev['archivos']) == [1, 6, 5]
        assert list(ev['actual_mb']) == [2, 1, 3]
        assert list(ev['disco_mb']) == [2, 5, 4]

    def test_sin_la_consulta_de_conteo(self, ev):
        assert ev['conteo_ms'].isna().all()

    def test_con_la_consulta_de_conteo(self, db, saved_runs):
        times = load_times(db, saved_runs[0]).replace({'consulta': {'q1': COUNT_QUERY}})
        ev = evidence(times, load_sizes(db, saved_runs[0]))

        assert list(ev['conteo_ms']) == pytest.approx([18, 7, 2])

    def test_sin_tamanos(self, db, saved_runs):
        ev = evidence(load_times(db, saved_runs[0]), load_sizes(db, saved_runs[0]).iloc[:0])

        assert ev['actual_mb'].isna().all()


# =============================================================================
class TestMeasured:

    def test_frases_con_las_cifras(self, ev):
        delta = measured(ev.iloc[1])

        assert delta[0] == ('Total de las 2 consultas: **47.0 ms** (0.60x Parquet); '
                            'el más rápido en 1 de 2.')
        assert delta[1] == 'Tamaño: 1.0 MB en la versión actual y 5.0 MB en disco (6 archivos).'

    def test_enteros_de_una_fila_con_columnas_mixtas(self, ev):
        # Con .loc, pandas pasa las columnas enteras de la fila a float.
        row = ev.set_index('formato').loc['delta']

        assert row['consultas'] == 2.0
        assert measured(row)[0].endswith('el más rápido en 1 de 2.')

    def test_un_archivo_en_singular(self, ev):
        assert measured(ev.iloc[0])[-1].endswith('(1 archivo).')

    def test_incluye_el_conteo_si_se_midio(self, db, saved_runs):
        times = load_times(db, saved_runs[0]).replace({'consulta': {'q1': COUNT_QUERY}})
        ev = evidence(times, load_sizes(db, saved_runs[0]))

        assert '`count(*)`: 2.0 ms.' in measured(ev.iloc[2])

    def test_sin_tamanos_no_los_menciona(self, db, saved_runs):
        ev = evidence(load_times(db, saved_runs[0]), load_sizes(db, saved_runs[0]).iloc[:0])

        assert len(measured(ev.iloc[0])) == 1


# =============================================================================
def test_to_markdown():
    df = pd.DataFrame([('`a`', 'b | c')], columns=['X', 'Y'])

    assert to_markdown(df) == '| X | Y |\n|---|---|\n| `a` | b \\| c |'
    assert to_markdown(CAPABILITIES).count('\n') == len(CAPABILITIES) + 1
