"""Los gráficos del dashboard muestran los mismos números que el informe.

Carga el CSV de la ejecución publicada en docs/benchmarks en una base
temporal y compara cada barra del gráfico de tiempos con la tabla de
resultados del informe Markdown y con el CSV.
"""
import csv
import re
from pathlib import Path

import pytest

from lakehouse.benchmarks.results_db import csv_started, load_csv, save
from lakehouse.dashboard.charts import time_chart
from lakehouse.dashboard.data import load_times


# =============================================================================
REPORT = Path(__file__).parents[2] / 'docs/benchmarks/duckdb_formatos_20261026_103447'


def _report_table() -> dict[tuple[str, str], float]:
    """Mediana en ms por (consulta, formato), de la tabla del informe."""
    text = REPORT.with_suffix('.md').read_text(encoding='utf-8')
    section = text.split('## Resultados (ms, mediana)')[1].split('\n## ')[0]
    lines = [line for line in section.splitlines() if line.startswith('|')]
    formats = [c.strip() for c in lines[0].strip('|').split('|')][1:-1]
    table = {}

    for line in lines[2:]:
        cells = [c.strip(' *') for c in line.strip('|').split('|')]

        if re.fullmatch(r'q\d\w*', cells[0]):
            for f, value in zip(formats, cells[1:-1]):
                table[cells[0], f] = float(value.replace(',', ''))

    return table


@pytest.fixture(scope='module')
def bars() -> dict[tuple[str, str], tuple[float, float, float]]:
    """Mediana, mínimo y máximo (ms) de cada barra del gráfico de tiempos."""
    import tempfile

    path = REPORT.with_suffix('.csv')
    run = csv_started(path)

    with tempfile.TemporaryDirectory() as tmp:
        db = Path(tmp) / 'resultados.duckdb'
        save(db, run, load_csv(path), [])
        fig = time_chart(load_times(db, run))

    return {
        (q, trace.name): (y, y - lo, y + hi)
        for trace in fig.data
        for q, y, lo, hi in zip(trace.x, trace.y, trace.error_y.arrayminus, trace.error_y.array)
    }


# =============================================================================
def test_coincide_con_la_tabla_del_informe(bars):
    table = _report_table()

    assert len(table) == 18
    assert {k: round(v[0], 1) for k, v in bars.items()} == table


def test_coincide_con_el_csv(bars):
    with REPORT.with_suffix('.csv').open(encoding='utf-8') as f:
        rows = list(csv.DictReader(f))

    assert len(rows) == len(bars)

    for row in rows:
        median, low, high = bars[row['consulta'], row['formato']]
        assert median / 1000 == pytest.approx(float(row['mediana_s']), abs=1e-6)
        assert low / 1000 == pytest.approx(float(row['min_s']), abs=1e-6)
        assert high / 1000 == pytest.approx(float(row['max_s']), abs=1e-6)
