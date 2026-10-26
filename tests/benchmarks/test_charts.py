"""Pruebas unitarias de lakehouse.benchmarks.charts."""
import pytest
from PIL import Image

from lakehouse.benchmarks.charts import plot_relative, plot_times
from lakehouse.benchmarks.formats import Measurement


# =============================================================================
def measurements(formats=('parquet', 'delta', 'iceberg'), queries=('q1', 'q2', 'q3')):
    """Mediciones con tiempos distintos por formato y consulta."""
    return [
        Measurement(f, q, (0.01 * (i + 1) * (j + 1), 0.012 * (i + 1) * (j + 1)), 1)
        for j, q in enumerate(queries)
        for i, f in enumerate(formats)
    ]


# =============================================================================
def test_plot_times_escribe_png(tmp_path):
    path = plot_times(measurements(), tmp_path / 'sub' / 'tiempos.png')

    assert path == tmp_path / 'sub' / 'tiempos.png'
    with Image.open(path) as img:
        assert img.format == 'PNG' and img.width > 1000


def test_plot_times_con_numero_par_de_consultas(tmp_path):
    path = plot_times(measurements(queries=('q1', 'q2')), tmp_path / 't.png')

    assert path.exists()


def test_plot_relative_escribe_png(tmp_path):
    path = plot_relative(measurements(), tmp_path / 'relativo.png')

    with Image.open(path) as img:
        assert img.format == 'PNG'


def test_plot_relative_sin_base(tmp_path):
    with pytest.raises(ValueError, match='base'):
        plot_relative(measurements(formats=('delta', 'iceberg')), tmp_path / 'r.png')

    assert not (tmp_path / 'r.png').exists()
