"""Pruebas unitarias de lakehouse.dashboard.charts."""
import pytest

from lakehouse.benchmarks.charts import COLORS
from lakehouse.dashboard.charts import DARK, colors, size_chart, time_chart
from lakehouse.dashboard.data import load_sizes, load_times


# =============================================================================
@pytest.fixture
def sizes(db, saved_runs):
    return load_sizes(db, saved_runs[0])


@pytest.fixture
def times(db, saved_runs):
    return load_times(db, saved_runs[0])


# =============================================================================
def test_colors_por_tema():
    assert colors() == COLORS
    assert colors(dark=True) == DARK
    assert set(DARK) == set(COLORS)


# =============================================================================
class TestSizeChart:

    def test_actual_y_disco_por_formato(self, sizes):
        current, disk = size_chart(sizes).data

        assert list(current.x) == list(disk.x) == ['parquet', 'delta', 'iceberg']
        assert list(current.y) == [2, 1, 3]
        assert list(disk.y) == [2, 5, 4]

    def test_color_del_formato_en_las_dos_barras(self, sizes):
        current, disk = size_chart(sizes, dark=True).data
        expected = [DARK[f] for f in sizes['formato']]

        assert list(current.marker.color) == expected
        assert list(disk.marker.pattern.fgcolor) == expected


# =============================================================================
class TestTimeChart:

    def test_una_serie_por_formato(self, times):
        fig = time_chart(times)

        assert [t.name for t in fig.data] == ['parquet', 'delta', 'iceberg']
        assert [t.marker.color for t in fig.data] == [COLORS[t.name] for t in fig.data]

    def test_mediana_y_rango(self, times):
        parquet = time_chart(times).data[0]

        assert list(parquet.x) == ['q1', 'q2']
        assert list(parquet.y) == pytest.approx([18, 60])
        # Las barras de error van de la mediana al mínimo y al máximo.
        assert list(parquet.error_y.arrayminus) == pytest.approx([1, 10])
        assert list(parquet.error_y.array) == pytest.approx([1, 10])

    def test_escala(self, times):
        assert time_chart(times).layout.yaxis.type is None
        assert time_chart(times, log=True).layout.yaxis.type == 'log'
