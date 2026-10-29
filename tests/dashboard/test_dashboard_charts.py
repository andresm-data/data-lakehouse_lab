"""Pruebas unitarias de lakehouse.dashboard.charts."""
import pytest

from lakehouse.benchmarks.charts import COLORS
from lakehouse.dashboard.charts import DARK, colors, month_chart, size_chart, time_chart
from lakehouse.dashboard.data import load_sizes, load_times
from lakehouse.dashboard.time_travel import compare, month_summary


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


# =============================================================================
def test_month_chart_version_y_actual(delta_source):
    comparison = compare(month_summary(delta_source, 0), month_summary(delta_source, 2))
    version, current = month_chart(comparison, 'delta', 'v0', dark=True).data

    assert (version.name, current.name) == ('v0', 'Actual')
    assert list(version.x) == list(current.x) == ['2025-01', '2025-02', '2025-03']
    assert list(version.y) == [2, 3, 0]
    assert list(current.y) == [2, 3, 1]
    assert current.marker.color == version.marker.pattern.fgcolor == DARK['delta']
