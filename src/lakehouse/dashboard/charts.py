"""Gráficos de Plotly del dashboard: tamaño y tiempo por formato, y viajes
por mes en una versión anterior.

Usan los mismos colores que las gráficas PNG del benchmark
(`lakehouse.benchmarks.charts.COLORS`); en el tema oscuro, los tonos de la
misma paleta ajustados para fondo oscuro.
"""
import pandas as pd
import plotly.graph_objects as go

from lakehouse.benchmarks.charts import COLORS as LIGHT


# =============================================================================
DARK = {'parquet': '#3987e5', 'delta': '#d95926', 'iceberg': '#199e70'}


# =============================================================================
def colors(dark: bool = False) -> dict[str, str]:
    """Color de cada formato para el tema claro u oscuro."""
    return DARK if dark else LIGHT


# =============================================================================
def _layout(fig: go.Figure, y_title: str) -> go.Figure:
    """Barras redondeadas, separadas, con la leyenda arriba y sin fondo."""
    fig.update_layout(
        barmode='group',
        bargap=0.3,
        bargroupgap=0.08,
        barcornerradius=4,
        legend={'orientation': 'h', 'yanchor': 'bottom', 'y': 1.02, 'x': 0},
        margin={'t': 40, 'r': 10, 'b': 10, 'l': 10},
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        hoverlabel={'namelength': -1},
    )
    fig.update_yaxes(title=y_title, rangemode='tozero')
    fig.update_xaxes(title=None)

    return fig


# =============================================================================
def size_chart(sizes: pd.DataFrame, dark: bool = False) -> go.Figure:
    """Tamaño de cada formato: versión actual (lleno) y en disco (rayado).

    Args:
        sizes: Resultado de `data.load_sizes`.
        dark (opcional): Colores del tema oscuro.
    """
    palette = [colors(dark)[f] for f in sizes['formato']]
    hover = '<b>%{x}</b><br>%{fullData.name}: %{y:,.1f} MB<br>%{customdata} archivos<extra></extra>'
    fig = go.Figure([
        go.Bar(
            name='Versión actual',
            x=sizes['formato'], y=sizes['actual_mb'],
            marker={'color': palette},
            text=sizes['actual_mb'], texttemplate='%{text:,.1f}', textposition='outside',
            customdata=sizes['archivos'], hovertemplate=hover,
        ),
        go.Bar(
            name='En disco (con versiones anteriores)',
            x=sizes['formato'], y=sizes['disco_mb'],
            marker={'color': 'rgba(0,0,0,0)', 'line': {'color': palette, 'width': 1.5},
                    'pattern': {'shape': '/', 'fgcolor': palette, 'solidity': 0.3}},
            text=sizes['disco_mb'], texttemplate='%{text:,.1f}', textposition='outside',
            customdata=sizes['archivos'], hovertemplate=hover,
        ),
    ])

    return _layout(fig, 'MB')


# =============================================================================
def time_chart(times: pd.DataFrame, log: bool = False, dark: bool = False) -> go.Figure:
    """Mediana por consulta y formato, con el rango mínimo – máximo.

    Args:
        times: Resultado de `data.load_times`.
        log (opcional): Eje en escala logarítmica, para ver juntas las
            consultas de pocos ms y las de cientos.
        dark (opcional): Colores del tema oscuro.
    """
    fig = go.Figure()

    for format_, df in times.groupby('formato', sort=False):
        fig.add_bar(
            name=format_,
            x=df['consulta'], y=df['mediana_ms'],
            marker={'color': colors(dark)[format_]},
            error_y={
                'type': 'data', 'symmetric': False, 'thickness': 1, 'width': 3,
                'array': df['max_ms'] - df['mediana_ms'],
                'arrayminus': df['mediana_ms'] - df['min_ms'],
            },
            customdata=df[['min_ms', 'max_ms']],
            hovertemplate=(
                '<b>%{x}</b> · %{fullData.name}<br>mediana %{y:,.1f} ms'
                '<br>rango %{customdata[0]:,.1f} – %{customdata[1]:,.1f} ms<extra></extra>'
            ),
        )

    _layout(fig, 'ms (mediana)')

    if log:
        fig.update_yaxes(type='log', rangemode='normal', dtick=1)  # 1, 10, 100 ms...

    return fig


# =============================================================================
def month_chart(comparison: pd.DataFrame, format_: str, version: str,
                dark: bool = False) -> go.Figure:
    """Viajes por mes en una versión anterior (rayado) y en la actual (lleno).

    Args:
        comparison: Resultado de `time_travel.compare`.
        format_: `delta` o `iceberg`, para el color.
        version: Nombre de la versión anterior en la leyenda.
        dark (opcional): Colores del tema oscuro.
    """
    color = colors(dark)[format_]
    hover = '<b>%{x}</b><br>%{fullData.name}: %{y:,} viajes<extra></extra>'
    fig = go.Figure([
        go.Bar(
            name=version,
            x=comparison['mes'], y=comparison['viajes_version'],
            marker={'color': 'rgba(0,0,0,0)', 'line': {'color': color, 'width': 1.5},
                    'pattern': {'shape': '/', 'fgcolor': color, 'solidity': 0.3}},
            hovertemplate=hover,
        ),
        go.Bar(
            name='Actual',
            x=comparison['mes'], y=comparison['viajes_actual'],
            marker={'color': color},
            hovertemplate=hover,
        ),
    ])

    return _layout(fig, 'Viajes')
