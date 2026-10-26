"""Gráficas del benchmark de consultas: tiempos y tiempo relativo a Parquet."""
import math
from pathlib import Path

import matplotlib

matplotlib.use('Agg')  # sin ventana: solo se guardan archivos

import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.ticker import FuncFormatter, LogLocator, NullLocator  # noqa: E402

from lakehouse.benchmarks.formats import (  # noqa: E402
    Measurement, _formats, _index, _queries,
)


# =============================================================================
# Primeros tres tonos de una paleta categórica validada para daltonismo.
COLORS = {'parquet': '#2a78d6', 'delta': '#eb6834', 'iceberg': '#1baf7a'}
SURFACE = '#fcfcfb'
TEXT = '#0b0b0b'
TEXT_MUTED = '#52514e'
GRID = '#e4e3df'
DPI = 150


# =============================================================================
def _style(ax: plt.Axes) -> None:
    """Ejes discretos: sin marco, cuadrícula suave detrás de las barras."""
    ax.set_facecolor(SURFACE)
    ax.tick_params(colors=TEXT_MUTED, length=0, labelsize=9)

    for side in ('top', 'right', 'left'):
        ax.spines[side].set_visible(False)

    ax.spines['bottom'].set_color(GRID)
    ax.grid(axis='x', color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)


# =============================================================================
def _legend(fig: plt.Figure, formats: list[str]) -> None:
    handles = [plt.Rectangle((0, 0), 1, 1, color=COLORS[f]) for f in formats]
    fig.legend(handles, formats, loc='upper right', ncol=len(formats),
               frameon=False, fontsize=9, labelcolor=TEXT)


# =============================================================================
def plot_times(measurements: list[Measurement], path: Path) -> Path:
    """Un panel por consulta con la mediana de cada formato en ms.

    Cada panel tiene su propia escala, porque las consultas tardan entre
    unos pocos y cientos de milisegundos. La línea fina marca el rango
    mínimo – máximo de las repeticiones.

    Returns:
        Path: Ruta del PNG escrito.
    """
    formats = _formats(measurements)
    queries = _queries(measurements)
    index = _index(measurements)
    cols = 2
    rows = math.ceil(len(queries) / cols)

    fig, axes = plt.subplots(rows, cols, figsize=(10, 1.1 + 0.65 * rows * len(formats)),
                             squeeze=False, facecolor=SURFACE)

    for ax, q in zip(axes.flat, queries):
        _style(ax)
        medians = [index[f, q].median_s * 1000 for f in formats]
        lows = [m - index[f, q].min_s * 1000 for m, f in zip(medians, formats)]
        highs = [index[f, q].max_s * 1000 - m for m, f in zip(medians, formats)]
        y = list(range(len(formats)))[::-1]

        ax.barh(y, medians, height=0.7, color=[COLORS[f] for f in formats],
                edgecolor=SURFACE, linewidth=2)
        ax.errorbar(medians, y, xerr=[lows, highs], fmt='none',
                    ecolor=TEXT_MUTED, elinewidth=1, capsize=0)

        top = max(index[f, q].max_s * 1000 for f in formats)
        ax.set_xlim(0, top * 1.28)

        for yi, f, m in zip(y, formats, medians):
            ax.text(index[f, q].max_s * 1000 + top * 0.03, yi, f'{m:,.1f}',
                    va='center', fontsize=9, color=TEXT)

        ax.set_yticks(y, formats)
        ax.set_title(q, loc='left', fontsize=10, color=TEXT)

    for ax in axes.flat[len(queries):]:
        ax.set_visible(False)

    fig.suptitle('Tiempo por consulta con DuckDB (ms, mediana)', x=0.01, ha='left',
                 fontsize=12, color=TEXT)
    _legend(fig, formats)
    fig.tight_layout(rect=(0, 0, 1, 0.95))

    return _save(fig, path)


# =============================================================================
def plot_relative(measurements: list[Measurement], path: Path,
                  baseline: str = 'parquet') -> Path:
    """Tiempo de cada formato dividido por el de la base, en escala log.

    En escala logarítmica, 2x más lento y 2x más rápido quedan a la misma
    distancia de la línea 1x.

    Returns:
        Path: Ruta del PNG escrito.

    Raises:
        ValueError: Si la base no está entre los formatos medidos.
    """
    formats = _formats(measurements)

    if baseline not in formats:
        raise ValueError(f'La base {baseline!r} no está entre los formatos medidos')

    others = [f for f in formats if f != baseline]
    queries = _queries(measurements)
    index = _index(measurements)
    ratios = {(f, q): index[f, q].median_s / index[baseline, q].median_s
              for f in others for q in queries}

    fig, ax = plt.subplots(figsize=(10, 0.9 + 0.75 * len(queries)), facecolor=SURFACE)
    _style(ax)

    height = 0.8 / len(others)

    for i, f in enumerate(others):
        y = [len(queries) - 1 - j + (len(others) / 2 - i - 0.5) * height
             for j in range(len(queries))]
        values = [ratios[f, q] for q in queries]
        # Las barras salen de 1x hacia la izquierda (más rápido) o la derecha.
        ax.barh(y, [v - 1 for v in values], left=1, height=height,
                color=COLORS[f], edgecolor=SURFACE, linewidth=2)

        for yi, v in zip(y, values):
            ax.text(v * (1.06 if v >= 1 else 1 / 1.06), yi, f'{v:.2f}x',
                    va='center', ha='left' if v >= 1 else 'right',
                    fontsize=9, color=TEXT)

    ax.set_xscale('log')
    low = min(ratios.values())
    high = max(ratios.values())
    ax.set_xlim(min(low, 1) / 1.6, max(high, 1) * 1.6)
    ax.xaxis.set_major_locator(LogLocator(base=2))
    ax.xaxis.set_minor_locator(NullLocator())
    ax.xaxis.set_major_formatter(FuncFormatter(lambda v, _: f'{v:g}x'))
    ax.axvline(1, color=TEXT_MUTED, linewidth=1)
    ax.set_yticks(range(len(queries))[::-1], queries)
    ax.set_xlabel(f'← más rápido que {baseline}     más lento que {baseline} →',
                  fontsize=9, color=TEXT_MUTED)

    fig.suptitle(f'Tiempo relativo a {baseline} (mediana, escala log)', x=0.01,
                 ha='left', fontsize=12, color=TEXT)
    _legend(fig, others)
    fig.tight_layout(rect=(0, 0, 1, 0.94))

    return _save(fig, path)


# =============================================================================
def _save(fig: plt.Figure, path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, dpi=DPI, facecolor=SURFACE)
    plt.close(fig)

    return path
