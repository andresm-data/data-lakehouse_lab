"""Lectura de la base DuckDB de resultados del benchmark para el dashboard.

Cada función abre la base en solo lectura y la cierra al terminar, para no
bloquearla mientras `lh-bench-formats` o `lh-bench-db` escriben en ella.
"""
from datetime import datetime
from pathlib import Path

import duckdb
import pandas as pd

from lakehouse.benchmarks.queries import FORMATS


# =============================================================================
MB = 2**20  # igual que en el README: 205,094,276 bytes = 195.6 MB


# =============================================================================
def _query(db: Path, sql: str, params: list | None = None) -> pd.DataFrame:
    with duckdb.connect(str(db), read_only=True) as con:
        return con.execute(sql, params or []).df()


# =============================================================================
def runs(db: Path) -> list[datetime]:
    """Ejecuciones guardadas, de la más reciente a la más antigua.

    Returns:
        list[datetime]: Vacía si la base no existe todavía.
    """
    if not db.exists():
        return []

    df = _query(db, 'SELECT DISTINCT ejecucion FROM tiempos ORDER BY ejecucion DESC')

    return [ts.to_pydatetime() for ts in df['ejecucion']]


# =============================================================================
def load_times(db: Path, run: datetime) -> pd.DataFrame:
    """Tiempos de una ejecución en ms, ordenados por consulta y formato.

    Returns:
        pd.DataFrame: Columnas `consulta`, `formato`, `filas`, `mediana_ms`,
            `min_ms`, `max_ms` y `repeticiones`.
    """
    return _query(db, """
        SELECT consulta, formato, filas,
               mediana_s * 1000 AS mediana_ms,
               min_s * 1000 AS min_ms,
               max_s * 1000 AS max_ms,
               len(tiempos_s) AS repeticiones
        FROM tiempos
        WHERE ejecucion = ?
        ORDER BY consulta, list_position(?, formato)
    """, [run, list(FORMATS)])


# =============================================================================
def load_sizes(db: Path, run: datetime) -> pd.DataFrame:
    """Tamaños de una ejecución en MB, en el orden de `FORMATS`.

    Returns:
        pd.DataFrame: Columnas `formato`, `archivos`, `actual_mb` (archivos
            de datos de la versión actual) y `disco_mb` (todo el directorio).
    """
    return _query(db, """
        SELECT formato, archivos,
               bytes_actuales / ? AS actual_mb,
               bytes_disco / ? AS disco_mb
        FROM tamanos
        WHERE ejecucion = ?
        ORDER BY list_position(?, formato)
    """, [MB, MB, run, list(FORMATS)])
