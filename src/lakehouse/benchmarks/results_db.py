"""Guarda los resultados del benchmark de formatos en una base DuckDB.

La base tiene dos tablas, con una marca `ejecucion` por cada corrida:

- `tiempos`: una fila por consulta y formato, con la mediana, el mínimo,
  el máximo y todos los tiempos medidos (en segundos).
- `tamanos`: una fila por formato, con el tamaño de los archivos de datos
  de la versión actual de la tabla y el total que ocupa en disco.

El tamaño en disco incluye lo que la versión actual ya no usa: el log de
Delta, los metadatos de Iceberg y los archivos de versiones anteriores que
siguen ahí para el *time travel*.

`lh-bench-db` carga en la base un CSV de `lh-bench-formats` ya generado y
mide el tamaño actual de las tablas.
"""
import argparse
import csv
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

import duckdb
import pyarrow as pa
from deltalake import DeltaTable
from pyiceberg.table import StaticTable

from lakehouse.benchmarks.formats import (
    DELTA, ICEBERG_TABLE, PARQUET, Measurement, iceberg_location,
)
from lakehouse.iceberg.catalog import CATALOG_DB, WAREHOUSE
from lakehouse.paths import DATA_DIR, DOCS_DIR


# =============================================================================
RESULTS_DB = DATA_DIR / 'benchmarks/resultados.duckdb'
CSV_PREFIX = 'duckdb_formatos_'
CSV_STAMP = '%Y%m%d_%H%M%S'

SCHEMA = """
CREATE TABLE IF NOT EXISTS tiempos (
    ejecucion TIMESTAMP NOT NULL,
    consulta VARCHAR NOT NULL,
    formato VARCHAR NOT NULL,
    filas BIGINT NOT NULL,
    mediana_s DOUBLE NOT NULL,
    min_s DOUBLE NOT NULL,
    max_s DOUBLE NOT NULL,
    tiempos_s DOUBLE[] NOT NULL,
    PRIMARY KEY (ejecucion, consulta, formato)
);
CREATE TABLE IF NOT EXISTS tamanos (
    ejecucion TIMESTAMP NOT NULL,
    formato VARCHAR NOT NULL,
    archivos INTEGER NOT NULL,
    bytes_actuales BIGINT NOT NULL,
    bytes_disco BIGINT NOT NULL,
    PRIMARY KEY (ejecucion, formato)
);
"""


# =============================================================================
@dataclass(frozen=True)
class TableSize:
    """Tamaño de una tabla: versión actual y total en disco."""
    format_: str
    files: int
    current_bytes: int
    disk_bytes: int


# =============================================================================
def _disk_bytes(path: Path) -> int:
    """Bytes de un archivo, o de todos los archivos bajo un directorio."""
    if path.is_file():
        return path.stat().st_size

    return sum(p.stat().st_size for p in path.rglob('*') if p.is_file())


# =============================================================================
def table_size(format_: str, location: str) -> TableSize:
    """Mide una tabla a partir de la misma ubicación que usa el benchmark.

    Args:
        format_: `parquet`, `delta` o `iceberg`.
        location: Archivo o directorio Parquet, directorio de la tabla
            Delta o `metadata.json` actual de la tabla Iceberg.

    Raises:
        ValueError: Si el formato no es válido.
        FileNotFoundError: Si la ubicación no existe.
    """
    if format_ == 'parquet':
        path = Path(location)

        if not path.exists():
            raise FileNotFoundError(f'No existe {path}')

        files = 1 if path.is_file() else len(list(path.rglob('*.parquet')))
        size = _disk_bytes(path)

        return TableSize(format_, files, size, size)

    if format_ == 'delta':
        actions = pa.table(DeltaTable(location).get_add_actions(flatten=True))

        return TableSize(
            format_, actions.num_rows,
            sum(actions['size_bytes'].to_pylist()),
            _disk_bytes(Path(location)),
        )

    if format_ == 'iceberg':
        table = StaticTable.from_metadata(location)
        files = table.inspect.files()

        return TableSize(
            format_, files.num_rows,
            sum(files['file_size_in_bytes'].to_pylist()),
            _disk_bytes(Path(urlparse(table.location()).path)),
        )

    raise ValueError(f'Formato no válido: {format_}')


# =============================================================================
def table_sizes(locations: dict[str, str]) -> list[TableSize]:
    """Mide cada tabla de `locations`, en el mismo orden."""
    return [table_size(f, loc) for f, loc in locations.items()]


# =============================================================================
def save(
    db: Path,
    started: datetime,
    measurements: list[Measurement],
    sizes: list[TableSize]
) -> None:
    """Guarda una ejecución del benchmark; reemplaza la anterior si ya existe.

    Args:
        db: Archivo DuckDB; se crea con sus tablas si no existe.
        started: Marca de la ejecución (fecha y hora de inicio).
        measurements: Mediciones de `formats.run` o de `load_csv`.
        sizes: Tamaños de `table_sizes`.
    """
    db.parent.mkdir(parents=True, exist_ok=True)

    with duckdb.connect(str(db)) as con:
        con.execute(SCHEMA)
        con.execute('BEGIN TRANSACTION')

        for table in ('tiempos', 'tamanos'):
            con.execute(f'DELETE FROM {table} WHERE ejecucion = ?', [started])

        if measurements:
            con.executemany('INSERT INTO tiempos VALUES (?, ?, ?, ?, ?, ?, ?, ?)', [
                [started, m.query, m.format_, m.rows,
                 m.median_s, m.min_s, m.max_s, list(m.times)]
                for m in measurements
            ])

        if sizes:
            con.executemany('INSERT INTO tamanos VALUES (?, ?, ?, ?, ?)', [
                [started, s.format_, s.files, s.current_bytes, s.disk_bytes]
                for s in sizes
            ])

        con.execute('COMMIT')


# =============================================================================
def load_csv(path: Path) -> list[Measurement]:
    """Lee las mediciones de un CSV escrito por `formats.write_csv`."""
    with path.open(newline='', encoding='utf-8') as f:
        return [
            Measurement(
                row['formato'], row['consulta'],
                tuple(float(t) for t in row['tiempos_s'].split()),
                int(row['filas']),
            )
            for row in csv.DictReader(f)
        ]


# =============================================================================
def csv_started(path: Path) -> datetime:
    """Fecha y hora de inicio, a partir del nombre `duckdb_formatos_<...>.csv`."""
    stamp = path.stem.removeprefix(CSV_PREFIX)

    return datetime.strptime(stamp, CSV_STAMP)


# =============================================================================
def latest_csv(directory: Path) -> Path | None:
    """CSV más reciente de `lh-bench-formats` en `directory`."""
    files = sorted(directory.glob(f'{CSV_PREFIX}*.csv'), key=csv_started)

    return files[-1] if files else None


# =============================================================================
def main() -> int:
    """Punto de entrada del comando `lh-bench-db`.

    Returns:
        int: `0` si la ejecución quedó guardada, `1` si hubo un error.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--csv', type=Path,
                        help='CSV de lh-bench-formats (por defecto: el más reciente '
                        'de docs/benchmarks)')
    parser.add_argument('--db', type=Path, default=RESULTS_DB,
                        help=f'Base DuckDB de resultados (por defecto: {RESULTS_DB})')
    parser.add_argument('--parquet', type=Path, default=PARQUET)
    parser.add_argument('--delta', type=Path, default=DELTA)
    parser.add_argument('--iceberg', default=ICEBERG_TABLE)
    parser.add_argument('--catalogo', type=Path, default=CATALOG_DB)
    parser.add_argument('--warehouse', type=Path, default=WAREHOUSE)
    args = parser.parse_args()

    path = args.csv or latest_csv(DOCS_DIR / 'benchmarks')

    if path is None:
        print('Error: no hay CSV de lh-bench-formats en docs/benchmarks', file=sys.stderr)

        return 1

    try:
        locations = {
            'parquet': str(args.parquet),
            'delta': str(args.delta),
            'iceberg': iceberg_location(args.iceberg, args.catalogo, args.warehouse),
        }
        started = csv_started(path)
        measurements = load_csv(path)
        sizes = table_sizes(locations)
        save(args.db, started, measurements, sizes)

    except Exception as e:  # csv, pyiceberg, deltalake y duckdb lanzan errores de varios tipos
        print(f'Error: {e}', file=sys.stderr)

        return 1

    print(f'{path.name}: {len(measurements)} mediciones y {len(sizes)} tamaños -> {args.db}')

    return 0


# =============================================================================
if __name__ == "__main__":
    raise SystemExit(main())
