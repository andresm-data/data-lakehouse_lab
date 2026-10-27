"""Compara con DuckDB las mismas consultas sobre Parquet, Delta e Iceberg."""
import argparse
import csv
import math
import statistics
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import duckdb

from lakehouse.benchmarks.queries import FORMATS, QUERIES, connect, source_sql
from lakehouse.iceberg.catalog import CATALOG_DB, NAMESPACE, WAREHOUSE, get_catalog
from lakehouse.paths import DATA_DIR, DOCS_DIR


# =============================================================================
PARQUET = DATA_DIR / 'interim/yellow_tripdata.parquet'
DELTA = DATA_DIR / 'delta/yellow_tripdata_pyarrow'
ICEBERG_TABLE = f'{NAMESPACE}.yellow_tripdata'
REL_TOL = 1e-9


# =============================================================================
@dataclass(frozen=True)
class Measurement:
    """Tiempos de una consulta en un formato, sin contar el calentamiento."""
    format_: str
    query: str
    times: tuple[float, ...]
    rows: int

    @property
    def median_s(self) -> float:
        return statistics.median(self.times)

    @property
    def min_s(self) -> float:
        return min(self.times)

    @property
    def max_s(self) -> float:
        return max(self.times)


# =============================================================================
def iceberg_location(
    identifier: str = ICEBERG_TABLE,
    db: Path = CATALOG_DB,
    warehouse: Path = WAREHOUSE
) -> str:
    """Archivo `metadata.json` actual de la tabla Iceberg, según el catálogo.

    `iceberg_scan` de DuckDB no lee el catálogo SQL de PyIceberg, así que
    se le pasa directamente el archivo de metadatos del snapshot actual.
    """
    return get_catalog(db, warehouse).load_table(identifier).metadata_location


# =============================================================================
def _time(con: duckdb.DuckDBPyConnection, sql: str) -> tuple[float, list]:
    """Ejecuta la consulta hasta traer todas las filas y mide los segundos."""
    start = time.perf_counter()
    rows = con.sql(sql).fetchall()

    return time.perf_counter() - start, rows


# =============================================================================
def run(
    locations: dict[str, str],
    queries: dict[str, str] | None = None,
    repetitions: int = 5,
    warmup: int = 1
) -> tuple[list[Measurement], dict[tuple[str, str], list]]:
    """Ejecuta cada consulta en cada formato y mide el tiempo.

    Cada formato tiene su propia conexión. Para cada consulta se recorren
    los formatos uno tras otro, de modo que los tres la ejecutan con la
    caché del sistema operativo en un estado parecido. Las ejecuciones de
    calentamiento no se miden: sirven para que los archivos ya estén en
    memoria y se compare la lectura de cada formato, no el disco.

    Args:
        locations: Ubicación de la tabla por formato (ver `source_sql`).
        queries (opcional): Consultas por nombre. Por defecto, `QUERIES`.
        repetitions (opcional): Ejecuciones medidas por consulta y formato.
            Por defecto 5.
        warmup (opcional): Ejecuciones previas sin medir. Por defecto 1.

    Returns:
        tuple: Las mediciones, en orden de consulta y formato, y el
            resultado de la última ejecución de cada (formato, consulta).

    Raises:
        ValueError: Si `repetitions` es menor que 1, `warmup` es negativo
            o algún formato no es válido; se valida antes de medir.
    """
    if repetitions < 1:
        raise ValueError('El número de repeticiones debe ser al menos 1')

    if warmup < 0:
        raise ValueError('El calentamiento no puede ser negativo')

    for format_, location in locations.items():
        source_sql(format_, location)

    queries = queries or QUERIES
    connections = {f: connect(f, loc) for f, loc in locations.items()}
    measurements, results = [], {}

    try:
        for name, sql in queries.items():
            for format_, con in connections.items():
                for _ in range(warmup):
                    _time(con, sql)

                times, rows = [], []

                for _ in range(repetitions):
                    elapsed, rows = _time(con, sql)
                    times.append(elapsed)

                measurements.append(Measurement(format_, name, tuple(times), len(rows)))
                results[format_, name] = rows

    finally:
        for con in connections.values():
            con.close()

    return measurements, results


# =============================================================================
def _same(a: object, b: object, rel_tol: float) -> bool:
    """Compara dos valores; los decimales con tolerancia relativa."""
    if isinstance(a, float) and isinstance(b, float):
        return math.isclose(a, b, rel_tol=rel_tol)

    return a == b


# =============================================================================
def compare_results(
    results: dict[tuple[str, str], list],
    baseline: str = 'parquet',
    rel_tol: float = REL_TOL
) -> list[str]:
    """Diferencias entre el resultado de cada formato y el de la base.

    Las sumas de decimales pueden cambiar en los últimos dígitos según el
    orden en que se lean los archivos, por eso se usa `rel_tol`.

    Returns:
        list[str]: Un mensaje por consulta y formato que no coincide; vacía
            si todos los resultados son iguales.
    """
    problems = []

    for (format_, query), rows in results.items():
        if format_ == baseline or (baseline, query) not in results:
            continue

        base = results[baseline, query]

        if len(rows) != len(base):
            problems.append(
                f'{query}: {format_} devuelve {len(rows)} filas y '
                f'{baseline} {len(base)}'
            )
            continue

        for i, (row_a, row_b) in enumerate(zip(base, rows)):
            if len(row_a) != len(row_b) or not all(
                _same(a, b, rel_tol) for a, b in zip(row_a, row_b)
            ):
                problems.append(f'{query}: {format_} difiere de {baseline} en la fila {i + 1}')
                break

    return problems


# =============================================================================
def _formats(measurements: list[Measurement]) -> list[str]:
    """Formatos medidos, en el orden de `FORMATS`."""
    present = {m.format_ for m in measurements}

    return [f for f in FORMATS if f in present]


# =============================================================================
def _queries(measurements: list[Measurement]) -> list[str]:
    """Consultas medidas, en el orden en que se ejecutaron."""
    return list(dict.fromkeys(m.query for m in measurements))


# =============================================================================
def _index(measurements: list[Measurement]) -> dict[tuple[str, str], Measurement]:
    return {(m.format_, m.query): m for m in measurements}


# =============================================================================
def _display(location: str) -> str:
    """Ubicación relativa al directorio actual, si está dentro de él.

    El `metadata.json` de Iceberg llega como URI absoluta (`file:/...`);
    en el informe se muestra relativa para que no dependa de la máquina.
    """
    path = Path(location.removeprefix('file:'))

    if path.is_absolute() and path.is_relative_to(Path.cwd()):
        return str(path.relative_to(Path.cwd()))

    return location


# =============================================================================
def report(
    measurements: list[Measurement],
    locations: dict[str, str],
    repetitions: int,
    warmup: int,
    started: datetime,
    problems: list[str],
    charts: list[Path] | None = None,
    queries: dict[str, str] | None = None
) -> str:
    """Genera el informe en Markdown con la tabla de resultados.

    Args:
        measurements: Mediciones obtenidas con `run`.
        locations: Ubicación de la tabla por formato.
        repetitions: Ejecuciones medidas por consulta.
        warmup: Ejecuciones de calentamiento por consulta.
        started: Fecha y hora de inicio.
        problems: Diferencias encontradas con `compare_results`.
        charts (opcional): Gráficas a enlazar, en el mismo directorio que
            el informe.
        queries (opcional): Texto de las consultas. Por defecto, `QUERIES`.

    Returns:
        str: Informe en Markdown.
    """
    queries = queries or QUERIES
    formats = _formats(measurements)
    index = _index(measurements)
    total = {f: sum(index[f, q].median_s for q in _queries(measurements)) for f in formats}

    lines = [
        '# Benchmark de consultas con DuckDB: Parquet, Delta e Iceberg',
        '',
        f'- Fecha: {started:%Y-%m-%d %H:%M:%S}',
        f'- DuckDB: {duckdb.__version__}',
        f'- Repeticiones: {repetitions} medidas + {warmup} de calentamiento '
        '(tiempos: mediana)',
        '- Resultados iguales en los tres formatos: ' +
        ('sí' if not problems else 'no'),
        '',
        '| Formato | Origen |',
        '|---|---|',
    ]
    lines += [f'| {f} | `{source_sql(f, _display(locations[f]))}` |' for f in formats]
    lines += [
        '',
        '## Resultados (ms, mediana)',
        '',
        '| Consulta | ' + ' | '.join(formats) + ' | Más rápido |',
        '|---|' + '---:|' * len(formats) + '---|',
    ]

    for q in _queries(measurements):
        values = {f: index[f, q].median_s for f in formats}
        best = min(values, key=values.get)
        cells = [
            f'**{values[f] * 1000:,.1f}**' if f == best else f'{values[f] * 1000:,.1f}'
            for f in formats
        ]
        lines.append(f'| {q} | ' + ' | '.join(cells) + f' | {best} |')

    best_total = min(total, key=total.get)
    lines.append('| **Total** | ' + ' | '.join(
        f'**{total[f] * 1000:,.1f}**' if f == best_total else f'{total[f] * 1000:,.1f}'
        for f in formats
    ) + f' | {best_total} |')

    if 'parquet' in formats:
        lines += [
            '',
            '## Tiempo relativo a Parquet',
            '',
            '| Consulta | ' + ' | '.join(formats) + ' |',
            '|---|' + '---:|' * len(formats),
        ]
        for q in _queries(measurements):
            base = index['parquet', q].median_s
            lines.append(
                f'| {q} | ' +
                ' | '.join(f'{index[f, q].median_s / base:.2f}x' for f in formats) + ' |'
            )

    lines += [
        '',
        '## Dispersión (ms, mínimo – máximo)',
        '',
        '| Consulta | ' + ' | '.join(formats) + ' |',
        '|---|' + '---:|' * len(formats),
    ]
    for q in _queries(measurements):
        lines.append(f'| {q} | ' + ' | '.join(
            f'{index[f, q].min_s * 1000:,.1f} – {index[f, q].max_s * 1000:,.1f}'
            for f in formats
        ) + ' |')

    if problems:
        lines += ['', '## Diferencias entre formatos', '']
        lines += [f'- {p}' for p in problems]

    if charts:
        lines += ['', '## Gráficas', '']
        lines += [f'![{c.stem}]({c.name})' for c in charts]

    lines += ['', '## Consultas', '']

    for q in _queries(measurements):
        lines += [f'### {q}', '', '```sql', queries[q].strip(), '```', '']

    return '\n'.join(lines).rstrip() + '\n'


# =============================================================================
def write_csv(measurements: list[Measurement], path: Path) -> None:
    """Guarda una fila por consulta y formato, con los tiempos en segundos."""
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open('w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['consulta', 'formato', 'filas', 'mediana_s', 'min_s', 'max_s', 'tiempos_s'])

        for m in measurements:
            writer.writerow([
                m.query, m.format_, m.rows,
                f'{m.median_s:.6f}', f'{m.min_s:.6f}', f'{m.max_s:.6f}',
                ' '.join(f'{t:.6f}' for t in m.times),
            ])


# =============================================================================
def main() -> int:
    """Punto de entrada del comando `lh-bench-formats`.

    Returns:
        int: `0` si el benchmark terminó y los resultados coinciden, `1` si
            hubo un error o algún formato devolvió otro resultado.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--parquet', type=Path, default=PARQUET,
                        help=f'Archivo Parquet (por defecto: {PARQUET})')
    parser.add_argument('--delta', type=Path, default=DELTA,
                        help=f'Tabla Delta (por defecto: {DELTA})')
    parser.add_argument('--iceberg', default=ICEBERG_TABLE,
                        help=f'Tabla Iceberg en el catálogo (por defecto: {ICEBERG_TABLE})')
    parser.add_argument('--catalogo', type=Path, default=CATALOG_DB)
    parser.add_argument('--warehouse', type=Path, default=WAREHOUSE)
    parser.add_argument('-r', '--repeticiones', type=int, default=5,
                        help='Ejecuciones medidas por consulta; se reporta la mediana '
                        '(por defecto: 5)')
    parser.add_argument('--calentamiento', type=int, default=1,
                        help='Ejecuciones sin medir antes de cada consulta (por defecto: 1)')
    parser.add_argument('--dir-salida', type=Path, default=DOCS_DIR / 'benchmarks',
                        help='Directorio del informe (por defecto: docs/benchmarks)')
    args = parser.parse_args()
    started = datetime.now()

    try:
        locations = {
            'parquet': str(args.parquet),
            'delta': str(args.delta),
            'iceberg': iceberg_location(args.iceberg, args.catalogo, args.warehouse),
        }
        measurements, results = run(locations, None, args.repeticiones, args.calentamiento)

    except Exception as e:  # pyiceberg y duckdb lanzan errores de varios tipos
        print(f'Error: {e}', file=sys.stderr)

        return 1

    # Importado aquí para que formats no dependa de charts al cargarse:
    # charts importa funciones de este módulo.
    from lakehouse.benchmarks.charts import plot_relative, plot_times

    problems = compare_results(results)
    stem = args.dir_salida / f'duckdb_formatos_{started:%Y%m%d_%H%M%S}'
    write_csv(measurements, stem.with_suffix('.csv'))
    charts = [
        plot_times(measurements, stem.with_name(stem.name + '_tiempos.png')),
        plot_relative(measurements, stem.with_name(stem.name + '_relativo.png')),
    ]

    text = report(measurements, locations, args.repeticiones, args.calentamiento,
                  started, problems, charts)
    stem.with_suffix('.md').write_text(text, encoding='utf-8')

    print(text)
    print(f'Listo -> {stem}.md')

    for p in problems:
        print(f'Diferencia: {p}', file=sys.stderr)

    return 1 if problems else 0


# =============================================================================
if __name__ == "__main__":
    raise SystemExit(main())
