"""Añade un lote con una columna nueva a la tabla Delta (evolución de esquema)."""
import argparse
from datetime import datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
from deltalake import DeltaTable, write_deltalake

from lakehouse.delta.merge_table import KEY, METRICS_DIR, TARGET, _partition
from lakehouse.delta.time_travel import _markdown_table


# =============================================================================
COLUMN = 'trip_duration_min'
PARTITION = ['year', 'month']
SHIFT = timedelta(seconds=2)
SAMPLE = 3


# =============================================================================
def build_batch(target: Path, year: int, month: int, rows: int) -> pa.Table:
    """Arma un lote de viajes nuevos con la columna `COLUMN`.

    Toma filas de la partición, desplaza sus fechas `SHIFT` para que sean
    viajes nuevos y descarta las que ya existen en la tabla. `COLUMN` es la
    duración del viaje en minutos, redondeada a 2 decimales.

    Args:
        target (Path): Tabla Delta de donde salen las filas.
        year (int): Año de la partición.
        month (int): Mes de la partición.
        rows (int): Número máximo de filas del lote.

    Returns:
        pa.Table: Lote con el esquema de la tabla más `COLUMN` al final.
    """
    dataset = DeltaTable(target).to_pyarrow_dataset()
    partition = _partition(year, month)

    keys = dataset.to_table(columns=KEY, filter=partition)
    batch = dataset.head(rows * 2, filter=partition)

    if COLUMN in batch.schema.names:
        batch = batch.drop_columns([COLUMN])

    names = batch.schema.names

    for column in ('tpep_pickup_datetime', 'tpep_dropoff_datetime'):
        batch = batch.set_column(
            names.index(column), column, pc.add(batch[column], SHIFT)
        )

    batch = batch.join(keys, KEY, join_type='left anti')
    batch = batch.select(names).slice(0, rows)

    elapsed = pc.subtract(batch['tpep_dropoff_datetime'],
                          batch['tpep_pickup_datetime'])
    minutes = pc.divide(pc.cast(pc.cast(elapsed, pa.int64()), pa.float64()),
                        60_000_000.0)

    return batch.append_column(COLUMN, pc.round(minutes, 2))


# =============================================================================
def append_batch(target: Path, batch: pa.Table) -> None:
    """Añade el lote a la tabla y agrega al esquema las columnas nuevas.

    Con `schema_mode='merge'` las filas anteriores no se reescriben: al
    leerlas, las columnas nuevas aparecen como `null`.

    Args:
        target (Path): Tabla Delta destino.
        batch (pa.Table): Filas a añadir; puede tener columnas nuevas.
    """
    write_deltalake(target, batch, mode='append', schema_mode='merge',
                    partition_by=PARTITION)


# =============================================================================
def verify(target: Path, version_before: int, year: int, month: int) -> dict:
    """Comprueba el resultado del append comparando con la versión anterior.

    Args:
        target (Path): Tabla Delta.
        version_before (int): Versión de la tabla antes del append.
        year (int): Año de la partición de donde salen los ejemplos.
        month (int): Mes de la partición de donde salen los ejemplos.

    Returns:
        dict: `version_antes`, `version_despues`, `operacion` (operación y
            modo de la última entrada de `history()`), `columna_antes` y
            `columna_despues` (si `COLUMN` está en cada esquema),
            `filas_antes` (total de la versión anterior), `nulos` y
            `no_nulos` (filas actuales sin y con valor en `COLUMN`), y
            `ejemplo_antiguas` y `ejemplo_nuevas` (unas filas de cada tipo).
    """
    before = DeltaTable(target, version=version_before)
    table = DeltaTable(target)
    dataset = table.to_pyarrow_dataset()
    last = table.history(1)[0]

    # Se lee la columna y se filtra en memoria: filtrar con `is_null()` en el
    # dataset da resultados erróneos en los archivos que no tienen la columna.
    values = dataset.to_table(columns=[COLUMN])[COLUMN]
    month_rows = dataset.to_table(
        columns=[*KEY, COLUMN], filter=_partition(year, month)
    )
    is_null = pc.is_null(month_rows[COLUMN])

    return {
        'version_antes': version_before,
        'version_despues': table.version(),
        'operacion': f'{last["operation"]} '
                     f'({last["operationParameters"].get("mode")})',
        'columna_antes': COLUMN in before.schema().to_arrow().names,
        'columna_despues': COLUMN in table.schema().to_arrow().names,
        'filas_antes': before.to_pyarrow_dataset().count_rows(),
        'nulos': values.null_count,
        'no_nulos': len(values) - values.null_count,
        'ejemplo_antiguas': month_rows.filter(is_null).slice(0, SAMPLE),
        'ejemplo_nuevas': month_rows.filter(pc.invert(is_null)).slice(0, SAMPLE),
    }


# =============================================================================
def render(target: Path, rows: int, result: dict) -> str:
    """Arma el informe en Markdown con la verificación del append.

    Args:
        target (Path): Tabla Delta.
        rows (int): Filas del lote añadido.
        result (dict): Salida de `verify`.

    Returns:
        str: Contenido del informe.
    """
    before, after = result['version_antes'], result['version_despues']
    sample = pa.concat_tables(
        [result['ejemplo_antiguas'], result['ejemplo_nuevas']]
    ).to_pylist()

    lines = [
        f'# Evolución de esquema en Delta: columna `{COLUMN}`',
        '',
        f'- Fecha: {datetime.now():%Y-%m-%d %H:%M:%S}',
        f'- Tabla: `{target}`',
        f'- Lote añadido: {rows:,} filas con '
        "`write_deltalake(..., mode='append', schema_mode='merge')`",
        '',
        '## Verificación',
        '',
        *_markdown_table(
            ['Comprobación', f'Versión {before}', f'Versión {after}'],
            [
                [f'`{COLUMN}` en el esquema', _yes_no(result['columna_antes']),
                 _yes_no(result['columna_despues'])],
                ['Filas en la tabla', f'{result["filas_antes"]:,}',
                 f'{result["nulos"] + result["no_nulos"]:,}'],
            ],
        ),
        '',
        f'- Versión nueva: {before} → {after}, operación '
        f'`{result["operacion"]}`',
        f'- Filas con `{COLUMN}` nulo: {result["nulos"]:,} '
        f'(las {result["filas_antes"]:,} filas anteriores)',
        f'- Filas con `{COLUMN}` con valor: {result["no_nulos"]:,} '
        '(el lote nuevo)',
        '',
        'Ejemplo de filas anteriores (nulo) y nuevas (con valor):',
        '',
        *_markdown_table(
            [*KEY, COLUMN],
            [[r[c] for c in KEY] + [_null(r[COLUMN])] for r in sample],
        ),
        '',
    ]

    return '\n'.join(lines)


# =============================================================================
def _null(value: float | None) -> str:
    """El valor como texto, o `null` si falta."""
    return 'null' if value is None else str(value)


# =============================================================================
def _yes_no(value: bool) -> str:
    """`Sí` o `No`."""
    return 'Sí' if value else 'No'


# =============================================================================
def save_report(text: str, out_dir: Path) -> Path:
    """Guarda el informe como `schema_evolution_<AAAAMMDD_HHMMSS>.md`.

    Args:
        text (str): Contenido del informe.
        out_dir (Path): Directorio de salida; se crea si no existe.

    Returns:
        Path: Ruta del informe.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f'schema_evolution_{datetime.now():%Y%m%d_%H%M%S}.md'
    path.write_text(text)

    return path


# =============================================================================
def main() -> int:
    """Arma el lote, lo añade a la tabla, verifica y guarda el informe.

    Returns:
        int: `0` si la verificación es exitosa, `1` si falla.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--tabla', type=Path, default=TARGET)
    parser.add_argument('--salida', type=Path, default=METRICS_DIR)
    parser.add_argument('--year', type=int, default=2025)
    parser.add_argument('--month', type=int, default=1)
    parser.add_argument('--filas', type=int, default=100)
    args = parser.parse_args()

    version_before = DeltaTable(args.tabla).version()
    batch = build_batch(args.tabla, args.year, args.month, args.filas)
    append_batch(args.tabla, batch)

    result = verify(args.tabla, version_before, args.year, args.month)
    path = save_report(render(args.tabla, batch.num_rows, result), args.salida)

    print(f'Versión: {result["version_antes"]} -> {result["version_despues"]}')
    print(f'Filas con {COLUMN} nulo:    {result["nulos"]:,}')
    print(f'Filas con {COLUMN} con valor: {result["no_nulos"]:,}')
    print(f'Informe -> {path}')

    ok = (
        result['version_despues'] == result['version_antes'] + 1
        and result['nulos'] == result['filas_antes']
        and result['no_nulos'] == batch.num_rows
    )

    return 0 if ok else 1


# =============================================================================
if __name__ == "__main__":
    raise SystemExit(main())
