"""Demuestra un MERGE (upsert) sobre una copia de la tabla Delta de taxis."""
import argparse
import json
import shutil
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
from deltalake import DeltaTable


# =============================================================================
ORIGIN = Path('data/delta/yellow_tripdata_pyarrow')
TARGET = Path('data/delta_demo/yellow_tripdata')
METRICS_DIR = Path('docs/delta')
KEY = ['VendorID', 'tpep_pickup_datetime', 'PULocationID', 'DOLocationID']
TIP_FIX = 1.0
SHIFT = timedelta(seconds=1)


# =============================================================================
def copy_table(origin: Path, target: Path, reset: bool = False) -> None:
    """Copia la tabla Delta para no modificar la original.

    Args:
        origin (Path): Tabla Delta original; su versión 0 queda intacta.
        target (Path): Directorio de la copia.
        reset (bool): Si es `True` y la copia ya existe, la borra y vuelve
            a copiar.
    """
    if reset and target.exists():
        shutil.rmtree(target)

    if not target.exists():
        shutil.copytree(origin, target)


# =============================================================================
def _partition(year: int, month: int) -> ds.Expression:
    """Construye el filtro de una partición.

    Args:
        year (int): Año de la partición.
        month (int): Mes de la partición.

    Returns:
        ds.Expression: Filtro `year == year AND month == month`, para que
            pyarrow solo lea los archivos de esa partición.
    """
    return (ds.field('year') == year) & (ds.field('month') == month)


# =============================================================================
def check_unique_key(table: pa.Table) -> None:
    """Comprueba que la clave compuesta `KEY` no se repita en la tabla.

    Si una fila destino coincide con varias filas de la fuente, el MERGE
    falla; por eso se valida la fuente antes de ejecutarlo.

    Args:
        table (pa.Table): Fuente del MERGE; debe tener las columnas de `KEY`.

    Raises:
        ValueError: Si alguna clave aparece más de una vez; el mensaje indica
            cuántas claves están repetidas.
    """
    counts = table.group_by(KEY).aggregate([([], 'count_all')])
    repeated = counts.filter(pc.field('count_all') > 1).num_rows

    if repeated:
        raise ValueError(f'{repeated} claves repetidas en la fuente del MERGE')


# =============================================================================
def build_source(
    target: Path, year: int, month: int, updates: int, inserts: int
) -> pa.Table:
    """Arma la fuente del MERGE: filas corregidas más filas nuevas.

    Solo lee la partición pedida: las columnas de `KEY` de todo el mes y
    unas pocas filas completas como muestra.

    - Corregidas: filas del mes cuya clave es única en la tabla, con
      `tip_amount` y `total_amount` aumentados en `TIP_FIX`.
    - Nuevas: filas de la muestra con las fechas de recogida y llegada
      desplazadas `SHIFT`, solo si su clave no existe en la tabla.

    Args:
        target (Path): Tabla Delta sobre la que se hará el MERGE.
        year (int): Año de la partición.
        month (int): Mes de la partición.
        updates (int): Número máximo de filas a corregir.
        inserts (int): Número máximo de filas nuevas.

    Returns:
        pa.Table: Fuente con el mismo esquema que la tabla; primero las filas
            corregidas y después las nuevas.

    Raises:
        ValueError: Si la fuente resultante repite alguna clave.
    """
    dataset = DeltaTable(target).to_pyarrow_dataset()
    partition = _partition(year, month)

    keys = dataset.to_table(columns=KEY, filter=partition)
    counts = keys.group_by(KEY).aggregate([([], 'count_all')])
    unique = counts.filter(pc.field('count_all') == 1).select(KEY)

    sample = dataset.head((updates + inserts) * 2, filter=partition)
    names = sample.schema.names

    to_update = sample.join(unique, KEY, join_type='inner').slice(0, updates)
    to_update = to_update.select(names)
    to_update = to_update.set_column(
        names.index('tip_amount'), 'tip_amount',
        pc.add(to_update['tip_amount'], TIP_FIX)
    ).set_column(
        names.index('total_amount'), 'total_amount',
        pc.add(to_update['total_amount'], TIP_FIX)
    )

    shifted = sample.slice(updates)

    for column in ('tpep_pickup_datetime', 'tpep_dropoff_datetime'):
        shifted = shifted.set_column(
            names.index(column), column, pc.add(shifted[column], SHIFT)
        )

    to_insert = shifted.join(keys, KEY, join_type='left anti')
    to_insert = to_insert.select(names).slice(0, inserts)

    source = pa.concat_tables([to_update, to_insert])
    check_unique_key(source)

    return source


# =============================================================================
def merge(target: Path, source: pa.Table, year: int, month: int) -> dict:
    """Ejecuta el MERGE: actualiza las filas que coinciden e inserta el resto.

    La condición une destino (`t`) y fuente (`s`) por las columnas de `KEY`
    e incluye `year` y `month`, para que Delta solo lea y reescriba los
    archivos de esa partición.

    Args:
        target (Path): Tabla Delta destino.
        source (pa.Table): Filas a actualizar o insertar, sin claves repetidas.
        year (int): Año de la partición afectada.
        month (int): Mes de la partición afectada.

    Returns:
        dict: Métricas que devuelve `deltalake`, como
            `num_target_rows_updated`, `num_target_rows_inserted` y
            `num_target_files_removed`.
    """
    predicate = ' AND '.join(
        [f't.year = {year}', f't.month = {month}']
        + [f't.{c} = s.{c}' for c in KEY]
    )

    return (
        DeltaTable(target)
        .merge(source, predicate, source_alias='s', target_alias='t')
        .when_matched_update_all()
        .when_not_matched_insert_all()
        .execute()
    )


# =============================================================================
def save_metrics(metrics: dict, target: Path, out_dir: Path) -> Path:
    """Guarda las métricas del MERGE en un JSON con fecha y hora.

    Además de las métricas, guarda la fecha, la ruta de la tabla y la
    versión que dejó el MERGE.

    Args:
        metrics (dict): Métricas que devuelve `merge`.
        target (Path): Tabla Delta sobre la que se hizo el MERGE.
        out_dir (Path): Directorio de salida; se crea si no existe.

    Returns:
        Path: Ruta del archivo `merge_<AAAAMMDD_HHMMSS>.json`.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    now = datetime.now()
    path = out_dir / f'merge_{now:%Y%m%d_%H%M%S}.json'
    data = {
        'fecha': now.isoformat(timespec='seconds'),
        'tabla': str(target),
        'version': DeltaTable(target).version(),
        'metricas': metrics,
    }
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False) + '\n')

    return path


# =============================================================================
def main() -> int:
    """Copia la tabla, arma la fuente, ejecuta el MERGE y guarda las métricas.

    Returns:
        int: `0` si el MERGE se ejecuta, `1` si la fuente tiene claves
            repetidas.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--origen', type=Path, default=ORIGIN)
    parser.add_argument('--destino', type=Path, default=TARGET)
    parser.add_argument('--metricas', type=Path, default=METRICS_DIR)
    parser.add_argument('--year', type=int, default=2025)
    parser.add_argument('--month', type=int, default=1)
    parser.add_argument('--actualizar', type=int, default=300)
    parser.add_argument('--insertar', type=int, default=50)
    parser.add_argument('--reset', action='store_true',
                        help='vuelve a copiar la tabla original antes del MERGE')
    args = parser.parse_args()

    copy_table(args.origen, args.destino, args.reset)

    try:
        source = build_source(
            args.destino, args.year, args.month, args.actualizar, args.insertar
        )

    except ValueError as e:
        print(f'Error: {e}', file=sys.stderr)

        return 1

    metrics = merge(args.destino, source, args.year, args.month)
    path = save_metrics(metrics, args.destino, args.metricas)

    print(f'Filas en la fuente:   {metrics["num_source_rows"]:,}')
    print(f'Filas actualizadas:   {metrics["num_target_rows_updated"]:,}')
    print(f'Filas insertadas:     {metrics["num_target_rows_inserted"]:,}')
    print(f'Archivos reescritos:  {metrics["num_target_files_removed"]:,} '
          f'eliminados, {metrics["num_target_files_added"]:,} añadidos')
    print(f'Métricas -> {path}')

    return 0


# =============================================================================
if __name__ == "__main__":
    raise SystemExit(main())
