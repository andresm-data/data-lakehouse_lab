"""Crea la tabla Iceberg de taxis con hidden partitioning por mes y la carga."""
import argparse
import shutil
import sys
from pathlib import Path
from urllib.parse import urlparse

import pyarrow as pa
import pyarrow.parquet as pq
from deltalake import DeltaTable
from pyiceberg.catalog import Catalog
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import assign_fresh_schema_ids
from pyiceberg.table import Table
from pyiceberg.transforms import MonthTransform

from lakehouse.iceberg.catalog import (
    CATALOG_DB, NAMESPACE, WAREHOUSE, ensure_namespace, get_catalog,
)


# =============================================================================
ORIGIN = Path('data/interim/yellow_tripdata.parquet')
DELTA = Path('data/delta/yellow_tripdata_pyarrow')
IDENTIFIER = f'{NAMESPACE}.yellow_tripdata'
DATE_COLUMN = 'tpep_pickup_datetime'
PARTITION_NAME = 'pickup_month'
COUNT_KEY = ['year', 'month']


# =============================================================================
def _counts(table: pa.Table) -> dict[tuple, int]:
    """Cuenta las filas de cada (`year`, `month`)."""
    grouped = table.group_by(COUNT_KEY).aggregate([([], 'count_all')])

    return {
        tuple(row[c] for c in COUNT_KEY): row['count_all']
        for row in grouped.to_pylist()
    }


# =============================================================================
def describe_spec(table: Table) -> str:
    """Describe la partición como `month(tpep_pickup_datetime) -> pickup_month`."""
    schema = table.schema()

    return ', '.join(
        f'{f.transform}({schema.find_column_name(f.source_id)}) -> {f.name}'
        for f in table.spec().fields
    ) or 'sin partición'


# =============================================================================
def create_table(catalog: Catalog, identifier: str, schema: pa.Schema) -> Table:
    """Crea la tabla Iceberg particionada por `month(tpep_pickup_datetime)`.

    La partición es una transformación sobre la columna de fecha: no se
    agrega ninguna columna nueva a la tabla. Se pasa al crear la tabla para
    que sea su única spec (`spec_id = 0`); si se agregara después con
    `update_spec`, la tabla tendría además una spec 0 vacía. Si la tabla ya
    existe, se borra del catálogo junto con sus archivos y se vuelve a crear.

    Args:
        catalog (Catalog): Catálogo de Iceberg.
        identifier (str): Nombre `namespace.tabla`.
        schema (pa.Schema): Esquema Arrow de los datos; Iceberg asigna los
            IDs de columna.

    Returns:
        Table: Tabla vacía (sin snapshots).
    """
    if catalog.table_exists(identifier):
        location = urlparse(catalog.load_table(identifier).location()).path
        catalog.drop_table(identifier)
        shutil.rmtree(location, ignore_errors=True)

    # La spec referencia la columna por ID, así que el esquema Arrow se
    # convierte antes a uno de Iceberg con IDs asignados.
    iceberg_schema = assign_fresh_schema_ids(
        Catalog._convert_schema_if_needed(schema)
    )
    spec = PartitionSpec(PartitionField(
        source_id=iceberg_schema.find_field(DATE_COLUMN).field_id,
        field_id=1000, transform=MonthTransform(), name=PARTITION_NAME,
    ))

    return catalog.create_table(
        identifier, schema=iceberg_schema, partition_spec=spec
    )


# =============================================================================
def load(table: Table, origin: Path) -> list[tuple[int, int]]:
    """Carga el Parquet en la tabla con un `append` por mes.

    pyiceberg no acepta un flujo de lotes en tablas particionadas, y las
    11 M de filas no caben juntas en memoria; por eso se escribe un mes a la
    vez. Cada `append` crea un snapshot.

    Args:
        table (Table): Tabla Iceberg de destino.
        origin (Path): Parquet de origen con las columnas `year` y `month`.

    Returns:
        list[tuple[int, int]]: Los (`year`, `month`) cargados, en orden.
    """
    months = sorted(_counts(pq.read_table(origin, columns=COUNT_KEY)))

    for year, month in months:
        data = pq.read_table(
            origin, filters=[('year', '=', year), ('month', '=', month)]
        )
        table.append(data, snapshot_properties={'source-month': f'{year}-{month:02d}'})
        print(f'  append {year}-{month:02d}: {data.num_rows:,} filas')
        del data

    return months


# =============================================================================
def validate_rows(table: Table, origin: Path, delta: Path | None) -> None:
    """Compara las filas por mes de la tabla Iceberg con el Parquet y Delta.

    Args:
        table (Table): Tabla Iceberg.
        origin (Path): Parquet de origen.
        delta (Path | None): Tabla Delta creada desde el mismo origen; si es
            `None` o no existe, solo se compara con el Parquet.

    Raises:
        ValueError: Si el total o algún mes no coincide.
    """
    sources = {'Parquet': _counts(pq.read_table(origin, columns=COUNT_KEY))}

    if delta is not None and delta.exists():
        dataset = DeltaTable(delta).to_pyarrow_dataset()
        sources['Delta'] = _counts(dataset.to_table(columns=COUNT_KEY))

    iceberg = _counts(table.scan(selected_fields=tuple(COUNT_KEY)).to_arrow())
    sources['Iceberg'] = iceberg

    for name, counts in sources.items():
        print(f'Filas en {name + ":":<9}{sum(counts.values()):>12,}')

    keys = sorted(set().union(*sources.values()))

    for key in keys:
        values = [counts.get(key, 0) for counts in sources.values()]
        mark = 'OK' if len(set(values)) == 1 else 'DIFERENTE'
        print(f'  year={key[0]} month={key[1]}: '
              + ' vs '.join(f'{v:,}' for v in values) + f' [{mark}]')

    if any(counts != iceberg for counts in sources.values()):
        raise ValueError('Las filas de la tabla Iceberg no coinciden')


# =============================================================================
def main() -> int:
    """Crea la tabla Iceberg, la carga y valida sus filas.

    Returns:
        int: `0` si la validación es exitosa, `1` si falla.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('origen', type=Path, nargs='?', default=ORIGIN)
    parser.add_argument('--delta', type=Path, default=DELTA)
    parser.add_argument('--catalogo', type=Path, default=CATALOG_DB)
    parser.add_argument('--warehouse', type=Path, default=WAREHOUSE)
    parser.add_argument('--tabla', default=IDENTIFIER)
    args = parser.parse_args()

    catalog = get_catalog(args.catalogo, args.warehouse)
    ensure_namespace(catalog, args.tabla.rsplit('.', 1)[0])

    schema = pq.read_schema(args.origen)
    table = create_table(catalog, args.tabla, schema)
    print(f'Tabla {args.tabla} creada, partición: {describe_spec(table)}')

    load(table, args.origen)
    table.refresh()

    try:
        validate_rows(table, args.origen, args.delta)

    except ValueError as e:
        print(f'Error: {e}', file=sys.stderr)

        return 1

    print(f'Listo -> {table.location()}')

    return 0


# =============================================================================
if __name__ == "__main__":
    raise SystemExit(main())
