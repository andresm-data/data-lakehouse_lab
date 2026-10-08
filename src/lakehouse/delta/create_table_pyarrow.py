"""Crea una tabla Delta particionada por mes a partir de un Parquet, con pyarrow."""
import argparse
import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from deltalake import DeltaTable, write_deltalake


# =============================================================================
ORIGIN = Path('data/interim/yellow_tripdata.parquet')
TARGET = Path('data/delta/yellow_tripdata_pyarrow')
PARTITION = ['year', 'month']


# =============================================================================
def _counts(table: pa.Table) -> dict[tuple, int]:
    """Cuenta las filas de cada partición (`year`, `month`)."""
    grouped = table.group_by(PARTITION).aggregate([([], 'count_all')])

    return {
        tuple(row[c] for c in PARTITION): row['count_all']
        for row in grouped.to_pylist()
    }


# =============================================================================
def create_table(origin: Path, target: Path) -> None:
    """Escribe el Parquet como tabla Delta particionada por año y mes.

    El Parquet se lee por lotes y se entrega a `deltalake` como un flujo,
    por lo que no se carga completo en memoria. Si la tabla ya existe, se
    sobrescribe.

    Args:
        origin: Archivo Parquet de origen; debe tener las columnas `year`
            y `month`.
        target: Directorio de la tabla Delta.
    """
    source = pq.ParquetFile(origin)
    reader = pa.RecordBatchReader.from_batches(
        source.schema_arrow, source.iter_batches()
    )
    write_deltalake(
        target, reader, partition_by=PARTITION, mode='overwrite',
        schema_mode='overwrite'
    )


# =============================================================================
def validate_rows(origin: Path, target: Path) -> None:
    """Compara las filas de la tabla Delta con las del Parquet original.

    Compara el total de filas y el número de filas de cada partición.

    Args:
        origin: Archivo Parquet de origen.
        target: Directorio de la tabla Delta.

    Raises:
        ValueError: Si el total o alguna partición no coincide.
    """
    expected = _counts(pq.read_table(origin, columns=PARTITION))
    delta = DeltaTable(target).to_pyarrow_dataset()
    actual = _counts(delta.to_table(columns=PARTITION))

    total_expected = sum(expected.values())
    total_actual = sum(actual.values())

    print(f'Filas en Parquet: {total_expected:,}')
    print(f'Filas en Delta:   {total_actual:,}')

    for key in sorted(expected.keys() | actual.keys()):
        mark = 'OK' if expected.get(key) == actual.get(key) else 'DIFERENTE'
        print(f'  year={key[0]} month={key[1]}: '
              f'{expected.get(key, 0):,} vs {actual.get(key, 0):,} [{mark}]')

    if expected != actual:
        raise ValueError('Las filas de la tabla Delta no coinciden con el Parquet')


# =============================================================================
def main() -> int:
    """Crea la tabla Delta y valida sus filas.

    Returns:
        int: `0` si la validación es exitosa, `1` si falla.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('origen', type=Path, nargs='?', default=ORIGIN)
    parser.add_argument('destino', type=Path, nargs='?', default=TARGET)
    args = parser.parse_args()

    create_table(args.origen, args.destino)

    try:
        validate_rows(args.origen, args.destino)

    except ValueError as e:
        print(f'Error: {e}', file=sys.stderr)

        return 1

    print(f'Listo -> {args.destino}')

    return 0


# =============================================================================
if __name__ == "__main__":
    raise SystemExit(main())
