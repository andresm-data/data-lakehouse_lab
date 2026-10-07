"""Convierte un archivo Parquet o CSV a Parquet o a CSV."""
from pathlib import Path

from lakehouse.ingest._cli import base_parser, run
from lakehouse.ingest._io import (
    DEFAULT,
    common_schema,
    remove_target,
    scan,
    write_file,
    write_partitioned,
)
from lakehouse.ingest._validation import validate


# =============================================================================
def convert(
    origin: Path,
    target: Path,
    format_: str = 'parquet',
    compression: str | None = None,
    partition: list[str] | None = None,
    overwrite: bool = False
) -> None:
    """Realiza la conversión de formato de acuerdo a los parámetros
    proporcionados.

    La lectura se realiza por lotes para limitar el uso de memoria. Si se
    indican columnas de partición, la salida es un dataset en estructura de
    directorios estilo Hive; en caso contrario, se genera un único archivo.

    Args:
        origin: Ruta del archivo Parquet o CSV de origen; un CSV puede
            estar comprimido.
        target: Ruta del archivo de salida, o directorio raíz del
            dataset cuando se especifica `partition`.
        format_ (opcional): Formato de destino ('parquet' o 'csv').
            Por defecto 'parquet'.
        compression (opcional): Códec de compresión a aplicar.
            Si es `None`, se usa el valor por defecto del formato definido en
            `DEFAULT`. Por defecto `None`.
        partition (opcional): Columnas por las que se particiona la salida.
            Por defecto `None`.
        overwrite (opcional): Si es `True`, elimina el destino existente
            (archivo o directorio completo) antes de escribir.
            Por defecto `False`.
    """
    compression = compression or DEFAULT[format_]

    validate(origin, target, format_, compression, partition, overwrite)

    # Elimina el destino previo para no mezclar datos antiguos con los nuevos
    if overwrite:
        remove_target(target)

    schema = common_schema([origin])
    batches = scan([origin], schema)

    if partition:
        write_partitioned(batches, schema, target, format_, compression, partition)

    else:
        write_file(batches, schema, target, format_, compression)


# =============================================================================
def main() -> int:
    """Punto de entrada del comando `lh-convert`.

    Returns:
        int: Código de salida del proceso: `0` si la conversión fue exitosa,
            `1` si ocurrió un error.
    """
    parser = base_parser(
        __doc__,
        'Archivo Parquet o CSV de entrada (el CSV puede estar comprimido)',
        'Archivo de salida, o directorio si se usa --particiones',
        many=False
    )
    parser.add_argument(
        '-p',
        '--particiones',
        nargs='+',
        metavar='COL',
        help='Columnas de partición (estilo Hive)'
    )
    args = parser.parse_args()

    return run(
        convert, args.origen, args.destino,
        args.formato, args.compresion, args.particiones, args.sobrescribir
    )


# =============================================================================
if __name__ == "__main__":
    raise SystemExit(main())
