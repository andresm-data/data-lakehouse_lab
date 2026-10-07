"""Une varios archivos Parquet o CSV en un único archivo Parquet o CSV."""
from pathlib import Path

from lakehouse.ingest._cli import base_parser, run
from lakehouse.ingest._io import (
    DEFAULT,
    common_schema,
    expand_origins,
    remove_target,
    scan,
    write_file,
)
from lakehouse.ingest._validation import validate


# =============================================================================
def merge(
    origins: list[Path],
    target: Path,
    format_: str = 'parquet',
    compression: str | None = None,
    overwrite: bool = False
) -> None:
    """Une varios archivos Parquet o CSV en un único archivo.

    Los archivos de origen deben ser todos del mismo formato y compartir el
    esquema; los CSV pueden estar comprimidos. Un directorio se recorre de
    forma recursiva y se lee como dataset particionado estilo Hive: las
    particiones (por ejemplo `year=2025`) se agregan como columnas en la
    salida. Los archivos se leen de uno en uno y por lotes para limitar el
    uso de memoria.

    Args:
        origins: Archivos de origen, o una lista con un único directorio.
        target: Ruta del archivo de salida.
        format_ (opcional): Formato de destino ('parquet' o 'csv').
            Por defecto 'parquet'.
        compression (opcional): Códec de compresión a aplicar. Si es
            `None`, se usa el valor por defecto del formato definido en
            `DEFAULT`. Por defecto `None`.
        overwrite (opcional): Si es `True`, elimina el destino existente
            antes de escribir. Por defecto `False`.
    """
    compression = compression or DEFAULT[format_]
    files, base_dir = expand_origins(origins)

    for f in files:
        validate(f, target, format_, compression, None, overwrite)

    schema = common_schema(files, base_dir)

    if overwrite:
        remove_target(target)

    write_file(scan(files, schema, base_dir), schema, target, format_, compression)


# =============================================================================
def main() -> int:
    """Punto de entrada del comando `lh-merge`.

    Returns:
        int: Código de salida del proceso: `0` si la unión fue exitosa,
            `1` si ocurrió un error.
    """
    parser = base_parser(
        __doc__,
        'Archivos Parquet o CSV de entrada, o un directorio (dataset '
        'particionado)',
        'Archivo de salida',
        many=True
    )
    args = parser.parse_args()

    return run(
        merge, args.origen, args.destino,
        args.formato, args.compresion, args.sobrescribir
    )


# =============================================================================
if __name__ == "__main__":
    raise SystemExit(main())
