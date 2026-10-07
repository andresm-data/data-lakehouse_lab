"""Validaciones comunes a los comandos de ingesta."""
from pathlib import Path

import pyarrow as pa

from lakehouse.ingest._io import AVAILABLE_COMPRESSIONS, input_format, read_schema


# =============================================================================
def validate(
    origin: Path,
    target: Path,
    format_: str,
    compression: str,
    partition: list[str] | None,
    overwrite: bool
) -> None:
    """Valida los parámetros de entrada del proceso.

    Args:
        origin: Ruta del archivo Parquet o CSV de origen.
        target: Ruta de salida; archivo o directorio si se particiona.
        format_: Formato de destino ('parquet' o 'csv').
        compression: Códec de compresión ya resuelto para el formato.
        partition: Columnas de partición estilo Hive, o `None` para
            generar un único archivo.
        overwrite: Si es `True`, permite escribir sobre un destino
            existente.

    Raises:
        ValueError: Si la compresión no es válida para el formato indicado.
        ValueError: Si el códec no está disponible en el build de `pyarrow`.
        ValueError: Si se solicita CSV particionado con compresión.
        FileNotFoundError: Si el archivo de origen no existe.
        ValueError: Si el formato del origen no está soportado.
        FileExistsError: Si el destino ya existe y no se permite
            sobrescribir.
        ValueError: Si el destino coincide con el origen o lo contiene.
        ValueError: Si alguna columna de partición no existe en el esquema
            del origen.
    """
    # Compresión no disponible
    if compression not in AVAILABLE_COMPRESSIONS[format_]:
        raise ValueError(
            f'Compresión "{compression}" no válida para {format_}. '
            f'Opciones: {", ".join(AVAILABLE_COMPRESSIONS[format_])}'
        )

    elif compression != 'none' and not pa.Codec.is_available(compression):
        raise ValueError(
            f'Este build de pyarrow no incluye el códec "{compression}"'
        )

    # Formatos no disponibles
    if format_ == 'csv' and partition and compression != 'none':
        raise ValueError(
            'CSV particionado no admite compresión con pyarrow; use -c none'
        )

    # Rutas
    if not origin.is_file():
        raise FileNotFoundError(f'No existe el archivo de origen: {origin}')

    input_format(origin)

    if target.exists() and not overwrite:
        raise FileExistsError(f'{target} ya existe (use --sobrescribir)')

    if target.resolve() in (origin.resolve(), *origin.resolve().parents):
        raise ValueError(
            f'El destino {target} contiene o coincide con el origen {origin}'
        )

    if partition:
        missing = [c for c in partition if c not in read_schema(origin).names]

        if missing:
            raise ValueError(f'Columnas inexistentes: {missing}')
