"""Argumentos y ejecución comunes a los comandos de ingesta."""
import argparse
import sys
from collections.abc import Callable
from pathlib import Path

import pyarrow as pa

from lakehouse.ingest._io import AVAILABLE_COMPRESSIONS


# =============================================================================
def base_parser(
    description: str,
    origin_help: str,
    target_help: str,
    many: bool
) -> argparse.ArgumentParser:
    """Crea un parser con los argumentos comunes a todos los comandos.

    Args:
        description: Descripción del comando para la ayuda.
        origin_help: Texto de ayuda del argumento `origen`.
        target_help: Texto de ayuda del argumento `destino`.
        many: Si es `True`, `origen` acepta uno o más valores.

    Returns:
        argparse.ArgumentParser: Parser con `origen`, `destino`,
            `--formato`, `--compresion` y `--sobrescribir`.
    """
    parser = argparse.ArgumentParser(
        description=description,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        'origen', type=Path, nargs='+' if many else None, help=origin_help
    )
    parser.add_argument('destino', type=Path, help=target_help)
    parser.add_argument(
        '-f',
        '--formato',
        choices=AVAILABLE_COMPRESSIONS,
        default='parquet',
        help='Formato de salida (por defecto: parquet)'
    )
    parser.add_argument(
        '-c',
        '--compresion',
        help='Parquet: ' +
        '/'.join(AVAILABLE_COMPRESSIONS['parquet']) +
        ' | CSV: ' + '/'.join(AVAILABLE_COMPRESSIONS['csv'])
    )
    parser.add_argument('--sobrescribir', action='store_true')

    return parser


# =============================================================================
def run(func: Callable[..., None], origin: object, target: Path, *args) -> int:
    """Ejecuta un proceso y reporta el resultado por consola.

    Los errores de validación, de E/S y de pyarrow se muestran en stderr en
    lugar de propagarse.

    Args:
        func: Función del proceso; recibe `origin`, `target` y `args`.
        origin: Origen del proceso.
        target: Destino del proceso.
        *args: Resto de argumentos de `func`.

    Returns:
        int: Código de salida del proceso: `0` si fue exitoso, `1` si
            ocurrió un error.
    """
    try:
        func(origin, target, *args)

    except (ValueError, OSError, pa.ArrowException) as e:
        print(f'Error: {e}', file=sys.stderr)

        return 1

    print(f'Listo -> {target}')

    return 0
