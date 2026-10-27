"""Directorios base de datos y de informes, en un solo lugar.

Por defecto son relativos al directorio de trabajo (`data/` y `docs/`), como
siempre. Se pueden mover con `LAKEHOUSE_DATA_DIR` y `LAKEHOUSE_DOCS_DIR`, por
ejemplo para ejecutar los comandos desde otro directorio o dentro de Docker.
"""
import os
from pathlib import Path


# =============================================================================
DATA_DIR = Path(os.environ.get('LAKEHOUSE_DATA_DIR', 'data'))
DOCS_DIR = Path(os.environ.get('LAKEHOUSE_DOCS_DIR', 'docs'))
