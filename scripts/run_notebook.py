"""Ejecuta un notebook desde la raíz del repositorio y guarda las salidas en el mismo archivo.

Uso: python scripts/run_notebook.py [notebook]   (por defecto, notebooks/comparacion_formatos.ipynb)

Termina con código 1 si alguna celda lanza una excepción; en ese caso el
notebook no se sobrescribe.
"""
import sys
from pathlib import Path

import nbformat
from nbclient import NotebookClient
from nbclient.exceptions import CellExecutionError

NOTEBOOK = Path('notebooks/comparacion_formatos.ipynb')
TIMEOUT_S = 600


def main() -> int:
    path = Path(sys.argv[1]) if len(sys.argv) > 1 else NOTEBOOK
    nb = nbformat.read(path, as_version=4)
    client = NotebookClient(nb, timeout=TIMEOUT_S, kernel_name='python3',
                            resources={'metadata': {'path': str(Path.cwd())}})

    try:
        client.execute()
    except CellExecutionError as error:
        print(error, file=sys.stderr)
        return 1

    nbformat.write(nb, path)
    print(f'Notebook ejecutado: {path}')

    return 0


if __name__ == '__main__':
    sys.exit(main())
