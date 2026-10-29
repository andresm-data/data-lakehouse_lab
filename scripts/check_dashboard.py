"""Comprueba que las 3 pestañas del dashboard cargan con los datos reales.

Ejecuta la app con AppTest de Streamlit (sin navegador), recorre cada
pestaña y cada tabla de la pestaña de time travel, e imprime lo que
muestra cada una. Termina con código 1 si alguna lanza una excepción o
no tiene datos.

    python scripts/check_dashboard.py              # en el entorno local
    make docker-dashboard-check                     # dentro del contenedor
"""
import sys
from pathlib import Path

from streamlit.testing.v1 import AppTest

import lakehouse.dashboard
from lakehouse.dashboard import time_travel


# =============================================================================
APP = Path(lakehouse.dashboard.__file__).with_name('app.py')
TABS = ['Benchmarks', 'Snapshots y time travel', 'Guía de formatos']


def check(ok: bool, message: str, errors: list[str]) -> None:
    print(f'  [{"ok" if ok else "ERROR"}] {message}')

    if not ok:
        errors.append(message)


# =============================================================================
def main() -> int:
    errors: list[str] = []
    app = AppTest.from_file(str(APP), default_timeout=120).run()

    print('App')
    check(not app.exception, 'abre sin excepciones', errors)
    check([t.label for t in app.tabs] == TABS, f'pestañas: {[t.label for t in app.tabs]}', errors)

    if errors:
        return 1

    print(f'1. {TABS[0]}')
    tab = app.tabs[0]
    check(len(tab.dataframe) == 2, f'{len(tab.dataframe)} tablas de resultados', errors)
    check(len(tab.get('plotly_chart')) == 2, f'{len(tab.get("plotly_chart"))} gráficos', errors)

    print(f'2. {TABS[1]}')

    for source in time_travel.TABLES:
        box = next(s for s in app.tabs[1].selectbox if s.label == 'Tabla')
        box.set_value(source.name).run()
        tab = app.tabs[1]
        shown = ([f'{m.label}: {m.value}' for m in tab.metric]
                 or [w.value.split('.')[0] for w in [*tab.warning, *tab.info]])
        check(not app.exception and bool(tab.dataframe),
              f'{source.name}: {len(tab.dataframe[0].value) if tab.dataframe else 0} versiones; '
              f'{" · ".join(shown)}', errors)

    print(f'3. {TABS[2]}')
    tab = app.tabs[2]
    text = '\n'.join(m.value for m in tab.markdown)
    check(len(tab.subheader) == 5, f'{len(tab.subheader)} secciones', errors)
    check('Total de las' in text, 'cifras del benchmark en la guía', errors)

    print('Resultado:', 'todo bien' if not errors else f'{len(errors)} errores')

    return 1 if errors else 0


# =============================================================================
if __name__ == '__main__':
    sys.exit(main())
