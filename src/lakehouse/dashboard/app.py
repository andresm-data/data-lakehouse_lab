"""App de Streamlit del proyecto.

Se abre con `make dashboard` o con:

    poetry run streamlit run src/lakehouse/dashboard/app.py
"""
from datetime import datetime, timedelta

import pandas as pd
import streamlit as st

from lakehouse.benchmarks import results_db
from lakehouse.dashboard import time_travel
from lakehouse.dashboard.charts import month_chart, size_chart, time_chart
from lakehouse.dashboard.data import load_sizes, load_times, runs


# =============================================================================
@st.cache_data(ttl=300, show_spinner='Leyendo la versión...')
def _month_summary(source: time_travel.Source, version: int) -> pd.DataFrame | None:
    return time_travel.month_summary(source, version)


def _version_label(row: pd.Series, format_: str) -> str:
    name = f'v{row["version"]}' if format_ == 'delta' else str(row['version'])
    mark = ' (actual)' if row['actual'] else ''

    return f'{name} · {row["fecha"]:%Y-%m-%d %H:%M:%S} UTC · {row["operacion"]}{mark}'


# =============================================================================
def benchmarks_tab() -> None:
    st.header('Parquet, Delta Lake e Iceberg con DuckDB')
    db = results_db.RESULTS_DB
    available = runs(db)

    if not available:
        st.info(
            f'No hay resultados en `{db}`. Ejecuta `make bench-formats`, o carga '
            'un CSV ya generado con `poetry run lh-bench-db`.'
        )
        return

    run = st.selectbox('Ejecución', available, format_func=lambda r: f'{r:%Y-%m-%d %H:%M:%S}')
    times = load_times(db, run)
    sizes = load_sizes(db, run)

    st.caption(
        f'Fuente: `{db}` · {times["repeticiones"].max()} repeticiones medidas por '
        'consulta; los tiempos son la mediana.'
    )

    dark = st.context.theme.type == 'dark'
    left, right = st.columns([2, 3])

    with left:
        st.subheader('Tamaño')
        st.plotly_chart(size_chart(sizes, dark), key='tamano')

    with right:
        st.subheader('Tiempo por consulta')
        log = st.toggle('Escala logarítmica', help='Las consultas van de unos pocos ms a cientos')
        st.plotly_chart(time_chart(times, log, dark), key='tiempo')

    st.subheader('Datos')
    st.dataframe(sizes, hide_index=True, column_config={
        'actual_mb': st.column_config.NumberColumn('Versión actual (MB)', format='%.1f'),
        'disco_mb': st.column_config.NumberColumn('En disco (MB)', format='%.1f'),
    })
    st.dataframe(times, hide_index=True, column_config={
        'mediana_ms': st.column_config.NumberColumn('Mediana (ms)', format='%.1f'),
        'min_ms': st.column_config.NumberColumn('Mín (ms)', format='%.1f'),
        'max_ms': st.column_config.NumberColumn('Máx (ms)', format='%.1f'),
    })


# =============================================================================
def snapshots_tab() -> None:
    st.header('Snapshots y time travel')
    st.markdown(
        'Cada escritura en Delta crea una **versión** numerada; en Iceberg, un '
        '**snapshot** con un ID aleatorio. Elige una tabla y una versión anterior '
        'para comparar sus datos con los de la versión actual.'
    )

    sources = {s.name: s for s in time_travel.TABLES}
    source = sources[st.selectbox('Tabla', sources)]
    st.caption(f'`{source.location}` · {source.note}')

    if not time_travel.exists(source):
        command = time_travel.COMMANDS.get(source.location, 'make pipeline')
        st.info(f'La tabla `{source.location}` no existe. Créala con `{command}`.')
        return

    history = time_travel.versions(source)
    current = history[history['actual']].iloc[0]
    st.subheader('Historial')
    st.dataframe(
        history.assign(version=history['version'].astype(str)),
        hide_index=True,
        column_config={
            'version': 'Versión' if source.format_ == 'delta' else 'snapshot_id',
            'fecha': st.column_config.DatetimeColumn('Fecha (UTC)', format='YYYY-MM-DD HH:mm:ss'),
            'operacion': 'Operación',
            'filas': st.column_config.NumberColumn('Filas', format='%d'),
            'actual': 'Actual',
            'en_linea_actual': st.column_config.CheckboxColumn(
                'En la línea actual', help='La actual o una de sus antecesoras'),
        },
    )

    st.subheader('Consultar una versión anterior')
    mode = st.radio('Consultar por', ['Versión', 'Fecha y hora (UTC)'], horizontal=True)

    if mode == 'Versión':
        labels = {_version_label(r, source.format_): int(r['version'])
                  for _, r in history.iterrows()}
        version = labels[st.selectbox('Versión', labels)]
    else:
        # El selector de hora va por minutos: por defecto, el minuto siguiente
        # a la primera versión, para que la consulta ya la encuentre.
        start = (history['fecha'].iloc[0] + timedelta(minutes=1)).replace(second=0, microsecond=0)
        day_col, time_col = st.columns(2)
        day = day_col.date_input('Fecha', start.date(), key='dia')
        moment = time_col.time_input('Hora', start.time(), step=60, key='hora')
        st.caption('Se usa la versión vigente en ese minuto. Para elegir entre versiones '
                   'del mismo minuto, consulta por versión.')
        version = time_travel.as_of(source, datetime.combine(day, moment))

        if version is None:
            st.warning('La tabla todavía no existía en esa fecha.')
            return

    row = history[history['version'] == version].iloc[0]
    st.markdown(f'Versión consultada: **{_version_label(row, source.format_)}**')

    if not row['en_linea_actual']:
        st.info(
            'Este snapshot ya no está en la línea de la tabla (se deshizo con un '
            'rollback), pero se puede leer hasta que se expire.'
        )

    before = _month_summary(source, version)

    if before is None:
        st.warning(
            'Los archivos de esta versión ya no están en disco: VACUUM los borró. '
            'El time travel solo llega hasta la retención de VACUUM.'
        )
        return

    after = _month_summary(source, int(current['version']))
    comparison = time_travel.compare(before, after)
    name = f'v{version}' if source.format_ == 'delta' else f'Snapshot {version}'

    cols = st.columns(3)
    cols[0].metric('Viajes en la versión', f'{comparison["viajes_version"].sum():,}',
                   f'{-comparison["diferencia"].sum():+,} frente a la actual', delta_color='off')
    cols[1].metric('Viajes en la actual', f'{comparison["viajes_actual"].sum():,}')
    cols[2].metric('Nulos de congestion_surcharge',
                   f'{comparison["nulos_congestion_version"].sum():,}',
                   f'{comparison["nulos_congestion_actual"].sum():,} en la actual',
                   delta_color='off')

    st.plotly_chart(month_chart(comparison, source.format_, name,
                                st.context.theme.type == 'dark'), key='meses')
    st.dataframe(comparison, hide_index=True, column_config={
        'mes': 'Mes',
        'viajes_version': st.column_config.NumberColumn(f'Viajes ({name})', format='%d'),
        'viajes_actual': st.column_config.NumberColumn('Viajes (actual)', format='%d'),
        'diferencia': st.column_config.NumberColumn('Diferencia', format='%+d'),
        'total_version': st.column_config.NumberColumn(f'total_amount ({name})', format='%.2f'),
        'total_actual': st.column_config.NumberColumn('total_amount (actual)', format='%.2f'),
        'nulos_congestion_version': st.column_config.NumberColumn(
            f'Nulos congestion ({name})', format='%d'),
        'nulos_congestion_actual': st.column_config.NumberColumn(
            'Nulos congestion (actual)', format='%d'),
    })


# =============================================================================
st.set_page_config(page_title='Lakehouse Lab', layout='wide')
st.title('Lakehouse Lab')

benchmarks, snapshots = st.tabs(['Benchmarks', 'Snapshots y time travel'])

with benchmarks:
    benchmarks_tab()

with snapshots:
    snapshots_tab()
