"""App de Streamlit del proyecto.

Se abre con `make dashboard` o con:

    poetry run streamlit run src/lakehouse/dashboard/app.py
"""
import streamlit as st

from lakehouse.benchmarks import results_db
from lakehouse.dashboard.data import load_sizes, load_times, runs


# =============================================================================
st.set_page_config(page_title='Lakehouse Lab', layout='wide')
st.title('Lakehouse Lab')

(benchmarks,) = st.tabs(['Benchmarks'])

with benchmarks:
    st.header('Parquet, Delta Lake e Iceberg con DuckDB')
    db = results_db.RESULTS_DB
    available = runs(db)

    if not available:
        st.info(
            f'No hay resultados en `{db}`. Ejecuta `make bench-formats`, o carga '
            'un CSV ya generado con `poetry run lh-bench-db`.'
        )
        st.stop()

    run = st.selectbox('Ejecución', available, format_func=lambda r: f'{r:%Y-%m-%d %H:%M:%S}')
    times = load_times(db, run)
    sizes = load_sizes(db, run)

    st.caption(
        f'Fuente: `{db}` · {times["repeticiones"].max()} repeticiones medidas por '
        'consulta; los tiempos son la mediana.'
    )

    st.subheader('Tamaño')
    st.dataframe(sizes, hide_index=True, column_config={
        'actual_mb': st.column_config.NumberColumn('Versión actual (MB)', format='%.1f'),
        'disco_mb': st.column_config.NumberColumn('En disco (MB)', format='%.1f'),
    })

    st.subheader('Tiempo por consulta')
    st.dataframe(times, hide_index=True, column_config={
        'mediana_ms': st.column_config.NumberColumn('Mediana (ms)', format='%.1f'),
        'min_ms': st.column_config.NumberColumn('Mín (ms)', format='%.1f'),
        'max_ms': st.column_config.NumberColumn('Máx (ms)', format='%.1f'),
    })
