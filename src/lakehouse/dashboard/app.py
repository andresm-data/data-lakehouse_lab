"""App de Streamlit del proyecto.

Se abre con `make dashboard` o con:

    poetry run streamlit run src/lakehouse/dashboard/app.py
"""
import streamlit as st


# =============================================================================
st.set_page_config(page_title='Lakehouse Lab', layout='wide')
st.title('Lakehouse Lab')

(benchmarks,) = st.tabs(['Benchmarks'])

with benchmarks:
    st.header('Parquet, Delta Lake e Iceberg con DuckDB')
