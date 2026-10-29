"""Guía de formatos: cuándo usar Parquet, Delta Lake o Iceberg.

El texto resume lo que el proyecto comprobó en las semanas 1 a 3 (ver el
README). Las cifras del benchmark no se escriben aquí: `evidence` las
calcula desde la base de resultados, para que la guía muestre siempre la
ejecución elegida.
"""
import pandas as pd

from lakehouse.benchmarks.queries import FORMATS


# =============================================================================
NAMES = {'parquet': 'Parquet', 'delta': 'Delta Lake', 'iceberg': 'Apache Iceberg'}
COUNT_QUERY = 'q1_conteo'

GUIDE: dict[str, dict[str, list[str]]] = {
    'parquet': {
        'usar': [
            'Datos que se escriben una vez y se leen muchas veces.',
            'Compartir archivos con cualquier herramienta: no necesita extensiones ni catálogo.',
            'Archivo histórico o línea base para medir cuánto cuestan Delta e Iceberg.',
        ],
        'evitar': [
            'Hay que actualizar o borrar filas: se reescribe el archivo completo.',
            'Varios procesos escriben a la vez: no hay transacciones.',
            'Se necesita historial de versiones o validar el esquema al escribir.',
        ],
        'proyecto': [
            'Sin formato de tabla no se valida el esquema al escribir; las pruebas de la semana 2 lo muestran.',
            'Con zstd fue el mejor equilibrio de tamaño y lectura, y leyó entre 5.5 y 10 veces '
            'más rápido que CSV con el mismo códec.',
        ],
    },
    'delta': {
        'usar': [
            'Correcciones e inserciones frecuentes con `MERGE`, `UPDATE` y `DELETE`.',
            'Consultas analíticas con DuckDB sobre una tabla que también recibe escrituras.',
            'Un historial lineal y fácil de seguir: versiones numeradas 0, 1, 2...',
        ],
        'evitar': [
            'La partición va a cambiar: en Delta eso obliga a reescribir toda la tabla.',
            'No se puede programar el mantenimiento: sin `OPTIMIZE` se acumulan archivos '
            'pequeños y sin `VACUUM` crece el disco.',
            'Se necesita volver a versiones antiguas durante mucho tiempo: `VACUUM` las borra.',
        ],
        'proyecto': [
            'El `MERGE` de 350 filas reescribió solo el archivo de enero, porque la condición '
            'incluía la partición.',
            'Agregar una columna con `schema_mode=\'merge\'` no reescribió ningún archivo.',
            '`VACUUM` con retención de 0 h liberó 144 MB, y la versión 0 dejó de poder leerse.',
            'La partición es estilo Hive: `year` y `month` son columnas físicas que hay que '
            'calcular al escribir.',
        ],
    },
    'iceberg': {
        'usar': [
            'La partición puede cambiar con el volumen de datos (por ejemplo, de mes a día).',
            'Filtrar por la columna original (una fecha) sin columnas de partición extra.',
            'Varios motores sobre la misma tabla con un catálogo compartido.',
            'Deshacer una escritura mala moviendo el puntero a un snapshot anterior.',
        ],
        'evitar': [
            'No se quiere operar un catálogo: Iceberg lo necesita para saber cuál es el '
            'metadata.json actual.',
            'Escriben motores distintos y no se fijan las propiedades de la tabla (códec, '
            'tamaño de row group).',
        ],
        'proyecto': [
            'Pasar la partición de `month()` a `day()` solo agregó una spec: no movió ningún archivo.',
            'Con *hidden partitioning*, el filtro por fecha leyó solo el archivo de febrero.',
            '`rollback_to_snapshot` volvió atrás sin escribir datos; el snapshot deshecho se '
            'puede leer hasta que se expire.',
            'Su lentitud en el benchmark viene de la escritura, no del formato: Spark dejó '
            'febrero en GZIP y en un solo row group, que DuckDB lee con un solo hilo.',
        ],
    },
}

CAPABILITIES = pd.DataFrame(
    [
        ('Transacciones ACID', 'No', 'Sí, con el log `_delta_log`', 'Sí, con snapshots y el catálogo'),
        ('Valida el esquema al escribir', 'No', 'Sí', 'Sí'),
        ('Agregar columnas', 'Sin control: cada archivo trae su esquema', 'Sin reescribir (`schema_mode=\'merge\'`)',
         'Sin reescribir'),
        ('Time travel', 'No', 'Por número de versión o fecha', 'Por `snapshot_id` o fecha'),
        ('Deshacer cambios', 'No', '`RESTORE`: escribe un commit nuevo',
         'Rollback: mueve el puntero, sin escribir datos'),
        ('Partición', 'Carpetas estilo Hive', 'Columnas físicas (`year`, `month`)',
         'Transformación oculta (`month(fecha)`)'),
        ('Cambiar la partición', 'Reescribir todo', 'Reescribir todo', 'Sin reescribir: nueva spec'),
        ('`UPDATE` / `DELETE` / `MERGE`', 'No', 'Sí; reescribe los archivos afectados',
         'Sí; reescribe los archivos afectados'),
        ('Mantenimiento', 'Ninguno', '`OPTIMIZE` y `VACUUM`', 'Compactar y expirar snapshots'),
        ('Necesita catálogo', 'No', 'No (la ruta basta)', 'Sí'),
    ],
    columns=['Capacidad', 'Parquet', 'Delta Lake', 'Iceberg'],
)

DECISIONS = pd.DataFrame(
    [
        ('Archivos de solo lectura para compartir con cualquier herramienta', 'Parquet',
         'Lo lee cualquier motor sin extensiones ni catálogo.'),
        ('Corregir o insertar filas con frecuencia', 'Delta Lake',
         'El `MERGE` reescribió solo la partición afectada.'),
        ('Consultas analíticas rápidas con DuckDB', 'Delta Lake',
         'Fue el más rápido en la mayoría de las consultas del benchmark.'),
        ('Una partición que va a cambiar', 'Iceberg',
         'La evolución de partición no reescribió archivos.'),
        ('Filtrar por fecha sin columnas de partición', 'Iceberg',
         'El *hidden partitioning* podó los otros meses solo con la fecha.'),
        ('Conteos y metadatos al instante', 'Iceberg',
         '`count(*)` sale de los manifiestos, sin abrir los Parquet.'),
        ('Volver a un estado anterior', 'Iceberg o Delta Lake',
         'Iceberg mueve el puntero; Delta escribe un commit con `RESTORE`. En los dos, '
         'solo mientras los archivos sigan en disco.'),
    ],
    columns=['Si necesitas...', 'Usa', 'Por qué (lo que midió el proyecto)'],
)


# =============================================================================
def evidence(times: pd.DataFrame, sizes: pd.DataFrame) -> pd.DataFrame:
    """Resumen de una ejecución del benchmark por formato.

    Args:
        times: Resultado de `data.load_times`.
        sizes: Resultado de `data.load_sizes`.

    Returns:
        pd.DataFrame: Una fila por formato, en el orden de `FORMATS`, con
            `total_ms` (suma de las medianas), `relativo` (frente a Parquet),
            `ganadas` (consultas en las que fue el más rápido), `consultas`,
            `conteo_ms` (`COUNT_QUERY`, si se midió), `archivos`,
            `actual_mb` y `disco_mb`.
    """
    fastest = times.loc[times.groupby('consulta')['mediana_ms'].idxmin(), 'formato']
    counts = times[times['consulta'] == COUNT_QUERY].set_index('formato')['mediana_ms']
    df = times.groupby('formato').agg(total_ms=('mediana_ms', 'sum'))
    df['ganadas'] = fastest.value_counts().reindex(df.index, fill_value=0)
    df['consultas'] = times['consulta'].nunique()
    df['conteo_ms'] = counts.reindex(df.index)
    df = df.join(sizes.set_index('formato')[['archivos', 'actual_mb', 'disco_mb']])
    df = df.reindex([f for f in FORMATS if f in df.index])
    df.insert(1, 'relativo', df['total_ms'] / df.loc['parquet', 'total_ms']
              if 'parquet' in df.index else float('nan'))

    return df.rename_axis('formato').reset_index()


# =============================================================================
def measured(row: pd.Series) -> list[str]:
    """Frases con las cifras de un formato, a partir de una fila de `evidence`."""
    # Una fila de un DataFrame con columnas mixtas llega con los enteros como float.
    queries, wins = int(row['consultas']), int(row['ganadas'])
    lines = [
        f'Total de las {queries} consultas: **{row["total_ms"]:,.1f} ms** '
        f'({row["relativo"]:.2f}x Parquet); el más rápido en {wins} de {queries}.',
    ]

    if pd.notna(row['conteo_ms']):
        lines.append(f'`count(*)`: {row["conteo_ms"]:,.1f} ms.')

    if pd.notna(row['actual_mb']):
        files = int(row['archivos'])
        lines.append(
            f'Tamaño: {row["actual_mb"]:,.1f} MB en la versión actual y '
            f'{row["disco_mb"]:,.1f} MB en disco ({files} archivo{"s" * (files != 1)}).'
        )

    return lines


# =============================================================================
def to_markdown(df: pd.DataFrame) -> str:
    """Tabla Markdown, para que Streamlit muestre el código y las cursivas."""
    def row(cells) -> str:
        return '| ' + ' | '.join(str(c).replace('|', r'\|') for c in cells) + ' |'

    return '\n'.join([row(df.columns), '|' + '---|' * len(df.columns),
                      *(row(r) for r in df.itertuples(index=False))])
