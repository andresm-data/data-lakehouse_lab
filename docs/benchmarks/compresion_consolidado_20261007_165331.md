# Benchmark de compresión consolidado: Parquet vs CSV

- Fecha: 2026-10-07 16:53:31 (duración total: 55 min)
- Origen: `data/interim/yellow_tripdata.parquet` (195.6 MB, zstd; 11,198,001 filas, 22 columnas, 173 row groups), generado con `lh-merge` desde `data/raw/yellow_tripdata.parquet` (enero a marzo de 2025)
- Códecs: todos los que admite cada formato. Parquet: `none`, `snappy`, `gzip`, `brotli`, `lz4`, `zstd`; Parquet no admite `bz2`. CSV: `none`, `gzip`, `bz2`, `lz4`, `zstd`.
- Repeticiones: 6 por códec. El resumen muestra promedio ± desviación estándar; el detalle, cada ejecución.
- Columnas de la lectura parcial: `tpep_pickup_datetime`, `trip_distance`, `total_amount`
- Equipo: AMD Ryzen 5 5500U (12 hilos), 7 GB de RAM, Python 3.14.7, PyArrow 25.0.1

## Metodología

Se aplicó el mismo procedimiento de `lh-bench-compression`, sin modificar el código del proyecto, y se agregó la lectura de 3 columnas. En cada repetición, para cada formato y códec:

1. **Escritura**: `convert` desde el Parquet consolidado. Incluye leer el origen, un costo bajo y parecido para todos los códecs.
2. **Lectura completa**: `_read` del benchmark, que recorre por lotes todas las columnas con `scan`.
3. **Lectura de 3 columnas**:
   - Parquet: `ParquetFile.iter_batches(columns=...)`, que solo lee y descomprime las columnas pedidas.
   - CSV: `pyarrow.csv.open_csv` con `include_columns`. Igual hay que descomprimir y recorrer todo el archivo, pero solo se convierten las 3 columnas.
4. **Tamaño**: tamaño en disco del archivo resultante; es el mismo en todas las repeticiones.

Cada lectura se hace justo después de escribir el archivo, así que suele salir de la caché del sistema operativo. Lo que se mide es sobre todo descompresión y decodificación, no el acceso al disco.

## Resumen: Parquet

| Compresión | Tamaño (MB) | % vs CSV sin comprimir | Escritura (s) | Lectura completa (s) | Lectura 3 columnas (s) | 3 col. / completa |
|---|---:|---:|---:|---:|---:|---:|
| none | 259.2 | 20.5 % | 5.87 ± 0.64 | 0.74 ± 0.10 | 0.26 ± 0.12 | 36 % |
| snappy | 224.4 | 17.7 % | 5.81 ± 0.21 | 0.74 ± 0.11 | 0.29 ± 0.04 | 39 % |
| gzip | 180.6 | 14.3 % | 66.49 ± 0.83 | 0.91 ± 0.04 | 0.47 ± 0.05 | 51 % |
| brotli | 174.5 | 13.8 % | 19.88 ± 0.45 | 0.99 ± 0.07 | 0.55 ± 0.05 | 55 % |
| lz4 | 220.5 | 17.4 % | 7.51 ± 0.64 | 1.11 ± 0.20 | 0.32 ± 0.04 | 29 % |
| zstd | 195.5 | 15.5 % | 7.55 ± 0.40 | 0.87 ± 0.07 | 0.32 ± 0.03 | 37 % |

## Resumen: CSV

| Compresión | Tamaño (MB) | % vs CSV sin comprimir | Escritura (s) | Lectura completa (s) | Lectura 3 columnas (s) | 3 col. / completa |
|---|---:|---:|---:|---:|---:|---:|
| none | 1,264.6 | 100.0 % | 15.48 ± 0.54 | 7.25 ± 0.44 | 3.71 ± 0.13 | 51 % |
| gzip | 203.7 | 16.1 % | 157.50 ± 3.87 | 6.67 ± 0.33 | 3.40 ± 0.10 | 51 % |
| bz2 | 117.2 | 9.3 % | 118.12 ± 0.74 | 31.43 ± 1.52 | 31.47 ± 1.94 | 100 % |
| lz4 | 369.5 | 29.2 % | 15.72 ± 0.20 | 6.37 ± 0.07 | 3.22 ± 0.06 | 51 % |
| zstd | 223.2 | 17.6 % | 16.86 ± 0.26 | 6.48 ± 0.12 | 3.33 ± 0.09 | 51 % |

## Comparación directa (mismo códec, promedios)

| Compresión | Tamaño Parquet / CSV | Escritura Parquet / CSV | Lectura completa Parquet / CSV | Lectura 3 col. Parquet / CSV |
|---|---:|---:|---:|---:|
| none | 0.20× | 0.38× | 0.10× | 0.07× |
| gzip | 0.89× | 0.42× | 0.14× | 0.14× |
| lz4 | 0.60× | 0.48× | 0.18× | 0.10× |
| zstd | 0.88× | 0.45× | 0.13× | 0.10× |

## Observaciones

- **Tamaño**: sin compresión, Parquet ocupa el 20 % de lo que ocupa el CSV gracias a su codificación columnar (diccionarios, RLE, tipos binarios). Al comprimir, la diferencia se reduce: un CSV con gzip o zstd ocupa solo entre un 13 % y un 14 % más que el Parquet con el mismo códec. El archivo más pequeño es el CSV con bz2 (117 MB), seguido de Parquet con brotli (175 MB) y con gzip (181 MB).
- **Escritura**:
  - En Parquet hay tres grupos: `none`, `snappy`, `lz4` y `zstd` (5.8 a 7.6 s), brotli (20 s) y gzip (66 s).
  - En CSV, `none`, `lz4` y `zstd` tardan entre 15.5 y 17 s, mientras que bz2 tarda 118 s y gzip 158 s.
  - Parquet escribe entre 2 y 2.6 veces más rápido que CSV con el mismo códec.
- **Lectura completa**: Parquet tarda entre 0.7 y 1.1 s con cualquier códec. CSV tarda entre 6.4 y 7.3 s, porque el costo lo pone el análisis del texto y no la descompresión, salvo con bz2, que sube a 31 s. Con el mismo códec, Parquet lee entre 5.5 y 10 veces más rápido.
- **Lectura de 3 columnas**: aquí se nota la ventaja del formato columnar. Parquet tarda entre el 29 % y el 55 % del tiempo de la lectura completa, con 0.26 a 0.55 s. CSV queda siempre en un 51 %, porque tiene que descomprimir y recorrer todas las filas, y con bz2 no mejora nada (100 %). En Parquet, gzip y brotli reducen menos el tiempo porque descomprimir cada página cuesta más.
- **Variación entre ejecuciones**: los códecs rápidos de Parquet (`none`, `snappy`, `lz4`, `zstd`) difieren entre sí menos que su propia variación. En la corrida previa de 3 repeticiones lz4 y zstd quedaron por delante de `none` y snappy, y en esta corrida al revés, así que su orden no es concluyente. Las diferencias grandes (gzip y brotli frente al resto, CSV frente a Parquet, bz2 en lectura) sí se repiten en todas las ejecuciones.
- **Balance**: Parquet con zstd ofrece el mejor equilibrio: queda 12 % por encima del mínimo de Parquet (brotli), escribe en el grupo rápido y lee 3 columnas en 0.32 s. Brotli conviene si importa el tamaño y se escribe poco. Gzip no aporta ventajas sobre brotli o zstd en ninguno de los dos formatos.

## Limitaciones

- Lecturas con caché caliente: en frío, o con almacenamiento remoto, los archivos más pequeños ganarían ventaja relativa.
- Una sola máquina con 7 GB de RAM y poca memoria libre (con swap en uso antes de empezar). La presión de memoria puede explicar algunas ejecuciones más lentas, como la primera de varios códecs.
- El tiempo de escritura incluye leer y decodificar el Parquet de origen.

## Ejecuciones: Parquet

| Compresión | Ejecución | Escritura (s) | Lectura completa (s) | Lectura 3 columnas (s) |
|---|---:|---:|---:|---:|
| none | 1 | 6.70 | 0.88 | 0.48 |
| none | 2 | 6.67 | 0.73 | 0.28 |
| none | 3 | 5.69 | 0.83 | 0.28 |
| none | 4 | 5.45 | 0.65 | 0.14 |
| none | 5 | 5.37 | 0.71 | 0.18 |
| none | 6 | 5.33 | 0.62 | 0.22 |
| snappy | 1 | 6.22 | 0.94 | 0.34 |
| snappy | 2 | 5.80 | 0.71 | 0.29 |
| snappy | 3 | 5.73 | 0.66 | 0.26 |
| snappy | 4 | 5.73 | 0.78 | 0.34 |
| snappy | 5 | 5.67 | 0.69 | 0.23 |
| snappy | 6 | 5.69 | 0.65 | 0.26 |
| gzip | 1 | 66.35 | 0.92 | 0.40 |
| gzip | 2 | 65.92 | 0.86 | 0.45 |
| gzip | 3 | 66.01 | 0.88 | 0.47 |
| gzip | 4 | 66.28 | 0.94 | 0.55 |
| gzip | 5 | 68.15 | 0.96 | 0.50 |
| gzip | 6 | 66.21 | 0.92 | 0.42 |
| brotli | 1 | 19.77 | 1.02 | 0.50 |
| brotli | 2 | 19.38 | 0.92 | 0.53 |
| brotli | 3 | 19.41 | 0.96 | 0.52 |
| brotli | 4 | 20.14 | 0.92 | 0.55 |
| brotli | 5 | 20.54 | 1.00 | 0.64 |
| brotli | 6 | 20.07 | 1.10 | 0.53 |
| lz4 | 1 | 8.47 | 1.26 | 0.39 |
| lz4 | 2 | 7.78 | 1.44 | 0.36 |
| lz4 | 3 | 7.20 | 1.07 | 0.28 |
| lz4 | 4 | 6.76 | 0.95 | 0.30 |
| lz4 | 5 | 6.99 | 1.00 | 0.32 |
| lz4 | 6 | 7.83 | 0.97 | 0.28 |
| zstd | 1 | 8.11 | 0.84 | 0.28 |
| zstd | 2 | 7.45 | 0.90 | 0.30 |
| zstd | 3 | 7.47 | 0.96 | 0.31 |
| zstd | 4 | 7.53 | 0.82 | 0.31 |
| zstd | 5 | 7.80 | 0.93 | 0.37 |
| zstd | 6 | 6.91 | 0.77 | 0.34 |

## Ejecuciones: CSV

| Compresión | Ejecución | Escritura (s) | Lectura completa (s) | Lectura 3 columnas (s) |
|---|---:|---:|---:|---:|
| none | 1 | 15.93 | 7.81 | 3.89 |
| none | 2 | 16.37 | 7.03 | 3.62 |
| none | 3 | 15.16 | 6.97 | 3.64 |
| none | 4 | 15.21 | 6.96 | 3.57 |
| none | 5 | 15.09 | 6.91 | 3.83 |
| none | 6 | 15.10 | 7.80 | 3.72 |
| gzip | 1 | 157.64 | 6.52 | 3.33 |
| gzip | 2 | 165.03 | 7.30 | 3.56 |
| gzip | 3 | 156.25 | 6.43 | 3.34 |
| gzip | 4 | 154.39 | 6.48 | 3.37 |
| gzip | 5 | 156.71 | 6.75 | 3.48 |
| gzip | 6 | 155.00 | 6.53 | 3.34 |
| bz2 | 1 | 118.24 | 31.42 | 29.95 |
| bz2 | 2 | 117.73 | 29.11 | 34.50 |
| bz2 | 3 | 117.95 | 32.47 | 29.35 |
| bz2 | 4 | 119.01 | 32.22 | 31.75 |
| bz2 | 5 | 118.80 | 33.16 | 32.81 |
| bz2 | 6 | 116.98 | 30.23 | 30.47 |
| lz4 | 1 | 15.72 | 6.30 | 3.32 |
| lz4 | 2 | 15.55 | 6.41 | 3.24 |
| lz4 | 3 | 15.89 | 6.29 | 3.15 |
| lz4 | 4 | 15.62 | 6.30 | 3.20 |
| lz4 | 5 | 15.50 | 6.43 | 3.20 |
| lz4 | 6 | 16.02 | 6.45 | 3.23 |
| zstd | 1 | 16.86 | 6.53 | 3.38 |
| zstd | 2 | 16.74 | 6.45 | 3.48 |
| zstd | 3 | 17.37 | 6.57 | 3.34 |
| zstd | 4 | 16.78 | 6.58 | 3.27 |
| zstd | 5 | 16.83 | 6.47 | 3.22 |
| zstd | 6 | 16.60 | 6.26 | 3.30 |

