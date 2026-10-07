# Benchmark de compresión: parquet

- Fecha: 2026-10-06 19:41:02
- Origen: `data/landing/yellow_tripdata_2025-01.parquet` (56.4 MB)
- Repeticiones: 3 (tiempos: mediana)

| Compresión | Escritura (s) | Tamaño (MB) | Lectura (s) |
|---|---:|---:|---:|
| zstd | 1.80 | 60.8 | 0.21 |
| gzip | 20.34 | 56.1 | 0.22 |
| lz4 | 1.58 | 68.7 | 0.20 |
| snappy | 1.60 | 69.9 | 0.21 |
