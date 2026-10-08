# Evolución de esquema en Delta: columna `trip_duration_min`

- Fecha: 2026-10-08 17:13:29
- Tabla: `data/delta_demo/yellow_tripdata`
- Lote añadido: 100 filas con `write_deltalake(..., mode='append', schema_mode='merge')`

## Verificación

| Comprobación | Versión 1 | Versión 2 |
|---|---|---|
| `trip_duration_min` en el esquema | No | Sí |
| Filas en la tabla | 11,198,051 | 11,198,151 |

- Versión nueva: 1 → 2, operación `WRITE (Append)`
- Filas con `trip_duration_min` nulo: 11,198,051 (las 11,198,051 filas anteriores)
- Filas con `trip_duration_min` con valor: 100 (el lote nuevo)

Ejemplo de filas anteriores (nulo) y nuevas (con valor):

| VendorID | tpep_pickup_datetime | PULocationID | DOLocationID | trip_duration_min |
|---|---|---|---|---|
| 1 | 2025-01-01 00:44:04 | 141 | 141 | null |
| 1 | 2025-01-01 00:14:47 | 170 | 170 | null |
| 1 | 2025-01-01 00:30:07 | 229 | 141 | null |
| 1 | 2025-01-01 00:44:06 | 141 | 141 | 1.95 |
| 1 | 2025-01-01 00:14:49 | 170 | 170 | 1.47 |
| 1 | 2025-01-01 00:30:09 | 229 | 141 | 6.68 |
