# Notas: actualizar un mes completo en Delta e Iceberg con Spark

Ejecución del 2026-10-22 con `lh-spark-update-month` ([informe](update_month_20261022_200501.md), [datos](update_month_20261022_200501.json)). Se actualizó febrero de 2025 (3,577,542 viajes) en las dos tablas de taxis.

## Qué se hizo

- **Cambio**: en 806,937 viajes del mes, `congestion_surcharge` y `Airport_fee` venían nulos; se pusieron en 0.0. Se reescribió el mes completo, no solo esas filas.
- **Delta**: `df.write.format('delta').mode('overwrite').option('replaceWhere', 'year = 2025 AND month = 2')`. Delta rechaza la escritura si alguna fila no cumple el predicado.
- **Iceberg**: `df.writeTo('lakehouse.lakehouse.yellow_tripdata').overwrite(condición)`, con la condición sobre `tpep_pickup_datetime` en `[2025-02-01, 2025-03-01)`. Iceberg la resuelve con la partición oculta `month()`. Se escribe con literales `TIMESTAMP_NTZ` porque la columna no tiene zona horaria.
- En los dos formatos el DataFrame se leyó de la misma tabla que se reescribe; no hubo conflicto, porque cada lectura usa una versión o snapshot fijo.

## Comparación

| Medida | Delta | Iceberg |
|---|---|---|
| Versión / snapshot | 0 → 1 | `3030128708969914628` → `2174020378674165115` |
| Operación registrada | `WRITE` con `predicate` | `overwrite` |
| Tiempo de escritura | 12.31 s | 14.89 s |
| Archivos del mes | 1 → 4 | 2 → 1 |
| Tamaño del mes | 70.0 MB → 70.0 MB | 59.3 MB → 54.4 MB |
| Compresión nueva | SNAPPY | GZIP |
| Metadatos nuevos | 4 archivos (21.6 KB) | 8 archivos (29.1 KB) |
| Tabla en disco | 220.1 → 290.7 MB | 186.1 → 240.9 MB |
| Filas del mes / otros meses | Iguales / sin cambios | Iguales / sin cambios |

Después de actualizar, los dos formatos tienen el mismo contenido en febrero: mismas filas, mismas sumas de `total_amount`, `congestion_surcharge` y `Airport_fee`, y cero nulos.

## Cómo se comportó cada formato

- **Delta escribió 4 archivos**: el Parquet original tenía 4 row groups y Spark los leyó en 4 tareas, cada una escribió su archivo (3 de ~21 MB y uno de 8 MB). No hubo shuffle. Un `OPTIMIZE` los volvería a juntar.
- **Iceberg escribió 1 archivo**: para tablas particionadas Spark usa la distribución `hash` por defecto, que agrupa cada partición en una tarea antes de escribir. Ese shuffle y la compresión GZIP son la explicación más probable de que tardara 2.6 s más (no se midieron por separado).
- **Versionado**: Delta agrega un commit (`00000000000000000001.json`) con el predicado y métricas (`numRemovedFiles=1`, `numOutputRows=3577542`, `numDeletedRows=3577542`). Iceberg crea un `metadata.json`, una manifest list y dos manifiestos (uno con el archivo agregado y otro con los retirados).
- **Espacio**: ninguno de los dos borra los archivos viejos. Delta crece 70.6 MB y Iceberg 54.8 MB hasta un `VACUUM` o hasta expirar el snapshot anterior. Por eso la versión 0 de Delta y el snapshot anterior de Iceberg siguen disponibles para time travel o rollback.
- **Otros meses**: enero y marzo no cambiaron en ningún formato; el script lo comprueba con las filas por mes antes y después.

## Problemas encontrados

| Problema | Causa | Qué hacer |
|---|---|---|
| Los archivos nuevos de Iceberg quedaron en GZIP, aunque la tabla tenía ZSTD. | La tabla se creó con PyIceberg, que usa ZSTD pero no guarda `write.parquet.compression-codec`. Sin esa propiedad, Iceberg en Java usa GZIP. | Fijar la propiedad en la tabla: `ALTER TABLE lakehouse.lakehouse.yellow_tripdata SET TBLPROPERTIES ('write.parquet.compression-codec'='zstd')`. |
| Delta pasó de 1 a 4 archivos en el mes. | Spark escribe un archivo por tarea, y lee un archivo por row group. | Correr `OPTIMIZE` después, o reparticionar el DataFrame antes de escribir. |
| `deltalake` 1.x devuelve `get_add_actions()` como tabla de arro3, no de pyarrow. | Cambio de la librería. | Convertir con `pa.table(...)`. |
| Los tamaños en disco incluyen los `.crc` que escribe Spark. | El sistema de archivos local de Hadoop guarda un checksum por archivo. | Los archivos activos se miden con el log de Delta y los manifiestos de Iceberg, no con el disco. |

## Cómo repetirlo

```bash
poetry run lh-spark-update-month --mes 2025-02    # informe .md y .json en docs/spark/
```

La actualización es idempotente: una segunda ejecución no encuentra nulos, pero igual reescribe el mes y crea otra versión y otro snapshot.
