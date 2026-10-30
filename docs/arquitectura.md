# Arquitectura

Recorrido de los datos, desde la descarga de NYC TLC hasta el dashboard y el notebook. Cada caja de proceso es un comando `lh-*` de `pyproject.toml`; `scripts/pipeline.sh` los ejecuta en este orden.

```mermaid
flowchart LR
    tlc[("NYC TLC<br/>CloudFront")]

    subgraph ingesta["1 · Ingesta (PyArrow)"]
        landing["data/landing/<br/>Parquet por mes + zonas CSV"]
        raw["data/raw/<br/>year=.../month=..."]
        interim["data/interim/<br/>yellow_tripdata.parquet"]
    end

    subgraph tablas["2 · Formatos de tabla"]
        delta["Delta<br/>data/delta/yellow_tripdata_pyarrow<br/>data/delta_demo/"]
        iceberg["Iceberg<br/>data/iceberg/warehouse/"]
        catalogo[("Catálogo SQLite<br/>data/iceberg/catalog.db")]
    end

    subgraph motores["3 · Procesamiento"]
        spark["Spark 4.1<br/>delta-spark + iceberg-runtime<br/>lh-spark-*"]
        duckdb["DuckDB 2.0<br/>lh-bench-formats"]
    end

    subgraph salidas["4 · Resultados"]
        resultados[("data/benchmarks/<br/>resultados.duckdb")]
        informes["docs/<br/>informes .md, .csv, .png, .json"]
        dashboard["Dashboard Streamlit<br/>:8501"]
        notebook["notebooks/<br/>comparacion_formatos.ipynb"]
    end

    tlc -- "curl" --> landing
    landing -- "lh-partition-date" --> raw
    raw -- "lh-merge" --> interim
    interim -- "lh-delta-* (deltalake)" --> delta
    interim -- "lh-iceberg-* (PyIceberg)" --> iceberg
    iceberg <--> catalogo

    delta <-->|"lectura y update de un mes"| spark
    iceberg <-->|"lectura y update de un mes"| spark
    catalogo -. "mismo catálogo" .-> spark

    interim -- "read_parquet" --> duckdb
    delta -- "delta_scan" --> duckdb
    iceberg -- "iceberg_scan(metadata.json)" --> duckdb

    duckdb --> resultados
    duckdb --> informes
    spark --> informes

    resultados --> dashboard
    delta -- "versiones" --> dashboard
    iceberg -- "snapshots" --> dashboard
    resultados --> notebook
    informes --> notebook
```

## Piezas

| Capa | Qué hace | Código |
|---|---|---|
| Ingesta | Descarga los tres meses de 2025, los particiona por fecha de recogida y los une en un Parquet consolidado, que es la línea base sin formato de tabla. | `scripts/pipeline.sh`, `src/lakehouse/ingest/` |
| Delta | Escribe la tabla con `deltalake` (sin Spark) y practica MERGE, *time travel*, evolución de esquema, OPTIMIZE y VACUUM sobre una copia (`delta_demo`). | `src/lakehouse/delta/` |
| Iceberg | Crea el catálogo SQLite, la tabla con *hidden partitioning*, un snapshot por mes, evolución de partición y rollback. | `src/lakehouse/iceberg/` |
| Spark | Lee y escribe las tres tablas, y reescribe febrero en Delta e Iceberg. Usa el mismo catálogo que PyIceberg. | `src/lakehouse/spark/` |
| Benchmark | Las mismas seis consultas con DuckDB sobre los tres formatos; guarda informes en `docs/benchmarks/` y cada ejecución en la base de resultados. | `src/lakehouse/benchmarks/` |
| Dashboard | Tres pestañas: benchmarks, snapshots y *time travel*, y guía de formatos. | `src/lakehouse/dashboard/` |
| Notebook | Comparación final con las mediciones guardadas y una medición en vivo. | `notebooks/comparacion_formatos.ipynb` |

## En Docker

`make demo` construye la imagen (Python 3.14, Poetry, Java 21) y ejecuta todo dentro de contenedores:

```mermaid
flowchart LR
    imagen["Imagen lakehouse-lab<br/>Python 3.14 · Poetry · Java 21"]
    pipeline["servicio lakehouse<br/>scripts/pipeline.sh"]
    tests["servicio tests<br/>pytest"]
    dash["servicio dashboard<br/>Streamlit :8501"]

    vdata[("volumen<br/>lakehouse-data")]
    vivy[("volumen<br/>ivy-cache<br/>jars de Maven")]
    docs["./docs del host"]

    imagen --> pipeline & tests & dash
    pipeline --> vdata
    pipeline --> docs
    pipeline --- vivy
    tests --- vivy
    vdata --> dash
```

- **Los datos viven en un volumen**, no en el `data/` del host: el catálogo de Iceberg guarda rutas absolutas, que dentro del contenedor empiezan por `/app/data`.
- **Los informes sí se escriben en el `docs/` del repositorio**, para versionarlos.
- **Los jars de Spark** (Delta, Iceberg, SQLite JDBC) se descargan de Maven la primera vez y quedan en `ivy-cache`.
