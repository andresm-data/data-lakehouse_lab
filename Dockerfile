# Imagen del proyecto: Python, Poetry y Java 21 para Spark.
# La misma versión de Python con la que se generó poetry.lock.
FROM python:3.14-slim-trixie

ARG POETRY_VERSION=2.4.1
ARG UID=1000
ARG GID=1000

# Java 21 para Spark (ver docs/spark/versiones.md); curl para descargar los datos de NYC TLC.
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-21-jre-headless curl make \
    && rm -rf /var/lib/apt/lists/* \
    # Enlace estable al JDK, sin depender de la arquitectura (amd64, arm64...).
    && ln -s "$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")" /usr/lib/jvm/java-21

ENV JAVA_HOME=/usr/lib/jvm/java-21 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    POETRY_HOME=/opt/poetry \
    POETRY_VIRTUALENVS_IN_PROJECT=true \
    POETRY_NO_INTERACTION=1 \
    POETRY_CACHE_DIR=/tmp/poetry-cache

# Poetry en su propio entorno, separado del entorno del proyecto.
RUN python -m venv "$POETRY_HOME" \
    && "$POETRY_HOME/bin/pip" install "poetry==$POETRY_VERSION" \
    && ln -s "$POETRY_HOME/bin/poetry" /usr/local/bin/poetry

# Usuario sin privilegios con el mismo UID/GID del host, para que los archivos
# escritos en los volúmenes (data/, docs/) no queden como root.
RUN groupadd --gid "$GID" lakehouse \
    && useradd --uid "$UID" --gid "$GID" --create-home lakehouse

WORKDIR /app
RUN chown lakehouse:lakehouse /app
USER lakehouse

# Primero solo las dependencias: esta capa se reutiliza mientras no cambie el lock.
COPY --chown=lakehouse:lakehouse pyproject.toml poetry.lock README.md ./
RUN poetry install --no-root && rm -rf "$POETRY_CACHE_DIR"

# Después el código, para instalar el paquete y sus comandos lh-*.
COPY --chown=lakehouse:lakehouse src ./src
COPY --chown=lakehouse:lakehouse tests ./tests
COPY --chown=lakehouse:lakehouse Makefile ./
RUN poetry install && rm -rf "$POETRY_CACHE_DIR"

ENV PATH="/app/.venv/bin:$PATH"

CMD ["bash"]
