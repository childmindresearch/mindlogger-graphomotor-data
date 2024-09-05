
ARG DEBIAN_BASE_VERSION=bookworm
ARG PYTHON_VERSION=3.12
ARG POETRY_VERSION=1.8.3
# Python module to run in container.
ARG MAIN_MODULE=mindlogger_graphomotor.__main__:main

FROM python:${PYTHON_VERSION}-${DEBIAN_BASE_VERSION} AS builder
ARG POETRY_VERSION
RUN pip install poetry==${POETRY_VERSION}

ENV POETRY_NO_INTERACTION=1 \
    POETRY_VIRTUALENVS_IN_PROJECT=1 \
    POETRY_VIRTUALENVS_CREATE=1 \
    POETRY_CACHE_DIR=/tmp/poetry_cache

WORKDIR /opt/run

COPY pyproject.toml poetry.lock ./
RUN touch README.md

RUN --mount=type=cache,target=$POETRY_CACHE_DIR poetry install --without dev --no-root

FROM python:${PYTHON_VERSION}-slim-${DEBIAN_BASE_VERSION} AS runtime
ARG MAIN_MODULE
ENV MAIN_MODULE=${MAIN_MODULE:-mindlogger_graphomotor.__main__:main}

ENV VIRTUAL_ENV=/opt/run/.venv \
    PATH="/opt/run/.venv/bin:$PATH" \
    #POETRY_NO_INTERACTION=1 \
    #POETRY_VIRTUALENVS_IN_PROJECT=1 \
    #POETRY_VIRTUALENVS_CREATE=1 \
    #POETRY_CACHE_DIR=/tmp/poetry_cache

# Copy python application into container.
# COPY src/ /opt/run/src

# Copy virtual environment from builder.
COPY --from=builder ${VIRTUAL_ENV} ${VIRTUAL_ENV}

# Install application into virtual environment.
# RUN --mount=type=cache,target=$POETRY_CACHE_DIR poetry install --only-root --compile

ENTRYPOINT ["python", "-m", "mindlogger_graphomotor.__main__"]
