# Shared image for producer, consumer, and health-server.
# All three are the etl package with a different entrypoint module --
# select which one runs via `command` in the k8s Deployment, e.g.:
#   ["python", "-m", "etl.entrypoints.producer"]
#   ["python", "-m", "etl.entrypoints.consumer"]
#   ["python", "-m", "etl.entrypoints.health_server"]
#   ["python", "-m", "etl.entrypoints.schema_apply"]

FROM python:3.11-slim AS builder

WORKDIR /build

COPY pyproject.toml README.md ./
COPY etl/ etl/

RUN python -m venv /venv \
    && /venv/bin/pip install --no-cache-dir --upgrade pip \
    && /venv/bin/pip install --no-cache-dir .

FROM python:3.11-slim AS runtime

RUN groupadd --system app && useradd --system --gid app --no-create-home app

COPY --from=builder /venv /venv
COPY etl/ /app/etl/
COPY data/reference/ /app/data/reference/
# SchemaManager resolves migrations relative to etl/'s install location
# (Path(__file__).parents[3] / "clickhouse" / "migrations") -- needed by
# schema_apply.py, unused by producer/consumer/health_server.
COPY clickhouse/migrations/ /app/clickhouse/migrations/

ENV PATH="/venv/bin:${PATH}" \
    PYTHONUNBUFFERED=1

WORKDIR /app
USER app

STOPSIGNAL SIGTERM
