# syntax=docker/dockerfile:1
# phlo-observer — multi-stage, non-root, no build toolchain in the final image.

FROM python:3.12-slim AS build

ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy
COPY --from=ghcr.io/astral-sh/uv:0.12.1 /uv /uvx /usr/local/bin/

WORKDIR /workspace
COPY pyproject.toml uv.lock ./
COPY packages/observe-core ./packages/observe-core
COPY packages/phlo-observe-sdk ./packages/phlo-observe-sdk
COPY services/phlo-observer ./services/phlo-observer
RUN uv sync --all-packages --frozen --no-dev --no-editable

FROM python:3.12-slim AS runtime

ARG VCS_REF=unknown
ARG VERSION=dev

LABEL org.opencontainers.image.title="phlo-observer" \
      org.opencontainers.image.description="Phlo observability ingestion and query service" \
      org.opencontainers.image.source="https://github.com/phlohouse/phlo-observe" \
      org.opencontainers.image.revision="${VCS_REF:-unknown}" \
      org.opencontainers.image.version="${VERSION:-dev}"

RUN groupadd --system --gid 1000 observer && \
    useradd --system --uid 1000 --gid observer --no-create-home observer && \
    mkdir -p /var/lib/phlo-observe && \
    chown observer:observer /var/lib/phlo-observe

WORKDIR /app
COPY --from=build --chown=observer:observer /workspace/.venv /app/.venv
COPY --from=build --chown=observer:observer /workspace/services/phlo-observer/migrations /app/migrations

ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PHLO_OBSERVER_HOST=0.0.0.0 \
    PHLO_OBSERVER_PORT=8080 \
    PHLO_OBSERVER_MIGRATIONS_DIR=/app/migrations \
    OBSERVE_SPOOL_DIR=/var/lib/phlo-observe/spool

USER observer
EXPOSE 8080

HEALTHCHECK --interval=10s --timeout=5s --retries=6 --start-period=10s \
  CMD ["python", "-c", "import urllib.request,sys;sys.exit(0 if urllib.request.urlopen('http://localhost:8080/health/live',timeout=4).status==200 else 1)"]

# Graceful shutdown: uvicorn handles SIGTERM; retention task stops via lifespan.
ENTRYPOINT ["python", "-m", "phlo_observer.cli"]
CMD ["serve"]
