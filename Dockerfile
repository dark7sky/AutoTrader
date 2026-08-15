FROM python:3.11-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app
COPY pyproject.toml README.md ./
COPY kis_ai_scalper ./kis_ai_scalper
COPY schemas ./schemas
COPY config ./config
COPY docs ./docs

FROM base AS test
COPY tests ./tests
RUN pip install --no-cache-dir -e ".[dev]"
CMD ["python", "-m", "pytest"]

FROM base AS runtime
RUN pip install --no-cache-dir . \
    && useradd --create-home --uid 10001 --shell /usr/sbin/nologin appuser \
    && mkdir -p /app/data \
    && chown -R appuser:appuser /app

USER appuser
# Production containers run only the explicitly configured service command.
CMD ["python", "-m", "kis_ai_scalper.cli", "service-loop"]
