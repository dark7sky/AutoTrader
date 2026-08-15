FROM python:3.11-slim

WORKDIR /app
COPY pyproject.toml README.md ./
COPY kis_ai_scalper ./kis_ai_scalper
COPY schemas ./schemas
COPY tests ./tests
COPY config ./config
COPY docs ./docs

RUN pip install --no-cache-dir -e .[dev]

# Phase 0 is intentionally test-only. No trading daemon is started.
CMD ["python", "-m", "pytest"]
