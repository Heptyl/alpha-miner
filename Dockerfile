FROM python:3.12-slim

ENV PYTHONUTF8=1 \
    PYTHONIOENCODING=utf-8 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    UV_LINK_MODE=copy

RUN pip install --no-cache-dir uv

WORKDIR /app
COPY pyproject.toml uv.lock README.md ./
COPY src ./src
COPY cli ./cli
RUN uv sync --frozen

WORKDIR /workspace
ENTRYPOINT ["/app/.venv/bin/python"]
CMD ["-m", "cli", "--help"]
