FROM python:3.11-slim

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

# Install dependencies first (cached layer unless pyproject.toml changes)
COPY pyproject.toml .
RUN uv sync --no-dev --no-install-project

# Copy source
COPY src/ src/

# Install the project itself
RUN uv sync --no-dev

CMD ["uv", "run", "python", "-m", "src.main"]
