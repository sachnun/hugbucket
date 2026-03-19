FROM python:3.14-slim AS base

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

# Install dependencies first (cached layer)
COPY pyproject.toml uv.lock ./
RUN uv sync --no-dev --no-install-project

# Copy source and install project
COPY . .
RUN uv sync --no-dev

EXPOSE 9000
EXPOSE 2121
EXPOSE 30000-30099

ENTRYPOINT ["uv", "run", "hugbucket"]
