# ── EchoStream Cloud Run Job ─────────────────────────────────────────────────
#
# Multi-stage build: keeps the final image small (~150MB vs ~800MB)
#   Stage 1 (builder): installs uv + dependencies in a virtual environment
#   Stage 2 (runtime): copies only the venv + source code — no build tools
#
# Why Python 3.11-slim?
#   - "slim" = Debian without dev packages (gcc, headers) → smaller image
#   - 3.11 matches our pyproject.toml requires-python = ">=3.11"
#
# How to build and test locally:
#   docker build -t echostream .
#   docker run --env-file .env echostream

# ── Stage 1: Builder ────────────────────────────────────────────────────────
FROM python:3.11-slim AS builder

# Install uv — our package manager (much faster than pip)
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

# Copy dependency files first (Docker layer caching: dependencies change
# less often than source code, so this layer gets cached across builds)
COPY pyproject.toml uv.lock ./

# Install production dependencies only (no dev extras like pytest)
# --no-dev skips [project.optional-dependencies].dev
RUN uv sync --frozen --no-dev --no-editable

# Copy source code (after deps so code changes don't bust the dep cache)
COPY src/ src/

# ── Stage 2: Runtime ────────────────────────────────────────────────────────
FROM python:3.11-slim AS runtime

WORKDIR /app

# Copy the virtual environment with all installed packages
COPY --from=builder /app/.venv /app/.venv

# Copy our source code
COPY --from=builder /app/src /app/src

# Add the venv's bin to PATH so `python` resolves to the venv's Python
ENV PATH="/app/.venv/bin:$PATH"

# Cloud Run sets these, but we provide sensible defaults for local testing
ENV SPOTIFY_OPEN_BROWSER="false"
ENV SPOTIFY_CACHE_PATH="/tmp/.spotify_cache"

# The entrypoint: Cloud Run Job executes this command
CMD ["python", "-m", "echostream.main"]
