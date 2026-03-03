# ============================================================
# FitTrack Pro ETL Pipeline — Multi-stage Dockerfile
# ============================================================
# Stage 1: Install dependencies in a builder image
# Stage 2: Copy only what's needed into a slim runtime image
#
# LEARN: Multi-stage builds keep your final image small. The builder
# stage may pull in compilers and dev headers, but the runtime stage
# only copies the installed packages — typically 3-5x smaller.
# ============================================================

# ----------------------------------------------------------
# Stage 1: Builder
# ----------------------------------------------------------
FROM python:3.12-slim AS builder

WORKDIR /app

COPY pyproject.toml ./

RUN pip install --no-cache-dir --prefix=/install ".[dashboard]"

# ----------------------------------------------------------
# Stage 2: Runtime
# ----------------------------------------------------------
FROM python:3.12-slim AS runtime

WORKDIR /app

# Copy installed packages from builder
COPY --from=builder /install /usr/local

# Copy project source and config
COPY src/ ./src/
COPY config/ ./config/
COPY run_demo.py ./
COPY pyproject.toml ./
COPY .streamlit/ ./.streamlit/
COPY CLAUDE.md ./

# Copy sample data for demo
COPY data/sample/ ./data/sample/

# Create runtime directories
RUN mkdir -p data/bronze data/silver data/gold data/incoming/processed \
             data/incoming/errors logs reports

# Default: run the pipeline demo
ENTRYPOINT ["python", "run_demo.py"]
