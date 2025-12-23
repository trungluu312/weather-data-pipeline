# Stage 1: Builder
FROM python:3.11-slim as builder

WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    libgdal-dev \
    git \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies to a temporary location
COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

# Stage 2: Runtime
FROM python:3.11-slim

WORKDIR /app

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy installed python dependencies from builder
COPY --from=builder /install /usr/local

# Copy application code
COPY common ./common
COPY ingestion ./ingestion
COPY orchestration ./orchestration
COPY transform ./transform
COPY scripts ./scripts

# Create data/logs directories
RUN mkdir -p /app/data /app/logs
RUN chmod +x /app/scripts/run_serve.sh

# Install dbt dependencies
RUN python -m dbt.cli.main deps --profiles-dir transform --project-dir transform

# Set the default command to start both the server and the worker
EXPOSE 4200
CMD ["/app/scripts/run_serve.sh"]
