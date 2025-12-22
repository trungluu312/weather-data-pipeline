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

# Copy installed python dependencies from builder
COPY --from=builder /install /usr/local

# Copy application code
COPY common ./common
COPY ingestion ./ingestion
COPY orchestration ./orchestration
COPY transform ./transform

# Create data/logs directories
RUN mkdir -p /app/data /app/logs

# Install dbt dependencies (needs to run in the final image as it writes to ./transform/dbt_packages)
RUN python -m dbt.cli.main deps --profiles-dir transform --project-dir transform

CMD ["python", "-m", "orchestration.deploy"]
