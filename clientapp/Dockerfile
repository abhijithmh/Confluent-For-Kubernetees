# ==============================================================================
# Production Dockerfile for Generic Kafka Consumer on Kubernetes
# ==============================================================================
FROM python:3.12-slim AS base

# Prevent Python from writing .pyc files and enable unbuffered logging
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Create non-root system user and group
RUN groupadd --gid 10001 appgroup && \
    useradd --uid 10001 --gid 10001 --shell /bin/false --no-create-home appuser

WORKDIR /app

# Install dependencies first for efficient layer caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code and package files
COPY pyproject.toml README.md ./
COPY src/ ./src/

# Install the package
RUN pip install --no-cache-dir .

# Switch to non-root user
USER 10001:10001

# Expose health probe ports (8080: consumer, 8081: producer)
EXPOSE 8080 8081

# Default command (can be overridden with 'kafka-producer' in Kubernetes pod spec)
CMD ["kafka-consumer"]
