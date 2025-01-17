FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY deploy/docker/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY src/ src/
COPY configs/ configs/

# Set environment variables
ENV PYTHONPATH=/app
ENV CONFIG_PATH=/app/configs

# Create non-root user
RUN useradd -m appuser && chown -R appuser /app
USER appuser

# Command to run the application
ENTRYPOINT ["python", "-m", "src.main"]
