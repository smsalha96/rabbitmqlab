# ── Build stage ──────────────────────────────────────────────────────────────
# Use a specific patch version for reproducible builds.
FROM python:3.10-slim

# Set working directory
WORKDIR /app

# Install dependencies first (cached layer — only rebuilds when requirements change)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application source
COPY producer.py .
COPY consumer.py .

# Default entrypoint is overridden per-deployment in incident-apps.yaml:
#   producer → uvicorn producer:app --host 0.0.0.0 --port 80
#   consumer → python consumer.py
