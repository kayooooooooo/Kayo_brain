FROM python:3.11-slim

# Prevent Python from writing .pyc files and buffering stdout/stderr
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Install dependencies first (cached layer)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy app code + state file
COPY kayo_brain.py .
COPY kayo_state.json .

# Kayo starts its own Flask health server on $PORT (Render injects this)
CMD ["python", "kayo_brain.py"]
