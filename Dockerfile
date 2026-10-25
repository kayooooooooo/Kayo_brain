FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# v78: ffmpeg for voice notes (edge-tts MP3 -> Telegram OGG/Opus)
RUN apt-get update && apt-get install -y --no-install-recommends ffmpeg

WORKDIR /app

# Install dependencies first (cached layer)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy app code + state file
COPY kayo_brain.py .
COPY ciel_banner.jpg .
COPY kayo_state.json .

# Ciel starts her own Flask health server on $PORT (Render injects this)
CMD ["python", "kayo_brain.py"]
