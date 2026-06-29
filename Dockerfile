# syntax=docker/dockerfile:1

FROM python:3.12-slim

# Don't write .pyc files; flush stdout/stderr immediately so `docker logs`
# (and `docker compose logs -f`) shows output in real time.
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Install dependencies first for better layer caching.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Application code.
COPY main.py .
COPY src/ ./src/

# Persisted state (Steam session cookies) lives here — mount a volume on it
# so logins survive container restarts.
RUN mkdir -p /data && useradd --create-home --uid 1000 app && chown -R app /app /data
USER app

# Config comes from environment variables (see .env.example); cookies persist
# in the mounted /data volume so the bot avoids a full re-login each restart.
CMD ["python", "main.py", "--cookies", "/data/cookies.json"]
