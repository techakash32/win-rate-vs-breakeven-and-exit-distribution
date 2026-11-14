FROM python:3.13-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PORT=8000

WORKDIR /app

# Install dependencies first so this layer is cached between code changes
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# App code (see .dockerignore)
COPY . .
RUN chmod +x start.sh

# Run as non-root; logs/ holds paper-trading state + the dashboard's data
RUN useradd --create-home appuser \
    && mkdir -p /app/logs \
    && chown -R appuser:appuser /app
USER appuser

# Persist paper-trading state across restarts by mounting a volume here
VOLUME ["/app/logs"]

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD python -c "import os,urllib.request; urllib.request.urlopen('http://127.0.0.1:%s/api/dashboard' % os.environ.get('PORT','8000'), timeout=4)" || exit 1

# Starts the paper-live engine in the background + the dashboard in the foreground
CMD ["./start.sh"]