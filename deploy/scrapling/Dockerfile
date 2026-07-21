FROM python:3.12-slim-bookworm@sha256:d50fb7611f86d04a3b0471b46d7557818d88983fc3136726336b2a4c657aa30b

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PLAYWRIGHT_BROWSERS_PATH=/ms-playwright \
    HOME=/tmp \
    XDG_CACHE_HOME=/tmp/.cache

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
    && python -m playwright install --with-deps chromium \
    && useradd --create-home --home-dir /home/scrapling --shell /usr/sbin/nologin scrapling \
    && mkdir -p /data/profile \
    && chown -R scrapling:scrapling /data /ms-playwright /home/scrapling

COPY --chown=scrapling:scrapling app.py .

USER scrapling
EXPOSE 8080

HEALTHCHECK --interval=10s --timeout=3s --retries=6 CMD ["python", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8080/healthz', timeout=2).read()"]
CMD ["python", "/app/app.py"]
