FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
	PYTHONUNBUFFERED=1 \
	APP_HOST=0.0.0.0 \
	APP_PORT=8088 \
	DATA_DIR=/app/data

WORKDIR /app

RUN apt-get update \
	&& apt-get install -y --no-install-recommends curl \
	&& rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY app.py /app/app.py
COPY templates /app/templates
COPY static /app/static

RUN useradd --create-home --uid 10001 appuser \
	&& mkdir -p /app/data/uploads \
	&& chown -R appuser:appuser /app

USER appuser

EXPOSE 8088

HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
  CMD curl -fsS http://127.0.0.1:8088/healthz || exit 1

CMD ["python", "app.py"]
