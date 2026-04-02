# Open WebUI Upload Gateway

This service provides a browser-based upload point for coworkers.

Features:
- Upload documents to Open WebUI knowledge bases from a GUI.
- Knowledge base list fetched live from Open WebUI.
- Jobs queued and processed file-by-file with parallel workers.
- Automatic retries with backoff for transient failures.
- Buckets in UI: Waiting, Completed, Failed.
- SQLite persistence so queue/history survive restarts.

## Quick Start (Docker)

Build:

```bash
docker build -t openwebui-upload-gateway .
```

Run (host network, local-only):

```bash
docker run --rm \
  --name openwebui-upload-gateway \
  --network host \
  -e OPENWEBUI_BASE_URL=http://127.0.0.1:3000 \
  -e OPENWEBUI_API_KEY=YOUR_API_KEY \
  -e OPENWEBUI_API_KEY_HEADER=Authorization \
  -e OPENWEBUI_API_KEY_PREFIX=Bearer \
  -e WORKER_COUNT=3 \
  -e MAX_ATTEMPTS=4 \
  -e OPENWEBUI_KB_LIST_PATHS=/api/v1/knowledge,/api/knowledge \
  -e OPENWEBUI_UPLOAD_PATH_TEMPLATE=/api/v1/knowledge/{kb_id}/file \
  -v upload-gateway-data:/app/data \
  openwebui-upload-gateway
```

Open:

- http://127.0.0.1:8088

## Docker Compose Service Snippet

Add this service to your existing compose file:

```yaml
  upload-gateway:
    build: ./upload-manager
    container_name: upload-gateway
    network_mode: host
    restart: always
    environment:
      - APP_PORT=8088
      - OPENWEBUI_BASE_URL=http://127.0.0.1:3000
      - OPENWEBUI_API_KEY=${OPENWEBUI_API_KEY}
      - OPENWEBUI_API_KEY_HEADER=Authorization
      - OPENWEBUI_API_KEY_PREFIX=Bearer
      - WORKER_COUNT=3
      - MAX_ATTEMPTS=4
      - BASE_RETRY_SECONDS=2
      - MAX_RETRY_SECONDS=60
      - OPENWEBUI_KB_LIST_PATHS=/api/v1/knowledge/,/api/v1/knowledge,/api/knowledge/,/api/knowledge
      - OPENWEBUI_UPLOAD_PATH_TEMPLATE=/api/v1/knowledge/{kb_id}/file
    volumes:
      - upload_gateway_data:/app/data
```

And add volume:

```yaml
volumes:
  upload_gateway_data:
```

## Environment Variables

- `APP_HOST` (default: `0.0.0.0`)
- `APP_PORT` (default: `8088`)
- `DATA_DIR` (default: `/app/data`)
- `OPENWEBUI_BASE_URL` (default: `http://127.0.0.1:3000`)
- `OPENWEBUI_API_KEY` (required in secure setups)
- `OPENWEBUI_API_KEY_HEADER` (default: `Authorization`)
- `OPENWEBUI_API_KEY_PREFIX` (default: `Bearer`)
- `OPENWEBUI_KB_LIST_PATHS` (comma-separated list of candidate paths)
- `OPENWEBUI_UPLOAD_PATH_TEMPLATE` (supports `{kb_id}` placeholder)
- `WORKER_COUNT` (default: `3`)
- `MAX_ATTEMPTS` (default: `4`)
- `BASE_RETRY_SECONDS` (default: `2`)
- `MAX_RETRY_SECONDS` (default: `60`)
- `UPLOAD_TIMEOUT_SECONDS` (default: `300`)
- `POLL_IDLE_SECONDS` (default: `1`)
- `ALLOWED_EXTENSIONS` (comma-separated)
- `MAX_UPLOAD_BYTES` (default: `209715200`)

## Notes on Open WebUI Endpoint Versions

Open WebUI API paths can differ by version. If KB loading or uploads fail:
- Check your Open WebUI API docs or browser network calls.
- Update:
  - `OPENWEBUI_KB_LIST_PATHS`
  - `OPENWEBUI_UPLOAD_PATH_TEMPLATE`

The app is designed so you can tune these without code changes.
