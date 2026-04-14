# Open WebUI – Smart Proxy Upload Gateway

A browser-based upload gateway that extracts document content **locally** via
[Docling](https://github.com/DS4SD/docling) with optimised per-format settings
(Hungarian OCR, accurate table detection) before pushing it to Open WebUI.

## How It Works

```
 Browser                Upload Gateway              Docling            Open WebUI
 ──────                 ──────────────              ───────            ──────────
 1. Upload files ──────▶ Save to temp dir
                         2. POST /v1/convert/file ──▶ Extract content
                                                      (OCR, tables, …)
                         ◀── Markdown content ───────
                         3. POST /api/v1/files/?process=false ──────────▶ Store raw file
                         ◀── file_id ──────────────────────────────────
                         4. POST /api/v1/files/{id}/data/content/update ▶ Replace content
                         5. POST /api/v1/knowledge/{kb}/file/add ───────▶ Add to KB & embed
```

### Smart Proxy Pipeline (per file)

| Step | Action | Target |
|------|--------|--------|
| 1 | **Extract** content via Docling (or plain-text read) | `docling:5001` |
| 2 | **Upload** raw file to Open WebUI with `process=false` | `openwebui:3000` |
| 3 | **Push** extracted Markdown content back to the file record | `openwebui:3000` |
| 4 | **Add** file to knowledge base (triggers embedding) | `openwebui:3000` |

If Docling is unreachable or extraction fails, the gateway automatically falls
back to **legacy mode**: upload with `process=true` so Open WebUI uses its own
content pipeline.

## Features

- **Per-format Docling profiles** – PDF (OCR + dlparse_v2), DOCX, XLSX, PPTX, HTML
- **Force OCR toggle** – for scanned PDFs / image-based documents
- **Hungarian + English OCR** via EasyOCR
- **Accurate table detection** for PDF / DOCX / PPTX; fast mode for XLSX
- **Automatic legacy fallback** when Docling is down
- **Plain-text fast path** – `.txt`, `.md`, `.csv` are read directly (no Docling)
- **Job queue** with SQLite persistence (survives restarts)
- **Parallel workers** (configurable, default 3)
- **Retry with exponential back-off**
- **HungaroControl-branded UI** with live Docling health indicator

## Quick Start (Docker Compose)

```bash
docker compose up -d --build
```

Open: **http://127.0.0.1:8088**

Before starting, provide a valid Open WebUI API token in your shell or `.env`:

```bash
export OPENWEBUI_API_KEY="<your-openwebui-api-token>"
```

## Docker Compose Service

```yaml
services:
  upload-gateway:
    build:
      context: .
      dockerfile: Dockerfile
    container_name: upload-gateway
    network_mode: host
    restart: always
    environment:
      - OPENWEBUI_BASE_URL=http://127.0.0.1:3000
      - OPENWEBUI_API_KEY=${OPENWEBUI_API_KEY}
      - DOCLING_BASE_URL=http://127.0.0.1:5001
      - DOCLING_OCR_LANGS=hu,en
      - WORKER_COUNT=3
    volumes:
      - upload_gateway_data:/app/data

volumes:
  upload_gateway_data:
```

## Environment Variables

### Application

| Variable | Default | Description |
|----------|---------|-------------|
| `APP_HOST` | `0.0.0.0` | Bind address |
| `APP_PORT` | `8088` | Bind port |
| `DATA_DIR` | `/app/data` | Persistent data directory (SQLite DB + uploads) |

### Open WebUI

| Variable | Default | Description |
|----------|---------|-------------|
| `OPENWEBUI_BASE_URL` | `http://127.0.0.1:3000` | Open WebUI base URL |
| `OPENWEBUI_API_KEY` | *(empty)* | API key for authentication |
| `OPENWEBUI_API_KEY_HEADER` | `Authorization` | Header name |
| `OPENWEBUI_API_KEY_PREFIX` | `Bearer` | Header value prefix |
| `OPENWEBUI_KB_LIST_PATHS` | `/api/v1/knowledge/,...` | Comma-separated KB listing endpoints |
| `OPENWEBUI_FILE_UPLOAD_PATHS` | `/api/v1/files/,...` | File upload endpoints |
| `OPENWEBUI_KB_ADD_FILE_PATH_TEMPLATES` | `/api/v1/knowledge/{kb_id}/file/add,...` | KB add-file endpoints |
| `OPENWEBUI_CONTENT_UPDATE_PATHS` | `/api/v1/files/{file_id}/data/content/update` | Content update endpoints |
| `OPENWEBUI_UPLOAD_PATH_TEMPLATE` | `/api/v1/knowledge/{kb_id}/file` | Legacy upload endpoint |
| `OPENWEBUI_UPLOAD_CANDIDATES` | `POST\|/api/v1/knowledge/{kb_id}/file,...` | Legacy upload candidates |

### Docling (Smart Proxy)

| Variable | Default | Description |
|----------|---------|-------------|
| `DOCLING_BASE_URL` | `http://127.0.0.1:5001` | Docling-serve base URL |
| `DOCLING_API_KEY` | *(empty)* | Docling API key (if secured) |
| `DOCLING_OCR_LANGS` | `hu,en` | OCR languages (comma-separated) |
| `DOCLING_OCR_ENGINE` | `easyocr` | OCR engine (`easyocr`, `tesseract_cli`, etc.) |
| `DOCLING_PDF_BACKEND` | `dlparse_v2` | PDF parsing backend |
| `DOCLING_TABLE_MODE_PDF` | `accurate` | Table detection mode for PDF |
| `DOCLING_TABLE_MODE_DOCX` | `accurate` | Table detection mode for DOCX |
| `DOCLING_TABLE_MODE_XLSX` | `fast` | Table detection mode for XLSX |
| `DOCLING_TABLE_MODE_PPTX` | `accurate` | Table detection mode for PPTX |
| `DOCLING_IMAGE_EXPORT` | `placeholder` | Image export mode |
| `DOCLING_DOCUMENT_TIMEOUT` | `600` | Per-document timeout (seconds) |
| `DOCLING_READ_TIMEOUT` | `600` | HTTP read timeout for sync calls |
| `DOCLING_ASYNC_MAX_WAIT` | `900` | Max wait for async conversions |
| `DOCLING_PREFER_ASYNC` | `false` | Use async Docling API |

### Workers & Retry

| Variable | Default | Description |
|----------|---------|-------------|
| `WORKER_COUNT` | `3` | Number of parallel upload workers |
| `MAX_ATTEMPTS` | `4` | Max retry attempts per file |
| `BASE_RETRY_SECONDS` | `2` | Initial backoff delay |
| `MAX_RETRY_SECONDS` | `60` | Max backoff delay |
| `POLL_IDLE_SECONDS` | `1` | Idle polling interval |
| `UPLOAD_TIMEOUT_SECONDS` | `300` | HTTP timeout for Open WebUI uploads |

### Limits

| Variable | Default | Description |
|----------|---------|-------------|
| `ALLOWED_EXTENSIONS` | `.pdf,.doc,.docx,.txt,.md,.csv,.xlsx,.ppt,.pptx,.html,.htm` | Accepted file types |
| `MAX_UPLOAD_BYTES` | `209715200` (200 MB) | Max upload size |

## Project Structure

```
upload-manager/
├── app.py                 # Flask app – routes, job queue, worker pipeline
├── docling_client.py      # Direct Docling API client (sync/async)
├── docling_profiles.py    # Per-format conversion parameter profiles
├── templates/
│   └── index.html         # HungaroControl-branded SPA
├── static/
│   ├── HungaroControl_logos_noslogan.png
│   └── HungaroControl_symbol.png
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
└── README.md
```

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/` | Web UI |
| `GET` | `/healthz` | Liveness probe |
| `GET` | `/api/health/docling` | Docling health check |
| `GET` | `/api/knowledge-bases` | List knowledge bases |
| `GET` | `/api/jobs` | List all jobs (grouped by status) |
| `POST` | `/api/jobs` | Queue new upload jobs |
| `POST` | `/api/jobs/<id>/retry` | Retry a failed job |
| `POST` | `/api/jobs/waiting/clear` | Clear waiting jobs |
| `POST` | `/api/jobs/failed/clear` | Clear failed jobs |
| `POST` | `/api/jobs/clear` | Clear all jobs |

## Troubleshooting

- **Docling shows "offline"** – Check that `docling-serve` is running on the
  configured `DOCLING_BASE_URL`. The gateway will still work via legacy mode.
- **OCR quality is poor** – Try enabling Force OCR for the upload, or switch
  `DOCLING_OCR_ENGINE` to `tesseract_cli`.
- **Large files time out** – Increase `DOCLING_READ_TIMEOUT` / `DOCLING_ASYNC_MAX_WAIT`
  and `UPLOAD_TIMEOUT_SECONDS`.
- **KB not appearing** – Verify your `OPENWEBUI_API_KEY` has access to the KB.
  Try adding more path variants to `OPENWEBUI_KB_LIST_PATHS`.
