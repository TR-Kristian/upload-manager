"""
Direct Docling API client.

Calls docling-serve ``POST /v1/convert/file`` with per-format optimised
parameters (from docling_profiles) and returns the extracted Markdown text.

Supports:
  * Synchronous single-file conversion
  * Health-check endpoint
  * Configurable timeouts & retries (uses the shared HTTP session factory)
"""

from __future__ import annotations

import base64
import json
import logging
import os
import time
from pathlib import Path
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from docling_profiles import get_profile, is_plaintext, needs_docling

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
DOCLING_BASE_URL: str = os.getenv("DOCLING_BASE_URL", "http://127.0.0.1:5001").rstrip("/")
DOCLING_API_KEY: str = os.getenv("DOCLING_API_KEY", "")
DOCLING_CONVERT_PATH: str = os.getenv("DOCLING_CONVERT_PATH", "/v1/convert/file")
DOCLING_CONVERT_SOURCE_PATH: str = os.getenv("DOCLING_CONVERT_SOURCE_PATH", "/v1/convert/source")
DOCLING_ASYNC_CONVERT_PATH: str = os.getenv("DOCLING_ASYNC_CONVERT_PATH", "/v1/convert/file/async")
DOCLING_STATUS_PATH: str = os.getenv("DOCLING_STATUS_PATH", "/v1/status/poll/{task_id}")
DOCLING_RESULT_PATH: str = os.getenv("DOCLING_RESULT_PATH", "/v1/result/{task_id}")
DOCLING_HEALTH_PATH: str = os.getenv("DOCLING_HEALTH_PATH", "/health")

DOCLING_CONNECT_TIMEOUT: float = float(os.getenv("DOCLING_CONNECT_TIMEOUT", "10"))
DOCLING_READ_TIMEOUT: float = float(os.getenv("DOCLING_READ_TIMEOUT", "600"))
DOCLING_ASYNC_POLL_INTERVAL: float = float(os.getenv("DOCLING_ASYNC_POLL_INTERVAL", "3"))
DOCLING_ASYNC_MAX_WAIT: float = float(os.getenv("DOCLING_ASYNC_MAX_WAIT", "900"))

DOCLING_RETRY_TOTAL: int = int(os.getenv("DOCLING_RETRY_TOTAL", "2"))
DOCLING_RETRY_BACKOFF: float = float(os.getenv("DOCLING_RETRY_BACKOFF", "1.0"))
DOCLING_POOL_CONNECTIONS: int = int(os.getenv("DOCLING_POOL_CONNECTIONS", "10"))
DOCLING_POOL_MAXSIZE: int = int(os.getenv("DOCLING_POOL_MAXSIZE", "20"))

# Whether to prefer async conversion (better for very large files).
DOCLING_PREFER_ASYNC: bool = os.getenv("DOCLING_PREFER_ASYNC", "false").lower() in ("1", "true", "yes")


def _create_docling_session() -> requests.Session:
    retry = Retry(
        total=DOCLING_RETRY_TOTAL,
        connect=DOCLING_RETRY_TOTAL,
        read=0,  # don't auto-retry reads – conversion can be slow
        backoff_factor=DOCLING_RETRY_BACKOFF,
        status_forcelist=(429, 502, 503, 504),
        allowed_methods=frozenset({"GET", "POST"}),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(
        max_retries=retry,
        pool_connections=DOCLING_POOL_CONNECTIONS,
        pool_maxsize=DOCLING_POOL_MAXSIZE,
    )
    session = requests.Session()
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    if DOCLING_API_KEY:
        session.headers["X-Api-Key"] = DOCLING_API_KEY
    return session


# Module-level session (created lazily).
_session: requests.Session | None = None


def _get_session() -> requests.Session:
    global _session
    if _session is None:
        _session = _create_docling_session()
    return _session


# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------

def check_health() -> dict:
    """Return Docling health status.  Keys: ok (bool), detail (str)."""
    try:
        resp = _get_session().get(
            f"{DOCLING_BASE_URL}{DOCLING_HEALTH_PATH}",
            timeout=(DOCLING_CONNECT_TIMEOUT, 10),
        )
        if resp.status_code < 400:
            return {"ok": True, "detail": f"Docling reachable ({resp.status_code})"}
        return {"ok": False, "detail": f"Docling unhealthy ({resp.status_code}): {resp.text[:200]}"}
    except Exception as exc:
        return {"ok": False, "detail": f"Docling unreachable: {exc}"}


# ---------------------------------------------------------------------------
# Synchronous conversion
# ---------------------------------------------------------------------------

def _extract_markdown(payload: dict) -> str:
    """Pull Markdown text out of a Docling response payload."""
    # Single-document response:
    doc = payload.get("document") or {}
    md = doc.get("md_content") or ""
    if md:
        return md.strip()

    # Fallback: text_content
    text = doc.get("text_content") or ""
    if text:
        return text.strip()

    # Some versions return top-level md_content
    md = payload.get("md_content") or ""
    if md:
        return md.strip()

    return ""


def _convert_sync(file_path: str, filename: str, force_ocr: bool = False) -> str:
    """Call ``POST /v1/convert/file`` synchronously."""
    url = f"{DOCLING_BASE_URL}{DOCLING_CONVERT_PATH}"
    profile_fields = get_profile(filename, force_ocr=force_ocr)

    with open(file_path, "rb") as fobj:
        files_part = [("files", (filename, fobj, "application/octet-stream"))]
        resp = _get_session().post(
            url,
            data=profile_fields,
            files=files_part,
            timeout=(DOCLING_CONNECT_TIMEOUT, DOCLING_READ_TIMEOUT),
        )

    if resp.status_code >= 400:
        body = resp.text[:500].replace("\n", " ")
        raise RuntimeError(
            f"Docling sync conversion failed ({resp.status_code}): {body}"
        )

    try:
        payload = resp.json()
    except Exception:
        # If the response is not JSON but 2xx, try to use the raw text.
        raw = resp.text.strip()
        if raw:
            return raw
        raise RuntimeError("Docling returned empty non-JSON response")

    md = _extract_markdown(payload)
    if not md:
        # Log the payload structure for debugging.
        logger.warning(
            "Docling returned 2xx but no markdown found. Keys: %s",
            list(payload.keys()) if isinstance(payload, dict) else type(payload),
        )
        # Last resort: json dump of document
        doc = payload.get("document")
        if isinstance(doc, dict) and doc.get("json_content"):
            return json.dumps(doc["json_content"], ensure_ascii=False)
        raise RuntimeError("Docling returned success but no extractable content")

    return md


# ---------------------------------------------------------------------------
# Async conversion (for very large files)
# ---------------------------------------------------------------------------

def _convert_async(file_path: str, filename: str, force_ocr: bool = False) -> str:
    """Submit an async job to Docling and poll until completion."""
    url = f"{DOCLING_BASE_URL}{DOCLING_ASYNC_CONVERT_PATH}"
    profile_fields = get_profile(filename, force_ocr=force_ocr)

    with open(file_path, "rb") as fobj:
        files_part = [("files", (filename, fobj, "application/octet-stream"))]
        resp = _get_session().post(
            url,
            data=profile_fields,
            files=files_part,
            timeout=(DOCLING_CONNECT_TIMEOUT, 60),
        )

    if resp.status_code >= 400:
        body = resp.text[:500].replace("\n", " ")
        raise RuntimeError(f"Docling async submit failed ({resp.status_code}): {body}")

    task_info = resp.json()
    task_id = task_info.get("task_id")
    if not task_id:
        raise RuntimeError(f"Docling async submit returned no task_id: {task_info}")

    # Poll for completion.
    deadline = time.time() + DOCLING_ASYNC_MAX_WAIT
    while time.time() < deadline:
        poll_url = f"{DOCLING_BASE_URL}{DOCLING_STATUS_PATH.format(task_id=task_id)}"
        poll_resp = _get_session().get(poll_url, timeout=(DOCLING_CONNECT_TIMEOUT, 30))
        if poll_resp.status_code >= 400:
            time.sleep(DOCLING_ASYNC_POLL_INTERVAL)
            continue

        poll_data = poll_resp.json()
        status = poll_data.get("task_status", "").lower()
        if status == "success":
            break
        if status == "failure":
            raise RuntimeError(f"Docling async conversion failed: {poll_data}")
        time.sleep(DOCLING_ASYNC_POLL_INTERVAL)
    else:
        raise RuntimeError(f"Docling async conversion timed out after {DOCLING_ASYNC_MAX_WAIT}s")

    # Fetch result.
    result_url = f"{DOCLING_BASE_URL}{DOCLING_RESULT_PATH.format(task_id=task_id)}"
    result_resp = _get_session().get(result_url, timeout=(DOCLING_CONNECT_TIMEOUT, DOCLING_READ_TIMEOUT))
    if result_resp.status_code >= 400:
        raise RuntimeError(
            f"Docling result fetch failed ({result_resp.status_code}): {result_resp.text[:300]}"
        )

    payload = result_resp.json()
    md = _extract_markdown(payload)
    if not md:
        raise RuntimeError("Docling async conversion returned no extractable content")
    return md


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def extract_with_docling(
    file_path: str,
    filename: str,
    *,
    force_ocr: bool = False,
) -> str:
    """Extract text from *file_path* using Docling with optimised settings.

    Returns Markdown text.  Raises ``RuntimeError`` on failure.
    """
    if DOCLING_PREFER_ASYNC:
        return _convert_async(file_path, filename, force_ocr=force_ocr)
    return _convert_sync(file_path, filename, force_ocr=force_ocr)


def read_plaintext(file_path: str) -> str:
    """Read a plain-text file with common encoding fallbacks."""
    path = Path(file_path)
    for encoding in ("utf-8", "utf-8-sig", "latin-1", "cp1250"):
        try:
            return path.read_text(encoding=encoding).strip()
        except (UnicodeDecodeError, UnicodeError):
            continue
    # Last resort: read as bytes and decode replacing errors
    return path.read_bytes().decode("utf-8", errors="replace").strip()


def extract_content(
    file_path: str,
    filename: str,
    *,
    force_ocr: bool = False,
) -> tuple[str, str]:
    """High-level extraction.

    Returns ``(content, engine)`` where *engine* is one of
    ``"docling"``, ``"plaintext"``.

    Raises ``RuntimeError`` when extraction fails.
    """
    if is_plaintext(filename):
        return read_plaintext(file_path), "plaintext"

    if needs_docling(filename):
        md = extract_with_docling(file_path, filename, force_ocr=force_ocr)
        return md, "docling"

    # Unknown extension – try plain-text read as last resort.
    try:
        return read_plaintext(file_path), "plaintext-fallback"
    except Exception:
        raise RuntimeError(
            f"Unsupported file type '{Path(filename).suffix}' and plain-text read failed"
        )
