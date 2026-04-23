"""
HungaroControl – Knowledge Upload Gateway  (Smart Proxy Edition)

Pipeline:
  1. User uploads files via the web UI.
  2. Workers pick jobs from the SQLite queue (max WORKER_COUNT in parallel).
  3. For each file:
     a) Extract content ourselves:
        - Plain text → read directly
        - PDF / DOCX / XLSX / PPTX → call Docling directly with optimised
          per-format parameters (bypassing Open WebUI's extraction).
     b) Upload the raw file to Open WebUI with ``process=false``.
     c) Push the extracted content via
        ``POST /api/v1/files/{id}/data/content/update``.
     d) Attach the file to the target knowledge base via
        ``POST /api/v1/knowledge/{kb_id}/file/add``.
"""

import hashlib
import json
import os
import random
import sqlite3
import threading
import time
import uuid
import logging
from datetime import datetime, timezone
from pathlib import Path

import requests
from flask import Flask, jsonify, render_template, request
from werkzeug.utils import secure_filename
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from docling_client import check_health as docling_check_health, extract_content
from docling_profiles import needs_docling, is_plaintext
from enrichment import enrich_content
from qdrant_sparse import (
	wait_and_ensure_sparse,
	inject_sparse_vectors,
	force_init_collection,
	list_collections,
	collection_info,
)

QDRANT_URL = os.getenv("QDRANT_URL", "http://127.0.0.1:6333")
QDRANT_COLLECTION_PREFIX = os.getenv("QDRANT_COLLECTION_PREFIX", "")

# ═══════════════════════════════════════════════════════════════════════════
# Configuration
# ═══════════════════════════════════════════════════════════════════════════

APP_PORT = int(os.getenv("APP_PORT", "8088"))
APP_HOST = os.getenv("APP_HOST", "0.0.0.0")


def env_value_or_file(name: str, default: str = "") -> str:
	"""Read a config value from NAME or NAME_FILE (Docker secret style)."""
	value = os.getenv(name)
	if value:
		return value

	file_path = os.getenv(f"{name}_FILE", "").strip()
	if file_path:
		try:
			return Path(file_path).read_text(encoding="utf-8").strip()
		except Exception as exc:
			logging.getLogger(__name__).warning("Unable to read %s from %s: %s", name, file_path, exc)

	return default

OPENWEBUI_BASE_URL = os.getenv("OPENWEBUI_BASE_URL", "http://127.0.0.1:3000").rstrip("/")
OPENWEBUI_API_KEY = env_value_or_file("OPENWEBUI_API_KEY", "")
OPENWEBUI_API_KEY_HEADER = os.getenv("OPENWEBUI_API_KEY_HEADER", "Authorization")
OPENWEBUI_API_KEY_PREFIX = os.getenv("OPENWEBUI_API_KEY_PREFIX", "Bearer")
OPENWEBUI_TRY_NO_AUTH = os.getenv("OPENWEBUI_TRY_NO_AUTH", "false").lower() in ("1", "true", "yes", "on")

# Open WebUI endpoint paths (configurable for version differences)
OPENWEBUI_KB_LIST_PATHS = [
	p.strip()
	for p in os.getenv(
		"OPENWEBUI_KB_LIST_PATHS",
		"/api/v1/knowledge/,/api/v1/knowledge,/api/knowledge/,/api/knowledge,/api/v1/knowledge/search?page=1",
	).split(",")
	if p.strip()
]

OPENWEBUI_FILE_UPLOAD_PATHS = [
	p.strip()
	for p in os.getenv("OPENWEBUI_FILE_UPLOAD_PATHS", "/api/v1/files/,/api/v1/files").split(",")
	if p.strip()
]

OPENWEBUI_CONTENT_UPDATE_PATHS = [
	p.strip()
	for p in os.getenv(
		"OPENWEBUI_CONTENT_UPDATE_PATHS",
		"/api/v1/files/{file_id}/data/content/update,/api/files/{file_id}/data/content/update",
	).split(",")
	if p.strip()
]

OPENWEBUI_KB_ADD_FILE_PATH_TEMPLATES = [
	p.strip()
	for p in os.getenv(
		"OPENWEBUI_KB_ADD_FILE_PATH_TEMPLATES",
		"/api/v1/knowledge/{kb_id}/file/add,/api/knowledge/{kb_id}/file/add",
	).split(",")
	if p.strip()
]

# Fallback: legacy direct-upload endpoints (used when smart proxy fails)
OPENWEBUI_UPLOAD_CANDIDATES = [
	p.strip()
	for p in os.getenv(
		"OPENWEBUI_UPLOAD_CANDIDATES",
		"POST|/api/v1/knowledge/{kb_id}/file,POST|/api/v1/knowledge/{kb_id}/files,POST|/api/knowledge/{kb_id}/file,POST|/api/knowledge/{kb_id}/files,PUT|/api/v1/knowledge/{kb_id}/file",
	).split(",")
	if p.strip()
]

OPENWEBUI_FILE_STATUS_PATH_TEMPLATES = [
	p.strip()
	for p in os.getenv(
		"OPENWEBUI_FILE_STATUS_PATH_TEMPLATES",
		"/api/v1/files/{file_id}/process/status,/api/files/{file_id}/process/status",
	).split(",")
	if p.strip()
]

# HTTP tuning
HTTP_CONNECT_TIMEOUT_SECONDS = float(os.getenv("HTTP_CONNECT_TIMEOUT_SECONDS", "5"))
HTTP_READ_TIMEOUT_SECONDS = float(os.getenv("HTTP_READ_TIMEOUT_SECONDS", "30"))
HTTP_RETRY_TOTAL = int(os.getenv("HTTP_RETRY_TOTAL", "3"))
HTTP_RETRY_BACKOFF = float(os.getenv("HTTP_RETRY_BACKOFF", "0.5"))
HTTP_POOL_CONNECTIONS = int(os.getenv("HTTP_POOL_CONNECTIONS", "20"))
HTTP_POOL_MAXSIZE = int(os.getenv("HTTP_POOL_MAXSIZE", "50"))

# Worker tuning
WORKER_COUNT = int(os.getenv("WORKER_COUNT", "3"))
MAX_ATTEMPTS = int(os.getenv("MAX_ATTEMPTS", "4"))
BASE_RETRY_SECONDS = float(os.getenv("BASE_RETRY_SECONDS", "2"))
MAX_RETRY_SECONDS = float(os.getenv("MAX_RETRY_SECONDS", "60"))
POLL_IDLE_SECONDS = float(os.getenv("POLL_IDLE_SECONDS", "1"))
UPLOAD_TIMEOUT_SECONDS = int(os.getenv("UPLOAD_TIMEOUT_SECONDS", "300"))
FILE_PROCESS_WAIT_SECONDS = int(os.getenv("FILE_PROCESS_WAIT_SECONDS", "600"))
FILE_PROCESS_POLL_SECONDS = float(os.getenv("FILE_PROCESS_POLL_SECONDS", "2"))

# File constraints
ALLOWED_EXTENSIONS = {
	ext.strip().lower()
	for ext in os.getenv(
		"ALLOWED_EXTENSIONS",
		".pdf,.doc,.docx,.txt,.md,.csv,.xlsx,.ppt,.pptx,.html,.htm",
	).split(",")
	if ext.strip()
}
MAX_UPLOAD_BYTES = int(os.getenv("MAX_UPLOAD_BYTES", str(200 * 1024 * 1024)))

# Paths
DATA_DIR = Path(os.getenv("DATA_DIR", "/app/data"))
UPLOAD_DIR = DATA_DIR / "uploads"
DB_PATH = DATA_DIR / "uploader.db"

# ═══════════════════════════════════════════════════════════════════════════
# Flask app
# ═══════════════════════════════════════════════════════════════════════════

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = MAX_UPLOAD_BYTES
logger = logging.getLogger(__name__)
logging.basicConfig(
	level=logging.INFO,
	format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

# ═══════════════════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════════════════

def utc_now_iso() -> str:
	return datetime.now(timezone.utc).isoformat()


def epoch_seconds() -> float:
	return time.time()


def file_hash(path: str) -> str:
	"""SHA-256 of a file for deduplication logging."""
	h = hashlib.sha256()
	with open(path, "rb") as f:
		for chunk in iter(lambda: f.read(65536), b""):
			h.update(chunk)
	return h.hexdigest()[:16]


def ensure_dirs() -> None:
	DATA_DIR.mkdir(parents=True, exist_ok=True)
	UPLOAD_DIR.mkdir(parents=True, exist_ok=True)


def get_conn() -> sqlite3.Connection:
	conn = sqlite3.connect(DB_PATH, timeout=30, check_same_thread=False)
	conn.execute("PRAGMA journal_mode=WAL")
	conn.execute("PRAGMA synchronous=NORMAL")
	conn.execute("PRAGMA busy_timeout=5000")
	conn.row_factory = sqlite3.Row
	return conn


def create_http_session() -> requests.Session:
	retry = Retry(
		total=HTTP_RETRY_TOTAL,
		connect=HTTP_RETRY_TOTAL,
		read=HTTP_RETRY_TOTAL,
		status=HTTP_RETRY_TOTAL,
		backoff_factor=HTTP_RETRY_BACKOFF,
		status_forcelist=(429, 500, 502, 503, 504),
		allowed_methods=frozenset({"GET", "POST", "PUT", "DELETE"}),
		raise_on_status=False,
	)
	adapter = HTTPAdapter(
		max_retries=retry,
		pool_connections=HTTP_POOL_CONNECTIONS,
		pool_maxsize=HTTP_POOL_MAXSIZE,
	)
	session = requests.Session()
	session.mount("http://", adapter)
	session.mount("https://", adapter)
	return session


# ═══════════════════════════════════════════════════════════════════════════
# Database
# ═══════════════════════════════════════════════════════════════════════════

def init_db() -> None:
	with get_conn() as conn:
		conn.execute(
			"""
			CREATE TABLE IF NOT EXISTS jobs (
				id TEXT PRIMARY KEY,
				filename TEXT NOT NULL,
				stored_path TEXT NOT NULL,
				kb_id TEXT NOT NULL,
				kb_name TEXT,
				status TEXT NOT NULL,
				attempt_count INTEGER NOT NULL DEFAULT 0,
				max_attempts INTEGER NOT NULL,
				next_attempt_at REAL NOT NULL DEFAULT 0,
				last_error TEXT,
				engine TEXT,
				force_ocr INTEGER NOT NULL DEFAULT 0,
				created_at TEXT NOT NULL,
				updated_at TEXT NOT NULL,
				completed_at TEXT
			)
			"""
		)
		conn.execute(
			"CREATE INDEX IF NOT EXISTS idx_jobs_status_next ON jobs(status, next_attempt_at)"
		)

		# Migrate: add columns if missing (safe for existing DBs).
		existing_cols = {
			row[1] for row in conn.execute("PRAGMA table_info(jobs)").fetchall()
		}
		if "engine" not in existing_cols:
			conn.execute("ALTER TABLE jobs ADD COLUMN engine TEXT")
		if "force_ocr" not in existing_cols:
			conn.execute("ALTER TABLE jobs ADD COLUMN force_ocr INTEGER NOT NULL DEFAULT 0")


def recover_interrupted_jobs() -> None:
	now = utc_now_iso()
	with get_conn() as conn:
		conn.execute(
			"UPDATE jobs SET status='waiting', updated_at=? WHERE status='processing'",
			(now,),
		)


# ═══════════════════════════════════════════════════════════════════════════
# Open WebUI auth
# ═══════════════════════════════════════════════════════════════════════════

_BASE_HEADERS = {"Accept": "application/json"}
_openwebui_headers_cache: dict | None = None
_openwebui_headers_lock = threading.Lock()


def build_auth_headers() -> dict:
	"""Return headers including the API key (and Accept: application/json)."""
	headers = dict(_BASE_HEADERS)
	if OPENWEBUI_API_KEY:
		if OPENWEBUI_API_KEY_PREFIX:
			headers[OPENWEBUI_API_KEY_HEADER] = f"{OPENWEBUI_API_KEY_PREFIX} {OPENWEBUI_API_KEY}"
		else:
			headers[OPENWEBUI_API_KEY_HEADER] = OPENWEBUI_API_KEY
	return headers


def _headers_signature(headers: dict) -> tuple:
	"""Stable signature for header de-duplication."""
	return tuple(sorted((str(k).lower(), str(v)) for k, v in headers.items()))


def _is_auth_mode(headers: dict) -> bool:
	"""Whether these headers include an auth credential beyond base headers."""
	for key in headers.keys():
		if key.lower() not in {"accept", "content-type"}:
			return True
	return False


def build_header_options() -> list[dict]:
	"""Return a deduplicated list of Open WebUI auth header variants.

	Open WebUI deployments vary by version and proxy setup.  This helper tries
	compatible header styles in a deterministic order and optionally includes
	a no-auth fallback for unsecured local deployments.
	"""
	options: list[dict] = []
	seen: set[tuple] = set()

	def add(headers: dict) -> None:
		normalized = {k: v for k, v in headers.items() if v is not None and str(v) != ""}
		sig = _headers_signature(normalized)
		if sig not in seen:
			seen.add(sig)
			options.append(normalized)

	if OPENWEBUI_API_KEY:
		# User-configured header/prefix always first.
		add(build_auth_headers())

		# Common Open WebUI/API gateway variants.
		add({**_BASE_HEADERS, "Authorization": f"Bearer {OPENWEBUI_API_KEY}"})
		add({**_BASE_HEADERS, "Authorization": OPENWEBUI_API_KEY})
		add({**_BASE_HEADERS, "X-Api-Key": OPENWEBUI_API_KEY})
		add({**_BASE_HEADERS, "X-API-Key": OPENWEBUI_API_KEY})

	if not OPENWEBUI_API_KEY or OPENWEBUI_TRY_NO_AUTH:
		add(dict(_BASE_HEADERS))

	return options


def resolve_openwebui_headers(session: requests.Session) -> dict:
	"""Probe Open WebUI and cache a working auth header option."""
	global _openwebui_headers_cache
	if _openwebui_headers_cache is not None:
		return dict(_openwebui_headers_cache)

	probe_paths = [
		"/api/v1/knowledge/",
		"/api/v1/auths/",
	]

	with _openwebui_headers_lock:
		if _openwebui_headers_cache is not None:
			return dict(_openwebui_headers_cache)

		errors = []
		for headers in build_header_options():
			mode = "auth" if _is_auth_mode(headers) else "no-auth"
			for path in probe_paths:
				url = f"{OPENWEBUI_BASE_URL}{path}"
				try:
					resp = session.get(
						url,
						headers=headers,
						timeout=(HTTP_CONNECT_TIMEOUT_SECONDS, HTTP_READ_TIMEOUT_SECONDS),
					)
					if resp.status_code in (401, 403):
						errors.append(f"GET {path} [{mode}] -> {resp.status_code}")
						continue
					if resp.status_code < 500:
						_openwebui_headers_cache = dict(headers)
						return dict(headers)
				except Exception as exc:
					errors.append(f"GET {path} [{mode}] -> {exc}")

		if errors:
			raise RuntimeError(
				"Unable to authenticate to Open WebUI while probing headers. Tried: "
				+ "; ".join(errors)
			)
		raise RuntimeError("Unable to authenticate to Open WebUI: no header options available")


# ═══════════════════════════════════════════════════════════════════════════
# Knowledge base list
# ═══════════════════════════════════════════════════════════════════════════

def normalize_kb_items(payload) -> list:
	if isinstance(payload, list):
		items = payload
	elif isinstance(payload, dict):
		for key in ("data", "items", "knowledge", "result"):
			if isinstance(payload.get(key), list):
				items = payload[key]
				break
		else:
			items = []
	else:
		items = []

	normalized = []
	for item in items:
		if not isinstance(item, dict):
			continue
		kb_id = item.get("id") or item.get("knowledge_id") or item.get("uuid")
		name = item.get("name") or item.get("title") or str(kb_id)
		if kb_id:
			normalized.append({"id": str(kb_id), "name": str(name)})
	return normalized


def fetch_knowledge_bases() -> list:
	session = create_http_session()
	preferred = resolve_openwebui_headers(session)
	header_options = [preferred]
	for candidate in build_header_options():
		if _headers_signature(candidate) != _headers_signature(preferred):
			header_options.append(candidate)
	errors = []
	for path in OPENWEBUI_KB_LIST_PATHS:
		url = f"{OPENWEBUI_BASE_URL}{path}"
		for headers in header_options:
			auth_mode = "auth" if _is_auth_mode(headers) else "no-auth"
			try:
				response = session.get(
					url,
					headers=headers,
					timeout=(HTTP_CONNECT_TIMEOUT_SECONDS, HTTP_READ_TIMEOUT_SECONDS),
				)
				if response.status_code >= 400:
					errors.append(f"GET {path} [{auth_mode}] -> {response.status_code}")
					continue
				try:
					return normalize_kb_items(response.json())
				except Exception as exc:
					ct = response.headers.get("content-type", "?")
					body = response.text[:120].replace("\n", " ")
					errors.append(f"GET {path} [{auth_mode}] -> bad JSON ({exc}); ct={ct}; body={body}")
			except Exception as exc:
				errors.append(f"GET {path} [{auth_mode}] -> {exc}")

	if errors:
		raise RuntimeError(f"Unable to fetch knowledge bases. Tried: {'; '.join(errors)}")
	raise RuntimeError("Unable to fetch knowledge bases: no valid endpoint configured")


# ═══════════════════════════════════════════════════════════════════════════
# Job queue
# ═══════════════════════════════════════════════════════════════════════════

def allowed_file(filename: str) -> bool:
	return Path(filename).suffix.lower() in ALLOWED_EXTENSIONS


def enqueue_job(
	filename: str,
	stored_path: str,
	kb_id: str,
	kb_name: str | None,
	force_ocr: bool = False,
) -> str:
	job_id = str(uuid.uuid4())
	now = utc_now_iso()
	with get_conn() as conn:
		conn.execute(
			"""
			INSERT INTO jobs (
				id, filename, stored_path, kb_id, kb_name,
				status, attempt_count, max_attempts, next_attempt_at,
				last_error, engine, force_ocr,
				created_at, updated_at, completed_at
			) VALUES (?, ?, ?, ?, ?, 'waiting', 0, ?, 0, NULL, NULL, ?,
					  ?, ?, NULL)
			""",
			(job_id, filename, stored_path, kb_id, kb_name, MAX_ATTEMPTS,
			 1 if force_ocr else 0, now, now),
		)
	return job_id


def get_job(job_id: str):
	with get_conn() as conn:
		row = conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
		return dict(row) if row else None


def list_jobs() -> list:
	with get_conn() as conn:
		rows = conn.execute("SELECT * FROM jobs ORDER BY created_at DESC").fetchall()
	return [dict(r) for r in rows]


def update_job_status(
	job_id: str,
	status: str,
	error: str | None = None,
	engine: str | None = None,
) -> None:
	now = utc_now_iso()
	completed_at = now if status == "completed" else None
	with get_conn() as conn:
		conn.execute(
			"""
			UPDATE jobs
			SET status=?, last_error=?, engine=COALESCE(?, engine),
				updated_at=?, completed_at=?
			WHERE id=?
			""",
			(status, error, engine, now, completed_at, job_id),
		)


def schedule_retry(job_id: str, attempt_count: int, message: str) -> None:
	delay = min(MAX_RETRY_SECONDS, BASE_RETRY_SECONDS * (2 ** max(0, attempt_count - 1)))
	jitter = random.uniform(0, 0.5)
	next_at = epoch_seconds() + delay + jitter
	now = utc_now_iso()
	with get_conn() as conn:
		conn.execute(
			"UPDATE jobs SET status='waiting', next_attempt_at=?, last_error=?, updated_at=? WHERE id=?",
			(next_at, message, now, job_id),
		)


def mark_failed(job_id: str, message: str) -> None:
	update_job_status(job_id, "failed", message)


def claim_next_job():
	now_epoch = epoch_seconds()
	now_iso = utc_now_iso()
	try:
		with get_conn() as conn:
			conn.execute("BEGIN IMMEDIATE")
			row = conn.execute(
				"""
				SELECT * FROM jobs
				WHERE status='waiting' AND next_attempt_at <= ?
				ORDER BY created_at ASC LIMIT 1
				""",
				(now_epoch,),
			).fetchone()
			if not row:
				conn.commit()
				return None
			conn.execute(
				"UPDATE jobs SET status='processing', attempt_count=attempt_count+1, updated_at=? WHERE id=?",
				(now_iso, row["id"]),
			)
			conn.commit()
	except sqlite3.OperationalError as exc:
		if "locked" in str(exc).lower():
			return None
		raise
	return get_job(row["id"])


# ═══════════════════════════════════════════════════════════════════════════
# Open WebUI helpers
# ═══════════════════════════════════════════════════════════════════════════

def _extract_file_id(payload) -> str | None:
	if isinstance(payload, dict):
		for key in ("id", "file_id"):
			v = payload.get(key)
			if v:
				return str(v)
		for ck in ("data", "file", "item", "result"):
			c = payload.get(ck)
			if isinstance(c, dict):
				for key in ("id", "file_id"):
					v = c.get(key)
					if v:
						return str(v)
	return None


def _upload_file_no_process(job: dict, headers: dict, session: requests.Session) -> str:
	"""Upload file to Open WebUI with process=false.  Returns file_id."""
	for upload_path in OPENWEBUI_FILE_UPLOAD_PATHS:
		url = f"{OPENWEBUI_BASE_URL}{upload_path}"
		with open(job["stored_path"], "rb") as fobj:
			resp = session.post(
				url,
				headers=headers,
				params={"process": "false"},
				files={"file": (job["filename"], fobj)},
				timeout=(HTTP_CONNECT_TIMEOUT_SECONDS, UPLOAD_TIMEOUT_SECONDS),
			)

		if resp.status_code in (404, 405):
			continue
		if resp.status_code >= 400:
			body = resp.text[:350].replace("\n", " ")
			raise RuntimeError(f"File upload (no-process) failed ({resp.status_code}) on {upload_path}: {body}")

		try:
			payload = resp.json()
		except Exception:
			payload = {}

		file_id = _extract_file_id(payload)
		if file_id:
			return file_id
		raise RuntimeError(f"File upload succeeded on {upload_path} but no file_id in response")

	raise RuntimeError("All Open WebUI file upload paths returned 404/405")


def _push_content(file_id: str, content: str, headers: dict, session: requests.Session) -> None:
	"""Push extracted content back to Open WebUI via content update endpoint."""
	errors = []
	for template in OPENWEBUI_CONTENT_UPDATE_PATHS:
		path = template.format(file_id=file_id)
		url = f"{OPENWEBUI_BASE_URL}{path}"
		resp = session.post(
			url,
			headers={**headers, "Content-Type": "application/json"},
			json={"content": content},
			timeout=(HTTP_CONNECT_TIMEOUT_SECONDS, UPLOAD_TIMEOUT_SECONDS),
		)
		if resp.status_code < 400:
			logger.info("Content pushed via %s for file %s (%d chars)", path, file_id, len(content))
			return
		if resp.status_code in (404, 405):
			errors.append(f"POST {path} -> {resp.status_code}")
			continue
		body = resp.text[:350].replace("\n", " ")
		raise RuntimeError(f"Content update failed ({resp.status_code}) on {path}: {body}")

	raise RuntimeError(f"All content update paths failed: {'; '.join(errors)}")


def _add_file_to_kb(file_id: str, kb_id: str, headers: dict, session: requests.Session) -> None:
	"""Attach file to knowledge base."""
	errors = []
	for template in OPENWEBUI_KB_ADD_FILE_PATH_TEMPLATES:
		path = template.format(kb_id=kb_id)
		url = f"{OPENWEBUI_BASE_URL}{path}"
		resp = session.post(
			url,
			headers={**headers, "Content-Type": "application/json"},
			json={"file_id": file_id},
			timeout=(HTTP_CONNECT_TIMEOUT_SECONDS, UPLOAD_TIMEOUT_SECONDS),
		)
		if resp.status_code < 400:
			logger.info("File %s added to KB %s via %s", file_id, kb_id, path)
			return
		if resp.status_code in (404, 405):
			errors.append(f"POST {path} -> {resp.status_code}")
			continue
		body = resp.text[:350].replace("\n", " ")
		raise RuntimeError(f"KB add-file failed ({resp.status_code}) on {path}: {body}")

	raise RuntimeError(f"All KB add-file paths failed: {'; '.join(errors)}")


# ═══════════════════════════════════════════════════════════════════════════
# Legacy fallback (upload via Open WebUI processing)
# ═══════════════════════════════════════════════════════════════════════════

def wait_for_openwebui_file_processing(file_id: str, headers: dict) -> tuple[bool, str]:
	deadline = time.time() + FILE_PROCESS_WAIT_SECONDS
	summaries = []
	session = create_http_session()

	while time.time() < deadline:
		for tmpl in OPENWEBUI_FILE_STATUS_PATH_TEMPLATES:
			path = tmpl.format(file_id=file_id)
			url = f"{OPENWEBUI_BASE_URL}{path}"
			try:
				resp = session.get(url, headers=headers, timeout=(HTTP_CONNECT_TIMEOUT_SECONDS, HTTP_READ_TIMEOUT_SECONDS))
				if resp.status_code in (404, 405):
					summaries.append(f"GET {path} -> {resp.status_code}")
					continue
				if resp.status_code >= 400:
					body = resp.text[:200].replace("\n", " ")
					return False, f"Status check failed ({resp.status_code}) on {path}: {body}"
				payload = resp.json()
				st = str(payload.get("status", "")).lower()
				if st == "completed":
					return True, f"GET {path} -> completed"
				if st == "failed":
					err = payload.get("error") or payload.get("detail") or "processing failed"
					return False, f"Processing failed on {path}: {err}"
				summaries.append(f"GET {path} -> {st or 'pending'}")
			except Exception as exc:
				summaries.append(f"GET {path} -> {exc}")
		time.sleep(FILE_PROCESS_POLL_SECONDS)

	return False, f"Timed out. Tried: {', '.join(summaries[-10:])}"


def _legacy_upload(job: dict, headers: dict, session: requests.Session) -> None:
	"""Fallback: upload with process=true and let Open WebUI call Docling."""
	file_id = None
	for upload_path in OPENWEBUI_FILE_UPLOAD_PATHS:
		url = f"{OPENWEBUI_BASE_URL}{upload_path}"
		with open(job["stored_path"], "rb") as fobj:
			resp = session.post(
				url,
				headers=headers,
				params={"process": "true", "process_in_background": "false"},
				files={"file": (job["filename"], fobj)},
				timeout=(HTTP_CONNECT_TIMEOUT_SECONDS, UPLOAD_TIMEOUT_SECONDS),
			)
		if resp.status_code in (404, 405):
			continue
		if resp.status_code >= 400:
			body = resp.text[:350].replace("\n", " ")
			raise RuntimeError(f"Legacy upload failed ({resp.status_code}) on {upload_path}: {body}")
		try:
			file_id = _extract_file_id(resp.json())
		except Exception:
			pass
		break

	if not file_id:
		# Try direct KB upload candidates
		for candidate in OPENWEBUI_UPLOAD_CANDIDATES:
			if "|" in candidate:
				method, tmpl = candidate.split("|", 1)
				method = method.strip().upper() or "POST"
			else:
				method, tmpl = "POST", candidate.strip()
			path = tmpl.strip().format(kb_id=job["kb_id"])
			url = f"{OPENWEBUI_BASE_URL}{path}"
			with open(job["stored_path"], "rb") as fobj:
				resp = session.request(
					method, url, headers=headers,
					files={"file": (job["filename"], fobj)},
					timeout=(HTTP_CONNECT_TIMEOUT_SECONDS, UPLOAD_TIMEOUT_SECONDS),
				)
			if resp.status_code < 400:
				return  # success via direct upload
			if resp.status_code not in (404, 405):
				body = resp.text[:350].replace("\n", " ")
				raise RuntimeError(f"Legacy direct upload failed ({resp.status_code}) on {method} {path}: {body}")
		raise RuntimeError("All legacy upload paths exhausted")

	# Wait for Open WebUI processing
	ok, detail = wait_for_openwebui_file_processing(file_id, headers)
	if not ok:
		raise RuntimeError(f"Legacy processing failed: {detail}")

	# Attach to KB
	_add_file_to_kb(file_id, job["kb_id"], headers, session)


# ═══════════════════════════════════════════════════════════════════════════
# Smart proxy upload (main path)
# ═══════════════════════════════════════════════════════════════════════════

def upload_to_openwebui(job: dict) -> str:
	"""Process and upload a single job.  Returns the engine used."""
	session = create_http_session()
	headers = resolve_openwebui_headers(session)
	force_ocr = bool(job.get("force_ocr", 0))
	filename = job["filename"]
	stored_path = job["stored_path"]

	fallback_reason: str | None = None

	# --- Step 1: Extract content locally ---
	try:
		content, engine = extract_content(stored_path, filename, force_ocr=force_ocr)
		logger.info(
			"Extracted %d chars from %s via %s (force_ocr=%s)",
			len(content), filename, engine, force_ocr,
		)
	except Exception as extract_err:
		fallback_reason = f"Extraction failed: {extract_err}"
		logger.warning(
			"Local extraction failed for %s: %s – falling back to legacy",
			filename, extract_err,
		)
		content, engine = "", ""

	if not content.strip():
		if not fallback_reason:
			fallback_reason = "Extraction returned empty content"
		logger.warning("Falling back to legacy for %s (%s)", filename, fallback_reason)
		try:
			_legacy_upload(job, headers, session)
		except Exception as legacy_err:
			raise RuntimeError(
				f"Smart proxy failed ({fallback_reason}), then legacy also failed: {legacy_err}"
			)
		# Record why we fell back so the user can see it
		update_job_status(job["id"], "processing", error=f"[fallback] {fallback_reason}", engine="legacy-openwebui")
		return "legacy-openwebui"

	# --- Step 1.5: Enrich content with LLM summary + keywords ---
	enriched_meta: dict = {}
	try:
		content, enriched_meta = enrich_content(content, filename)
		if enriched_meta.get("enriched"):
			logger.info(
				"Enriched %s: summary=%s keywords=%d",
				filename,
				enriched_meta.get("summary_ok"),
				enriched_meta.get("keyword_count", 0),
			)
	except Exception as enrich_err:
		logger.warning("Enrichment failed for %s (continuing without): %s", filename, enrich_err)

	# --- Step 2: Upload raw file to Open WebUI (process=false) ---
	file_id: str | None = None
	try:
		file_id = _upload_file_no_process(job, headers, session)
		logger.info("File %s stored in Open WebUI as %s", filename, file_id)
	except Exception as upload_err:
		raise RuntimeError(f"Step 2 – Open WebUI file upload failed: {upload_err}")

	# --- Step 3: Push extracted content ---
	try:
		_push_content(file_id, content, headers, session)
	except Exception as push_err:
		logger.warning(
			"Content push failed for %s (file_id=%s): %s – attempting legacy re-upload",
			filename, file_id, push_err,
		)
		# File exists in Open WebUI but has no content.  Try legacy as rescue.
		try:
			_legacy_upload(job, headers, session)
			update_job_status(job["id"], "processing", error=f"[fallback] Content push failed: {push_err}", engine="legacy-openwebui")
			return "legacy-openwebui"
		except Exception as legacy_err:
			raise RuntimeError(
				f"Step 3 – Content push failed ({push_err}), then legacy rescue also failed: {legacy_err}"
			)

	# --- Step 4: Attach to knowledge base ---
	try:
		_add_file_to_kb(file_id, job["kb_id"], headers, session)
	except Exception as kb_err:
		raise RuntimeError(
			f"Step 4 – File uploaded & content pushed (file_id={file_id}) but KB attach failed: {kb_err}"
		)

	# --- Step 5: Inject sparse vectors into Qdrant (background) ---
	kb_id = job["kb_id"]
	def _background_sparse():
		# Qdrant collection name == KB id in Open WebUI.
		# On a fresh DB the collection may not exist yet when this thread starts,
		# so we poll until Open WebUI creates it, then configure sparse vectors.
		collection_name = f"{QDRANT_COLLECTION_PREFIX}{kb_id}"
		wait_and_ensure_sparse(collection_name)
		inject_sparse_vectors(collection_name, file_id)

	threading.Thread(target=_background_sparse, daemon=True).start()

	return engine


# ═══════════════════════════════════════════════════════════════════════════
# Job cleanup
# ═══════════════════════════════════════════════════════════════════════════

def _delete_stored_files(rows) -> None:
	for row in rows:
		sp = row["stored_path"]
		if sp:
			try:
				p = Path(sp)
				if p.exists():
					p.unlink()
			except Exception:
				pass


def clear_failed_jobs() -> int:
	with get_conn() as conn:
		rows = conn.execute("SELECT id, stored_path FROM jobs WHERE status='failed'").fetchall()
		conn.execute("DELETE FROM jobs WHERE status='failed'")
	_delete_stored_files(rows)
	return len(rows)


def clear_jobs_by_status(statuses: tuple[str, ...]) -> int:
	ph = ",".join(["?" for _ in statuses])
	with get_conn() as conn:
		rows = conn.execute(f"SELECT id, stored_path FROM jobs WHERE status IN ({ph})", statuses).fetchall()
		conn.execute(f"DELETE FROM jobs WHERE status IN ({ph})", statuses)
	_delete_stored_files(rows)
	return len(rows)


def clear_all_jobs() -> int:
	with get_conn() as conn:
		rows = conn.execute("SELECT id, stored_path FROM jobs").fetchall()
		conn.execute("DELETE FROM jobs")
	_delete_stored_files(rows)
	return len(rows)


# ═══════════════════════════════════════════════════════════════════════════
# Workers
# ═══════════════════════════════════════════════════════════════════════════

def run_worker(stop_event: threading.Event) -> None:
	while not stop_event.is_set():
		job = claim_next_job()
		if not job:
			time.sleep(POLL_IDLE_SECONDS)
			continue
		if not Path(job["stored_path"]).exists():
			mark_failed(job["id"], f"Stored file not found: {job['stored_path']}")
			continue

		try:
			engine = upload_to_openwebui(job)
			update_job_status(job["id"], "completed", None, engine=engine)
		except Exception as exc:
			logger.exception("Worker error for job %s (%s)", job["id"], job["filename"])
			latest = get_job(job["id"])
			if not latest:
				continue
			error_msg = str(exc)
			if latest["attempt_count"] < latest["max_attempts"]:
				schedule_retry(latest["id"], latest["attempt_count"], error_msg)
			else:
				mark_failed(latest["id"], error_msg)


def start_workers() -> tuple[threading.Event, list[threading.Thread]]:
	stop_event = threading.Event()
	threads = []
	for idx in range(WORKER_COUNT):
		t = threading.Thread(
			target=run_worker, args=(stop_event,),
			name=f"upload-worker-{idx + 1}", daemon=True,
		)
		t.start()
		threads.append(t)
	return stop_event, threads


# ═══════════════════════════════════════════════════════════════════════════
# Serialisation
# ═══════════════════════════════════════════════════════════════════════════

def serialize_job(row: dict) -> dict:
	return {
		"id": row["id"],
		"filename": row["filename"],
		"kb_id": row["kb_id"],
		"kb_name": row.get("kb_name"),
		"status": row["status"],
		"attempt_count": row["attempt_count"],
		"max_attempts": row["max_attempts"],
		"last_error": row.get("last_error"),
		"engine": row.get("engine"),
		"force_ocr": bool(row.get("force_ocr", 0)),
		"created_at": row["created_at"],
		"updated_at": row["updated_at"],
		"completed_at": row.get("completed_at"),
	}


# ═══════════════════════════════════════════════════════════════════════════
# Routes
# ═══════════════════════════════════════════════════════════════════════════

@app.route("/api/debug/openwebui", methods=["GET"])
def api_debug_openwebui():
	"""Diagnostic: probe Open WebUI connectivity and auth.  Do not expose publicly."""
	session = create_http_session()
	results = []
	probe_paths = [
		"/api/v1/knowledge/",
		"/api/v1/auths/",      # reveals if the server is even Open WebUI
		"/health",             # Open WebUI liveness
	]
	for path in probe_paths:
		url = f"{OPENWEBUI_BASE_URL}{path}"
		for label, hdrs in [("auth", build_auth_headers()), ("no-auth", dict(_BASE_HEADERS))]:
			try:
				r = session.get(url, headers=hdrs, timeout=(5, 10))
				ct = r.headers.get("content-type", "")
				body_snip = r.text[:200].replace("\n", " ")
				results.append({
					"path": path, "mode": label,
					"status": r.status_code, "content_type": ct,
					"body_snip": body_snip,
				})
			except Exception as exc:
				results.append({"path": path, "mode": label, "error": str(exc)})
	return jsonify({
		"base_url": OPENWEBUI_BASE_URL,
		"api_key_set": bool(OPENWEBUI_API_KEY),
		"api_key_prefix": OPENWEBUI_API_KEY_PREFIX,
		"api_key_header": OPENWEBUI_API_KEY_HEADER,
		"api_key_last4": OPENWEBUI_API_KEY[-4:] if OPENWEBUI_API_KEY else None,
		"probes": results,
	})


@app.route("/api/qdrant/init-hybrid", methods=["POST"])
def api_qdrant_init_hybrid():
	"""
	Force-initialize all Qdrant collections (or a specific one) to be hybrid-ready
	by adding the sparse vector field.  POST body (optional JSON):
	  { "collection": "<name>" }   → init only that collection
	  {}                           → init all collections
	"""
	body = request.get_json(silent=True) or {}
	specific = body.get("collection", "").strip()

	if specific:
		names = [specific]
	else:
		names = list_collections()

	results = []
	for name in names:
		res = force_init_collection(name)
		results.append({"collection": name, **res})

	return jsonify({"initialized": len(results), "results": results})


@app.route("/api/qdrant/collections", methods=["GET"])
def api_qdrant_collections():
	"""List all Qdrant collections with hybrid-readiness status."""
	names = list_collections()
	return jsonify({"collections": [collection_info(n) for n in names]})


@app.route("/")
def index():
	return render_template("index.html")


@app.route("/healthz")
def healthz():
	return jsonify({"status": "ok", "time": utc_now_iso()})


@app.route("/api/health/docling", methods=["GET"])
def api_docling_health():
	result = docling_check_health()
	code = 200 if result["ok"] else 503
	return jsonify(result), code


@app.route("/api/knowledge-bases", methods=["GET"])
def api_knowledge_bases():
	try:
		kbs = fetch_knowledge_bases()
		return jsonify({"items": kbs})
	except Exception as exc:
		return jsonify({"error": str(exc)}), 502


@app.route("/api/jobs", methods=["POST"])
def api_create_jobs():
	kb_id = (request.form.get("kb_id") or "").strip()
	kb_name = (request.form.get("kb_name") or "").strip() or None
	force_ocr = request.form.get("force_ocr", "").lower() in ("1", "true", "yes", "on")
	if not kb_id:
		return jsonify({"error": "kb_id is required"}), 400

	files = request.files.getlist("files")
	if not files:
		return jsonify({"error": "At least one file is required"}), 400

	created_ids = []
	rejected = []

	for f in files:
		original_name = f.filename or ""
		cleaned_name = secure_filename(original_name)
		if not cleaned_name:
			rejected.append({"filename": original_name, "reason": "Invalid filename"})
			continue
		if not allowed_file(cleaned_name):
			rejected.append({
				"filename": cleaned_name,
				"reason": f"Extension not allowed (allowed: {sorted(ALLOWED_EXTENSIONS)})",
			})
			continue

		disk_name = f"{uuid.uuid4()}_{cleaned_name}"
		disk_path = UPLOAD_DIR / disk_name
		f.save(disk_path)
		job_id = enqueue_job(cleaned_name, str(disk_path), kb_id, kb_name, force_ocr=force_ocr)
		created_ids.append(job_id)

	return jsonify({"created_ids": created_ids, "rejected": rejected}), 201


@app.route("/api/jobs", methods=["GET"])
def api_list_jobs():
	rows = [serialize_job(j) for j in list_jobs()]

	buckets: dict[str, list] = {"waiting": [], "completed": [], "failed": []}
	for row in rows:
		if row["status"] in ("waiting", "processing"):
			buckets["waiting"].append(row)
		elif row["status"] == "completed":
			buckets["completed"].append(row)
		elif row["status"] == "failed":
			buckets["failed"].append(row)

	return jsonify({
		"waiting": buckets["waiting"],
		"completed": buckets["completed"],
		"failed": buckets["failed"],
		"all": rows,
	})


@app.route("/api/jobs/<job_id>", methods=["GET"])
def api_get_job(job_id: str):
	row = get_job(job_id)
	if not row:
		return jsonify({"error": "Job not found"}), 404
	return jsonify(serialize_job(row))


@app.route("/api/jobs/<job_id>/retry", methods=["POST"])
def api_retry_job(job_id: str):
	row = get_job(job_id)
	if not row:
		return jsonify({"error": "Job not found"}), 404
	if row["status"] != "failed":
		return jsonify({"error": "Only failed jobs can be retried"}), 400

	now = utc_now_iso()
	with get_conn() as conn:
		conn.execute(
			"UPDATE jobs SET status='waiting', next_attempt_at=0, last_error=NULL, updated_at=? WHERE id=?",
			(now, job_id),
		)
	return jsonify({"ok": True})


@app.route("/api/jobs/failed/clear", methods=["POST"])
def api_clear_failed_jobs():
	return jsonify({"ok": True, "deleted": clear_failed_jobs()})


@app.route("/api/jobs/waiting/clear", methods=["POST"])
def api_clear_waiting_jobs():
	return jsonify({"ok": True, "deleted": clear_jobs_by_status(("waiting", "processing"))})


@app.route("/api/jobs/clear", methods=["POST"])
def api_clear_all_jobs():
	return jsonify({"ok": True, "deleted": clear_all_jobs()})


# ═══════════════════════════════════════════════════════════════════════════
# Bootstrap
# ═══════════════════════════════════════════════════════════════════════════

def bootstrap() -> tuple[threading.Event, list[threading.Thread]]:
	ensure_dirs()
	init_db()
	recover_interrupted_jobs()

	# Log Docling connectivity at startup.
	health = docling_check_health()
	if health["ok"]:
		logger.info("Docling health: %s", health["detail"])
	else:
		logger.warning("Docling health: %s  (will fall back to legacy mode)", health["detail"])

	return start_workers()


if __name__ == "__main__":
	bootstrap()
	app.run(host=APP_HOST, port=APP_PORT)
