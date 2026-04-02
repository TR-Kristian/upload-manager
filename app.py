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


APP_PORT = int(os.getenv("APP_PORT", "8088"))
APP_HOST = os.getenv("APP_HOST", "0.0.0.0")

OPENWEBUI_BASE_URL = os.getenv("OPENWEBUI_BASE_URL", "http://127.0.0.1:3000").rstrip("/")
OPENWEBUI_API_KEY = os.getenv("OPENWEBUI_API_KEY", "")
OPENWEBUI_API_KEY_HEADER = os.getenv("OPENWEBUI_API_KEY_HEADER", "Authorization")
OPENWEBUI_API_KEY_PREFIX = os.getenv("OPENWEBUI_API_KEY_PREFIX", "Bearer")

# Keep paths configurable because Open WebUI endpoint paths can vary by version.
OPENWEBUI_KB_LIST_PATHS = [
	p.strip()
	for p in os.getenv(
		"OPENWEBUI_KB_LIST_PATHS",
		"/api/v1/knowledge/,/api/v1/knowledge,/api/knowledge/,/api/knowledge,/api/v1/knowledge/search?page=1",
	).split(",")
	if p.strip()
]
OPENWEBUI_UPLOAD_PATH_TEMPLATE = os.getenv(
	"OPENWEBUI_UPLOAD_PATH_TEMPLATE", "/api/v1/knowledge/{kb_id}/file"
)
OPENWEBUI_UPLOAD_CANDIDATES = [
	p.strip()
	for p in os.getenv(
		"OPENWEBUI_UPLOAD_CANDIDATES",
		"POST|/api/v1/knowledge/{kb_id}/file,POST|/api/v1/knowledge/{kb_id}/files,POST|/api/knowledge/{kb_id}/file,POST|/api/knowledge/{kb_id}/files,PUT|/api/v1/knowledge/{kb_id}/file",
	).split(",")
	if p.strip()
]
OPENWEBUI_FILE_UPLOAD_PATHS = [
	p.strip()
	for p in os.getenv("OPENWEBUI_FILE_UPLOAD_PATHS", "/api/v1/files/,/api/v1/files").split(",")
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
OPENWEBUI_FILE_STATUS_PATH_TEMPLATES = [
	p.strip()
	for p in os.getenv(
		"OPENWEBUI_FILE_STATUS_PATH_TEMPLATES",
		"/api/v1/files/{file_id}/process/status,/api/files/{file_id}/process/status",
	).split(",")
	if p.strip()
]

HTTP_CONNECT_TIMEOUT_SECONDS = float(os.getenv("HTTP_CONNECT_TIMEOUT_SECONDS", "5"))
HTTP_READ_TIMEOUT_SECONDS = float(os.getenv("HTTP_READ_TIMEOUT_SECONDS", "30"))
HTTP_RETRY_TOTAL = int(os.getenv("HTTP_RETRY_TOTAL", "3"))
HTTP_RETRY_BACKOFF = float(os.getenv("HTTP_RETRY_BACKOFF", "0.5"))
HTTP_POOL_CONNECTIONS = int(os.getenv("HTTP_POOL_CONNECTIONS", "20"))
HTTP_POOL_MAXSIZE = int(os.getenv("HTTP_POOL_MAXSIZE", "50"))

WORKER_COUNT = int(os.getenv("WORKER_COUNT", "3"))
MAX_ATTEMPTS = int(os.getenv("MAX_ATTEMPTS", "4"))
BASE_RETRY_SECONDS = float(os.getenv("BASE_RETRY_SECONDS", "2"))
MAX_RETRY_SECONDS = float(os.getenv("MAX_RETRY_SECONDS", "60"))
POLL_IDLE_SECONDS = float(os.getenv("POLL_IDLE_SECONDS", "1"))
UPLOAD_TIMEOUT_SECONDS = int(os.getenv("UPLOAD_TIMEOUT_SECONDS", "300"))
FILE_PROCESS_WAIT_SECONDS = int(os.getenv("FILE_PROCESS_WAIT_SECONDS", "600"))
FILE_PROCESS_POLL_SECONDS = float(os.getenv("FILE_PROCESS_POLL_SECONDS", "2"))

ALLOWED_EXTENSIONS = {
	ext.strip().lower()
	for ext in os.getenv(
		"ALLOWED_EXTENSIONS",
		".pdf,.doc,.docx,.txt,.md,.csv,.xlsx,.ppt,.pptx",
	).split(",")
	if ext.strip()
}
MAX_UPLOAD_BYTES = int(os.getenv("MAX_UPLOAD_BYTES", str(200 * 1024 * 1024)))

DATA_DIR = Path(os.getenv("DATA_DIR", "/app/data"))
UPLOAD_DIR = DATA_DIR / "uploads"
DB_PATH = DATA_DIR / "uploader.db"


app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = MAX_UPLOAD_BYTES
logger = logging.getLogger(__name__)


def utc_now_iso() -> str:
	return datetime.now(timezone.utc).isoformat()


def epoch_seconds() -> float:
	return time.time()


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
				created_at TEXT NOT NULL,
				updated_at TEXT NOT NULL,
				completed_at TEXT
			)
			"""
		)
		conn.execute(
			"CREATE INDEX IF NOT EXISTS idx_jobs_status_next ON jobs(status, next_attempt_at)"
		)


def recover_interrupted_jobs() -> None:
	now = utc_now_iso()
	with get_conn() as conn:
		conn.execute(
			"""
			UPDATE jobs
			SET status='waiting', updated_at=?
			WHERE status='processing'
			""",
			(now,),
		)


def build_auth_headers() -> dict:
	headers = {}
	if OPENWEBUI_API_KEY:
		if OPENWEBUI_API_KEY_PREFIX:
			headers[OPENWEBUI_API_KEY_HEADER] = (
				f"{OPENWEBUI_API_KEY_PREFIX} {OPENWEBUI_API_KEY}"
			)
		else:
			headers[OPENWEBUI_API_KEY_HEADER] = OPENWEBUI_API_KEY
	return headers


def build_header_options() -> list[dict]:
	auth_headers = build_auth_headers()
	options = [auth_headers] if auth_headers else []
	options.append({})

	unique_options = []
	seen = set()
	for headers in options:
		key = tuple(sorted(headers.items()))
		if key in seen:
			continue
		seen.add(key)
		unique_options.append(headers)
	return unique_options


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
	header_options = build_header_options()

	errors = []
	for path in OPENWEBUI_KB_LIST_PATHS:
		url = f"{OPENWEBUI_BASE_URL}{path}"
		for headers in header_options:
			auth_mode = "auth" if headers else "no-auth"
			try:
				response = session.get(
					url,
					headers=headers,
					timeout=(HTTP_CONNECT_TIMEOUT_SECONDS, HTTP_READ_TIMEOUT_SECONDS),
				)
				if response.status_code >= 400:
					errors.append(f"GET {path} [{auth_mode}] -> {response.status_code}")
					continue

				# Stop at the first endpoint that responds with 2xx, even if list is empty.
				try:
					return normalize_kb_items(response.json())
				except Exception as exc:
					content_type = response.headers.get("content-type", "unknown")
					body_preview = response.text[:120].replace("\n", " ")
					errors.append(
						f"GET {path} [{auth_mode}] -> invalid JSON ({exc}); content-type={content_type}; body={body_preview}"
					)
			except Exception as exc:
				errors.append(f"GET {path} [{auth_mode}] -> {exc}")

	if errors:
		raise RuntimeError(f"Unable to fetch knowledge bases. Tried: {', '.join(errors)}")
	raise RuntimeError("Unable to fetch knowledge bases: no valid endpoint configured")


def allowed_file(filename: str) -> bool:
	suffix = Path(filename).suffix.lower()
	return suffix in ALLOWED_EXTENSIONS


def enqueue_job(filename: str, stored_path: str, kb_id: str, kb_name: str | None) -> str:
	job_id = str(uuid.uuid4())
	now = utc_now_iso()
	with get_conn() as conn:
		conn.execute(
			"""
			INSERT INTO jobs (
				id, filename, stored_path, kb_id, kb_name,
				status, attempt_count, max_attempts, next_attempt_at,
				last_error, created_at, updated_at, completed_at
			) VALUES (?, ?, ?, ?, ?, 'waiting', 0, ?, 0, NULL, ?, ?, NULL)
			""",
			(job_id, filename, stored_path, kb_id, kb_name, MAX_ATTEMPTS, now, now),
		)
	return job_id


def get_job(job_id: str):
	with get_conn() as conn:
		row = conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
		return dict(row) if row else None


def list_jobs() -> list:
	with get_conn() as conn:
		rows = conn.execute(
			"SELECT * FROM jobs ORDER BY created_at DESC"
		).fetchall()
	return [dict(r) for r in rows]


def update_job_status(job_id: str, status: str, error: str | None = None) -> None:
	now = utc_now_iso()
	completed_at = now if status == "completed" else None
	with get_conn() as conn:
		conn.execute(
			"""
			UPDATE jobs
			SET status=?, last_error=?, updated_at=?, completed_at=?
			WHERE id=?
			""",
			(status, error, now, completed_at, job_id),
		)


def schedule_retry(job_id: str, attempt_count: int, message: str) -> None:
	delay = min(MAX_RETRY_SECONDS, BASE_RETRY_SECONDS * (2 ** max(0, attempt_count - 1)))
	jitter = random.uniform(0, 0.5)
	next_attempt_at = epoch_seconds() + delay + jitter
	now = utc_now_iso()
	with get_conn() as conn:
		conn.execute(
			"""
			UPDATE jobs
			SET status='waiting', next_attempt_at=?, last_error=?, updated_at=?
			WHERE id=?
			""",
			(next_attempt_at, message, now, job_id),
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
				SELECT *
				FROM jobs
				WHERE status='waiting' AND next_attempt_at <= ?
				ORDER BY created_at ASC
				LIMIT 1
				""",
				(now_epoch,),
			).fetchone()
			if not row:
				conn.commit()
				return None

			job_id = row["id"]
			conn.execute(
				"""
				UPDATE jobs
				SET status='processing', attempt_count=attempt_count+1, updated_at=?
				WHERE id=?
				""",
				(now_iso, job_id),
			)
			conn.commit()
	except sqlite3.OperationalError as exc:
		if "locked" in str(exc).lower():
			return None
		raise

	return get_job(job_id)


def _extract_file_id(payload) -> str | None:
	if isinstance(payload, dict):
		for key in ("id", "file_id"):
			value = payload.get(key)
			if value:
				return str(value)

		for container_key in ("data", "file", "item", "result"):
			container = payload.get(container_key)
			if isinstance(container, dict):
				for key in ("id", "file_id"):
					value = container.get(key)
					if value:
						return str(value)
	return None


def wait_for_openwebui_file_processing(file_id: str, headers: dict) -> tuple[bool, str]:
	deadline = time.time() + FILE_PROCESS_WAIT_SECONDS
	attempt_summaries = []
	session = create_http_session()

	while time.time() < deadline:
		for status_template in OPENWEBUI_FILE_STATUS_PATH_TEMPLATES:
			status_path = status_template.format(file_id=file_id)
			status_url = f"{OPENWEBUI_BASE_URL}{status_path}"
			try:
				response = session.get(
					status_url,
					headers=headers,
					timeout=(HTTP_CONNECT_TIMEOUT_SECONDS, HTTP_READ_TIMEOUT_SECONDS),
				)
				if response.status_code in (404, 405):
					attempt_summaries.append(f"GET {status_path} -> {response.status_code}")
					continue
				if response.status_code >= 400:
					body = response.text[:200].replace("\n", " ")
					return False, f"File processing status failed ({response.status_code}) on {status_path}: {body}"

				payload = response.json()
				status = str(payload.get("status", "")).lower()
				if status == "completed":
					return True, f"GET {status_path} -> completed"
				if status == "failed":
					error_text = payload.get("error") or payload.get("detail") or "processing failed"
					return False, f"File processing failed on {status_path}: {error_text}"
				attempt_summaries.append(f"GET {status_path} -> {status or 'pending'}")
			except Exception as exc:
				attempt_summaries.append(f"GET {status_path} -> {exc}")

		time.sleep(FILE_PROCESS_POLL_SECONDS)

	return False, f"Timed out waiting for file processing. Tried: {', '.join(attempt_summaries[-10:])}"


def _humanize_processing_error(error_text: str) -> str:
	if "127.0.0.1', port=5001" in error_text or "/v1/convert/file" in error_text:
		return (
			"Docling processing is unavailable in Open WebUI. "
			"The Open WebUI container cannot reach Docling at http://127.0.0.1:5001. "
			f"Original error: {error_text}"
		)
	return error_text


def upload_to_openwebui_via_file_add(job: dict, headers: dict) -> tuple[str, str]:
	kb_id = job["kb_id"]
	attempt_summaries = []
	session = create_http_session()

	for upload_path in OPENWEBUI_FILE_UPLOAD_PATHS:
		upload_url = f"{OPENWEBUI_BASE_URL}{upload_path}"
		with open(job["stored_path"], "rb") as file_obj:
			files = {"file": (job["filename"], file_obj)}
			upload_response = session.post(
				upload_url,
				headers=headers,
				params={"process": "true", "process_in_background": "false"},
				files=files,
				timeout=(HTTP_CONNECT_TIMEOUT_SECONDS, UPLOAD_TIMEOUT_SECONDS),
			)

		attempt_summaries.append(f"POST {upload_path} -> {upload_response.status_code}")
		if upload_response.status_code in (404, 405):
			continue
		if upload_response.status_code >= 400:
			body = upload_response.text[:350].replace("\n", " ")
			return "fatal", f"File upload failed ({upload_response.status_code}) on {upload_path}: {body}"

		try:
			upload_payload = upload_response.json()
		except Exception:
			upload_payload = {}

		file_id = _extract_file_id(upload_payload)
		if not file_id:
			return "fatal", (
				f"File upload succeeded but file id was missing from response on {upload_path}"
			)

		processed_ok, processed_detail = wait_for_openwebui_file_processing(file_id, headers)
		attempt_summaries.append(processed_detail)
		if not processed_ok:
			return "fatal", _humanize_processing_error(processed_detail)

		for add_template in OPENWEBUI_KB_ADD_FILE_PATH_TEMPLATES:
			add_path = add_template.format(kb_id=kb_id)
			add_url = f"{OPENWEBUI_BASE_URL}{add_path}"
			add_response = session.post(
				add_url,
				headers={**headers, "Content-Type": "application/json"},
				json={"file_id": file_id},
				timeout=(HTTP_CONNECT_TIMEOUT_SECONDS, UPLOAD_TIMEOUT_SECONDS),
			)

			attempt_summaries.append(
				f"POST {add_path} -> {add_response.status_code}"
			)
			if add_response.status_code < 400:
				return "success", ", ".join(attempt_summaries)
			if add_response.status_code in (404, 405):
				continue

			body = add_response.text[:350].replace("\n", " ")
			return "fatal", (
				f"Attach file to knowledge failed ({add_response.status_code}) on {add_path}: {body}"
			)

	return "route-mismatch", ", ".join(attempt_summaries)


def upload_to_openwebui(job: dict) -> None:
	kb_id = job["kb_id"]
	headers = build_auth_headers()
	session = create_http_session()

	file_add_result, detail = upload_to_openwebui_via_file_add(job, headers)
	if file_add_result == "success":
		return
	if file_add_result == "fatal":
		raise RuntimeError(detail)

	targets: list[tuple[str, str]] = []
	if OPENWEBUI_UPLOAD_PATH_TEMPLATE.strip():
		targets.append(("POST", OPENWEBUI_UPLOAD_PATH_TEMPLATE.strip()))

	for candidate in OPENWEBUI_UPLOAD_CANDIDATES:
		if "|" in candidate:
			method_raw, path_raw = candidate.split("|", 1)
			method = method_raw.strip().upper() or "POST"
			path_template = path_raw.strip()
		else:
			method = "POST"
			path_template = candidate.strip()
		if path_template:
			targets.append((method, path_template))

	# Preserve candidate order while removing duplicates.
	seen = set()
	unique_targets = []
	for method, template in targets:
		key = (method, template)
		if key in seen:
			continue
		seen.add(key)
		unique_targets.append((method, template))

	attempt_summaries = []
	last_error = None
	for method, path_template in unique_targets:
		upload_path = path_template.format(kb_id=kb_id)
		url = f"{OPENWEBUI_BASE_URL}{upload_path}"

		with open(job["stored_path"], "rb") as file_obj:
			files = {"file": (job["filename"], file_obj)}
			response = session.request(
				method,
				url,
				headers=headers,
				files=files,
				timeout=(HTTP_CONNECT_TIMEOUT_SECONDS, UPLOAD_TIMEOUT_SECONDS),
			)

		if response.status_code < 400:
			return

		body = response.text[:350].replace("\n", " ")
		attempt_summaries.append(f"{method} {upload_path} -> {response.status_code}")

		# Try the next candidate for endpoint/method mismatch scenarios.
		if response.status_code in (404, 405):
			last_error = RuntimeError(
				f"Open WebUI upload path mismatch ({response.status_code}) on {method} {upload_path}"
			)
			continue

		# Some versions may expect a different upload route; keep trying candidates.
		last_error = RuntimeError(
			f"Open WebUI upload failed ({response.status_code}) on {method} {upload_path}: {body}"
		)

	if last_error:
		raise RuntimeError(
			f"{last_error}. file/add attempts: {detail}. direct attempts: {', '.join(attempt_summaries)}"
		)

	raise RuntimeError("Open WebUI upload failed: no upload candidates configured")


def clear_failed_jobs() -> int:
	rows = []
	with get_conn() as conn:
		rows = conn.execute(
			"SELECT id, stored_path FROM jobs WHERE status='failed'"
		).fetchall()

		conn.execute("DELETE FROM jobs WHERE status='failed'")

	for row in rows:
		stored_path = row["stored_path"]
		if stored_path:
			try:
				path_obj = Path(stored_path)
				if path_obj.exists():
					path_obj.unlink()
			except Exception:
				# Cleanup failure should not block job table cleanup.
				pass

	return len(rows)


def clear_jobs_by_status(statuses: tuple[str, ...]) -> int:
	placeholders = ",".join(["?" for _ in statuses])
	rows = []
	with get_conn() as conn:
		rows = conn.execute(
			f"SELECT id, stored_path FROM jobs WHERE status IN ({placeholders})",
			statuses,
		).fetchall()

		conn.execute(
			f"DELETE FROM jobs WHERE status IN ({placeholders})",
			statuses,
		)

	for row in rows:
		stored_path = row["stored_path"]
		if stored_path:
			try:
				path_obj = Path(stored_path)
				if path_obj.exists():
					path_obj.unlink()
			except Exception:
				pass

	return len(rows)


def clear_all_jobs() -> int:
	rows = []
	with get_conn() as conn:
		rows = conn.execute("SELECT id, stored_path FROM jobs").fetchall()
		conn.execute("DELETE FROM jobs")

	for row in rows:
		stored_path = row["stored_path"]
		if stored_path:
			try:
				path_obj = Path(stored_path)
				if path_obj.exists():
					path_obj.unlink()
			except Exception:
				pass

	return len(rows)


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
			upload_to_openwebui(job)
			update_job_status(job["id"], "completed", None)
		except Exception as exc:
			logger.exception("Upload worker error for job %s", job["id"])
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
		thread = threading.Thread(
			target=run_worker,
			args=(stop_event,),
			name=f"upload-worker-{idx + 1}",
			daemon=True,
		)
		thread.start()
		threads.append(thread)
	return stop_event, threads


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
		"created_at": row["created_at"],
		"updated_at": row["updated_at"],
		"completed_at": row.get("completed_at"),
	}


@app.route("/")
def index():
	return render_template("index.html")


@app.route("/healthz")
def healthz():
	return jsonify({"status": "ok", "time": utc_now_iso()})


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
			rejected.append(
				{
					"filename": cleaned_name,
					"reason": f"Extension not allowed (allowed: {sorted(ALLOWED_EXTENSIONS)})",
				}
			)
			continue

		disk_name = f"{uuid.uuid4()}_{cleaned_name}"
		disk_path = UPLOAD_DIR / disk_name
		f.save(disk_path)
		job_id = enqueue_job(cleaned_name, str(disk_path), kb_id, kb_name)
		created_ids.append(job_id)

	return jsonify({"created_ids": created_ids, "rejected": rejected}), 201


@app.route("/api/jobs", methods=["GET"])
def api_list_jobs():
	rows = [serialize_job(j) for j in list_jobs()]

	buckets = {"waiting": [], "completed": [], "failed": []}
	for row in rows:
		if row["status"] in ("waiting", "processing"):
			buckets["waiting"].append(row)
		elif row["status"] == "completed":
			buckets["completed"].append(row)
		elif row["status"] == "failed":
			buckets["failed"].append(row)

	return jsonify(
		{
			"waiting": buckets["waiting"],
			"completed": buckets["completed"],
			"failed": buckets["failed"],
			"all": rows,
		}
	)


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
		return jsonify({"error": "Only failed jobs can be retried manually"}), 400

	now = utc_now_iso()
	with get_conn() as conn:
		conn.execute(
			"""
			UPDATE jobs
			SET status='waiting', next_attempt_at=0, last_error=NULL, updated_at=?
			WHERE id=?
			""",
			(now, job_id),
		)
	return jsonify({"ok": True})


@app.route("/api/jobs/failed/clear", methods=["POST"])
def api_clear_failed_jobs():
	deleted_count = clear_failed_jobs()
	return jsonify({"ok": True, "deleted": deleted_count})


@app.route("/api/jobs/waiting/clear", methods=["POST"])
def api_clear_waiting_jobs():
	deleted_count = clear_jobs_by_status(("waiting", "processing"))
	return jsonify({"ok": True, "deleted": deleted_count})


@app.route("/api/jobs/clear", methods=["POST"])
def api_clear_all_jobs():
	deleted_count = clear_all_jobs()
	return jsonify({"ok": True, "deleted": deleted_count})


def bootstrap() -> tuple[threading.Event, list[threading.Thread]]:
	ensure_dirs()
	init_db()
	recover_interrupted_jobs()
	return start_workers()


if __name__ == "__main__":
	bootstrap()
	app.run(host=APP_HOST, port=APP_PORT)
