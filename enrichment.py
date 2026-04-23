"""
Document enrichment for state-of-the-art RAG.

Before pushing content to Open WebUI, this module:
  1. Calls vLLM-chat (Gemma-4) to generate a concise document summary.
  2. Calls vLLM-chat to extract high-value domain keywords / named entities.
  3. Prepends both blocks to the Docling Markdown so they are embedded
     into every chunk that overlaps with the document start, and the
     summary + keywords are always retrievable as standalone chunks.

The enriched content is what gets pushed via the content-update endpoint
and subsequently chunked + embedded by Open WebUI.

Set ENRICHMENT_ENABLED=false to skip all enrichment (documents are still
uploaded and embedded, just without the summary/keyword prefix).
"""

from __future__ import annotations

import logging
import os
import re
import time
from typing import Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

# ─── Configuration ────────────────────────────────────────────────────────────
VLLM_CHAT_BASE_URL:    str   = os.getenv("VLLM_CHAT_BASE_URL",   "http://127.0.0.1:8001/v1")
VLLM_CHAT_API_KEY:     str   = os.getenv("VLLM_CHAT_API_KEY",    "Openclaw123")
VLLM_CHAT_MODEL:       str   = os.getenv("VLLM_CHAT_MODEL",      "gemma-4")
VLLM_CHAT_TIMEOUT:     float = float(os.getenv("VLLM_CHAT_TIMEOUT", "120"))
VLLM_CHAT_CONNECT_TO:  float = float(os.getenv("VLLM_CHAT_CONNECT_TIMEOUT", "10"))

ENRICHMENT_ENABLED:        bool = os.getenv("ENRICHMENT_ENABLED",  "true").lower() in ("1", "true", "yes")
ENRICHMENT_SUMMARY:        bool = os.getenv("ENRICHMENT_SUMMARY",  "true").lower() in ("1", "true", "yes")
ENRICHMENT_KEYWORDS:       bool = os.getenv("ENRICHMENT_KEYWORDS", "true").lower() in ("1", "true", "yes")
# Maximum characters of document text sent to the LLM (keeps latency bounded)
ENRICHMENT_MAX_INPUT_CHARS:    int = int(os.getenv("ENRICHMENT_MAX_INPUT_CHARS",    "12000"))
ENRICHMENT_SUMMARY_MAX_TOKENS: int = int(os.getenv("ENRICHMENT_SUMMARY_MAX_TOKENS", "350"))
ENRICHMENT_KEYWORDS_COUNT:     int = int(os.getenv("ENRICHMENT_KEYWORDS_COUNT",     "30"))
ENRICHMENT_KEYWORDS_MAX_TOKENS:int = int(os.getenv("ENRICHMENT_KEYWORDS_MAX_TOKENS","200"))


# ─── HTTP session (lazy singleton) ───────────────────────────────────────────

def _make_chat_session() -> requests.Session:
    retry = Retry(
        total=2, connect=2, read=0,
        backoff_factor=1.0,
        status_forcelist=(429, 502, 503, 504),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=4, pool_maxsize=8)
    s = requests.Session()
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    s.headers["Authorization"] = f"Bearer {VLLM_CHAT_API_KEY}"
    s.headers["Accept"] = "application/json"
    return s


_chat_session: Optional[requests.Session] = None


def _get_session() -> requests.Session:
    global _chat_session
    if _chat_session is None:
        _chat_session = _make_chat_session()
    return _chat_session


# ─── LLM call ─────────────────────────────────────────────────────────────────

def _chat_complete(system_prompt: str, user_prompt: str, max_tokens: int) -> str:
    """Call the vLLM /chat/completions endpoint. Returns assistant text."""
    url = f"{VLLM_CHAT_BASE_URL}/chat/completions"
    payload = {
        "model": VLLM_CHAT_MODEL,
        "messages": [
            {"role": "system",  "content": system_prompt},
            {"role": "user",    "content": user_prompt},
        ],
        "max_tokens": max_tokens,
        "temperature": 0.0,
        "top_p": 1.0,
    }
    resp = _get_session().post(
        url, json=payload,
        timeout=(VLLM_CHAT_CONNECT_TO, VLLM_CHAT_TIMEOUT),
    )
    if resp.status_code >= 400:
        raise RuntimeError(
            f"vLLM chat completions failed ({resp.status_code}): {resp.text[:400]}"
        )
    data = resp.json()
    try:
        return data["choices"][0]["message"]["content"].strip()
    except (KeyError, IndexError) as exc:
        raise RuntimeError(f"Unexpected vLLM response: {data}") from exc


# ─── Text helpers ─────────────────────────────────────────────────────────────

def _truncate_smart(text: str, max_chars: int) -> str:
    """Truncate to max_chars, preferring paragraph boundaries."""
    if len(text) <= max_chars:
        return text
    snip = text[:max_chars]
    last_para = snip.rfind("\n\n")
    if last_para > max_chars * 0.7:
        return snip[:last_para]
    return snip


# ─── Summary ─────────────────────────────────────────────────────────────────

def generate_summary(content: str, filename: str) -> str:
    """Ask Gemma-4 for a concise 3–5 sentence document summary."""
    truncated = _truncate_smart(content, ENRICHMENT_MAX_INPUT_CHARS)
    system = (
        "You are a precise technical document summarizer. "
        "Write a concise factual summary in 3 to 5 sentences. "
        "Focus on the main topic, key findings, decisions, and important details. "
        "Write in the same language as the document. "
        "Do not add any headings, bullet points, or preamble — only plain prose."
    )
    user = f"Document filename: {filename}\n\n{truncated}"
    return _chat_complete(system, user, ENRICHMENT_SUMMARY_MAX_TOKENS)


# ─── Keyword extraction ───────────────────────────────────────────────────────

def extract_keywords(content: str, filename: str) -> list[str]:
    """Ask Gemma-4 to extract high-value domain keywords and named entities."""
    truncated = _truncate_smart(content, ENRICHMENT_MAX_INPUT_CHARS)
    system = (
        f"You are a technical keyword extractor. "
        f"Extract exactly {ENRICHMENT_KEYWORDS_COUNT} high-value keywords and key phrases "
        "from the document that are most useful for information retrieval. "
        "Include: domain-specific terms, proper nouns, abbreviations, technical concepts, "
        "named entities (people, organisations, regulations, standards, locations, dates). "
        "Return ONLY a comma-separated list of keywords on a single line — nothing else. "
        "Preserve the original language of each term."
    )
    user = f"Document filename: {filename}\n\n{truncated}"
    raw = _chat_complete(system, user, ENRICHMENT_KEYWORDS_MAX_TOKENS)

    # Parse: split on commas/semicolons/newlines, strip markdown artefacts
    keywords: list[str] = []
    for kw in re.split(r"[,;\n]", raw):
        kw = kw.strip().strip("*_`•·-–—").strip()
        if kw and len(kw) > 1:
            keywords.append(kw)
    return keywords[:ENRICHMENT_KEYWORDS_COUNT]


# ─── Public API ───────────────────────────────────────────────────────────────

def enrich_content(content: str, filename: str) -> tuple[str, dict]:
    """
    Enrich Markdown content with an LLM-generated summary and keyword list.

    Returns ``(enriched_content, metadata_dict)``.
    If ENRICHMENT_ENABLED is False, returns the original content unchanged
    with an empty metadata dict.

    Failures in individual steps are logged as warnings and skipped —
    the document is always returned (possibly without enrichment).
    """
    if not ENRICHMENT_ENABLED:
        return content, {"enrichment": "disabled"}

    meta: dict = {}
    prefix_parts: list[str] = []

    if ENRICHMENT_SUMMARY:
        try:
            t0 = time.time()
            summary = generate_summary(content, filename)
            elapsed = round((time.time() - t0) * 1000)
            meta["summary_chars"] = len(summary)
            meta["summary_ms"] = elapsed
            prefix_parts.append(f"## Document Summary\n\n{summary}")
            logger.info(
                "Enrichment summary for %s: %d chars in %dms",
                filename, len(summary), elapsed,
            )
        except Exception as exc:
            logger.warning("Summary generation failed for %s: %s", filename, exc)
            meta["summary_error"] = str(exc)

    if ENRICHMENT_KEYWORDS:
        try:
            t0 = time.time()
            keywords = extract_keywords(content, filename)
            elapsed = round((time.time() - t0) * 1000)
            meta["keyword_count"] = len(keywords)
            meta["keyword_ms"] = elapsed
            if keywords:
                prefix_parts.append(f"## Key Terms\n\n{', '.join(keywords)}")
            logger.info(
                "Enrichment keywords for %s: %d terms in %dms",
                filename, len(keywords), elapsed,
            )
        except Exception as exc:
            logger.warning("Keyword extraction failed for %s: %s", filename, exc)
            meta["keywords_error"] = str(exc)

    if not prefix_parts:
        return content, meta

    # Separator line makes it visually clear where enrichment ends
    separator = "\n\n---\n\n## Document Content\n\n"
    enriched = "\n\n".join(prefix_parts) + separator + content
    meta["total_chars"] = len(enriched)
    meta["enriched"] = True
    return enriched, meta
