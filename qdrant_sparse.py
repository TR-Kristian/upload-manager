"""
Qdrant sparse-vector layer for hybrid search.

This module handles everything Qdrant-related that Open WebUI doesn't do:
  1. Ensure every KB collection has a ``sparse_vectors`` field configured
     for BM25-style sparse retrieval (using Qdrant's built-in FastEmbed
     BM25 model via the REST API, or our own via qdrant-client).
  2. After Open WebUI embeds a document (via the KB add-file step), find
     the points in the collection that belong to this file and upsert the
     sparse vector payload so hybrid queries can be run.

Architecture:
  - Open WebUI stores each KB as a Qdrant collection named after the KB id.
  - Each point has ``metadata.file_id`` set to the Open WebUI file_id.
  - We use qdrant-client to: (a) ensure sparse config on the collection,
    (b) compute BM25 sparse vectors via qdrant-client's built-in FastEmbed,
    (c) upsert those vectors onto the existing points.

Set SPARSE_ENABLED=false to disable this module completely (safe for fresh
installs that have not switched to hybrid search in Open WebUI yet).
"""

from __future__ import annotations

import logging
import os
import time
from typing import Optional

logger = logging.getLogger(__name__)

# ─── Configuration ────────────────────────────────────────────────────────────
SPARSE_ENABLED:            bool  = os.getenv("SPARSE_ENABLED",            "true").lower() in ("1","true","yes")
QDRANT_URL:                str   = os.getenv("QDRANT_URL",                 "http://127.0.0.1:6333")
QDRANT_API_KEY:            str   = os.getenv("QDRANT_API_KEY",             "")
# Name of the sparse-vector field to create inside the collection
SPARSE_VECTOR_NAME:        str   = os.getenv("SPARSE_VECTOR_NAME",         "text-sparse")
# FastEmbed model to use for BM25 sparse encoding
SPARSE_MODEL:              str   = os.getenv("SPARSE_MODEL",               "Qdrant/bm25")
OPENWEBUI_QDRANT_KNOWLEDGE_COLLECTION: str = os.getenv("OPENWEBUI_QDRANT_KNOWLEDGE_COLLECTION", "").strip()
# How long to wait for Open WebUI to finish embedding before we try the sparse inject
SPARSE_EMBED_GRACE_SECONDS:float = float(os.getenv("SPARSE_EMBED_GRACE_SECONDS", "20"))
# Batch size for point scroll + upsert
SPARSE_BATCH_SIZE:         int   = int(os.getenv("SPARSE_BATCH_SIZE",      "50"))
# Maximum total points to process per file (safety cap)
SPARSE_MAX_POINTS:         int   = int(os.getenv("SPARSE_MAX_POINTS",      "10000"))


# ─── Lazy client ─────────────────────────────────────────────────────────────

def _get_client():
    """Return a QdrantClient, or raise ImportError if qdrant-client is missing."""
    try:
        from qdrant_client import QdrantClient
    except ImportError as e:
        raise ImportError(
            "qdrant-client is not installed. "
            "Add 'qdrant-client[fastembed]>=1.9' to requirements.txt"
        ) from e
    kwargs: dict = {"url": QDRANT_URL, "prefer_grpc": False}
    if QDRANT_API_KEY:
        kwargs["api_key"] = QDRANT_API_KEY
    return QdrantClient(**kwargs)


def _get_sparse_encoder():
    """Return a TextEmbedding model for BM25 sparse encoding."""
    try:
        from fastembed import SparseTextEmbedding
    except ImportError as e:
        raise ImportError(
            "fastembed is not installed. "
            "Add 'fastembed>=0.3' to requirements.txt"
        ) from e
    return SparseTextEmbedding(model_name=SPARSE_MODEL)


# Module-level singletons (created on first use)
_client = None
_encoder = None


def _client_instance():
    global _client
    if _client is None:
        _client = _get_client()
    return _client


def _encoder_instance():
    global _encoder
    if _encoder is None:
        _encoder = _get_sparse_encoder()
    return _encoder


# ─── Collection management ────────────────────────────────────────────────────

def ensure_sparse_vector_config(collection_name: str) -> dict:
    """
    Ensure the collection has the sparse vector field configured.
    If the collection does not yet exist (Open WebUI hasn't created it),
    this is a no-op and will be retried later.
    Returns a status dict.
    """
    if not SPARSE_ENABLED:
        return {"ok": True, "skipped": "sparse disabled"}

    try:
        client = _client_instance()

        # Check if collection exists
        collections = [c.name for c in client.get_collections().collections]
        if collection_name not in collections:
            return {"ok": False, "reason": f"Collection '{collection_name}' does not exist yet"}

        info = client.get_collection(collection_name)
        existing_sparse = {}
        if info.config.params.sparse_vectors:
            existing_sparse = dict(info.config.params.sparse_vectors)

        if SPARSE_VECTOR_NAME in existing_sparse:
            return {"ok": True, "already_configured": True, "collection": collection_name}

        # Add sparse vector config
        from qdrant_client.models import SparseVectorParams, SparseIndexParams

        client.update_collection(
            collection_name=collection_name,
            sparse_vectors_config={
                SPARSE_VECTOR_NAME: SparseVectorParams(
                    index=SparseIndexParams(
                        on_disk=False,
                        full_scan_threshold=5000,
                    )
                )
            },
        )
        logger.info(
            "Sparse vector field '%s' added to collection '%s'",
            SPARSE_VECTOR_NAME, collection_name,
        )
        return {"ok": True, "configured": True, "collection": collection_name}

    except Exception as exc:
        logger.warning("ensure_sparse_vector_config failed for %s: %s", collection_name, exc)
        return {"ok": False, "error": str(exc)}


def force_init_collection(collection_name: str, dense_vector_size: int = 4096) -> dict:
    """
    Create or reconfigure a collection to be hybrid-ready.
    Called from the /api/qdrant/init-hybrid endpoint.
    Uses the dense vector size from Open WebUI's Qwen3-Embedding-8B (4096 dims).
    """
    if not SPARSE_ENABLED:
        return {"ok": False, "error": "SPARSE_ENABLED=false"}

    try:
        from qdrant_client.models import (
            VectorParams, Distance,
            SparseVectorParams, SparseIndexParams,
            ScalarQuantizationConfig, ScalarType,
            OptimizersConfigDiff,
        )

        client = _client_instance()
        collections = [c.name for c in client.get_collections().collections]

        if collection_name not in collections:
            client.create_collection(
                collection_name=collection_name,
                vectors_config=VectorParams(
                    size=dense_vector_size,
                    distance=Distance.COSINE,
                    quantization_config=ScalarQuantizationConfig(
                        scalar={"type": ScalarType.INT8, "always_ram": True}
                    ),
                ),
                sparse_vectors_config={
                    SPARSE_VECTOR_NAME: SparseVectorParams(
                        index=SparseIndexParams(on_disk=False, full_scan_threshold=5000)
                    )
                },
                optimizers_config=OptimizersConfigDiff(default_segment_number=4),
            )
            return {"ok": True, "created": True, "collection": collection_name}

        # Collection exists – add sparse config if missing
        result = ensure_sparse_vector_config(collection_name)
        result["reconfigured"] = True
        return result

    except Exception as exc:
        logger.error("force_init_collection failed for %s: %s", collection_name, exc)
        return {"ok": False, "error": str(exc)}


# ─── Sparse vector injection ──────────────────────────────────────────────────

def _get_text_for_point(point) -> Optional[str]:
    """Extract the embeddable text from a Qdrant point payload."""
    payload = point.payload or {}
    # Open WebUI stores the chunk text in various keys depending on version
    for key in ("page_content", "text", "content", "chunk", "data"):
        val = payload.get(key)
        if isinstance(val, str) and val.strip():
            return val.strip()
        # Nested inside metadata
        meta = payload.get("metadata") or {}
        val = meta.get(key)
        if isinstance(val, str) and val.strip():
            return val.strip()
    return None


def inject_sparse_vectors(
    collection_name: str,
    file_id: str,
    grace_seconds: float = SPARSE_EMBED_GRACE_SECONDS,
) -> dict:
    """
    Find all Qdrant points that belong to *file_id* in *collection_name*,
    compute BM25 sparse vectors for each, and upsert them.

    This is called in a background thread after the KB-add step completes,
    to give Open WebUI time to finish embedding.
    """
    if not SPARSE_ENABLED:
        return {"ok": True, "skipped": "sparse disabled"}

    if grace_seconds > 0:
        logger.debug(
            "Waiting %.0fs for Open WebUI to finish embedding file %s …",
            grace_seconds, file_id,
        )
        time.sleep(grace_seconds)

    try:
        from qdrant_client.models import PointVectors, SparseVector

        client = _client_instance()
        encoder = _encoder_instance()

        # Scroll through all points with matching file_id
        # Open WebUI stores file_id either in payload.metadata.file_id or payload.file_id
        from qdrant_client.models import Filter, FieldCondition, MatchValue

        file_filter = Filter(
            should=[
                Filter(must=[
                    FieldCondition(key="metadata.file_id", match=MatchValue(value=file_id))
                ]),
                Filter(must=[
                    FieldCondition(key="file_id", match=MatchValue(value=file_id))
                ]),
            ]
        )

        offset = None
        total_processed = 0
        total_batches = 0

        while total_processed < SPARSE_MAX_POINTS:
            scroll_result, next_offset = client.scroll(
                collection_name=collection_name,
                scroll_filter=file_filter,
                limit=SPARSE_BATCH_SIZE,
                offset=offset,
                with_payload=True,
                with_vectors=False,
            )

            if not scroll_result:
                break

            # Encode texts
            ids: list = []
            texts: list[str] = []
            for point in scroll_result:
                text = _get_text_for_point(point)
                if text:
                    ids.append(point.id)
                    texts.append(text)

            if ids:
                # Compute sparse vectors (batch)
                sparse_embeddings = list(encoder.embed(texts))
                point_vectors = []
                for pid, sparse_emb in zip(ids, sparse_embeddings):
                    point_vectors.append(
                        PointVectors(
                            id=pid,
                            vectors={
                                SPARSE_VECTOR_NAME: SparseVector(
                                    indices=sparse_emb.indices.tolist(),
                                    values=sparse_emb.values.tolist(),
                                )
                            },
                        )
                    )
                client.update_vectors(
                    collection_name=collection_name,
                    points=point_vectors,
                )
                total_processed += len(ids)
                total_batches += 1
                logger.info(
                    "Sparse inject batch %d: %d points for file %s in collection %s",
                    total_batches, len(ids), file_id, collection_name,
                )

            offset = next_offset
            if offset is None:
                break

        logger.info(
            "Sparse inject complete: %d points processed for file %s in %s",
            total_processed, file_id, collection_name,
        )
        return {"ok": True, "points_updated": total_processed, "batches": total_batches}

    except Exception as exc:
        logger.warning(
            "Sparse vector injection failed for file %s in %s: %s",
            file_id, collection_name, exc,
        )
        return {"ok": False, "error": str(exc)}


# ─── Convenience wrappers ─────────────────────────────────────────────────────

def wait_and_ensure_sparse(
    collection_name: str,
    max_wait_seconds: float = 120,
    poll_interval: float = 3,
) -> dict:
    """
    Poll until the collection exists (Open WebUI may create it a few seconds
    after the KB-add call returns), then add the sparse vector field.

    This is the right function to call from a background thread after a file
    is added to a knowledge base on a fresh / empty Qdrant instance.
    """
    if not SPARSE_ENABLED:
        return {"ok": True, "skipped": "sparse disabled"}

    deadline = time.time() + max_wait_seconds
    attempt = 0

    while time.time() < deadline:
        attempt += 1
        try:
            client = _client_instance()
            names = [c.name for c in client.get_collections().collections]
            if collection_name in names:
                result = ensure_sparse_vector_config(collection_name)
                result["wait_attempts"] = attempt
                logger.info(
                    "wait_and_ensure_sparse: collection '%s' ready after %d poll(s): %s",
                    collection_name, attempt, result,
                )
                return result
            logger.debug(
                "wait_and_ensure_sparse: collection '%s' not yet visible (attempt %d), retrying in %.0fs …",
                collection_name, attempt, poll_interval,
            )
        except Exception as exc:
            logger.warning(
                "wait_and_ensure_sparse: Qdrant probe error (attempt %d): %s", attempt, exc
            )
        time.sleep(poll_interval)

    return {
        "ok": False,
        "error": f"Collection '{collection_name}' did not appear within {max_wait_seconds}s",
        "wait_attempts": attempt,
    }


def list_collections() -> list[str]:
    try:
        return [c.name for c in _client_instance().get_collections().collections]
    except Exception as exc:
        logger.warning("Qdrant list_collections failed: %s", exc)
        return []


def resolve_knowledge_collection_name(kb_id: str) -> str:
    """Resolve which Qdrant collection stores Open WebUI knowledge chunks.

    Open WebUI deployments vary:
      - older layout: one collection per KB id
      - newer layout: shared collections like ``open-webui_knowledge``
    """
    names = list_collections()

    if OPENWEBUI_QDRANT_KNOWLEDGE_COLLECTION:
        if OPENWEBUI_QDRANT_KNOWLEDGE_COLLECTION in names:
            return OPENWEBUI_QDRANT_KNOWLEDGE_COLLECTION
        logger.warning(
            "Configured OPENWEBUI_QDRANT_KNOWLEDGE_COLLECTION '%s' not found; falling back to auto-detect",
            OPENWEBUI_QDRANT_KNOWLEDGE_COLLECTION,
        )

    if kb_id in names:
        return kb_id

    # Common Open WebUI shared collection names
    for candidate in ("open-webui_knowledge", "open_webui_knowledge", "knowledge"):
        if candidate in names:
            return candidate

    # Preserve existing behavior as final fallback.
    return kb_id


def collection_info(name: str) -> dict:
    try:
        info = _client_instance().get_collection(name)
        sparse = {}
        if info.config.params.sparse_vectors:
            sparse = {k: str(v) for k, v in info.config.params.sparse_vectors.items()}
        points_count = getattr(info, "points_count", None)
        if points_count is None:
            points_count = getattr(info, "vectors_count", None)
        return {
            "name": name,
            "points_count": points_count,
            "sparse_vectors_config": sparse,
            "has_sparse": SPARSE_VECTOR_NAME in sparse,
        }
    except Exception as exc:
        return {"name": name, "error": str(exc)}
