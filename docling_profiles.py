"""
Docling conversion profiles – optimized per file type for Hungarian RAG.

Each profile returns the multipart form fields that docling-serve's
``POST /v1/convert/file`` endpoint accepts.  The caller can override
``ocr_lang`` and ``force_ocr`` via env-vars or per-job flags.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
# Defaults (overridable via environment)
# ---------------------------------------------------------------------------
DEFAULT_OCR_LANGS: list[str] = [
    lang.strip()
    for lang in os.getenv("DOCLING_OCR_LANGS", "hu,en").split(",")
    if lang.strip()
]
DEFAULT_OCR_ENGINE: str = os.getenv("DOCLING_OCR_ENGINE", "easyocr")
DEFAULT_PDF_BACKEND: str = os.getenv("DOCLING_PDF_BACKEND", "dlparse_v2")
DEFAULT_TABLE_MODE_PDF: str = os.getenv("DOCLING_TABLE_MODE_PDF", "accurate")
DEFAULT_TABLE_MODE_XLSX: str = os.getenv("DOCLING_TABLE_MODE_XLSX", "fast")
DEFAULT_TABLE_MODE_DOCX: str = os.getenv("DOCLING_TABLE_MODE_DOCX", "accurate")
DEFAULT_TABLE_MODE_PPTX: str = os.getenv("DOCLING_TABLE_MODE_PPTX", "accurate")
DEFAULT_IMAGE_EXPORT: str = os.getenv("DOCLING_IMAGE_EXPORT", "placeholder")
DEFAULT_IMAGES_SCALE: str = os.getenv("DOCLING_IMAGES_SCALE", "2.0")
DEFAULT_DOCUMENT_TIMEOUT: str = os.getenv("DOCLING_DOCUMENT_TIMEOUT", "600")

# Extensions that are plain-text and don't need Docling at all.
PLAINTEXT_EXTENSIONS: frozenset[str] = frozenset({".txt", ".md", ".csv"})

# Extensions handled by Docling.
DOCLING_EXTENSIONS: frozenset[str] = frozenset({
    ".pdf", ".docx", ".doc", ".xlsx", ".pptx", ".ppt", ".html", ".htm",
})


def _ext(filename: str) -> str:
    return Path(filename).suffix.lower()


def needs_docling(filename: str) -> bool:
    """Return True if the file should be sent through Docling."""
    return _ext(filename) in DOCLING_EXTENSIONS


def is_plaintext(filename: str) -> bool:
    """Return True if we can just read the file as UTF-8 text."""
    return _ext(filename) in PLAINTEXT_EXTENSIONS


# ---------------------------------------------------------------------------
# Profile builders
# ---------------------------------------------------------------------------

def _base_fields(
    *,
    from_formats: list[str],
    to_formats: list[str] | None = None,
    do_ocr: bool = True,
    force_ocr: bool = False,
    ocr_engine: str | None = None,
    ocr_langs: list[str] | None = None,
    pdf_backend: str | None = None,
    table_mode: str = "accurate",
    do_table_structure: bool = True,
    include_images: bool = True,
    images_scale: str | None = None,
    image_export_mode: str | None = None,
    document_timeout: str | None = None,
) -> list[tuple[str, str]]:
    """Build the flat list of (key, value) pairs for multipart form data.

    Docling-serve's ``/v1/convert/file`` accepts these as repeated form
    fields (one per value for list-type params like ``from_formats``).
    """
    if to_formats is None:
        to_formats = ["md", "json"]
    if ocr_engine is None:
        ocr_engine = DEFAULT_OCR_ENGINE
    if ocr_langs is None:
        ocr_langs = DEFAULT_OCR_LANGS
    if pdf_backend is None:
        pdf_backend = DEFAULT_PDF_BACKEND
    if image_export_mode is None:
        image_export_mode = DEFAULT_IMAGE_EXPORT
    if images_scale is None:
        images_scale = DEFAULT_IMAGES_SCALE
    if document_timeout is None:
        document_timeout = DEFAULT_DOCUMENT_TIMEOUT

    fields: list[tuple[str, str]] = []

    for fmt in from_formats:
        fields.append(("from_formats", fmt))
    for fmt in to_formats:
        fields.append(("to_formats", fmt))

    fields.append(("do_ocr", str(do_ocr).lower()))
    fields.append(("force_ocr", str(force_ocr).lower()))
    fields.append(("ocr_engine", ocr_engine))
    for lang in ocr_langs:
        fields.append(("ocr_lang", lang))

    if pdf_backend:
        fields.append(("pdf_backend", pdf_backend))

    fields.append(("table_mode", table_mode))
    fields.append(("do_table_structure", str(do_table_structure).lower()))
    fields.append(("include_images", str(include_images).lower()))
    fields.append(("images_scale", images_scale))
    fields.append(("image_export_mode", image_export_mode))
    fields.append(("abort_on_error", "false"))
    fields.append(("document_timeout", document_timeout))

    return fields


def profile_pdf(*, force_ocr: bool = False) -> list[tuple[str, str]]:
    """Optimized profile for PDF files."""
    return _base_fields(
        from_formats=["pdf"],
        do_ocr=True,
        force_ocr=force_ocr,
        table_mode=DEFAULT_TABLE_MODE_PDF,
        include_images=True,
    )


def profile_docx(*, force_ocr: bool = False) -> list[tuple[str, str]]:
    """Optimized profile for Word documents."""
    return _base_fields(
        from_formats=["docx"],
        do_ocr=force_ocr,  # normally off for docx
        force_ocr=force_ocr,
        table_mode=DEFAULT_TABLE_MODE_DOCX,
        include_images=True,
    )


def profile_xlsx(*, force_ocr: bool = False) -> list[tuple[str, str]]:
    """Optimized profile for Excel spreadsheets."""
    return _base_fields(
        from_formats=["xlsx"],
        do_ocr=False,
        force_ocr=False,
        table_mode=DEFAULT_TABLE_MODE_XLSX,
        do_table_structure=True,
        include_images=False,
    )


def profile_pptx(*, force_ocr: bool = False) -> list[tuple[str, str]]:
    """Optimized profile for PowerPoint files."""
    return _base_fields(
        from_formats=["pptx"],
        do_ocr=True,
        force_ocr=force_ocr,
        table_mode=DEFAULT_TABLE_MODE_PPTX,
        include_images=True,
    )


def profile_html(*, force_ocr: bool = False) -> list[tuple[str, str]]:
    """Profile for HTML files."""
    return _base_fields(
        from_formats=["html"],
        do_ocr=False,
        force_ocr=False,
        table_mode="fast",
        include_images=False,
    )


# Map from normalised extension to profile function.
_PROFILE_MAP: dict[str, Any] = {
    ".pdf": profile_pdf,
    ".docx": profile_docx,
    ".doc": profile_docx,
    ".xlsx": profile_xlsx,
    ".pptx": profile_pptx,
    ".ppt": profile_pptx,
    ".html": profile_html,
    ".htm": profile_html,
}


def get_profile(filename: str, *, force_ocr: bool = False) -> list[tuple[str, str]]:
    """Return the Docling form fields for *filename*'s type.

    Raises ``ValueError`` if no profile exists (caller should fall back to
    plain-text read or let Open WebUI handle it).
    """
    ext = _ext(filename)
    builder = _PROFILE_MAP.get(ext)
    if builder is None:
        raise ValueError(f"No Docling profile for extension '{ext}'")
    return builder(force_ocr=force_ocr)
