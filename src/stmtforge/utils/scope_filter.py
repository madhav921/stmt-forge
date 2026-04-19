"""Scope filtering utilities to keep workflow limited to credit card statements."""

from __future__ import annotations

import io
import re
from pathlib import Path

from stmtforge.utils.config import load_config

_DEFAULT_IRRELEVANT_FILENAME_PATTERNS = [
    "SBI_wealth_Daily_News",
    "Ecowrap_",
    "HDFC Bank Insta Credit Card",
    "Application form",
    "KFS_",
    "Key Fact Statement",
    "MITC",
    "CARD_AGREEMENT",
    "EV25",
    "debit card statement",
]

_DEFAULT_IRRELEVANT_TEXT_KEYWORDS = [
    "wealth daily news",
    "ecowrap",
    "key fact statement",
    "most important terms",
    "card agreement",
    "application form",
    "welcome letter",
    "upgrade application",
    "debit card statement",
    "savings account statement",
    "current account statement",
    "loan account statement",
    "home loan",
    "personal loan",
    "fixed deposit",
    "recurring deposit",
]

_ICICI_SAVINGS_PATTERNS = [
    re.compile(r"Statement_\d{4}MTH\d{2}_\d+\.pdf", re.IGNORECASE),
    re.compile(r"Statement_[A-Z]{3}\d{4}_432681569\.pdf", re.IGNORECASE),
]

_HDFC_EMI_PATTERN = re.compile(r"^\d{6}ET\d+\.pdf$", re.IGNORECASE)
_DEBIT_STATEMENT_FILENAME_RE = re.compile(r"\bdebit\b.*\bstatement\b", re.IGNORECASE)

_CONFIG_PATTERNS_CACHE: list[str] | None = None


def _norm_text(value: str) -> str:
    return re.sub(r"\s+", " ", value.lower()).strip()


def _norm_filename(value: str) -> str:
    lowered = value.lower()
    return re.sub(r"[^a-z0-9]+", " ", lowered).strip()


def _load_config_irrelevant_patterns() -> list[str]:
    global _CONFIG_PATTERNS_CACHE
    if _CONFIG_PATTERNS_CACHE is not None:
        return _CONFIG_PATTERNS_CACHE

    try:
        config = load_config()
        configured = config.get("parsers", {}).get("irrelevant_filename_patterns", [])
        patterns = [str(p).strip() for p in configured if str(p).strip()]
    except Exception:
        patterns = []

    merged: list[str] = []
    seen = set()
    for pattern in _DEFAULT_IRRELEVANT_FILENAME_PATTERNS + patterns:
        key = pattern.lower()
        if key not in seen:
            seen.add(key)
            merged.append(pattern)

    _CONFIG_PATTERNS_CACHE = merged
    return merged


def is_irrelevant_filename(filename: str, bank: str = "unknown") -> bool:
    """Return True when filename clearly indicates non-credit-card document."""
    if not filename:
        return False

    raw_lower = filename.lower()
    normalized_name = _norm_filename(filename)

    for pattern in _load_config_irrelevant_patterns():
        pat_lower = pattern.lower()
        pat_normalized = _norm_filename(pattern)
        if pat_lower in raw_lower or (pat_normalized and pat_normalized in normalized_name):
            return True

    bank_norm = (bank or "unknown").lower()

    if bank_norm == "icici":
        for pattern in _ICICI_SAVINGS_PATTERNS:
            if pattern.match(filename):
                return True

    if bank_norm == "hdfc" and _HDFC_EMI_PATTERN.match(filename):
        return True

    if _DEBIT_STATEMENT_FILENAME_RE.search(normalized_name):
        return True

    return False


def is_irrelevant_statement_text(text: str) -> bool:
    """Return True when extracted statement text indicates out-of-scope content."""
    if not text:
        return False

    normalized = _norm_text(text)
    for keyword in _DEFAULT_IRRELEVANT_TEXT_KEYWORDS:
        if keyword in normalized:
            return True

    if "debit card" in normalized and "statement" in normalized:
        return True

    if "savings account" in normalized and "statement" in normalized:
        return True

    return False


def extract_pdf_preview_text(*, pdf_path: str | Path | None = None,
                             pdf_bytes: bytes | None = None,
                             max_pages: int = 2) -> str:
    """Extract text from first pages of a PDF for lightweight scope checks."""
    if not pdf_path and not pdf_bytes:
        return ""

    try:
        import pdfplumber

        if pdf_bytes is not None:
            with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
                pages = pdf.pages[:max_pages]
                return "\n".join((page.extract_text() or "") for page in pages)

        with pdfplumber.open(str(pdf_path)) as pdf:
            pages = pdf.pages[:max_pages]
            return "\n".join((page.extract_text() or "") for page in pages)
    except Exception:
        return ""


def is_irrelevant_pdf_path(pdf_path: str | Path, bank: str = "unknown") -> bool:
    """Return True when PDF filename/content indicates out-of-scope document."""
    path = Path(pdf_path)

    if is_irrelevant_filename(path.name, bank):
        return True

    text = extract_pdf_preview_text(pdf_path=path, max_pages=2)
    return is_irrelevant_statement_text(text)
