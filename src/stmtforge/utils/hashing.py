"""File hashing utilities for deduplication."""

import hashlib
from pathlib import Path


def file_hash(filepath: str | Path) -> str:
    """Compute SHA-256 hash of a file."""
    h = hashlib.sha256()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def content_hash(content: bytes) -> str:
    """Compute SHA-256 hash of bytes content."""
    return hashlib.sha256(content).hexdigest()
