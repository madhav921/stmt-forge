"""Privacy-first logging utilities for DPDP-aligned telemetry."""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import re
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from stmtforge.utils.config import load_config, resolve_path
from stmtforge.utils.logging_config import get_logger

logger = get_logger("privacy_logging")


EMAIL_RE = re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b")
PHONE_RE = re.compile(r"(?<!\d)(?:\+91[-\s]?)?[6-9]\d{9}(?!\d)")
PAN_RE = re.compile(r"\b[A-Z]{5}\d{4}[A-Z]\b")
CARD_RE = re.compile(r"\b(?:\d[ -]*){12,19}\d\b")
UPI_RE = re.compile(r"\b[\w.\-]{2,}@[A-Za-z]{2,}\b")
AADHAAR_RE = re.compile(r"\b\d{4}[\s-]?\d{4}[\s-]?\d{4}\b")
IFSC_RE = re.compile(r"\b[A-Z]{4}0[A-Z0-9]{6}\b")

SENSITIVE_KEYS = {
    "raw_text",
    "llm_raw_output",
    "cleaned_json",
    "description",
    "sender",
    "email_subject",
    "message_id",
    "path",
    "original_path",
    "unlocked_path",
    "search_query",
}


def _salt() -> str:
    env_salt = os.getenv("STMTFORGE_LOG_SALT", "")
    if env_salt.strip():
        return env_salt.strip()
    cfg = load_config().get("privacy_logging", {})
    return str(cfg.get("default_salt", "stmtforge-dev-salt"))


def pseudonymize_value(value: str) -> str:
    """Stable pseudonymization using HMAC-SHA256 with secret salt."""
    if value is None:
        return ""
    key = _salt().encode("utf-8")
    digest = hmac.new(key, str(value).encode("utf-8"), hashlib.sha256).hexdigest()
    return digest[:16]


def redact_text(text: str) -> str:
    """Redact common sensitive patterns from free text logs."""
    if not text:
        return text

    output = EMAIL_RE.sub("[REDACTED_EMAIL]", text)
    output = PHONE_RE.sub("[REDACTED_PHONE]", output)
    output = PAN_RE.sub("[REDACTED_PAN]", output)
    output = UPI_RE.sub("[REDACTED_UPI]", output)

    def _mask_card(match: re.Match) -> str:
        value = match.group(0)
        digits = re.sub(r"\D", "", value)
        if len(digits) < 12:
            return value
        return f"[REDACTED_CARD_{digits[-4:]}]"

    output = CARD_RE.sub(_mask_card, output)
    return output


def sanitize_payload(payload: Any) -> Any:
    """Recursively sanitize payload for privacy-safe logging."""
    if isinstance(payload, dict):
        sanitized: dict[str, Any] = {}
        for key, value in payload.items():
            k = str(key)
            if k in SENSITIVE_KEYS:
                sanitized[k] = "[REDACTED]"
                continue

            if k in {"filename", "file_name", "card_last4", "session_id", "user_id"}:
                sanitized[f"{k}_hash"] = pseudonymize_value(str(value))
                continue

            sanitized[k] = sanitize_payload(value)
        return sanitized

    if isinstance(payload, list):
        return [sanitize_payload(x) for x in payload]

    if isinstance(payload, str):
        return redact_text(payload)

    return payload


class PrivacyEventLogger:
    """Append-only JSONL event logger for server/client telemetry."""

    def __init__(self, channel: str = "server"):
        if channel not in {"server", "client"}:
            raise ValueError("channel must be 'server' or 'client'")

        self.channel = channel
        cfg = load_config().get("privacy_logging", {})
        self.enabled = bool(cfg.get("enabled", True))
        self.retention_days = int(cfg.get("retention_days", 30))

        processed_path = resolve_path(load_config()["data"].get("processed", "data/processed"))
        self.base_dir = processed_path.parent / "logs" / "events"
        self.base_dir.mkdir(parents=True, exist_ok=True)

    def log_event(self, event_type: str, payload: dict | None = None,
                  source: str = "system", severity: str = "info") -> None:
        """Write one sanitized event to JSONL."""
        if not self.enabled:
            return

        now = datetime.now()
        data = {
            "event_id": uuid.uuid4().hex,
            "ts": now.isoformat(timespec="seconds"),
            "channel": self.channel,
            "event_type": event_type,
            "source": source,
            "severity": severity,
            "payload": sanitize_payload(payload or {}),
        }

        out_path = self.base_dir / f"{self.channel}_{now.strftime('%Y%m%d')}.jsonl"

        try:
            with open(out_path, "a", encoding="utf-8") as fh:
                fh.write(json.dumps(data, ensure_ascii=False) + "\n")
            self._cleanup_old_files()
        except Exception as exc:
            logger.warning(f"Privacy event logging failed: {exc}")

    def _cleanup_old_files(self) -> None:
        cutoff = datetime.now() - timedelta(days=self.retention_days)
        for file in self.base_dir.glob(f"{self.channel}_*.jsonl"):
            try:
                if datetime.fromtimestamp(file.stat().st_mtime) < cutoff:
                    file.unlink(missing_ok=True)
            except Exception:
                continue
