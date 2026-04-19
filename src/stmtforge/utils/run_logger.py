"""Structured per-run activity log for StmtForge."""

import json
import uuid
from datetime import datetime
from pathlib import Path

from stmtforge.utils.config import load_config, resolve_path
from stmtforge.utils.logging_config import get_logger
from stmtforge.utils.privacy_logging import pseudonymize_value, sanitize_payload

logger = get_logger("run_logger")


class RunLogger:
    """Accumulates structured run-level events and persists them as JSON."""

    def __init__(self, mode: dict = None):
        self._run_id = datetime.now().strftime("%Y%m%d_%H%M%S") + "_" + uuid.uuid4().hex[:6]
        self._started_at = datetime.now().isoformat(timespec="seconds")
        self._completed_at: str | None = None
        self._mode = mode or {}
        self._steps: dict = {
            "gmail_fetch": None,
            "organize":    None,
            "backfill":    None,
            "cleanup":     None,
            "parse":       None,
        }
        self._summary: dict = {}

    def log_gmail_fetch(self, skipped: bool, emails_scanned: int = 0,
                        downloaded: list = None):
        downloaded = downloaded or []
        by_bank: dict[str, int] = {}
        for item in downloaded:
            bank = item.get("bank", "unknown")
            by_bank[bank] = by_bank.get(bank, 0) + 1

        self._steps["gmail_fetch"] = {
            "skipped": skipped,
            "emails_scanned": emails_scanned,
            "new_pdfs_downloaded": len(downloaded),
            "by_bank": by_bank,
        }

    def log_organize(self, result: dict):
        self._steps["organize"] = {
            "moved_raw":      result.get("moved_raw", 0),
            "moved_unlocked": result.get("moved_unlocked", 0),
        }

    def log_backfill(self, result: dict):
        self._steps["backfill"] = {
            "transactions_updated": result.get("transactions", 0),
            "statements_updated":   result.get("statements", 0),
        }

    def log_cleanup(self, irrelevant: dict, csb: dict = None):
        self._steps["cleanup"] = {
            "statements_marked_irrelevant": irrelevant.get("statements_marked", 0),
            "transactions_deleted":         irrelevant.get("transactions_deleted", 0),
            "csb_credits_fixed":            (csb or {}).get("promoted_to_credit", 0),
            "csb_noise_deleted":            (csb or {}).get("deleted_noise", 0),
        }

    def log_statement(self, *, filename: str, bank: str, card_name: str = None,
                      status: str, method: str = None, confidence: float = None,
                      txn_count: int = 0, new_inserted: int = 0,
                      df=None):
        total_debit = 0.0
        total_credit = 0.0
        if df is not None and not df.empty and "amount" in df.columns and "type" in df.columns:
            total_debit  = float(df.loc[df["type"] == "debit",  "amount"].sum())
            total_credit = float(df.loc[df["type"] == "credit", "amount"].sum())

        entry = {
            "filename_hash": pseudonymize_value(filename or ""),
            "file_ext":    Path(filename).suffix.lower() if filename else "",
            "bank":        bank,
            "card_name":   card_name,
            "status":      status,
            "method":      method,
            "confidence":  round(confidence, 3) if confidence is not None else None,
            "txn_count":   txn_count,
            "new_inserted": new_inserted,
            "total_debit":  round(total_debit, 2),
            "total_credit": round(total_credit, 2),
            "net_spend":    round(total_debit - total_credit, 2),
        }

        if self._steps["parse"] is None:
            self._steps["parse"] = {
                "total_discovered":        0,
                "total_processed":         0,
                "total_skipped":           0,
                "total_already_processed": 0,
                "new_transactions_inserted": 0,
                "statements":              [],
            }

        step = self._steps["parse"]
        step["statements"].append(entry)

        if status == "skipped":
            step["total_skipped"] += 1
        elif status == "already_processed":
            step["total_already_processed"] += 1
        else:
            step["total_processed"] += 1
            step["new_transactions_inserted"] += new_inserted

    def log_discovered(self, count: int):
        if self._steps["parse"] is None:
            self._steps["parse"] = {
                "total_discovered":        0,
                "total_processed":         0,
                "total_skipped":           0,
                "total_already_processed": 0,
                "new_transactions_inserted": 0,
                "statements":              [],
            }
        self._steps["parse"]["total_discovered"] = count

    def log_summary(self, db_summary: dict, new_transactions: int):
        self._summary = {
            "total_new_transactions":  new_transactions,
            "total_db_transactions":   db_summary.get("total_transactions", 0),
            "total_spend_db":          round(db_summary.get("total_spend", 0.0), 2),
            "banks_active":            db_summary.get("banks", []),
            "date_range":              db_summary.get("date_range", {}),
        }

    def finish(self):
        self._completed_at = datetime.now().isoformat(timespec="seconds")
        payload = self._build_payload()
        self._write(payload)
        return payload

    def _build_payload(self) -> dict:
        payload = {
            "run_id":       self._run_id,
            "started_at":   self._started_at,
            "completed_at": self._completed_at,
            "mode":         self._mode,
            "data_protection": {
                "policy": "DPDP-2023",
                "personal_data": "minimized",
                "identifiers": "pseudonymized",
            },
            "steps":        self._steps,
            "summary":      self._summary,
        }
        return sanitize_payload(payload)

    def _write(self, payload: dict):
        try:
            config = load_config()
            processed_path = resolve_path(config["data"].get("processed", "data/processed"))
            logs_dir = processed_path.parent / "logs"
            logs_dir.mkdir(parents=True, exist_ok=True)

            out_path = logs_dir / f"run_{self._run_id}.json"
            with open(out_path, "w", encoding="utf-8") as fh:
                json.dump(payload, fh, indent=2, default=str, ensure_ascii=False)

            logger.info(f"Run log written: {out_path}")
        except Exception as exc:
            logger.warning(f"Failed to write run log: {exc}")
