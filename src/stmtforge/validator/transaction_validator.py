"""Post-LLM transaction validation and cleanup."""

import re
from datetime import datetime

from stmtforge.utils.logging_config import get_logger

logger = get_logger("validator")


class TransactionValidator:
    """Validate and clean extracted transactions."""

    # Reasonable amount bounds
    MAX_AMOUNT = 500_000
    MIN_AMOUNT = 0.01

    def validate(self, transactions: list[dict]) -> list[dict]:
        """
        Full validation pipeline:
        1. Normalize fields
        2. Remove invalid entries
        3. Deduplicate
        4. Sort by date
        Returns cleaned list with confidence scores.
        """
        if not transactions:
            return []

        cleaned = []
        for txn in transactions:
            normalized = self._normalize(txn)
            if normalized:
                cleaned.append(normalized)

        deduped = self._deduplicate(cleaned)
        deduped.sort(key=lambda t: t.get("_sort_date", "0000-00-00"))

        # Remove internal sort key
        for txn in deduped:
            txn.pop("_sort_date", None)

        logger.info(
            f"Validation: {len(transactions)} input → "
            f"{len(cleaned)} valid → {len(deduped)} deduped"
        )
        return deduped

    def _normalize(self, txn: dict) -> dict | None:
        """Normalize a single transaction. Returns None if invalid."""
        # --- Date ---
        date_raw = str(txn.get("date", "")).strip()
        date_normalized = self._normalize_date(date_raw)
        if not date_normalized:
            logger.debug(f"Dropping txn with invalid date: {date_raw}")
            return None

        # --- Description ---
        desc = str(txn.get("description", "")).strip()
        desc = re.sub(r'\s+', ' ', desc)
        if not desc or len(desc) < 2:
            desc = "UNKNOWN TRANSACTION"

        # --- Amount ---
        amount = self._normalize_amount(txn.get("amount"))
        if amount is None:
            logger.debug(f"Dropping txn with invalid amount: {txn.get('amount')}")
            return None

        if amount > self.MAX_AMOUNT:
            logger.debug(f"Dropping txn with excessive amount: {amount}")
            return None
        if amount < self.MIN_AMOUNT:
            logger.debug(f"Dropping txn with zero/negative amount: {amount}")
            return None

        # --- Type ---
        txn_type = str(txn.get("type", "")).strip().lower()
        if txn_type not in ("debit", "credit"):
            # Infer from context
            txn_type = "debit"  # Default assumption

        # --- Confidence ---
        confidence = txn.get("confidence", 1.0)
        try:
            confidence = float(confidence)
            confidence = max(0.0, min(1.0, confidence))
        except (TypeError, ValueError):
            confidence = 1.0

        return {
            "date": date_normalized,
            "description": desc,
            "amount": round(amount, 2),
            "type": txn_type,
            "confidence": confidence,
            "_sort_date": date_normalized,
        }

    def _normalize_date(self, date_str: str) -> str | None:
        """
        Parse various date formats and return YYYY-MM-DD.
        Returns None if unparseable.
        """
        if not date_str:
            return None

        # Clean up separators
        date_str = date_str.strip().replace("\\", "/")

        formats = [
            "%d/%m/%Y",
            "%d-%m-%Y",
            "%d %b %Y",
            "%d %B %Y",
            "%d/%m/%y",
            "%d-%m-%y",
            "%d %b %y",
            "%Y-%m-%d",
            "%m/%d/%Y",
            "%d.%m.%Y",
            "%d.%m.%y",
        ]

        for fmt in formats:
            try:
                dt = datetime.strptime(date_str, fmt)
                # Reject future dates or very old dates
                if dt.year < 2000 or dt > datetime.now():
                    continue
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                continue

        return None

    def _normalize_amount(self, amount) -> float | None:
        """Parse amount from various formats."""
        if amount is None:
            return None

        if isinstance(amount, (int, float)):
            return abs(float(amount))

        amount_str = str(amount).strip()
        # Remove currency symbols and commas
        amount_str = re.sub(r'[₹$€£,\s]', '', amount_str)
        amount_str = re.sub(r'^Rs\.?', '', amount_str, flags=re.IGNORECASE)
        amount_str = re.sub(r'^INR\s*', '', amount_str, flags=re.IGNORECASE)

        # Handle Cr/Dr suffixes
        amount_str = re.sub(r'\s*(Cr|Dr|CR|DR)\.?$', '', amount_str)

        # Handle negative sign
        amount_str = amount_str.replace('(', '-').replace(')', '')

        try:
            return abs(float(amount_str))
        except ValueError:
            return None

    def _deduplicate(self, transactions: list[dict]) -> list[dict]:
        """Remove duplicate transactions based on date + amount + description."""
        seen = set()
        unique = []

        for txn in transactions:
            # Normalize description for comparison
            desc_key = re.sub(r'\s+', '', txn["description"].lower())
            key = f"{txn['date']}|{txn['amount']:.2f}|{desc_key}"

            if key not in seen:
                seen.add(key)
                unique.append(txn)
            else:
                logger.debug(f"Removed duplicate: {txn['description']} {txn['amount']}")

        return unique
