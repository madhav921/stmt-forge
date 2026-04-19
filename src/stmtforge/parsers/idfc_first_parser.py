"""IDFC First Bank credit card statement parser.

Handles IDFC First CC statement formats:
- Single-line: DD Mon YY DESCRIPTION AMOUNT DR/CR
- Multi-line: description on preceding line(s), DD Mon YY [Convert] AMOUNT DR/CR
- "Convert" keyword (EMI conversion tag) stripped from descriptions
"""

import re
from pathlib import Path

import pandas as pd

from stmtforge.parsers.base_parser import BaseParser, parse_date, parse_amount
from stmtforge.utils.logging_config import get_logger

logger = get_logger("parsers.idfc_first")

# Lines matching these indicate we've left the transaction section
_NOISE_PREFIXES = (
    "Refer ", "Enjoy ", "EMIfy", "Flexible tenure", "Convert your IDFC",
    "Check Eligibility", "Apply now", "Know More", "friends and earn",
    "reward points", "your Bank Account", "transactions to easy",
    "Share your credit", "your loved ones", "Add-on Card",
    "#UPI now", "Avail Quick Cash", "Unlimited, evergreen",
)


class IDFCFirstParser(BaseParser):
    BANK_NAME = "idfc_first"

    # Full transaction: DD Mon YY DESCRIPTION AMOUNT DR/CR
    TRANSACTION_RE = re.compile(
        r"(\d{2}\s+[A-Za-z]{3}\s+\d{2,4})\s+"
        r"(.+?)\s+"
        r"(\d[\d,]*\.\d{2})\s*"
        r"(CR|DR)\s*$",
        re.IGNORECASE,
    )

    # Bare date+amount (no description): DD Mon YY AMOUNT DR/CR
    TRANSACTION_BARE_RE = re.compile(
        r"(\d{2}\s+[A-Za-z]{3}\s+\d{2,4})\s+"
        r"(\d[\d,]*\.\d{2})\s*"
        r"(CR|DR)\s*$",
        re.IGNORECASE,
    )

    # Detect lines starting with a date pattern
    DATE_START_RE = re.compile(r"^\d{2}\s+[A-Za-z]{3}\s+\d{2}")

    CARD_RE = re.compile(r"Card Number:\s*XXXX\s*(\d{4})")

    def parse(self, pdf_path: str | Path) -> pd.DataFrame:
        pdf_path = Path(pdf_path)
        logger.info(f"IDFC First parsing: {pdf_path.name}")

        full_text = self.extractor.extract_text(pdf_path)
        card_last4 = self.extractor.detect_card_last4(full_text)
        ref_year = self._detect_year(full_text)

        records = self._parse_text(full_text, card_last4, ref_year)

        df = self._get_standard_df(records)
        logger.info(f"IDFC First extracted {len(df)} transactions from {pdf_path.name}")
        return df

    @staticmethod
    def _clean_desc(desc: str) -> str:
        """Strip 'Convert' tag and collapse whitespace."""
        desc = re.sub(r"\bConvert\b", "", desc)
        return re.sub(r"\s+", " ", desc).strip()

    @staticmethod
    def _is_noise(line: str) -> bool:
        return any(line.startswith(p) for p in _NOISE_PREFIXES)

    def _parse_text(self, text: str, card_last4: str, ref_year: int) -> list:
        lines = text.split("\n")
        records: list[dict] = []
        pending_desc: list[str] = []
        current_card = card_last4
        in_debits = False
        in_credits = False

        for line in lines:
            line = line.strip()
            if not line:
                pending_desc = []
                continue

            # Track card number sections
            cm = self.CARD_RE.search(line)
            if cm:
                current_card = cm.group(1)
                pending_desc = []
                in_debits = False
                in_credits = False
                continue

            # Track debit/credit sections
            if "Purchases, EMIs & Other Debits" in line:
                in_debits, in_credits = True, False
                pending_desc = []
                continue
            if "Payments & Other Credits" in line:
                in_debits, in_credits = False, True
                pending_desc = []
                continue

            if not in_debits and not in_credits:
                continue

            # Skip promotional noise
            if self._is_noise(line):
                pending_desc = []
                in_debits, in_credits = False, False
                continue

            # Try full transaction match (date + description + amount)
            m = self.TRANSACTION_RE.search(line)
            if m:
                date_str, desc, amt_str, cr_dr = m.groups()
                desc = self._clean_desc(desc)

                if desc and len(desc) > 3:
                    full_desc = desc
                elif pending_desc:
                    full_desc = pending_desc[-1]
                    if desc:
                        full_desc = full_desc + " " + desc
                    full_desc = self._clean_desc(full_desc)
                else:
                    full_desc = desc or ""

                date = parse_date(date_str, ref_year)
                amount = parse_amount(amt_str)

                if date and amount and amount > 0 and len(full_desc) > 2:
                    txn_type = self._resolve_type(cr_dr, in_credits)
                    records.append({
                        "date": date, "description": full_desc,
                        "amount": amount, "type": txn_type,
                        "card_last4": current_card, "balance": None,
                    })

                pending_desc = []
                continue

            # Try bare date+amount match (no description on this line)
            m = self.TRANSACTION_BARE_RE.search(line)
            if m:
                date_str, amt_str, cr_dr = m.groups()
                full_desc = self._clean_desc(pending_desc[-1]) if pending_desc else ""

                date = parse_date(date_str, ref_year)
                amount = parse_amount(amt_str)

                if date and amount and amount > 0 and len(full_desc) > 2:
                    txn_type = self._resolve_type(cr_dr, in_credits)
                    records.append({
                        "date": date, "description": full_desc,
                        "amount": amount, "type": txn_type,
                        "card_last4": current_card, "balance": None,
                    })

                pending_desc = []
                continue

            # Non-transaction line: accumulate as potential description
            pending_desc.append(line)

        return records

    @staticmethod
    def _resolve_type(cr_dr: str | None, in_credits: bool) -> str:
        if cr_dr and cr_dr.upper() == "CR":
            return "credit"
        if cr_dr and cr_dr.upper() == "DR":
            return "debit"
        return "credit" if in_credits else "debit"

    def _detect_year(self, text: str) -> int:
        years = re.findall(r"20[12]\d", text)
        if years:
            from collections import Counter
            return int(Counter(years).most_common(1)[0][0])
        return 2024
