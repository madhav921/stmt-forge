"""Federal Bank / Scapia credit card statement parser.

Handles Scapia format: DD-MM-YYYY DESCRIPTION CCC AMOUNT Dr/Cr
where CCC is a 2-3 digit card category code (e.g. '356') that gets stripped.
"""

import re
from pathlib import Path

import pandas as pd

from stmtforge.parsers.base_parser import BaseParser, parse_date, parse_amount, detect_debit_credit
from stmtforge.utils.logging_config import get_logger

logger = get_logger("parsers.federal")


class FederalParser(BaseParser):
    BANK_NAME = "federal"

    # Scapia format: DD-MM-YYYY DESC CARD_CODE AMOUNT Dr/Cr
    # Card code is 2-3 digits between description and amount
    TRANSACTION_RE_SCAPIA = re.compile(
        r"(\d{2}-\d{2}-\d{4})\s+"       # Date: DD-MM-YYYY
        r"(.+?)\s+"                       # Description (lazy)
        r"\d{2,3}\s+"                     # Card category code (2-3 digits, discarded)
        r"(\d[\d,]*\.\d{2})\s*"          # Amount
        r"(Cr|Dr)?\s*$",                  # Optional Cr/Dr
        re.IGNORECASE,
    )

    # Generic fallback: DD/MM/YYYY or DD-MM-YYYY DESC AMOUNT Cr/Dr
    TRANSACTION_RE = re.compile(
        r"(\d{2}[/-]\d{2}[/-]\d{2,4})\s+"
        r"(.+?)\s+"
        r"(\d[\d,]*\.\d{2})\s*"
        r"(Cr|Dr)?\s*$",
        re.IGNORECASE,
    )

    TRANSACTION_RE2 = re.compile(
        r"(\d{2}\s+[A-Za-z]{3}\s+\d{2,4})\s+"
        r"(.+?)\s+"
        r"(\d[\d,]*\.\d{2})\s*"
        r"(Cr|Dr)?\s*$",
        re.IGNORECASE,
    )

    def parse(self, pdf_path: str | Path) -> pd.DataFrame:
        pdf_path = Path(pdf_path)
        logger.info(f"Federal/Scapia parsing: {pdf_path.name}")

        full_text = self.extractor.extract_text(pdf_path)
        card_last4 = self.extractor.detect_card_last4(full_text)
        ref_year = self._detect_year(full_text)

        records = self._parse_text(full_text, card_last4, ref_year)
        if not records:
            records = self._parse_tables(pdf_path, card_last4, ref_year)

        df = self._get_standard_df(records)
        logger.info(f"Federal extracted {len(df)} transactions from {pdf_path.name}")
        return df

    def _parse_tables(self, pdf_path: Path, card_last4: str, ref_year: int) -> list:
        tables = self.extractor.extract_tables(pdf_path)
        records = []
        for table_info in tables:
            table = table_info["data"]
            if not table or len(table) < 2:
                continue
            for row in table[1:]:
                if not row or len(row) < 3:
                    continue
                date = parse_date(str(row[0] or ""), ref_year)
                if not date:
                    continue
                description = str(row[1] or "").strip()
                if not description or len(description) < 3:
                    continue
                amount = None
                txn_type = "debit"
                for col_idx in range(2, min(len(row), 5)):
                    cell = str(row[col_idx] or "")
                    amt = parse_amount(cell)
                    if amt and amt > 0:
                        amount = amt
                        txn_type = detect_debit_credit(cell, description)
                        break
                if amount:
                    records.append({
                        "date": date, "description": description,
                        "amount": amount, "type": txn_type,
                        "card_last4": card_last4, "balance": None,
                    })
        return records

    def _parse_text(self, text: str, card_last4: str, ref_year: int) -> list:
        records = []
        for line in text.split("\n"):
            line = line.strip()
            if not line:
                continue
            # Try Scapia-specific pattern first (strips card code)
            for pattern in [self.TRANSACTION_RE_SCAPIA, self.TRANSACTION_RE, self.TRANSACTION_RE2]:
                match = pattern.search(line)
                if match:
                    date_str, desc, amt_str, cr_dr = match.groups()
                    # Clean trailing card code digits from description (fallback patterns)
                    desc = re.sub(r"\s+\d{2,3}$", "", desc).strip()
                    date = parse_date(date_str, ref_year)
                    amount = parse_amount(amt_str)
                    if date and amount and amount > 0 and len(desc.strip()) > 2:
                        txn_type = "credit" if cr_dr and cr_dr.lower() == "cr" else "debit"
                        records.append({
                            "date": date, "description": desc.strip(),
                            "amount": amount, "type": txn_type,
                            "card_last4": card_last4, "balance": None,
                        })
                    break
        return records

    def _detect_year(self, text: str) -> int:
        years = re.findall(r"20[12]\d", text)
        if years:
            from collections import Counter
            return int(Counter(years).most_common(1)[0][0])
        return 2024
