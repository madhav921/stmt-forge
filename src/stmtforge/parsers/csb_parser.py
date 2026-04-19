"""CSB Bank / Jupiter / Edge credit card statement parser.

Handles:
- Jupiter format: DD/MM/YYYY or DD Mon YYYY DESCRIPTION AMOUNT Cr/Dr
- Edge format: DD Mon YYYY DESCRIPTION Rs. AMOUNT (no Cr/Dr, time on next line)
"""

import re
from pathlib import Path

import pandas as pd

from stmtforge.parsers.base_parser import BaseParser, parse_date, parse_amount, detect_debit_credit
from stmtforge.utils.logging_config import get_logger

logger = get_logger("parsers.csb")


class CSBParser(BaseParser):
    BANK_NAME = "csb"

    STATEMENT_PERIOD_RE = re.compile(
        r"^\d{2}\s+[A-Za-z]{3}\s+\d{4}\s*-\s*\d{2}\s+[A-Za-z]{3}\s+\d{4}$",
        re.IGNORECASE,
    )

    # Jupiter format: DD/MM/YYYY DESCRIPTION AMOUNT Cr/Dr
    TRANSACTION_RE = re.compile(
        r"(\d{2}[/-]\d{2}[/-]\d{2,4})\s+"
        r"(.+?)\s+"
        r"(\d[\d,]*(?:\.\d{2})?)\s*"
        r"(Cr|Dr)?\s*$",
        re.IGNORECASE,
    )

    # Jupiter format: DD Mon YYYY DESCRIPTION AMOUNT Cr/Dr
    TRANSACTION_RE2 = re.compile(
        r"(\d{2}\s+[A-Za-z]{3}\s+\d{2,4})\s+"
        r"(.+?)\s+"
        r"(\d[\d,]*(?:\.\d{2})?)\s*"
        r"(Cr|Dr)?\s*$",
        re.IGNORECASE,
    )

    # Edge format: DD Mon YYYY DESCRIPTION Rs. AMOUNT (no Cr/Dr)
    TRANSACTION_RE_EDGE = re.compile(
        r"(\d{2}\s+[A-Za-z]{3}\s+\d{4})\s+"   # Date: DD Mon YYYY
        r"(.+?)\s+"                              # Description (lazy)
        r"Rs\.\s*(\d[\d,]*(?:\.\d{2})?)\s*$",    # Rs. AMOUNT
        re.IGNORECASE,
    )

    TRANSACTION_RE_EDGE_SHORT_YEAR = re.compile(
        r"(\d{2}\s+[A-Za-z]{3}\s+\d{2})\s+"   # Date: DD Mon YY
        r"(.+?)\s+"
        r"Rs\.\s*(\d[\d,]*(?:\.\d{2})?)\s*$",
        re.IGNORECASE,
    )

    # Time-only lines to skip (Edge format puts time on next line)
    TIME_LINE_RE = re.compile(r"^\d{2}:\d{2}\s*(AM|PM)\s*$", re.IGNORECASE)

    @staticmethod
    def _clean_description(desc: str) -> str:
        """Normalize extracted description text and remove trailing currency token."""
        if not desc:
            return ""
        cleaned = re.sub(r"\s+", " ", desc).strip()
        cleaned = re.sub(r"\s+Rs\.?$", "", cleaned, flags=re.IGNORECASE).strip()
        return cleaned

    @staticmethod
    def _is_noise_description(desc: str) -> bool:
        """Reject non-merchant placeholders accidentally extracted from summary lines."""
        if not desc:
            return True
        normalized = desc.strip().lower().replace(" ", "")
        return normalized in {"rs", "rs.", "inr", "inr."}

    def parse(self, pdf_path: str | Path) -> pd.DataFrame:
        pdf_path = Path(pdf_path)
        logger.info(f"CSB/Jupiter/Edge parsing: {pdf_path.name}")

        full_text = self.extractor.extract_text(pdf_path)
        card_last4 = self.extractor.detect_card_last4(full_text)
        ref_year = self._detect_year(full_text)

        records = self._parse_tables(pdf_path, card_last4, ref_year)
        if not records:
            records = self._parse_text(full_text, card_last4, ref_year)

        df = self._get_standard_df(records)
        logger.info(f"CSB extracted {len(df)} transactions from {pdf_path.name}")
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
                description = self._clean_description(str(row[1] or ""))
                if not description or len(description) < 3 or self._is_noise_description(description):
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
        is_edge = bool(re.search(r"edge\s+csb", text, re.IGNORECASE))
        for line in text.split("\n"):
            line = line.strip()
            if not line:
                continue
            if self.STATEMENT_PERIOD_RE.match(line):
                continue
            # Skip time-only lines (Edge format)
            if self.TIME_LINE_RE.match(line):
                continue
            # Skip Edge end-of-transactions marker
            if "End of Transactions" in line:
                break
            for pattern in [
                self.TRANSACTION_RE_EDGE,
                self.TRANSACTION_RE_EDGE_SHORT_YEAR,
                self.TRANSACTION_RE,
                self.TRANSACTION_RE2,
            ]:
                match = pattern.search(line)
                if match:
                    groups = match.groups()
                    if pattern in (self.TRANSACTION_RE_EDGE, self.TRANSACTION_RE_EDGE_SHORT_YEAR):
                        date_str, desc, amt_str = groups
                        cr_dr = None
                    else:
                        date_str, desc, amt_str, cr_dr = groups
                    desc = self._clean_description(desc)
                    date = parse_date(date_str, ref_year)
                    amount = parse_amount(amt_str)
                    if date and amount and amount > 0 and len(desc.strip()) > 2 and not self._is_noise_description(desc):
                        if cr_dr:
                            txn_type = "credit" if cr_dr.lower() == "cr" else "debit"
                        else:
                            txn_type = detect_debit_credit(amt_str, desc)
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
