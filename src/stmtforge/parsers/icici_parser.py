"""ICICI Bank credit card statement parser."""

import re
from pathlib import Path

import pandas as pd

from stmtforge.parsers.base_parser import BaseParser, parse_date, parse_amount, detect_debit_credit
from stmtforge.utils.logging_config import get_logger

logger = get_logger("parsers.icici")


class ICICIParser(BaseParser):
    BANK_NAME = "icici"

    # ICICI patterns: typically DD/MM/YYYY or DD Mon YYYY
    TRANSACTION_RE = re.compile(
        r"(\d{2}/\d{2}/\d{4})\s+"
        r"(\d{2}/\d{2}/\d{4})\s+"   # ICICI often has 2 dates (transaction + posting)
        r"(.+?)\s+"
        r"(\d[\d,]*\.\d{2})\s*"
        r"(Cr|Dr)?\s*$",
        re.IGNORECASE,
    )

    TRANSACTION_RE_SINGLE = re.compile(
        r"(\d{2}/\d{2}/\d{4})\s+"
        r"(.+?)\s+"
        r"(\d[\d,]*\.\d{2})\s*"
        r"(Cr|Dr)?\s*$",
        re.IGNORECASE,
    )

    def parse(self, pdf_path: str | Path) -> pd.DataFrame:
        pdf_path = Path(pdf_path)
        logger.info(f"ICICI parsing: {pdf_path.name}")

        full_text = self.extractor.extract_text(pdf_path)
        card_last4 = self.extractor.detect_card_last4(full_text)
        ref_year = self._detect_year(full_text)

        records = self._parse_tables(pdf_path, card_last4, ref_year)
        if not records:
            records = self._parse_text(full_text, card_last4, ref_year)

        df = self._get_standard_df(records)
        logger.info(f"ICICI extracted {len(df)} transactions from {pdf_path.name}")
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

                # Try first cell as date
                date = parse_date(str(row[0] or ""), ref_year)
                if not date:
                    continue

                # Description could be in column 1 or 2 (if two date columns)
                desc_idx = 1
                if len(row) > 3 and parse_date(str(row[1] or ""), ref_year):
                    desc_idx = 2  # Skip second date column

                description = str(row[desc_idx] or "").strip() if desc_idx < len(row) else ""
                if not description or len(description) < 3:
                    continue

                # Find amount in remaining columns
                amount = None
                txn_type = "debit"
                for col_idx in range(desc_idx + 1, min(len(row), 6)):
                    cell = str(row[col_idx] or "")
                    amt = parse_amount(cell)
                    if amt and amt > 0:
                        amount = amt
                        txn_type = detect_debit_credit(cell, description)
                        break

                if amount:
                    records.append({
                        "date": date,
                        "description": description,
                        "amount": amount,
                        "type": txn_type,
                        "card_last4": card_last4,
                        "balance": None,
                    })

        return records

    def _parse_text(self, text: str, card_last4: str, ref_year: int) -> list:
        records = []
        lines = text.split("\n")

        for line in lines:
            line = line.strip()
            if not line:
                continue

            # Try dual-date pattern first
            match = self.TRANSACTION_RE.search(line)
            if match:
                date_str, _, description, amount_str, cr_dr = match.groups()
                date = parse_date(date_str, ref_year)
                amount = parse_amount(amount_str)
                if date and amount and amount > 0 and len(description.strip()) > 2:
                    txn_type = "credit" if cr_dr and cr_dr.lower() == "cr" else "debit"
                    records.append({
                        "date": date,
                        "description": description.strip(),
                        "amount": amount,
                        "type": txn_type,
                        "card_last4": card_last4,
                        "balance": None,
                    })
                continue

            # Try single-date pattern
            match = self.TRANSACTION_RE_SINGLE.search(line)
            if match:
                date_str, description, amount_str, cr_dr = match.groups()
                date = parse_date(date_str, ref_year)
                amount = parse_amount(amount_str)
                if date and amount and amount > 0 and len(description.strip()) > 2:
                    txn_type = "credit" if cr_dr and cr_dr.lower() == "cr" else "debit"
                    records.append({
                        "date": date,
                        "description": description.strip(),
                        "amount": amount,
                        "type": txn_type,
                        "card_last4": card_last4,
                        "balance": None,
                    })

        return records

    def _detect_year(self, text: str) -> int:
        years = re.findall(r"20[12]\d", text)
        if years:
            from collections import Counter
            return int(Counter(years).most_common(1)[0][0])
        return 2024
