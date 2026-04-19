"""Generic parser that works across multiple bank formats using heuristic extraction."""

import re
from pathlib import Path

import pandas as pd

from stmtforge.parsers.base_parser import BaseParser, parse_date, parse_amount, detect_debit_credit
from stmtforge.utils.logging_config import get_logger

logger = get_logger("parsers.generic")


class GenericParser(BaseParser):
    """
    A generic parser that uses heuristics to extract transactions from any
    credit card statement PDF. Works as a fallback when bank-specific parsers
    are not available.
    """

    BANK_NAME = "generic"

    # Regex patterns for transaction lines
    # Pattern: Date Description Amount [Cr/Dr]
    TRANSACTION_PATTERNS = [
        # DD/MM/YYYY or DD-MM-YYYY  Description  Amount
        re.compile(
            r"(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})\s+"  # date
            r"(.+?)\s+"                                # description
            r"(\d[\d,]*\.?\d{0,2})\s*"                # amount
            r"(Cr|Dr|CR|DR)?",                         # optional Cr/Dr
            re.IGNORECASE,
        ),
        # DD Mon YYYY  Description  Amount
        re.compile(
            r"(\d{1,2}\s+[A-Za-z]{3,9}\s+\d{2,4})\s+"  # date
            r"(.+?)\s+"                                    # description
            r"(\d[\d,]*\.?\d{0,2})\s*"                    # amount
            r"(Cr|Dr|CR|DR)?",                             # optional Cr/Dr
            re.IGNORECASE,
        ),
        # DD Mon  Description  Amount (no year)
        re.compile(
            r"(\d{1,2}\s+[A-Za-z]{3,9})\s+"  # date (no year)
            r"(.+?)\s+"                        # description
            r"(\d[\d,]*\.?\d{0,2})\s*"        # amount
            r"(Cr|Dr|CR|DR)?",                 # optional Cr/Dr
            re.IGNORECASE,
        ),
    ]

    # Lines to skip (headers, footers, summaries)
    SKIP_PATTERNS = [
        re.compile(r"^\s*$"),
        re.compile(r"statement|page\s+\d|total|opening|closing|minimum|due\s+date|credit\s+limit|available|payment\s+received", re.IGNORECASE),
        re.compile(r"customer\s+care|toll\s+free|helpline|grievance|registered\s+office", re.IGNORECASE),
        re.compile(r"^\s*date\s+.*description\s+.*amount", re.IGNORECASE),  # Table headers
    ]

    def parse(self, pdf_path: str | Path) -> pd.DataFrame:
        pdf_path = Path(pdf_path)
        logger.info(f"Generic parsing: {pdf_path.name}")

        # Strategy 1: Try table extraction first
        df = self._parse_from_tables(pdf_path)
        if df is not None and len(df) > 0:
            logger.info(f"Extracted {len(df)} transactions via table extraction")
            return df

        # Strategy 2: Line-by-line regex parsing
        df = self._parse_from_text(pdf_path)
        if df is not None and len(df) > 0:
            logger.info(f"Extracted {len(df)} transactions via text parsing")
            return df

        logger.warning(f"No transactions extracted from {pdf_path.name}")
        return pd.DataFrame(columns=["date", "description", "amount", "type",
                                      "card_name", "card_last4", "balance", "reward_points"])

    def _parse_from_tables(self, pdf_path: Path) -> pd.DataFrame | None:
        """Try to extract transactions from PDF tables."""
        tables = self.extractor.extract_tables(pdf_path)
        if not tables:
            return None

        full_text = self.extractor.extract_text(pdf_path)
        card_last4 = self.extractor.detect_card_last4(full_text)

        # Determine reference year from statement
        ref_year = self._detect_year(full_text)

        records = []
        for table_info in tables:
            table = table_info["data"]
            if not table or len(table) < 2:
                continue

            # Try to identify columns
            header = table[0] if table[0] else []
            col_map = self._identify_columns(header)

            for row in table[1:]:
                if not row:
                    continue

                record = self._extract_from_row(row, col_map, ref_year, card_last4)
                if record:
                    records.append(record)

        return self._get_standard_df(records) if records else None

    def _parse_from_text(self, pdf_path: Path) -> pd.DataFrame | None:
        """Parse transactions from raw text using regex."""
        full_text = self.extractor.extract_text(pdf_path)
        if not full_text:
            return None

        card_last4 = self.extractor.detect_card_last4(full_text)
        ref_year = self._detect_year(full_text)

        records = []
        lines = full_text.split("\n")

        for line in lines:
            line = line.strip()
            if not line:
                continue

            # Skip non-transaction lines
            if any(pat.search(line) for pat in self.SKIP_PATTERNS):
                continue

            record = self._parse_line(line, ref_year, card_last4)
            if record:
                records.append(record)

        return self._get_standard_df(records) if records else None

    def _parse_line(self, line: str, ref_year: int, card_last4: str) -> dict | None:
        """Try to parse a single line as a transaction."""
        for pattern in self.TRANSACTION_PATTERNS:
            match = pattern.search(line)
            if match:
                groups = match.groups()
                date_str = groups[0].strip()
                description = groups[1].strip()
                amount_str = groups[2].strip()
                cr_dr = groups[3].strip() if len(groups) > 3 and groups[3] else ""

                date = parse_date(date_str, ref_year)
                amount = parse_amount(amount_str)

                if date and amount and amount > 0 and len(description) > 2:
                    txn_type = detect_debit_credit(cr_dr, description)
                    return {
                        "date": date,
                        "description": description,
                        "amount": amount,
                        "type": txn_type,
                        "card_last4": card_last4,
                        "balance": None,
                    }
        return None

    def _identify_columns(self, header: list) -> dict:
        """Try to identify column positions from header row."""
        col_map = {"date": None, "description": None, "amount": None, "cr_dr": None, "balance": None}

        if not header:
            return col_map

        for i, cell in enumerate(header):
            if not cell:
                continue
            cell_lower = str(cell).lower().strip()

            if any(kw in cell_lower for kw in ["date", "txn", "transaction"]):
                if col_map["date"] is None:
                    col_map["date"] = i
            elif any(kw in cell_lower for kw in ["description", "particulars", "details", "narration", "merchant"]):
                col_map["description"] = i
            elif any(kw in cell_lower for kw in ["amount", "debit", "spend"]):
                if col_map["amount"] is None:
                    col_map["amount"] = i
            elif any(kw in cell_lower for kw in ["credit", "cr"]):
                col_map["cr_dr"] = i
            elif "balance" in cell_lower:
                col_map["balance"] = i

        return col_map

    def _extract_from_row(self, row: list, col_map: dict, ref_year: int, card_last4: str) -> dict | None:
        """Extract a transaction record from a table row."""
        if not row or all(not cell for cell in row):
            return None

        date = None
        description = None
        amount = None
        txn_type = "debit"
        balance = None

        # Use column map if available
        if col_map.get("date") is not None and col_map["date"] < len(row):
            date = parse_date(str(row[col_map["date"]] or ""), ref_year)
        if col_map.get("description") is not None and col_map["description"] < len(row):
            description = str(row[col_map["description"]] or "").strip()
        if col_map.get("amount") is not None and col_map["amount"] < len(row):
            amount = parse_amount(str(row[col_map["amount"]] or ""))
        if col_map.get("balance") is not None and col_map["balance"] < len(row):
            balance = parse_amount(str(row[col_map["balance"]] or ""))

        # If column map didn't work, try heuristic extraction
        if date is None:
            for cell in row:
                d = parse_date(str(cell or ""), ref_year)
                if d:
                    date = d
                    break

        if description is None:
            # Use the longest text cell as description
            text_cells = [(i, str(cell or "")) for i, cell in enumerate(row)
                          if cell and not parse_date(str(cell), ref_year)
                          and parse_amount(str(cell)) is None]
            if text_cells:
                description = max(text_cells, key=lambda x: len(x[1]))[1].strip()

        if amount is None:
            # Use last numeric cell as amount
            for cell in reversed(row):
                a = parse_amount(str(cell or ""))
                if a and a > 0:
                    amount = a
                    # Check for Cr/Dr indicator
                    txn_type = detect_debit_credit(str(cell or ""), description or "")
                    break

        if date and description and amount and amount > 0 and len(description) > 2:
            return {
                "date": date,
                "description": description,
                "amount": amount,
                "type": txn_type,
                "card_last4": card_last4,
                "balance": balance,
            }

        return None

    def _detect_year(self, text: str) -> int:
        """Detect the most likely year from the statement text."""
        years = re.findall(r"20[12]\d", text)
        if years:
            from collections import Counter
            most_common = Counter(years).most_common(1)[0][0]
            return int(most_common)
        return 2024
