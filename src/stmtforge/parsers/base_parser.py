"""Base parser class and common utilities for bank statement parsing."""

import re
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path

import pandas as pd

from stmtforge.pdf_processing.extractor import PDFExtractor
from stmtforge.utils.logging_config import get_logger

logger = get_logger("parsers.base")

# Common date formats found in Indian bank statements
DATE_FORMATS = [
    "%d/%m/%Y",
    "%d-%m-%Y",
    "%d %b %Y",
    "%d %b %y",
    "%d/%m/%y",
    "%d-%m-%y",
    "%d %B %Y",
    "%d %B %y",
    "%d/%m",
    "%d-%m",
    "%d %b",
    "%m/%d/%Y",
    "%Y-%m-%d",
]


def parse_date(date_str: str, reference_year: int = None) -> str | None:
    """Parse a date string trying multiple formats. Returns YYYY-MM-DD or None."""
    if not date_str or not isinstance(date_str, str):
        return None

    date_str = date_str.strip()
    # Remove extra whitespace
    date_str = re.sub(r"\s+", " ", date_str)

    for fmt in DATE_FORMATS:
        try:
            dt = datetime.strptime(date_str, fmt)
            # If year is missing or 1900, use reference year
            if dt.year == 1900 and reference_year:
                dt = dt.replace(year=reference_year)
            # Handle 2-digit years
            if dt.year < 100:
                dt = dt.replace(year=dt.year + 2000)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue

    return None


def parse_amount(amount_str: str) -> float | None:
    """Parse an amount string to float, handling Indian formats."""
    if not amount_str or not isinstance(amount_str, str):
        return None

    amount_str = amount_str.strip()
    # Remove currency symbols and labels (preserve decimal point)
    amount_str = re.sub(r"Rs\.?\s*", "", amount_str, flags=re.IGNORECASE)
    amount_str = re.sub(r"INR\s*", "", amount_str, flags=re.IGNORECASE)
    amount_str = amount_str.replace("₹", "").replace("$", "")
    # Remove commas (thousand separators)
    amount_str = amount_str.replace(",", "").strip()
    # Remove Dr/Cr suffixes
    amount_str = re.sub(r"\s*(Dr|Cr|DR|CR)\.?\s*$", "", amount_str)
    # Handle parentheses for negative (credit) amounts
    if amount_str.startswith("(") and amount_str.endswith(")"):
        amount_str = "-" + amount_str[1:-1]
    # Handle trailing minus
    if amount_str.endswith("-"):
        amount_str = "-" + amount_str[:-1]

    try:
        return abs(float(amount_str))
    except (ValueError, TypeError):
        return None


def detect_debit_credit(amount_str: str, text_context: str = "") -> str:
    """Detect if a transaction is debit or credit."""
    if not amount_str:
        return "debit"

    amount_str_lower = str(amount_str).lower().strip()
    context_lower = text_context.lower() if text_context else ""

    # Check for explicit indicators
    credit_indicators = [
        "cr", "credit", "refund", "reversal", "cashback", "reward",
        "repayment", "payment received", "received", "thank you",
    ]
    debit_indicators = ["dr", "debit", "purchase", "payment"]

    for ind in credit_indicators:
        if ind in amount_str_lower or ind in context_lower:
            return "credit"

    for ind in debit_indicators:
        if ind in amount_str_lower:
            return "debit"

    # Negative amounts are usually credits
    clean = re.sub(r"[^\d.-]", "", amount_str)
    try:
        if float(clean) < 0:
            return "credit"
    except (ValueError, TypeError):
        pass

    return "debit"


class BaseParser(ABC):
    """Base class for bank-specific statement parsers."""

    BANK_NAME = "unknown"

    def __init__(self):
        self.extractor = PDFExtractor()

    @abstractmethod
    def parse(self, pdf_path: str | Path) -> pd.DataFrame:
        """
        Parse a credit card statement PDF and return a DataFrame with columns:
        - date (str: YYYY-MM-DD)
        - description (str)
        - amount (float)
        - type (str: 'debit' or 'credit')
        - card_last4 (str or None)
        - balance (float or None)
        """
        pass

    def _get_standard_df(self, records: list) -> pd.DataFrame:
        """Convert list of record dicts to standardized DataFrame."""
        if not records:
            return pd.DataFrame(columns=[
                "date", "description", "amount", "type", "card_name",
                "card_last4", "balance", "reward_points"
            ])

        df = pd.DataFrame(records)

        # Ensure required columns exist
        for col in ["date", "description", "amount", "type"]:
            if col not in df.columns:
                df[col] = None

        for col in ["card_name", "card_last4", "balance", "reward_points"]:
            if col not in df.columns:
                df[col] = None

        # Clean up
        df = df[df["amount"].notna() & (df["amount"] > 0)]
        df = df[df["date"].notna()]
        df = df[df["description"].notna() & (df["description"].str.strip() != "")]

        # Remove duplicate rows
        df = df.drop_duplicates(subset=["date", "description", "amount", "type"])

        return df[["date", "description", "amount", "type", "card_name",
                    "card_last4", "balance", "reward_points"]]
