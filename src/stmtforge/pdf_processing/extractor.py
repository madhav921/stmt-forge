"""PDF data extraction module - extracts transaction tables from credit card statements."""

import re
from datetime import datetime
from pathlib import Path

import pdfplumber
import pandas as pd

from stmtforge.utils.logging_config import get_logger

logger = get_logger("pdf.extractor")


class PDFExtractor:
    """Extract structured data from PDF credit card statements."""

    def __init__(self):
        pass

    def extract_text(self, pdf_path: str | Path) -> str:
        """Extract all text from a PDF."""
        pdf_path = Path(pdf_path)
        text = ""
        try:
            with pdfplumber.open(pdf_path) as pdf:
                for page in pdf.pages:
                    page_text = page.extract_text()
                    if page_text:
                        text += page_text + "\n"
        except Exception as e:
            logger.error(f"Error extracting text from {pdf_path.name}: {e}")
        return text

    def extract_tables(self, pdf_path: str | Path) -> list:
        """Extract all tables from a PDF using pdfplumber."""
        pdf_path = Path(pdf_path)
        all_tables = []
        try:
            with pdfplumber.open(pdf_path) as pdf:
                for i, page in enumerate(pdf.pages):
                    tables = page.extract_tables()
                    if tables:
                        for table in tables:
                            all_tables.append({
                                "page": i + 1,
                                "data": table,
                            })
        except Exception as e:
            logger.error(f"Error extracting tables from {pdf_path.name}: {e}")
        return all_tables

    def extract_text_by_page(self, pdf_path: str | Path) -> list:
        """Extract text from each page separately."""
        pdf_path = Path(pdf_path)
        pages = []
        try:
            with pdfplumber.open(pdf_path) as pdf:
                for i, page in enumerate(pdf.pages):
                    text = page.extract_text()
                    pages.append({
                        "page": i + 1,
                        "text": text or "",
                    })
        except Exception as e:
            logger.error(f"Error extracting pages from {pdf_path.name}: {e}")
        return pages

    def detect_card_last4(self, text: str) -> str | None:
        """Try to detect the last 4 digits of the credit card from statement text."""
        # Common patterns for card number in statements
        patterns = [
            r"card\s*(?:no|number|#)?[:\s]*(?:xxxx[\s-]*){2,3}(\d{4})",
            r"(?:xxxx[\s-]*){2,3}(\d{4})",
            r"card\s*ending\s*(?:in\s*)?(\d{4})",
            r"\*{4,}\s*(\d{4})",
            r"(\d{4})\s*$",  # Last 4 at end of card-related line
        ]
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                last4 = match.group(1)
                if last4.isdigit() and len(last4) == 4:
                    return last4
        return None

    def detect_statement_period(self, text: str) -> dict:
        """Try to detect the statement period from text."""
        patterns = [
            r"statement\s*(?:period|date|from)\s*[:\s]*(\d{1,2}[\s/-]\w+[\s/-]\d{2,4})\s*(?:to|-)\s*(\d{1,2}[\s/-]\w+[\s/-]\d{2,4})",
            r"(\d{1,2}[\s/-]\w{3,9}[\s/-]\d{2,4})\s*(?:to|-)\s*(\d{1,2}[\s/-]\w{3,9}[\s/-]\d{2,4})",
            r"billing\s*period\s*[:\s]*(\d{1,2}[\s/-]\w+[\s/-]\d{2,4})\s*(?:to|-)\s*(\d{1,2}[\s/-]\w+[\s/-]\d{2,4})",
        ]
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return {
                    "start": match.group(1).strip(),
                    "end": match.group(2).strip(),
                }
        return {}

    def detect_card_name(self, text: str, card_identifiers: dict,
                         bank: str | None = None) -> str | None:
        """Detect card name from PDF text using configured identifiers.

        When *bank* is provided, only patterns whose configured bank matches
        are considered.  This prevents a generic pattern (e.g. "cashback")
        intended for one bank from matching another bank's statement.
        """
        text_lower = text.lower()
        for pattern, info in card_identifiers.items():
            if isinstance(info, dict):
                pattern_bank = info.get("bank")
                card_name = info.get("card_name")
            else:
                pattern_bank = None
                card_name = info

            # Skip patterns that belong to a different bank
            if bank and pattern_bank and pattern_bank != bank:
                continue

            if pattern.lower() in text_lower:
                return card_name
        return None

    def detect_reward_points(self, text: str) -> float | None:
        """Extract reward points balance from statement text."""
        patterns = [
            r"reward\s*points?\s*(?:balance|earned|available|total)\s*[:\s]*([0-9,]+(?:\.\d+)?)",
            r"(?:total|available|earned|accumulated)\s*reward\s*points?\s*[:\s]*([0-9,]+(?:\.\d+)?)",
            r"points?\s*(?:balance|summary|earned)\s*[:\s]*([0-9,]+(?:\.\d+)?)",
            r"(?:cashback|rewards?)\s*earned\s*[:\s]*(?:Rs\.?\s*|₹\s*)?([0-9,]+(?:\.\d+)?)",
            r"reward\s*points?\s*[:\s]*([0-9,]+(?:\.\d+)?)",
        ]
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                val = match.group(1).replace(",", "")
                try:
                    return float(val)
                except ValueError:
                    continue
        return None
