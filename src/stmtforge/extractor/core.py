"""
Multi-stage PDF text extraction module.

Implements a three-stage fallback pipeline:
  Stage 1: pdfplumber table extraction
  Stage 2: pdftotext layout-preserving extraction
  Stage 3: OCR via pdf2image + pytesseract

Each stage returns an ExtractionResult with the raw text and metadata.
"""

import shutil
import subprocess
from dataclasses import dataclass, field
from pathlib import Path

import pdfplumber

from stmtforge.utils.logging_config import get_logger

logger = get_logger("extractor.core")


@dataclass
class ExtractionResult:
    """Result from a single extraction stage."""
    text: str
    method: str                       # "table", "layout", "ocr"
    page_texts: list[str] = field(default_factory=list)
    table_row_count: int = 0
    confidence: float = 0.0           # 0.0–1.0 heuristic quality score
    success: bool = False


class PDFTextExtractor:
    """Three-stage fallback text extraction from PDFs."""

    def __init__(self, config: dict = None):
        self.config = config or {}
        self.table_min_rows = self.config.get("table_min_rows", 10)
        # OCR is used as fallback whenever the packages are installed.
        # Set ocr_enabled: false in config to hard-disable it.
        config_ocr = self.config.get("ocr_enabled", None)
        if config_ocr is False:
            self.ocr_enabled = False
        else:
            # Auto-detect: enable iff pdf2image + pytesseract are importable
            try:
                import importlib
                importlib.import_module("pdf2image")
                importlib.import_module("pytesseract")
                self.ocr_enabled = True
            except ImportError:
                self.ocr_enabled = False
                if config_ocr is True:
                    logger.warning(
                        "ocr_enabled is set to true in config but OCR packages are not "
                        "installed. Install with: pip install stmtforge[ocr]"
                    )

    # ── Stage 1: Table Extraction ────────────────────────────────

    def extract_tables(self, pdf_path: str | Path) -> ExtractionResult:
        """Extract tables using pdfplumber. Prefer if row count > threshold."""
        pdf_path = Path(pdf_path)
        result = ExtractionResult(text="", method="table")

        try:
            all_text_lines = []
            total_rows = 0

            with pdfplumber.open(pdf_path) as pdf:
                for page in pdf.pages:
                    tables = page.extract_tables()
                    if tables:
                        for table in tables:
                            for row in table:
                                if row and any(cell for cell in row):
                                    line = "  ".join(
                                        str(cell).strip() if cell else ""
                                        for cell in row
                                    )
                                    all_text_lines.append(line)
                                    total_rows += 1

            result.table_row_count = total_rows
            result.text = "\n".join(all_text_lines)

            if total_rows >= self.table_min_rows:
                result.success = True
                result.confidence = min(1.0, total_rows / 50.0)
                logger.info(
                    f"Table extraction OK: {total_rows} rows from {pdf_path.name}"
                )
            else:
                logger.info(
                    f"Table extraction insufficient: {total_rows} rows "
                    f"(need {self.table_min_rows}) from {pdf_path.name}"
                )

        except Exception as e:
            logger.error(f"Table extraction failed for {pdf_path.name}: {e}")

        return result

    # ── Stage 2: Layout Text Extraction ──────────────────────────

    def extract_layout_text(self, pdf_path: str | Path) -> ExtractionResult:
        """
        Extract text preserving layout alignment.
        Uses pdftotext -layout if available, else pdfplumber fallback.
        """
        pdf_path = Path(pdf_path)
        result = ExtractionResult(text="", method="layout")

        # Try pdftotext -layout first (poppler-utils)
        if shutil.which("pdftotext"):
            try:
                proc = subprocess.run(
                    ["pdftotext", "-layout", str(pdf_path), "-"],
                    capture_output=True,
                    text=True,
                    timeout=60,
                )
                if proc.returncode == 0 and proc.stdout.strip():
                    result.text = proc.stdout
                    result.success = True
                    result.confidence = 0.7
                    logger.info(
                        f"Layout extraction (pdftotext) OK: "
                        f"{len(result.text)} chars from {pdf_path.name}"
                    )
                    return result
            except subprocess.TimeoutExpired:
                logger.warning(f"pdftotext timed out for {pdf_path.name}")
            except Exception as e:
                logger.warning(f"pdftotext failed for {pdf_path.name}: {e}")

        # Fallback: pdfplumber page-by-page text
        try:
            page_texts = []
            with pdfplumber.open(pdf_path) as pdf:
                for page in pdf.pages:
                    text = page.extract_text()
                    if text:
                        page_texts.append(text)

            result.text = "\n\n".join(page_texts)
            result.page_texts = page_texts

            if result.text.strip():
                result.success = True
                result.confidence = 0.6
                logger.info(
                    f"Layout extraction (pdfplumber) OK: "
                    f"{len(result.text)} chars from {pdf_path.name}"
                )
            else:
                logger.info(f"Layout extraction empty for {pdf_path.name}")

        except Exception as e:
            logger.error(f"Layout text extraction failed for {pdf_path.name}: {e}")

        return result

    # ── Stage 3: OCR Extraction ──────────────────────────────────

    def extract_ocr(self, pdf_path: str | Path) -> ExtractionResult:
        """Convert PDF pages to images and run OCR via pytesseract."""
        pdf_path = Path(pdf_path)
        result = ExtractionResult(text="", method="ocr")

        if not self.ocr_enabled:
            logger.debug("OCR disabled in config, skipping")
            return result

        try:
            from pdf2image import convert_from_path
            import pytesseract
        except ImportError:
            logger.warning(
                "OCR dependencies not installed (pdf2image, pytesseract). "
                "Install with: pip install pdf2image pytesseract"
            )
            return result

        try:
            images = convert_from_path(str(pdf_path), dpi=300)
            page_texts = []

            for i, img in enumerate(images):
                text = pytesseract.image_to_string(img, lang="eng")
                page_texts.append(text)
                logger.debug(f"OCR page {i+1}: {len(text)} chars")

            result.text = "\n\n".join(page_texts)
            result.page_texts = page_texts

            if result.text.strip():
                result.success = True
                result.confidence = 0.4
                logger.info(
                    f"OCR extraction OK: {len(result.text)} chars from {pdf_path.name}"
                )
            else:
                logger.info(f"OCR produced empty text for {pdf_path.name}")

        except Exception as e:
            logger.error(f"OCR extraction failed for {pdf_path.name}: {e}")

        return result

    # ── Orchestrator ─────────────────────────────────────────────

    def extract(self, pdf_path: str | Path) -> ExtractionResult:
        """
        Run all stages in order: table → layout → OCR.
        Returns the best extraction result.
        """
        pdf_path = Path(pdf_path)
        logger.info(f"Starting multi-stage extraction for {pdf_path.name}")

        # Stage 1
        table_result = self.extract_tables(pdf_path)
        if table_result.success:
            return table_result

        # Stage 2
        layout_result = self.extract_layout_text(pdf_path)
        if layout_result.success:
            return layout_result

        # Stage 3
        ocr_result = self.extract_ocr(pdf_path)
        if ocr_result.success:
            return ocr_result

        # Return best non-empty result we have, even if below threshold
        for r in [table_result, layout_result, ocr_result]:
            if r.text.strip():
                r.confidence = 0.1
                logger.warning(
                    f"Using low-confidence {r.method} result for {pdf_path.name}"
                )
                return r

        logger.error(f"All extraction stages failed for {pdf_path.name}")
        return ExtractionResult(text="", method="none", confidence=0.0)

    def extract_all_stages(self, pdf_path: str | Path) -> list[ExtractionResult]:
        """Run all stages and return all results (for retry logic)."""
        pdf_path = Path(pdf_path)
        results = []

        results.append(self.extract_tables(pdf_path))
        results.append(self.extract_layout_text(pdf_path))

        if self.ocr_enabled:
            results.append(self.extract_ocr(pdf_path))

        return results
