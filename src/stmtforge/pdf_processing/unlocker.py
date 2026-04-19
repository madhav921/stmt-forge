"""PDF password removal module using pikepdf with qpdf fallback."""

import os
import shutil
import subprocess
from pathlib import Path

import pikepdf

from stmtforge.utils.config import load_config, get_all_passwords, resolve_path
from stmtforge.utils.logging_config import get_logger

logger = get_logger("pdf.unlocker")


class PDFUnlocker:
    """Removes PDF passwords using multiple strategies."""

    def __init__(self):
        self.config = load_config()
        self.passwords = get_all_passwords(self.config)
        self.output_dir = resolve_path(self.config["data"]["unlocked_pdfs"])
        self.output_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Loaded {len(self.passwords)} password candidates")

    def unlock(self, pdf_path: str | Path) -> Path | None:
        """
        Attempt to unlock a PDF file.
        Returns path to unlocked PDF, or original path if not encrypted.
        Returns None if unlock fails.
        """
        pdf_path = Path(pdf_path)
        if not pdf_path.exists():
            logger.error(f"PDF not found: {pdf_path}")
            return None

        # Check if already unencrypted
        if self._is_unencrypted(pdf_path):
            logger.debug(f"PDF is not encrypted: {pdf_path.name}")
            return self._copy_to_output(pdf_path)

        # Try pikepdf with each password
        result = self._try_pikepdf(pdf_path)
        if result:
            return result

        # Fallback: try qpdf
        result = self._try_qpdf(pdf_path)
        if result:
            return result

        logger.warning(f"Failed to unlock: {pdf_path.name} (tried {len(self.passwords)} passwords)")
        return None

    def _is_unencrypted(self, pdf_path: Path) -> bool:
        """Check if PDF is not encrypted."""
        try:
            with pikepdf.open(pdf_path) as pdf:
                return True
        except pikepdf.PasswordError:
            return False
        except Exception as e:
            logger.debug(f"Error checking encryption for {pdf_path.name}: {e}")
            return False

    def _get_output_path(self, pdf_path: Path) -> Path:
        """Generate output path preserving relative structure."""
        # Try to preserve bank/year_month structure
        parts = pdf_path.parts
        try:
            raw_idx = next(i for i, p in enumerate(parts) if p == "raw_pdfs")
            relative = Path(*parts[raw_idx + 1:])
        except StopIteration:
            relative = Path(pdf_path.name)

        output_path = self.output_dir / relative
        output_path.parent.mkdir(parents=True, exist_ok=True)
        return output_path

    def _copy_to_output(self, pdf_path: Path) -> Path:
        """Copy an unencrypted PDF to the output directory."""
        output_path = self._get_output_path(pdf_path)
        if not output_path.exists():
            shutil.copy2(pdf_path, output_path)
        return output_path

    def _try_pikepdf(self, pdf_path: Path) -> Path | None:
        """Try to unlock PDF using pikepdf."""
        output_path = self._get_output_path(pdf_path)

        for password in self.passwords:
            try:
                with pikepdf.open(pdf_path, password=password) as pdf:
                    pdf.save(output_path)
                    logger.info(f"Unlocked with pikepdf: {pdf_path.name}")
                    return output_path
            except pikepdf.PasswordError:
                continue
            except Exception as e:
                logger.debug(f"pikepdf error with password attempt on {pdf_path.name}: {e}")
                continue

        return None

    def _try_qpdf(self, pdf_path: Path) -> Path | None:
        """Try to unlock PDF using qpdf as fallback."""
        if not shutil.which("qpdf"):
            logger.debug("qpdf not available, skipping fallback")
            return None

        output_path = self._get_output_path(pdf_path)

        for password in self.passwords:
            try:
                cmd = [
                    "qpdf",
                    f"--password={password}",
                    "--decrypt",
                    str(pdf_path),
                    str(output_path),
                ]
                env = os.environ.copy()
                result = subprocess.run(
                    cmd,
                    capture_output=True,
                    text=True,
                    timeout=30,
                    env=env,
                )
                if result.returncode == 0:
                    logger.info(f"Unlocked with qpdf: {pdf_path.name}")
                    return output_path
                # Clean up failed output
                if output_path.exists():
                    output_path.unlink()
            except subprocess.TimeoutExpired:
                logger.debug(f"qpdf timeout on {pdf_path.name}")
                continue
            except Exception as e:
                logger.debug(f"qpdf error on {pdf_path.name}: {e}")
                continue

        return None

    def unlock_batch(self, pdf_paths: list) -> dict:
        """
        Unlock a batch of PDFs.
        Returns dict with 'success' and 'failed' lists.
        """
        results = {"success": [], "failed": []}

        for pdf_path in pdf_paths:
            unlocked = self.unlock(pdf_path)
            if unlocked:
                results["success"].append({
                    "original": str(pdf_path),
                    "unlocked": str(unlocked),
                })
            else:
                results["failed"].append(str(pdf_path))

        logger.info(
            f"Batch unlock complete: {len(results['success'])} success, "
            f"{len(results['failed'])} failed"
        )
        return results
