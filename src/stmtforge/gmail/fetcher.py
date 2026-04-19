"""Gmail message fetcher - downloads credit card statement PDFs."""

import base64
import re
from datetime import datetime, timedelta
from pathlib import Path

from stmtforge.utils.config import load_config, resolve_path
from stmtforge.utils.hashing import content_hash
from stmtforge.utils.logging_config import get_logger
from stmtforge.utils.scope_filter import (
    is_irrelevant_filename,
    is_irrelevant_statement_text,
    extract_pdf_preview_text,
)
from stmtforge.gmail.auth import get_gmail_service
from stmtforge.database.db import Database

logger = get_logger("gmail.fetcher")


class GmailFetcher:
    """Fetches credit card statement emails and downloads PDF attachments."""

    def __init__(self, db: Database = None):
        self.config = load_config()
        self.gmail_config = self.config["gmail"]
        self.service = None
        self.db = db or Database()
        self.raw_pdf_dir = resolve_path(self.config["data"]["raw_pdfs"])
        self.raw_pdf_dir.mkdir(parents=True, exist_ok=True)
        # Load card identifiers for card name detection
        self.card_identifiers = self.config.get("parsers", {}).get("card_identifiers", {})

    def _ensure_service(self):
        if self.service is None:
            self.service = get_gmail_service()

    def build_query(self, start_date: str = None, end_date: str = None) -> str:
        """Build Gmail search query dynamically."""
        search_config = self.gmail_config["search"]

        # Determine date range
        if start_date is None:
            last_fetch = self.db.get_last_fetch_date()
            if last_fetch:
                # Incremental: from last fetch minus some overlap
                lookback = search_config.get("incremental_lookback_days", 45)
                dt = datetime.strptime(last_fetch, "%Y-%m-%d") - timedelta(days=lookback)
                start_date = dt.strftime("%Y/%m/%d")
            else:
                # Initial fetch from configured start date
                initial = search_config.get("initial_start_date", "2024-06-01")
                start_date = initial.replace("-", "/")

        if end_date is None:
            end_date = datetime.now().strftime("%Y/%m/%d")

        # Build keyword clause
        keywords = search_config.get("keywords", ["credit card statement"])
        keyword_clause = " OR ".join(f'"{kw}"' for kw in keywords)

        # Build query
        query_parts = [
            f"after:{start_date}",
            f"before:{end_date}",
            "has:attachment",
            "filename:pdf",
            f"({keyword_clause})",
        ]

        query = " ".join(query_parts)
        logger.info(f"Gmail search query: {query}")
        return query

    def _identify_bank(self, sender_email: str, filename: str, subject: str = "") -> str:
        """Identify bank from sender email, filename, or subject."""
        parser_config = self.config.get("parsers", {})

        # Check email-to-bank mapping
        email_map = parser_config.get("email_to_bank", {})
        sender_lower = sender_email.lower()
        for pattern, bank in email_map.items():
            if pattern.lower() in sender_lower:
                return bank

        # Check filename-to-bank mapping
        filename_map = parser_config.get("filename_to_bank", {})
        combined = f"{filename} {subject}".lower()
        for pattern, bank in filename_map.items():
            if pattern.lower() in combined:
                return bank

        return "unknown"

    def _identify_card_info(self, subject: str, filename: str) -> tuple[str | None, str | None]:
        """Identify card name and inferred bank from subject/filename patterns."""
        search_text = f"{subject} {filename}".lower()
        for pattern, info in self.card_identifiers.items():
            if pattern.lower() in search_text:
                if isinstance(info, dict):
                    return info.get("card_name"), info.get("bank")
                return info, None
        return None, None

    def _extract_sender_email(self, headers: list) -> str:
        """Extract sender email from message headers."""
        for header in headers:
            if header["name"].lower() == "from":
                value = header["value"]
                match = re.search(r"<([^>]+)>", value)
                if match:
                    return match.group(1)
                return value.strip()
        return ""

    def _is_allowed_sender(self, sender_email: str) -> bool:
        """Check if sender is from an allowed domain."""
        allowed_domains = self.gmail_config.get("allowed_sender_domains", [])
        if not allowed_domains:
            return True  # Empty list means accept all

        sender_lower = sender_email.lower()
        return any(domain.lower() in sender_lower for domain in allowed_domains)

    def _extract_subject(self, headers: list) -> str:
        """Extract email subject from message headers."""
        for header in headers:
            if header["name"].lower() == "subject":
                return header["value"].strip()
        return ""

    def _identify_card_name(self, subject: str, filename: str) -> str | None:
        """Identify the specific credit card name from email subject or filename."""
        search_text = f"{subject} {filename}".lower()
        for pattern, info in self.card_identifiers.items():
            if pattern.lower() in search_text:
                return info.get("card_name") if isinstance(info, dict) else info
        return None

    def _is_credit_card_email(self, subject: str, snippet: str = "") -> bool:
        """Check if an email is a credit card statement (not savings/current account)."""
        exclude_keywords = self.gmail_config.get("search", {}).get("exclude_keywords", [])
        combined = f"{subject} {snippet}".lower()
        for kw in exclude_keywords:
            if kw.lower() in combined:
                logger.debug(f"Excluded (matched '{kw}'): {subject}")
                return False
        return True

    def _is_irrelevant_attachment(self, filename: str) -> bool:
        """Check if a PDF attachment filename matches irrelevant patterns."""
        return is_irrelevant_filename(filename, "unknown")

    def _extract_date(self, headers: list) -> str:
        """Extract date from message headers."""
        for header in headers:
            if header["name"].lower() == "date":
                raw = header["value"]
                # Try common date formats
                for fmt in [
                    "%a, %d %b %Y %H:%M:%S %z",
                    "%d %b %Y %H:%M:%S %z",
                    "%a, %d %b %Y %H:%M:%S %Z",
                ]:
                    try:
                        dt = datetime.strptime(raw.strip(), fmt)
                        return dt.strftime("%Y-%m-%d")
                    except ValueError:
                        continue
                # Fallback: extract date portion
                match = re.search(r"(\d{1,2}\s+\w+\s+\d{4})", raw)
                if match:
                    try:
                        dt = datetime.strptime(match.group(1), "%d %b %Y")
                        return dt.strftime("%Y-%m-%d")
                    except ValueError:
                        pass
        return datetime.now().strftime("%Y-%m-%d")

    def fetch_messages(self, query: str = None) -> list:
        """Fetch matching messages from Gmail."""
        self._ensure_service()

        if query is None:
            query = self.build_query()

        messages = []
        page_token = None

        while True:
            try:
                result = self.service.users().messages().list(
                    userId="me",
                    q=query,
                    pageToken=page_token,
                    maxResults=100,
                ).execute()

                batch = result.get("messages", [])
                messages.extend(batch)
                logger.info(f"Fetched {len(batch)} message IDs (total: {len(messages)})")

                page_token = result.get("nextPageToken")
                if not page_token:
                    break
            except Exception as e:
                logger.error(f"Error fetching messages: {e}")
                break

        logger.info(f"Total messages found: {len(messages)}")
        return messages

    def download_attachments(self, messages: list = None) -> list:
        """Download PDF attachments from messages. Returns list of downloaded file paths."""
        self._ensure_service()

        if messages is None:
            messages = self.fetch_messages()

        downloaded_files = []
        processed_msg_ids = self.db.get_processed_message_ids()

        for msg_info in messages:
            msg_id = msg_info["id"]

            # Skip already processed messages
            if msg_id in processed_msg_ids:
                logger.debug(f"Skipping already processed message: {msg_id}")
                continue

            try:
                msg = self.service.users().messages().get(
                    userId="me", id=msg_id, format="full"
                ).execute()
            except Exception as e:
                logger.error(f"Error fetching message {msg_id}: {e}")
                continue

            headers = msg.get("payload", {}).get("headers", [])
            sender = self._extract_sender_email(headers)
            email_date = self._extract_date(headers)
            email_subject = self._extract_subject(headers)
            snippet = msg.get("snippet", "")

            # Check allowed senders
            if not self._is_allowed_sender(sender):
                logger.debug(f"Sender not in allowed list: {sender}")
                self.db.record_message(msg_id, sender, email_date, "skipped_sender",
                                       email_subject=email_subject)
                continue

            # Filter out non-credit-card emails (savings, loans, etc.)
            if not self._is_credit_card_email(email_subject, snippet):
                logger.debug(f"Not a CC statement: {email_subject}")
                self.db.record_message(msg_id, sender, email_date, "skipped_not_cc",
                                       email_subject=email_subject)
                continue

            # Find PDF attachments
            parts = self._get_all_parts(msg.get("payload", {}))
            pdf_found = False

            for part in parts:
                filename = part.get("filename", "")
                if not filename.lower().endswith(".pdf"):
                    continue

                # Skip irrelevant PDF attachments by filename
                if self._is_irrelevant_attachment(filename):
                    logger.debug(f"Skipping irrelevant attachment: {filename}")
                    continue

                attachment_id = part.get("body", {}).get("attachmentId")
                if not attachment_id:
                    continue

                # Download attachment
                try:
                    att = self.service.users().messages().attachments().get(
                        userId="me", messageId=msg_id, id=attachment_id
                    ).execute()
                except Exception as e:
                    logger.error(f"Error downloading attachment {filename} from {msg_id}: {e}")
                    continue

                data = base64.urlsafe_b64decode(att["data"])
                file_hash = content_hash(data)

                # Check if this file was already downloaded (by hash)
                if self.db.is_file_processed(file_hash):
                    logger.debug(f"File already processed (hash match): {filename}")
                    continue

                # Determine bank and card name
                bank = self._identify_bank(sender, filename, email_subject)
                card_name = self._identify_card_name(email_subject, filename)

                # Enforce project scope: reject debit/savings/newsletter/non-statement PDFs.
                if is_irrelevant_filename(filename, bank):
                    logger.info(f"Skipping out-of-scope attachment by filename: {filename}")
                    continue

                preview_text = extract_pdf_preview_text(pdf_bytes=data, max_pages=2)
                if is_irrelevant_statement_text(preview_text):
                    logger.info(f"Skipping out-of-scope attachment by content: {filename}")
                    continue

                # Fallback: infer from card identifier patterns if bank unresolved
                if bank == "unknown":
                    inferred_card_name, inferred_bank = self._identify_card_info(
                        email_subject, filename
                    )
                    if inferred_bank:
                        bank = inferred_bank
                    if not card_name and inferred_card_name:
                        card_name = inferred_card_name

                # Parse year/month from email date
                try:
                    dt = datetime.strptime(email_date, "%Y-%m-%d")
                    ym = dt.strftime("%Y_%m")
                except ValueError:
                    ym = datetime.now().strftime("%Y_%m")

                save_dir = self.raw_pdf_dir / bank / ym
                save_dir.mkdir(parents=True, exist_ok=True)

                # Sanitize filename
                safe_name = re.sub(r'[<>:"/\\|?*]', "_", filename)
                save_path = save_dir / safe_name

                # Handle filename collision
                counter = 1
                while save_path.exists():
                    stem = save_path.stem
                    save_path = save_dir / f"{stem}_{counter}.pdf"
                    counter += 1

                with open(save_path, "wb") as f:
                    f.write(data)

                logger.info(f"Downloaded: {save_path} (bank={bank}, card={card_name}, hash={file_hash[:12]})")
                downloaded_files.append({
                    "path": str(save_path),
                    "bank": bank,
                    "card_name": card_name,
                    "filename": safe_name,
                    "sender": sender,
                    "email_date": email_date,
                    "email_subject": email_subject,
                    "message_id": msg_id,
                    "file_hash": file_hash,
                })
                pdf_found = True

            status = "downloaded" if pdf_found else "no_pdf"
            self.db.record_message(msg_id, sender, email_date, status,
                                   email_subject=email_subject)

        logger.info(f"Downloaded {len(downloaded_files)} new PDF(s)")
        return downloaded_files

    def _get_all_parts(self, payload: dict) -> list:
        """Recursively get all parts from a message payload."""
        parts = []
        if "parts" in payload:
            for part in payload["parts"]:
                parts.extend(self._get_all_parts(part))
        else:
            parts.append(payload)
        return parts

    def run(self) -> list:
        """Execute the full fetch pipeline. Returns list of downloaded file info dicts."""
        logger.info("Starting Gmail fetch...")
        messages = self.fetch_messages()
        downloaded = self.download_attachments(messages)
        if downloaded:
            self.db.update_last_fetch_date(datetime.now().strftime("%Y-%m-%d"))
        logger.info(f"Gmail fetch complete. {len(downloaded)} new file(s) downloaded.")
        return downloaded
