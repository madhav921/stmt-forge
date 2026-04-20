"""Gmail API authentication handler."""

import os
import stat
from pathlib import Path

try:
    from google.auth.transport.requests import Request
    from google.oauth2.credentials import Credentials
    from google_auth_oauthlib.flow import InstalledAppFlow
    from googleapiclient.discovery import build
    _GMAIL_DEPS_AVAILABLE = True
except ImportError:
    _GMAIL_DEPS_AVAILABLE = False

from stmtforge.utils.config import load_config, resolve_path
from stmtforge.utils.logging_config import get_logger

logger = get_logger("gmail.auth")


def get_gmail_service():
    """Authenticate and return a Gmail API service instance (read-only)."""
    if not _GMAIL_DEPS_AVAILABLE:
        raise ImportError(
            "Gmail dependencies are not installed. "
            "Run: pip install stmtforge[gmail]"
        )

    config = load_config()
    gmail_config = config["gmail"]

    scopes = gmail_config["scopes"]
    creds_file = resolve_path(gmail_config["credentials_file"])
    token_file = resolve_path(gmail_config["token_file"])

    creds = None

    if token_file.exists():
        try:
            creds = Credentials.from_authorized_user_file(str(token_file), scopes)
        except Exception as e:
            logger.warning(f"Failed to load token file, will re-authenticate: {e}")
            creds = None

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
                logger.info("Token refreshed successfully")
            except Exception as e:
                logger.warning(f"Token refresh failed, re-authenticating: {e}")
                creds = None

        if not creds:
            if not creds_file.exists():
                raise FileNotFoundError(
                    f"OAuth credentials file not found: {creds_file}\n"
                    "Download it from Google Cloud Console → APIs & Services → Credentials"
                )
            flow = InstalledAppFlow.from_client_secrets_file(str(creds_file), scopes)
            creds = flow.run_local_server(port=0)
            logger.info("New authentication completed")

        # Save token for future runs (owner-only permissions)
        token_file.parent.mkdir(parents=True, exist_ok=True)
        with open(token_file, "w") as f:
            f.write(creds.to_json())
        try:
            os.chmod(token_file, stat.S_IRUSR | stat.S_IWUSR)
        except OSError:
            pass  # Windows may not support POSIX permissions

    service = build("gmail", "v1", credentials=creds)
    logger.info("Gmail API service initialized")
    return service
