# Security Policy

## Supported Versions

| Version | Supported          |
| ------- | ------------------ |
| 0.1.x   | :white_check_mark: |

## Reporting a Vulnerability

If you discover a security vulnerability in StmtForge, **please do not open a
public issue.** Instead, report it privately:

- **Email:** [madhav921@users.noreply.github.com](mailto:madhav921@users.noreply.github.com)
- **Subject:** `[SECURITY] StmtForge — <brief description>`

You will receive an acknowledgement within **48 hours** and a detailed response
within **7 days**. Please include:

1. Steps to reproduce the vulnerability.
2. Affected version(s).
3. Any potential impact you have identified.

We will coordinate a fix and release timeline with you before any public
disclosure.

## Security Design Principles

StmtForge is designed around the following principles:

### Local-first processing

- All PDF parsing and transaction extraction happens **entirely on your
  machine**.
- No data is sent to external servers, APIs, or analytics endpoints.
- The optional LLM integration uses a **locally-hosted Ollama** instance — no
  cloud LLM calls.

### Credential handling

- PDF passwords are loaded into memory from `.env` and used only during the
  unlock step. They are **never written to logs, database, or disk**.
- Gmail OAuth tokens (`token.json`) are stored in the project directory and are
  excluded from version control via `.gitignore`.
- Sensitive environment variables (DOB, PAN, bank passwords) must be placed in
  `.env`, which is git-ignored by default.

### Privacy-preserving logging

- A `RedactionFilter` automatically strips email addresses, phone numbers, PAN
  numbers, and credit card numbers from **all** log output.
- Event-level privacy logging uses HMAC-based pseudonymization (DPDP-aligned)
  so that analytics can be performed without exposing PII.
- Raw transaction text is **never** logged at INFO level or below.

### Temporary file cleanup

- Unlocked PDFs are written to a dedicated `data/unlocked_pdfs/` directory and
  are excluded from version control.
- No temporary files are left behind after processing.

### Dependency security

- The project pins minimum versions for all dependencies in `pyproject.toml`.
- Gmail API access uses **read-only** OAuth scopes
  (`gmail.readonly`).

## Best Practices for Users

1. **Never commit `.env`, `credentials.json`, or `token.json`.** The default
   `.gitignore` already excludes them — do not override this.
2. **Use a dedicated Google Cloud project** with minimal scopes for Gmail
   access.
3. **Rotate your `STMTFORGE_LOG_SALT`** if you share event logs externally.
4. **Review `config.yaml`** before sharing — it may contain card names or bank
   identifiers personal to you.
