# StmtForge

[![Tests](https://github.com/madhav921/stmt-forge/actions/workflows/tests.yml/badge.svg)](https://github.com/madhav921/stmt-forge/actions/workflows/tests.yml)
[![Python 3.11+](https://img.shields.io/badge/python-3.11%2B-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)
[![Code style: ruff](https://img.shields.io/badge/code%20style-ruff-000000.svg)](https://github.com/astral-sh/ruff)

**A fully local, privacy-first credit card statement parser & analyzer for
Indian banks.**

StmtForge extracts transactions from bank-issued PDF statements using a hybrid
pipeline (deterministic parsers → table extraction → OCR → local LLM), stores
them in a local SQLite database, and presents insights through an interactive
Streamlit dashboard.

> **All data stays on your machine.** No cloud uploads. No external API calls.
> The optional LLM runs locally via [Ollama](https://ollama.com/).

---

## Features

| Capability | Details |
|---|---|
| **Gmail integration** | Fetches statement PDFs via Gmail API (read-only scope) |
| **PDF unlocking** | pikepdf / qpdf with configurable password patterns |
| **Hybrid extraction** | Deterministic → table → layout text → OCR → LLM fallback |
| **Local LLM** | Ollama (Qwen / Mistral / Llama3) for unstructured extraction |
| **9 bank parsers** | HDFC · ICICI · SBI · Axis · Kotak · Yes · CSB · Federal · IDFC First |
| **Multi-card support** | Per-card tracking across banks |
| **Auto-categorization** | Rule-based merchant categorization |
| **Validation** | Deduplication, date/amount checks, confidence scoring |
| **SQLite storage** | Local DB with incremental processing |
| **Dashboard** | Streamlit + Plotly charts, filters, CSV export |
| **Privacy logging** | DPDP-aligned pseudonymization; PII redacted from all logs |

---

## Installation

### From source (recommended)

```bash
git clone https://github.com/madhav921/stmt-forge.git
cd stmt-forge
python -m venv .venv
.venv\Scripts\activate        # Windows
# source .venv/bin/activate   # macOS / Linux
pip install -e ".[dashboard,dev]"
```

### Prerequisites

| Requirement | Purpose |
|---|---|
| Python 3.11+ | Runtime |
| [Ollama](https://ollama.com/) | Local LLM (optional but recommended) |
| Google Cloud project | Gmail API access (optional — manual PDF import works without it) |
| qpdf | Fallback PDF decryption (optional) |

---

## Quick Start

```bash
# 1. Initialize a project directory
mkdir ~/my-statements && cd ~/my-statements
stmtforge init          # creates config.yaml + data/ directories

# 2. (Optional) Set up Gmail — see docs below

# 3. Add PDF passwords to .env
cp .env.example .env    # then edit .env

# 4. Pull an Ollama model
ollama pull qwen2.5:3b

# 5. Run the pipeline
stmtforge run --local              # local PDFs only
stmtforge run --full               # full Gmail fetch + parse
stmtforge run --folder path/to/pdfs  # specific folder

# 6. Launch dashboard
stmtforge dashboard
```

---

## Privacy & Data Handling

StmtForge is designed around a **local-first, zero-upload** architecture.

| Concern | How it's handled |
|---|---|
| **Where data is processed** | Entirely on your local machine |
| **What data is accessed** | PDF files (local or Gmail), extracted transactions |
| **External network calls** | Gmail API (opt-in, read-only) and local Ollama only |
| **Analytics / tracking** | None — no telemetry, no phone-home |
| **Data storage** | Local SQLite database + local files only |
| **Log privacy** | All PII (emails, phones, PAN, card numbers) is automatically redacted from logs |

```
This tool processes credit card statements locally on your machine.

- No data is uploaded to any external server.
- No user data is stored or logged beyond your local project directory.
- No analytics or tracking is performed.
- All parsing and analysis happens entirely offline unless you explicitly
  enable Gmail integration.
```

### Gmail Integration (optional)

If you enable Gmail integration:

- The tool uses **read-only** access via the Gmail API
  (`gmail.readonly` scope).
- Only emails matching your configured filters (sender domains, keywords like
  "credit card statement") are accessed.
- Attachments are downloaded to your local `data/raw_pdfs/` directory.
- **No email content is stored or transmitted externally.**
- You can revoke access at any time from
  [Google Account Permissions](https://myaccount.google.com/permissions).

> Gmail is entirely optional. You can drop PDFs into `data/raw_pdfs/<bank>/`
> manually and run `stmtforge run --local`.

---

## Security Practices

| Practice | Implementation |
|---|---|
| **PDF passwords** | Loaded from `.env` into memory; never written to logs, database, or disk |
| **Log redaction** | `RedactionFilter` strips emails, phones, PAN numbers, card numbers from every log line |
| **Privacy logging** | HMAC pseudonymization for event logs (DPDP-aligned) |
| **OAuth tokens** | Stored locally (`token.json`); git-ignored by default |
| **Sensitive config** | `config.yaml` and `.env` are git-ignored; only sanitized templates are committed |
| **Temporary files** | Unlocked PDFs are kept in `data/unlocked_pdfs/`; no stray temp files |
| **Dependencies** | Minimum versions pinned; no unnecessary or exotic packages |

For full details and how to report vulnerabilities, see
[SECURITY.md](SECURITY.md).

---

## Supported Banks & Formats

StmtForge includes dedicated parsers for the following banks. Other formats fall
back to the generic parser + LLM extraction.

| Bank | Parser | Status |
|---|---|---|
| HDFC Bank | `hdfc_parser` | Tested |
| ICICI Bank | `icici_parser` | Tested |
| SBI Card | `sbi_parser` | Tested |
| Axis Bank | `axis_parser` | Tested |
| Kotak Mahindra | `kotak_parser` | Tested |
| Yes Bank | `yes_parser` | Tested |
| CSB Bank | `csb_parser` | Tested |
| Federal Bank | `federal_parser` | Tested |
| IDFC First Bank | `idfc_first_parser` | Tested |
| *(other)* | `generic_parser` + LLM | Best-effort |

> **Note:** Statement formats change over time. If a parser produces incorrect
> results for a recent statement, please open an issue.

---

## How It Works

```
PDF ─► Unlock ─► Deterministic Parser ─► Multi-Stage Extraction ─► LLM ─► Validation ─► SQLite
```

1. **PDF Unlock** — Tries password combinations (DOB, PAN, custom) via pikepdf.
2. **Deterministic Parser** — Bank-specific regex parser runs first. If ≥ 3
   transactions are found, done.
3. **Multi-Stage Extraction** (fallback):
   - Stage 1 — Table extraction (pdfplumber)
   - Stage 2 — Layout text (pdfplumber / pdftotext)
   - Stage 3 — OCR (pdf2image + Tesseract, optional)
4. **LLM Structuring** — Local Ollama with primary → hard-mode → validation
   prompts.
5. **Validation** — Date normalization, amount bounds, deduplication, confidence
   scoring.
6. **Categorization** — Rule-based merchant classification.
7. **Storage** — SQLite with transaction-level deduplication.

---

## Project Structure

```
stmt-forge/
├── src/stmtforge/           # Package source (src-layout)
│   ├── __init__.py
│   ├── cli.py               # CLI entry point
│   ├── run_pipeline.py      # Pipeline orchestrator
│   ├── hybrid_pipeline.py   # Hybrid extraction engine
│   ├── config_template.yaml # Default config template
│   ├── database/            # SQLite layer
│   ├── dashboard/           # Streamlit app
│   ├── extractor/           # Multi-stage text extraction
│   ├── gmail/               # Gmail OAuth & fetcher
│   ├── llm/                 # Ollama client & prompts
│   ├── parsers/             # Bank-specific parsers
│   ├── pdf_processing/      # PDF unlock & extraction
│   ├── utils/               # Config, logging, privacy, hashing
│   └── validator/           # Transaction validation
├── tests/                   # Test suite
├── .github/workflows/       # CI (GitHub Actions)
├── pyproject.toml           # Build configuration
├── .env.example             # Environment variable template
├── LICENSE                  # MIT
├── SECURITY.md              # Security policy
├── CONTRIBUTING.md          # Contributor guide
├── CODE_OF_CONDUCT.md       # Community standards
└── README.md                # This file
```

---

## Adding a New Bank Parser

```python
# src/stmtforge/parsers/mybank_parser.py
from stmtforge.parsers.base_parser import BaseParser, parse_date, parse_amount

class MyBankParser(BaseParser):
    BANK_NAME = "mybank"

    def parse(self, pdf_path):
        records = [...]  # Extract transactions
        return self._get_standard_df(records)
```

Then register in `src/stmtforge/parsers/registry.py` and add email / filename
mappings in `config_template.yaml`. See [CONTRIBUTING.md](CONTRIBUTING.md) for
details.

---

## Testing

```bash
pytest                 # run full suite
pytest -v              # verbose output
pytest tests/test_scope_filter.py  # single file
```

---

## Configuration

`stmtforge init` copies a sanitized `config_template.yaml` into your project
directory as `config.yaml`. Key sections:

| Section | Purpose |
|---|---|
| `gmail` | Sender domains, search keywords, attachment filters |
| `credit_cards` | Your banks and card names |
| `pdf_passwords` | Password patterns (auto-filled from `.env`) |
| `parsers` | Email → bank mapping, filename → bank mapping, card identifiers |
| `categories` | Merchant → category rules |
| `database` | SQLite path |
| `llm` | Ollama model, URL, temperature |
| `privacy_logging` | Retention period, pseudonymization salt |

---

## Disclaimer

This tool is intended for **personal use and convenience**.

While care has been taken to ensure accuracy:

- Parsing errors may occur depending on statement format changes by banks.
- Users should **verify extracted data** before making financial decisions.
- This is **not** a bank-grade or auditor-certified system.
- The authors assume no liability for incorrect transaction data.

---

## Contributing

We welcome contributions of all kinds — bug reports, new bank parsers,
documentation improvements, and code fixes. See
[CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## License

[MIT](LICENSE) — see the [LICENSE](LICENSE) file for details.
