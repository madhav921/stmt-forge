# Contributing to StmtForge

Thank you for your interest in contributing! This document explains how to get
started.

## Development Setup

```bash
git clone https://github.com/madhav921/stmt-forge.git
cd stmt-forge
python -m venv .venv
.venv\Scripts\activate   # Windows
# source .venv/bin/activate  # macOS / Linux
pip install -e ".[dev]"
```

Optional extras for local feature testing:

```bash
pip install -e ".[gmail]"   # Gmail fetch flow
pip install -e ".[ocr]"     # OCR fallback flow
pip install -e ".[all]"     # both Gmail and OCR extras
```

## Running Tests

```bash
pytest
```

All tests must pass before submitting a pull request.

## Project Layout

```
src/stmtforge/          # Package source
tests/                  # Test suite
```

Source code lives under `src/stmtforge/` using the
[src-layout](https://packaging.python.org/en/latest/discussions/src-layout-vs-flat-layout/)
convention.

## How to Contribute

### Reporting Bugs

Open an issue with:
- Python version and OS.
- Steps to reproduce.
- Expected vs actual behaviour.
- Redacted log output (no personal data).

### Adding a New Bank Parser

1. Create `src/stmtforge/parsers/<bank>_parser.py` inheriting from
   `BaseParser`.
2. Register the parser in `src/stmtforge/parsers/registry.py`.
3. Add sender-domain and filename mappings in `config_template.yaml`.
4. Add at least one test with a sample (anonymized) statement.

### General Guidelines

- Keep pull requests focused — one feature or fix per PR.
- Follow the existing code style (no linter is enforced yet, but consistency
  matters).
- Do **not** commit credentials, personal data, or real statement PDFs.
- Update `README.md` if your change affects user-facing behaviour.

## Security Vulnerabilities

Please report security issues **privately** — see [SECURITY.md](SECURITY.md).

## License

By contributing you agree that your contributions will be licensed under the
[MIT License](LICENSE).
