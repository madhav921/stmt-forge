"""Configuration loader for StmtForge."""

import os
import yaml
from pathlib import Path
from dotenv import load_dotenv


_config = None


def get_project_root() -> Path:
    """Return the project working directory.

    Resolution order:
      1. STMTFORGE_PROJECT_DIR environment variable (explicit override)
      2. Current working directory (default for normal usage)

    This ensures paths in config.yaml (e.g. "data/raw_pdfs") are resolved
    relative to wherever the user is running the tool, not relative to
    the installed package location in site-packages.
    """
    env_root = os.getenv("STMTFORGE_PROJECT_DIR", "").strip()
    if env_root:
        return Path(env_root).resolve()
    return Path.cwd()


def _default_config_path() -> Path:
    """Return the default config.yaml path to look for."""
    return get_project_root() / "config.yaml"


def load_config(config_path: str = None) -> dict:
    """Load configuration from YAML file and environment variables."""
    global _config
    if _config is not None and config_path is None:
        return _config

    load_dotenv(get_project_root() / ".env")

    if config_path is None:
        config_path = _default_config_path()
    else:
        config_path = Path(config_path)

    if not config_path.exists():
        raise FileNotFoundError(
            f"Configuration file not found: {config_path}\n"
            f"Run 'stmtforge init' to create a default config.yaml, or set "
            f"STMTFORGE_PROJECT_DIR to your project directory."
        )

    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    # Override with environment variables where applicable
    env_dob = os.getenv("DOB", "").strip()
    if env_dob:
        dob_patterns = _generate_dob_patterns(env_dob)
        config.setdefault("pdf_passwords", {})["dob_patterns"] = dob_patterns

    env_pan = os.getenv("PAN", "").strip()
    if env_pan:
        config.setdefault("pdf_passwords", {})["pan_numbers"] = [env_pan.upper()]

    env_custom = os.getenv("CUSTOM_PASSWORDS", "").strip()
    if env_custom:
        config.setdefault("pdf_passwords", {})["custom_passwords"] = [
            p.strip() for p in env_custom.split(",") if p.strip()
        ]

    _config = config
    return config


def reload_config(config_path: str = None) -> dict:
    """Force-reload configuration (clears cache)."""
    global _config
    _config = None
    return load_config(config_path)


def _generate_dob_patterns(dob: str) -> list:
    """Generate multiple password patterns from a DOB string (DDMMYYYY)."""
    patterns = [dob]
    if len(dob) == 8:
        dd, mm, yyyy = dob[:2], dob[2:4], dob[4:]
        yy = yyyy[2:]
        patterns.extend([
            f"{dd}{mm}{yyyy}",
            f"{dd}{mm}{yy}",
            f"{dd}-{mm}-{yyyy}",
            f"{dd}/{mm}/{yyyy}",
            f"{mm}{dd}{yyyy}",
            f"{yyyy}{mm}{dd}",
            f"{yyyy}-{mm}-{dd}",
            f"{dd}{mm}",
            yyyy,
        ])
    return list(set(p for p in patterns if p))


def get_all_passwords(config: dict) -> list:
    """Get all password candidates from config."""
    passwords = [""]  # Try empty password first
    pw_config = config.get("pdf_passwords", {})

    for dob in pw_config.get("dob_patterns", []):
        if dob:
            passwords.append(dob)

    for pan in pw_config.get("pan_numbers", []):
        if pan:
            passwords.append(pan)
            passwords.append(pan.lower())

    for name_pw in pw_config.get("name_passwords", []):
        if name_pw:
            passwords.append(name_pw)
            passwords.append(name_pw.upper())
            passwords.append(name_pw.lower())

    for custom in pw_config.get("custom_passwords", []):
        if custom:
            passwords.append(custom)

    # Cross-combine DOB + PAN patterns
    dobs = [d for d in pw_config.get("dob_patterns", []) if d]
    pans = [p for p in pw_config.get("pan_numbers", []) if p]
    names = [n for n in pw_config.get("name_passwords", []) if n]

    for pan in pans:
        for dob in dobs:
            if len(dob) >= 4:
                passwords.append(f"{pan}{dob[:4]}")
                passwords.append(f"{pan.lower()}{dob[:4]}")

    for name in names:
        for dob in dobs:
            if len(dob) >= 4:
                passwords.append(f"{name}{dob[:4]}")
                passwords.append(f"{name.lower()}{dob[:4]}")

    # Deduplicate while preserving order
    seen = set()
    unique = []
    for pw in passwords:
        if pw not in seen:
            seen.add(pw)
            unique.append(pw)
    return unique


def resolve_path(relative_path: str) -> Path:
    """Resolve a path relative to project root."""
    p = Path(relative_path)
    if p.is_absolute():
        return p
    return get_project_root() / p
