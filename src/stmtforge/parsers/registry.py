"""Parser registry - maps bank names to their specific parsers."""

from stmtforge.parsers.base_parser import BaseParser
from stmtforge.parsers.generic_parser import GenericParser
from stmtforge.parsers.hdfc_parser import HDFCParser
from stmtforge.parsers.icici_parser import ICICIParser
from stmtforge.parsers.sbi_parser import SBIParser
from stmtforge.parsers.axis_parser import AxisParser
from stmtforge.parsers.kotak_parser import KotakParser
from stmtforge.parsers.yes_parser import YesParser
from stmtforge.parsers.csb_parser import CSBParser
from stmtforge.parsers.federal_parser import FederalParser
from stmtforge.parsers.idfc_first_parser import IDFCFirstParser
from stmtforge.utils.logging_config import get_logger

logger = get_logger("parsers.registry")

# Registry of bank-specific parsers
_PARSERS: dict[str, type[BaseParser]] = {
    "hdfc": HDFCParser,
    "icici": ICICIParser,
    "sbi": SBIParser,
    "axis": AxisParser,
    "kotak": KotakParser,
    "yes": YesParser,
    "csb": CSBParser,
    "federal": FederalParser,
    "idfc_first": IDFCFirstParser,
}

# Singleton instances
_instances: dict[str, BaseParser] = {}


def get_parser(bank_name: str) -> BaseParser:
    """
    Get the appropriate parser for a bank.
    Falls back to GenericParser for unknown banks.
    """
    bank_lower = bank_name.lower().strip()

    if bank_lower not in _instances:
        parser_class = _PARSERS.get(bank_lower, GenericParser)
        _instances[bank_lower] = parser_class()
        if bank_lower not in _PARSERS:
            logger.info(f"No specific parser for '{bank_name}', using GenericParser")
        else:
            logger.debug(f"Using {parser_class.__name__} for '{bank_name}'")

    return _instances[bank_lower]


def list_available_parsers() -> list:
    """Return list of banks with dedicated parsers."""
    return list(_PARSERS.keys())


def register_parser(bank_name: str, parser_class: type[BaseParser]):
    """Register a new bank-specific parser at runtime."""
    _PARSERS[bank_name.lower().strip()] = parser_class
    # Clear cached instance
    _instances.pop(bank_name.lower().strip(), None)
    logger.info(f"Registered parser: {parser_class.__name__} for '{bank_name}'")
