"""Auto-categorization engine for credit card transactions."""

import re

from stmtforge.utils.config import load_config
from stmtforge.utils.logging_config import get_logger

logger = get_logger("categorizer")


class Categorizer:
    """Rule-based transaction categorization."""

    def __init__(self):
        config = load_config()
        self.rules = config.get("categories", {})
        self.default_category = self.rules.pop("_default", "Others")
        # Pre-compile patterns for performance
        self._compiled_rules = {}
        for category, keywords in self.rules.items():
            patterns = [re.escape(kw.lower()) for kw in keywords if kw]
            if patterns:
                self._compiled_rules[category] = re.compile(
                    "|".join(patterns), re.IGNORECASE
                )

    def categorize(self, description: str) -> str:
        """Categorize a transaction based on its description."""
        if not description:
            return self.default_category

        desc_lower = description.lower().strip()

        for category, pattern in self._compiled_rules.items():
            if pattern.search(desc_lower):
                return category

        return self.default_category

    def categorize_batch(self, descriptions: list) -> list:
        """Categorize a batch of descriptions."""
        return [self.categorize(desc) for desc in descriptions]

    def add_rule(self, category: str, keyword: str):
        """Add a new categorization rule at runtime."""
        if category not in self.rules:
            self.rules[category] = []
        self.rules[category].append(keyword)

        # Recompile pattern for this category
        patterns = [re.escape(kw.lower()) for kw in self.rules[category] if kw]
        if patterns:
            self._compiled_rules[category] = re.compile(
                "|".join(patterns), re.IGNORECASE
            )
