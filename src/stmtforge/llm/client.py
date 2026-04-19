"""Ollama LLM client for transaction extraction and validation."""

import json
import re
import time
from pathlib import Path

from stmtforge.utils.logging_config import get_logger

logger = get_logger("llm.client")


class OllamaClient:
    """Client for local Ollama LLM server."""

    def __init__(self, config: dict = None):
        self.config = config or {}
        self.model = self.config.get("model", "mistral")
        self.base_url = self.config.get("base_url", "http://localhost:11434")
        self.temperature = self.config.get("temperature", 0)
        self.max_retries = self.config.get("max_retries", 3)
        self.chunk_size = self.config.get("chunk_size", 4000)
        self.timeout = self.config.get("timeout", 120)

    def _call_ollama(self, prompt: str) -> str:
        """Make a request to Ollama API. Returns raw response text."""
        import urllib.request
        import urllib.error

        payload = json.dumps({
            "model": self.model,
            "prompt": prompt,
            "stream": False,
            "format": "json",
            "options": {
                "temperature": self.temperature,
            },
        }).encode("utf-8")

        req = urllib.request.Request(
            f"{self.base_url}/api/generate",
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )

        for attempt in range(1, self.max_retries + 1):
            try:
                with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                    body = json.loads(resp.read().decode("utf-8"))
                    return body.get("response", "")
            except urllib.error.URLError as e:
                logger.warning(
                    f"Ollama request failed (attempt {attempt}/{self.max_retries}): {e}"
                )
                if attempt < self.max_retries:
                    time.sleep(2 ** attempt)
            except Exception as e:
                logger.error(f"Unexpected error calling Ollama: {e}")
                if attempt < self.max_retries:
                    time.sleep(2 ** attempt)

        logger.error(f"All {self.max_retries} Ollama attempts failed")
        return ""

    def _parse_json_response(self, response: str) -> list:
        """Parse JSON array from LLM response, handling common issues."""
        if not response.strip():
            return []

        # Try direct parse first
        try:
            result = json.loads(response)
            if isinstance(result, list):
                return result
            if isinstance(result, dict):
                # LLM sometimes wraps in {"transactions": [...]}
                for key in ("transactions", "data", "results"):
                    if key in result and isinstance(result[key], list):
                        return result[key]
                return [result]
        except json.JSONDecodeError:
            pass

        # Try to extract JSON array from response text
        match = re.search(r'\[.*\]', response, re.DOTALL)
        if match:
            try:
                return json.loads(match.group())
            except json.JSONDecodeError:
                pass

        logger.warning("Could not parse JSON from LLM response")
        return []

    def _chunk_text(self, text: str) -> list[str]:
        """Split text into chunks that fit within context window."""
        if len(text) <= self.chunk_size:
            return [text]

        lines = text.split("\n")
        chunks = []
        current_chunk = []
        current_len = 0

        for line in lines:
            line_len = len(line) + 1
            if current_len + line_len > self.chunk_size and current_chunk:
                chunks.append("\n".join(current_chunk))
                current_chunk = []
                current_len = 0
            current_chunk.append(line)
            current_len += line_len

        if current_chunk:
            chunks.append("\n".join(current_chunk))

        return chunks

    def extract_transactions(self, text: str, prompt_template: str) -> list[dict]:
        """
        Extract transactions from text using the given prompt template.
        Handles chunking for long texts and merges results.
        """
        if not text.strip():
            return []

        chunks = self._chunk_text(text)
        all_transactions = []

        for i, chunk in enumerate(chunks):
            prompt = prompt_template.replace("{text}", chunk)
            logger.info(
                f"Sending chunk {i+1}/{len(chunks)} to LLM "
                f"({len(chunk)} chars, model={self.model})"
            )

            response = self._call_ollama(prompt)
            if not response:
                logger.warning(f"Empty response for chunk {i+1}")
                continue

            transactions = self._parse_json_response(response)
            logger.info(f"Chunk {i+1}: extracted {len(transactions)} transactions")
            all_transactions.extend(transactions)

        return all_transactions

    def validate_transactions(self, transactions: list[dict],
                              prompt_template: str) -> list[dict]:
        """Run validation prompt on extracted transactions."""
        if not transactions:
            return []

        txn_json = json.dumps(transactions, indent=2)
        prompt = prompt_template.replace("{transactions}", txn_json)

        logger.info(
            f"Sending {len(transactions)} transactions for LLM validation"
        )

        response = self._call_ollama(prompt)
        if not response:
            logger.warning("Validation returned empty response, keeping originals")
            return transactions

        validated = self._parse_json_response(response)
        if validated:
            logger.info(
                f"Validation complete: {len(transactions)} → {len(validated)} transactions"
            )
            return validated

        logger.warning("Validation parse failed, keeping originals")
        return transactions

    def is_available(self) -> bool:
        """Check if Ollama server is reachable."""
        import urllib.request
        import urllib.error

        try:
            req = urllib.request.Request(f"{self.base_url}/api/tags")
            with urllib.request.urlopen(req, timeout=5) as resp:
                return resp.status == 200
        except Exception:
            return False

    @property
    def raw_response(self) -> str:
        """Last raw response from LLM (for logging)."""
        return getattr(self, "_last_response", "")
