"""LangChain-based LLM client for BrinkQL examples.

Uses ``langchain-ollama`` (ChatOllama) so the examples work with any model
served by a local Ollama instance and can be swapped for any other LangChain
chat model (OpenAI, Anthropic, Groq, …) by changing one import.

Requires:
    langchain>=1.2.10
    langchain-ollama>=1.0.1
"""
from __future__ import annotations

import re

from langchain_core.messages import HumanMessage, SystemMessage
from langchain_ollama import ChatOllama


_DEFAULT_MODEL = "gpt-oss:latest"
_DEFAULT_BASE_URL = "http://localhost:11434"


def _strip_markdown_fences(text: str) -> str:
    """Remove ```json … ``` or ``` … ``` wrappers the model may add."""
    text = text.strip()
    text = re.sub(r"^```(?:json)?\s*\n?", "", text)
    text = re.sub(r"\n?```\s*$", "", text)
    return text.strip()


class OllamaError(Exception):
    """Raised when the Ollama endpoint is unreachable or returns an error."""


class OllamaClient:
    """LangChain ChatOllama wrapper for BrinkQL examples.

    Wraps a ``ChatOllama`` instance and exposes a simple two-method interface:
    :meth:`chat` returns the raw assistant text; :meth:`get_plan_json` strips
    any markdown code fences and returns a clean JSON string.

    Args:
        model: Ollama model tag, e.g. ``"gpt-oss:latest"`` or ``"llama3:latest"``.
        base_url: Base URL of the Ollama server.
        temperature: Sampling temperature (0 = deterministic).
        timeout: Per-request timeout in seconds.
    """

    def __init__(
        self,
        model: str = _DEFAULT_MODEL,
        base_url: str = _DEFAULT_BASE_URL,
        temperature: float = 0.0,
        timeout: int = 120,
    ) -> None:
        self.model = model
        self.base_url = base_url
        self._llm = ChatOllama(
            model=model,
            base_url=base_url,
            temperature=temperature,
            timeout=timeout,
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def chat(self, system: str, user: str) -> str:
        """Send a system + user message and return the raw reply text.

        Args:
            system: System prompt content.
            user: User prompt content.

        Returns:
            The assistant's raw reply string (may contain markdown fences).

        Raises:
            OllamaError: If the request fails or the server is unreachable.
        """
        messages = [
            SystemMessage(content=system),
            HumanMessage(content=user),
        ]
        try:
            response = self._llm.invoke(messages)
        except Exception as exc:
            raise OllamaError(
                f"Ollama call failed (model={self.model!r}, url={self.base_url!r}): {exc}"
            ) from exc
        return response.content

    def get_plan_json(self, system: str, user: str) -> str:
        """Call the LLM and return a clean JSON string (fences stripped).

        Args:
            system: System prompt content.
            user: User prompt content.

        Returns:
            JSON string with markdown code fences removed.

        Raises:
            OllamaError: If the LLM call fails.
        """
        raw = self.chat(system, user)
        return _strip_markdown_fences(raw)

    def is_available(self) -> bool:
        """Return True if the Ollama server is reachable.

        Uses a lightweight ``list`` call (GET /api/tags) so no model
        needs to be loaded to perform the health check.
        """
        import urllib.request
        try:
            url = self.base_url.rstrip("/") + "/api/tags"
            with urllib.request.urlopen(url, timeout=5):
                return True
        except Exception:
            return False

    # ------------------------------------------------------------------
    # LangChain integration helpers
    # ------------------------------------------------------------------

    @classmethod
    def from_langchain_model(cls, llm: object, model_name: str = "custom") -> "_ProxyClient":
        """Wrap any LangChain chat model as an OllamaClient-compatible object.

        Useful for swapping in OpenAI, Anthropic, or Groq models without
        changing the runner code::

            from langchain_openai import ChatOpenAI
            llm = ChatOpenAI(model="gpt-4o", temperature=0)
            client = OllamaClient.from_langchain_model(llm, model_name="gpt-4o")

        Args:
            llm: Any LangChain ``BaseChatModel`` instance.
            model_name: Display name stored in trial JSON.

        Returns:
            A ``_ProxyClient`` with the same ``chat`` / ``get_plan_json`` /
            ``is_available`` interface.
        """
        return _ProxyClient(llm=llm, model=model_name)


class _ProxyClient:
    """Thin adapter wrapping an arbitrary LangChain chat model."""

    def __init__(self, llm: object, model: str) -> None:
        self.model = model
        self._llm = llm

    def chat(self, system: str, user: str) -> str:
        messages = [
            SystemMessage(content=system),
            HumanMessage(content=user),
        ]
        try:
            response = self._llm.invoke(messages)  # type: ignore[union-attr]
        except Exception as exc:
            raise OllamaError(f"LLM call failed: {exc}") from exc
        return response.content

    def get_plan_json(self, system: str, user: str) -> str:
        return _strip_markdown_fences(self.chat(system, user))

    def is_available(self) -> bool:
        return True
