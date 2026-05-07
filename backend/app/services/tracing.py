from contextlib import asynccontextmanager
from typing import Any, AsyncIterator

from app.core.config import settings


class TraceClient:
    def __init__(self) -> None:
        self._client: Any | None = None
        if settings.langfuse_public_key and settings.langfuse_secret_key:
            try:
                from langfuse import Langfuse

                self._client = Langfuse(
                    public_key=settings.langfuse_public_key,
                    secret_key=settings.langfuse_secret_key,
                    host=settings.langfuse_host,
                )
            except Exception:
                self._client = None

    @asynccontextmanager
    async def trace(self, name: str, user_id: str | None = None, input: dict | None = None) -> AsyncIterator[Any]:
        trace = None
        if self._client:
            trace = self._client.trace(name=name, user_id=user_id, input=input or {})
        try:
            yield trace
        finally:
            if self._client:
                self._client.flush()

    def tool_span(self, trace: Any, name: str, input: dict, output: dict, latency_ms: int) -> None:
        if trace is None:
            return
        try:
            trace.span(name=name, input=input, output=output, metadata={"latency_ms": latency_ms})
        except Exception:
            return
