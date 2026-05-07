from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class ToolExecution:
    tool_name: str
    status: str
    input: dict[str, Any]
    output: dict[str, Any]
    latency_ms: int
    error: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def ui_actions(self) -> list[dict[str, Any]]:
        actions = self.metadata.get("ui_actions", [])
        return actions if isinstance(actions, list) else []
