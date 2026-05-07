from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

import httpx

from app.core.config import settings


@dataclass
class ChatFunctionCall:
    call_id: str
    name: str
    arguments: dict[str, Any]


class OpenRouterToolClient:
    def __init__(self) -> None:
        self.base_url = settings.openrouter_base_url.rstrip("/")
        self.model = settings.openrouter_model
        self.api_key = settings.openrouter_api_key

    @property
    def enabled(self) -> bool:
        return bool(self.api_key)

    async def create(
        self,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]],
        instructions: str,
    ) -> dict[str, Any]:
        if not self.api_key:
            raise RuntimeError("OPENROUTER_API_KEY is not configured")

        payload = {
            "model": self.model,
            "messages": [{"role": "system", "content": instructions}, *messages],
            "tools": [self._to_chat_tool(tool) for tool in tools],
            "tool_choice": "auto",
            "parallel_tool_calls": False,
            "temperature": 0.2,
        }
        async with httpx.AsyncClient(timeout=45) as client:
            response = await client.post(
                f"{self.base_url}/chat/completions",
                headers={
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": "application/json",
                    "HTTP-Referer": "http://localhost:3000",
                    "X-Title": "AI Data Engineer Assistant",
                },
                json=payload,
            )
            response.raise_for_status()
            return response.json()

    def get_message(self, response: dict[str, Any]) -> dict[str, Any]:
        return response["choices"][0]["message"]

    def get_text(self, response: dict[str, Any]) -> str:
        message = self.get_message(response)
        content = message.get("content")
        return content if isinstance(content, str) else ""

    def get_function_calls(self, response: dict[str, Any]) -> list[ChatFunctionCall]:
        message = self.get_message(response)
        calls: list[ChatFunctionCall] = []
        for tool_call in message.get("tool_calls") or []:
            if tool_call.get("type") != "function":
                continue
            function = tool_call.get("function") or {}
            calls.append(
                ChatFunctionCall(
                    call_id=str(tool_call["id"]),
                    name=str(function["name"]),
                    arguments=json.loads(function.get("arguments") or "{}"),
                )
            )
        return calls

    def assistant_message_for_history(self, response: dict[str, Any]) -> dict[str, Any]:
        return self.get_message(response)

    def tool_message(self, call_id: str, output: str) -> dict[str, Any]:
        return {"role": "tool", "tool_call_id": call_id, "content": output}

    @staticmethod
    def _to_chat_tool(tool: dict[str, Any]) -> dict[str, Any]:
        return {
            "type": "function",
            "function": {
                "name": tool["name"],
                "description": tool["description"],
                "parameters": tool["parameters"],
                "strict": tool.get("strict", True),
            },
        }
