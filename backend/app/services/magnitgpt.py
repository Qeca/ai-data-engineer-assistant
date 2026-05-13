import json
from dataclasses import dataclass
from typing import Any

import httpx

from app.core.config import settings


@dataclass
class MagnitGPTFunctionCall:
    call_id: str
    name: str
    arguments: dict[str, Any]


class MagnitGPTToolClient:
    def __init__(self) -> None:
        self.base_url = settings.magnitgpt_base_url.rstrip("/")
        self.model = settings.magnitgpt_model
        self.api_key = settings.magnitgpt_api_key

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
            raise RuntimeError("MAGNITGPT_API_KEY is not configured")

        payload = {
            "model": self.model,
            "messages": [{"role": "system", "content": instructions}, *messages],
            "tools": [self._to_chat_tool(tool) for tool in tools],
            "tool_choice": "auto",
            "parallel_tool_calls": False,
            "temperature": 0.2,
        }
        async with httpx.AsyncClient(
            timeout=settings.magnitgpt_timeout_seconds,
            verify=settings.magnitgpt_verify_ssl,
        ) as client:
            response = await client.post(
                f"{self.base_url}/chat/completions",
                headers={
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": "application/json",
                },
                json=payload,
            )
            response.raise_for_status()
            return response.json()

    def get_message(self, response: dict[str, Any]) -> dict[str, Any]:
        return response["choices"][0]["message"]

    def get_text(self, response: dict[str, Any]) -> str:
        content = self.get_message(response).get("content")
        return content if isinstance(content, str) else ""

    def get_function_calls(self, response: dict[str, Any]) -> list[MagnitGPTFunctionCall]:
        calls: list[MagnitGPTFunctionCall] = []
        for tool_call in self.get_message(response).get("tool_calls") or []:
            if tool_call.get("type") != "function":
                continue
            function = tool_call.get("function") or {}
            calls.append(
                MagnitGPTFunctionCall(
                    call_id=str(tool_call["id"]),
                    name=str(function["name"]),
                    arguments=self._parse_arguments(function.get("arguments")),
                )
            )
        return calls

    def assistant_message_for_history(self, response: dict[str, Any]) -> dict[str, Any]:
        return self.get_message(response)

    @staticmethod
    def tool_message(call_id: str, output: str) -> dict[str, Any]:
        return {"role": "tool", "tool_call_id": call_id, "content": output}

    @staticmethod
    def _parse_arguments(raw_arguments: Any) -> dict[str, Any]:
        if isinstance(raw_arguments, dict):
            return raw_arguments
        if not raw_arguments:
            return {}
        return json.loads(str(raw_arguments))

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
