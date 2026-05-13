import json
from dataclasses import dataclass
from typing import Any

import httpx

from app.core.config import settings


@dataclass
class FunctionCall:
    call_id: str
    name: str
    arguments: dict[str, Any]


class OpenAIResponsesClient:
    def __init__(self) -> None:
        self.base_url = settings.openai_base_url.rstrip("/")
        self.model = settings.openai_model
        self.api_key = settings.openai_api_key

    @property
    def enabled(self) -> bool:
        return bool(self.api_key)

    async def create(
        self,
        input_payload: str | list[dict[str, Any]],
        tools: list[dict[str, Any]],
        instructions: str,
        previous_response_id: str | None = None,
    ) -> dict[str, Any]:
        if not self.api_key:
            raise RuntimeError("OPENAI_API_KEY is not configured")

        payload: dict[str, Any] = {
            "model": self.model,
            "instructions": instructions,
            "input": input_payload,
            "tools": tools,
            "tool_choice": "auto",
            "parallel_tool_calls": False,
        }
        if previous_response_id:
            payload["previous_response_id"] = previous_response_id

        async with httpx.AsyncClient(timeout=45) as client:
            response = await client.post(
                f"{self.base_url}/responses",
                headers={
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": "application/json",
                },
                json=payload,
            )
            response.raise_for_status()
            return response.json()

    def get_text(self, response: dict[str, Any]) -> str:
        if response.get("output_text"):
            return str(response["output_text"])

        parts: list[str] = []
        for item in response.get("output", []):
            if item.get("type") != "message":
                continue
            for content in item.get("content", []):
                if content.get("type") in {"output_text", "text"} and content.get("text"):
                    parts.append(str(content["text"]))
        return "\n".join(parts).strip()

    def get_function_calls(self, response: dict[str, Any]) -> list[FunctionCall]:
        calls: list[FunctionCall] = []
        for item in response.get("output", []):
            if item.get("type") != "function_call":
                continue
            calls.append(
                FunctionCall(
                    call_id=str(item["call_id"]),
                    name=str(item["name"]),
                    arguments=json.loads(item.get("arguments") or "{}"),
                )
            )
        return calls
