from typing import Any

import httpx

from app.core.config import settings


class DebugSandboxClient:
    @property
    def enabled(self) -> bool:
        return bool(settings.agent_debugger_url)

    async def debug_python(self, code: str, error_log: str = "") -> dict[str, Any] | None:
        return await self.run_artifact("python", "artifact.py", code, error_log)

    async def run_artifact(
        self,
        artifact_type: str,
        artifact_name: str,
        code: str,
        error_log: str = "",
        arguments: list[str] | None = None,
        user_context: dict[str, str] | None = None,
    ) -> dict[str, Any] | None:
        if not settings.agent_debugger_url:
            return None

        async with httpx.AsyncClient(timeout=45) as client:
            response = await client.post(
                f"{settings.agent_debugger_url.rstrip('/')}/debug/python",
                json={
                    "artifact_type": artifact_type,
                    "artifact_name": artifact_name,
                    "code": code,
                    "error_log": error_log,
                    "arguments": arguments or [],
                    "user_id": (user_context or {}).get("id", "anonymous"),
                    "user_email": (user_context or {}).get("email", ""),
                    "user_role": (user_context or {}).get("role", ""),
                },
            )
            response.raise_for_status()
            return response.json()
