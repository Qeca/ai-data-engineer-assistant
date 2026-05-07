from __future__ import annotations

import asyncio
import json
import os
import signal
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any, Literal
from urllib.parse import urlparse

from mcp import ClientSession
from mcp.client.stdio import StdioServerParameters, stdio_client
from mcp.client.streamable_http import streamablehttp_client

from app.core.config import settings
from app.tools.base import ToolExecution


@dataclass(frozen=True)
class ExternalMCPServerConfig:
    product: str
    command: str
    args: list[str] = field(default_factory=list)
    env: dict[str, str] = field(default_factory=dict)
    description: str = ""
    transport: Literal["stdio", "streamable_http"] = "stdio"
    url: str | None = None
    startup_timeout_seconds: float = 30.0


class ExternalMCPGateway:
    def __init__(self, configs: dict[str, ExternalMCPServerConfig] | None = None) -> None:
        self.configs = configs or self.default_configs()

    def available_products(self) -> list[str]:
        return sorted(self.configs)

    def product_specs(self) -> list[dict[str, Any]]:
        return [
            {
                "product": config.product,
                "command": config.command,
                "args": config.args,
                "transport": config.transport,
                "url": config.url,
                "description": config.description,
            }
            for config in self.configs.values()
        ]

    async def list_tools(self, product: str) -> ToolExecution:
        started = time.perf_counter()
        config = self._config(product)
        if config is None:
            return self._error("MCPDiscoveryTool", {"product": product}, started, f"Unknown MCP product: {product}")

        try:
            async with self._connect(config) as (read_stream, write_stream):
                async with ClientSession(read_stream, write_stream) as session:
                    await session.initialize()
                    result = await session.list_tools()
            return ToolExecution(
                tool_name="MCPDiscoveryTool",
                status="success",
                input={"product": product},
                output={
                    "product": product,
                    "tools": [
                        {
                            "name": tool.name,
                            "description": tool.description,
                            "input_schema": tool.inputSchema,
                        }
                        for tool in result.tools
                    ],
                },
                latency_ms=self._elapsed_ms(started),
            )
        except Exception as exc:  # noqa: BLE001
            return self._error("MCPDiscoveryTool", {"product": product}, started, str(exc), exc.__class__.__name__)

    async def call_tool(self, product: str, tool_name: str, arguments: dict[str, Any]) -> ToolExecution:
        started = time.perf_counter()
        config = self._config(product)
        if config is None:
            return self._error("ExternalMCPTool", {"product": product, "tool_name": tool_name}, started, f"Unknown MCP product: {product}")

        try:
            async with self._connect(config) as (read_stream, write_stream):
                async with ClientSession(read_stream, write_stream) as session:
                    await session.initialize()
                    result = await session.call_tool(tool_name, arguments)
            return ToolExecution(
                tool_name="ExternalMCPTool",
                status="success",
                input={"product": product, "tool_name": tool_name, "arguments": arguments},
                output={
                    "product": product,
                    "tool_name": tool_name,
                    "content": [item.model_dump() for item in result.content],
                    "structured_content": result.structuredContent,
                    "is_error": result.isError,
                },
                latency_ms=self._elapsed_ms(started),
                error="mcp_tool_error" if result.isError else None,
            )
        except Exception as exc:  # noqa: BLE001
            return self._error(
                "ExternalMCPTool",
                {"product": product, "tool_name": tool_name, "arguments": arguments},
                started,
                str(exc),
                exc.__class__.__name__,
            )

    def _server_params(self, config: ExternalMCPServerConfig) -> StdioServerParameters:
        return StdioServerParameters(
            command=config.command,
            args=config.args,
            env={**os.environ, **config.env},
        )

    @asynccontextmanager
    async def _connect(self, config: ExternalMCPServerConfig) -> AsyncIterator[tuple[Any, Any]]:
        if config.transport == "stdio":
            async with stdio_client(self._server_params(config)) as (read_stream, write_stream):
                yield read_stream, write_stream
            return

        if config.url is None:
            raise ValueError(f"MCP product {config.product} requires url for streamable_http transport")

        process = await self._start_http_server(config)
        try:
            async with streamablehttp_client(config.url) as (read_stream, write_stream, _get_session_id):
                yield read_stream, write_stream
        finally:
            await self._stop_process(process)

    async def _start_http_server(self, config: ExternalMCPServerConfig) -> asyncio.subprocess.Process | None:
        process: asyncio.subprocess.Process | None = None
        if config.command:
            process = await asyncio.create_subprocess_exec(
                config.command,
                *config.args,
                env={**os.environ, **config.env},
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL,
                start_new_session=True,
            )

        await self._wait_for_http_port(config)
        return process

    async def _wait_for_http_port(self, config: ExternalMCPServerConfig) -> None:
        if config.url is None:
            raise ValueError(f"MCP product {config.product} requires url")

        parsed = urlparse(config.url)
        host = parsed.hostname or "127.0.0.1"
        port = parsed.port or (443 if parsed.scheme == "https" else 80)
        deadline = time.perf_counter() + config.startup_timeout_seconds
        while time.perf_counter() < deadline:
            try:
                reader, writer = await asyncio.open_connection(host, port)
                writer.close()
                await writer.wait_closed()
                return
            except OSError:
                await asyncio.sleep(0.25)

        raise TimeoutError(f"MCP product {config.product} did not open {host}:{port}")

    @staticmethod
    async def _stop_process(process: asyncio.subprocess.Process | None) -> None:
        if process is None or process.returncode is not None:
            return

        try:
            os.killpg(process.pid, signal.SIGTERM)
        except OSError:
            process.terminate()

        try:
            await asyncio.wait_for(process.wait(), timeout=5)
            return
        except asyncio.TimeoutError:
            pass

        try:
            os.killpg(process.pid, signal.SIGKILL)
        except OSError:
            process.kill()
        await process.wait()

    def _config(self, product: str) -> ExternalMCPServerConfig | None:
        return self.configs.get(product)

    @classmethod
    def default_configs(cls) -> dict[str, ExternalMCPServerConfig]:
        return {
            "database": cls._database_config(),
            "airflow": cls._airflow_config(),
            "spark": cls._spark_config(),
            "artifacts_git": cls._artifacts_git_config(),
            "artifacts_filesystem": cls._artifacts_filesystem_config(),
        }

    @staticmethod
    def _database_config() -> ExternalMCPServerConfig:
        return ExternalMCPServerConfig(
            product="database",
            command=settings.mcp_npx_command,
            args=[
                "-y",
                "@modelcontextprotocol/server-postgres",
                settings.mcp_database_url or ExternalMCPGateway._sync_database_url(),
            ],
            description="Official/reference PostgreSQL MCP server with schema and read-only query tools.",
        )

    @staticmethod
    def _airflow_config() -> ExternalMCPServerConfig:
        return ExternalMCPServerConfig(
            product="airflow",
            command=settings.mcp_uvx_command,
            args=["astro-airflow-mcp", "--transport", "stdio"],
            env={
                "AIRFLOW_API_URL": settings.airflow_base_url or "http://localhost:8080",
                "AIRFLOW_USERNAME": settings.airflow_username,
                "AIRFLOW_PASSWORD": settings.airflow_password,
            },
            description="Astronomer astro-airflow-mcp server for Airflow DAGs, runs, logs, and health.",
        )

    @staticmethod
    def _spark_config() -> ExternalMCPServerConfig:
        return ExternalMCPServerConfig(
            product="spark",
            command=settings.mcp_uvx_command,
            args=[
                "pyspark-mcp",
                "--master",
                settings.mcp_spark_master,
                "--host",
                "127.0.0.1",
                "--port",
                "8090",
            ],
            description="pyspark-mcp HTTP MCP server for Spark catalog and query plan inspection.",
            transport="streamable_http",
            url="http://127.0.0.1:8090/mcp",
            startup_timeout_seconds=300.0,
        )

    @staticmethod
    def _artifacts_git_config() -> ExternalMCPServerConfig:
        return ExternalMCPServerConfig(
            product="artifacts_git",
            command=settings.mcp_uvx_command,
            args=[
                "mcp-server-git",
                "--repository",
                settings.artifact_git_root or settings.artifact_root,
            ],
            description="Reference Git MCP server for artifact Git history and repository operations.",
        )

    @staticmethod
    def _artifacts_filesystem_config() -> ExternalMCPServerConfig:
        return ExternalMCPServerConfig(
            product="artifacts_filesystem",
            command=settings.mcp_npx_command,
            args=["-y", "@modelcontextprotocol/server-filesystem", settings.artifact_root],
            description="Reference filesystem MCP server restricted to the artifact root.",
        )

    @staticmethod
    def _sync_database_url() -> str:
        if settings.database_url.startswith("postgresql+asyncpg://"):
            return settings.database_url.replace("postgresql+asyncpg://", "postgresql://", 1)
        return settings.database_url

    @staticmethod
    def _error(
        tool_name: str,
        args: dict[str, Any],
        started: float,
        message: str,
        error: str = "mcp_error",
    ) -> ToolExecution:
        return ToolExecution(
            tool_name=tool_name,
            status="error",
            input=args,
            output={"error": message},
            latency_ms=ExternalMCPGateway._elapsed_ms(started),
            error=error,
        )

    @staticmethod
    def _elapsed_ms(started: float) -> int:
        return max(1, int((time.perf_counter() - started) * 1000))

    @staticmethod
    def output_as_json(execution: ToolExecution) -> str:
        return json.dumps(execution.output, ensure_ascii=False, default=str)
