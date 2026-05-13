import sys
import socket
from pathlib import Path

import pytest

from app.services.external_mcp import ExternalMCPGateway, ExternalMCPServerConfig


def free_port() -> int:
    with socket.socket() as sock:
        try:
            sock.bind(("127.0.0.1", 0))
        except PermissionError:
            pytest.skip("local sandbox does not allow binding test HTTP MCP ports")
        return int(sock.getsockname()[1])


@pytest.mark.asyncio
async def test_external_mcp_gateway_lists_and_calls_stdio_tools(tmp_path):
    server_path = tmp_path / "fake_mcp_server.py"
    server_path.write_text(
        """
from mcp.server.fastmcp import FastMCP

mcp = FastMCP("fake")


@mcp.tool()
def ping(text: str) -> str:
    return f"pong:{text}"


if __name__ == "__main__":
    mcp.run(transport="stdio")
""".strip(),
        encoding="utf-8",
    )
    gateway = ExternalMCPGateway(
        {
            "fake": ExternalMCPServerConfig(
                product="fake",
                command=sys.executable,
                args=[str(server_path)],
                description="test server",
            )
        }
    )

    tools = await gateway.list_tools("fake")
    result = await gateway.call_tool("fake", "ping", {"text": "ok"})

    assert tools.status == "success"
    assert tools.output["tools"][0]["name"] == "ping"
    assert result.status == "success"
    assert result.output["content"][0]["text"] == "pong:ok"


@pytest.mark.asyncio
async def test_external_mcp_gateway_lists_and_calls_streamable_http_tools(tmp_path):
    port = free_port()
    server_path = tmp_path / "fake_http_mcp_server.py"
    server_path.write_text(
        f"""
from mcp.server.fastmcp import FastMCP

mcp = FastMCP("fake-http", host="127.0.0.1", port={port}, streamable_http_path="/mcp", log_level="ERROR")


@mcp.tool()
def ping(text: str) -> str:
    return f"pong:{{text}}"


if __name__ == "__main__":
    mcp.run(transport="streamable-http")
""".strip(),
        encoding="utf-8",
    )
    gateway = ExternalMCPGateway(
        {
            "fake_http": ExternalMCPServerConfig(
                product="fake_http",
                command=sys.executable,
                args=[str(server_path)],
                description="test http server",
                transport="streamable_http",
                url=f"http://127.0.0.1:{port}/mcp",
                startup_timeout_seconds=10,
            )
        }
    )

    tools = await gateway.list_tools("fake_http")
    result = await gateway.call_tool("fake_http", "ping", {"text": "ok"})

    assert tools.status == "success"
    assert tools.output["tools"][0]["name"] == "ping"
    assert result.status == "success"
    assert result.output["content"][0]["text"] == "pong:ok"


def test_external_mcp_gateway_default_products():
    products = set(ExternalMCPGateway().available_products())

    assert {
        "database",
        "airflow",
        "spark",
        "artifacts_git",
        "artifacts_filesystem",
    }.issubset(products)
