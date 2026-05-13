import json
from typing import Any

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import select

from app.api.routes import agent as agent_route
from app.agent.orchestrator import AgentOrchestrator
from app.core.config import settings
from app.db.session import AsyncSessionLocal, init_db
from app.main import app
from app.models import ToolRun, User
from app.services.magnitgpt import MagnitGPTToolClient
from app.services.debug_sandbox import DebugSandboxClient
from app.services.external_mcp import ExternalMCPGateway
from app.services.openai_responses import OpenAIResponsesClient
from app.services.openrouter import OpenRouterToolClient
from app.tools.base import ToolExecution


class FakeOpenAIMCPClient:
    enabled = True

    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []
        self.parser = OpenAIResponsesClient()

    async def create(
        self,
        input_payload: str | list[dict[str, Any]],
        tools: list[dict[str, Any]],
        instructions: str,
        previous_response_id: str | None = None,
    ) -> dict[str, Any]:
        self.calls.append(
            {
                "input_payload": input_payload,
                "tools": tools,
                "instructions": instructions,
                "previous_response_id": previous_response_id,
            }
        )
        names = {tool["name"] for tool in tools}
        assert {"list_mcp_tools", "call_mcp_tool"}.issubset(names)
        assert "discovery -> call -> validate" in instructions

        if len(self.calls) == 1:
            assert previous_response_id is None
            assert "аналитической базе" in str(input_payload).lower()
            return {
                "id": "resp_list_tools",
                "output": [
                    {
                        "type": "function_call",
                        "call_id": "call_list_tools",
                        "name": "list_mcp_tools",
                        "arguments": json.dumps({"product": "database"}),
                    }
                ],
            }

        if len(self.calls) == 2:
            assert previous_response_id == "resp_list_tools"
            assert isinstance(input_payload, list)
            assert input_payload[0]["type"] == "function_call_output"
            assert "MCPDiscoveryTool" in input_payload[0]["output"]
            return {
                "id": "resp_query_database",
                "output": [
                    {
                        "type": "function_call",
                        "call_id": "call_database_query",
                        "name": "call_mcp_tool",
                        "arguments": json.dumps(
                            {
                                "product": "database",
                                "tool_name": "query",
                                "arguments": {
                                    "sql": "select count(*) as orders from orders",
                                },
                            }
                        ),
                    }
                ],
            }

        assert previous_response_id == "resp_query_database"
        assert isinstance(input_payload, list)
        assert "ExternalMCPTool" in input_payload[0]["output"]
        return {
            "id": "resp_final",
            "output_text": "Через MCP database проверил orders: 42742.",
            "output": [],
        }

    def get_function_calls(self, response: dict[str, Any]):
        return self.parser.get_function_calls(response)

    @staticmethod
    def get_text(response: dict[str, Any]) -> str:
        return str(response.get("output_text") or "")


class FakeOpenAISiteClient:
    enabled = True

    def __init__(self) -> None:
        self.calls = 0
        self.parser = OpenAIResponsesClient()

    async def create(
        self,
        input_payload: str | list[dict[str, Any]],
        tools: list[dict[str, Any]],
        instructions: str,
        previous_response_id: str | None = None,
    ) -> dict[str, Any]:
        self.calls += 1
        if self.calls == 1:
            return {
                "id": "resp_navigate",
                "output": [
                    {
                        "type": "function_call",
                        "call_id": "call_navigate",
                        "name": "navigate_site",
                        "arguments": json.dumps({"screen": "spark"}),
                    }
                ],
            }
        return {"id": "resp_final", "output_text": "Открыл Spark через function call.", "output": []}

    def get_function_calls(self, response: dict[str, Any]):
        return self.parser.get_function_calls(response)

    @staticmethod
    def get_text(response: dict[str, Any]) -> str:
        return str(response.get("output_text") or "")


class FakeMagnitGPTAirflowClient:
    enabled = True

    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []
        self.parser = MagnitGPTToolClient()

    async def create(
        self,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]],
        instructions: str,
    ) -> dict[str, Any]:
        self.calls.append({"messages": messages, "tools": tools, "instructions": instructions})
        names = {tool["name"] for tool in tools}
        assert "list_pipelines" in names
        assert "Не выдумывай статусы" in instructions

        if len(self.calls) == 1:
            return self._tool_response("tool_list_pipelines", "list_pipelines", {})

        assert messages[-1]["role"] == "tool"
        assert "orders_sync" in messages[-1]["content"]
        return {
            "choices": [
                {
                    "message": {
                        "role": "assistant",
                        "content": (
                            "В Airflow сейчас видны DAG: orders_sync, clickstream_aggregation, "
                            "dw_nightly_refresh, ml_feature_pipeline. Проблемный: clickstream_aggregation."
                        ),
                    }
                }
            ]
        }

    def get_function_calls(self, response: dict[str, Any]):
        return self.parser.get_function_calls(response)

    def get_text(self, response: dict[str, Any]) -> str:
        return self.parser.get_text(response)

    def assistant_message_for_history(self, response: dict[str, Any]) -> dict[str, Any]:
        return self.parser.assistant_message_for_history(response)

    def tool_message(self, call_id: str, output: str) -> dict[str, Any]:
        return self.parser.tool_message(call_id, output)

    @staticmethod
    def _tool_response(call_id: str, name: str, arguments: dict[str, Any]) -> dict[str, Any]:
        return {
            "choices": [
                {
                    "message": {
                        "role": "assistant",
                        "content": None,
                        "tool_calls": [
                            {
                                "id": call_id,
                                "type": "function",
                                "function": {
                                    "name": name,
                                    "arguments": json.dumps(arguments),
                                },
                            }
                        ],
                    }
                }
            ]
        }


class FakeOpenRouterArtifactClient:
    enabled = True

    def __init__(self, dag_code: str) -> None:
        self.calls: list[dict[str, Any]] = []
        self.dag_code = dag_code
        self.parser = OpenRouterToolClient()

    async def create(
        self,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]],
        instructions: str,
    ) -> dict[str, Any]:
        self.calls.append({"messages": messages, "tools": tools, "instructions": instructions})
        names = {tool["name"] for tool in tools}
        assert {"write_airflow_dag", "check_airflow_dag_sandbox"}.issubset(names)
        assert "После записи DAG запускай check_airflow_dag_sandbox" in instructions

        if len(self.calls) == 1:
            assert messages[-1]["role"] == "user"
            return self._tool_response(
                "tool_write_dag",
                "write_airflow_dag",
                {
                    "dag_id": "orders_sync",
                    "code": self.dag_code,
                    "message": "agent scenario test",
                },
            )

        if len(self.calls) == 2:
            assert messages[-1]["role"] == "tool"
            assert "ArtifactTool" in messages[-1]["content"]
            return self._tool_response(
                "tool_check_dag",
                "check_airflow_dag_sandbox",
                {
                    "dag_id": "orders_sync",
                    "code": self.dag_code,
                    "error_log": "",
                },
            )

        assert messages[-1]["role"] == "tool"
        assert "AirflowSandboxTool" in messages[-1]["content"]
        return {"choices": [{"message": {"role": "assistant", "content": "DAG создан и проверен в sandbox."}}]}

    def get_function_calls(self, response: dict[str, Any]):
        return self.parser.get_function_calls(response)

    def get_text(self, response: dict[str, Any]) -> str:
        return self.parser.get_text(response)

    def assistant_message_for_history(self, response: dict[str, Any]) -> dict[str, Any]:
        return self.parser.assistant_message_for_history(response)

    def tool_message(self, call_id: str, output: str) -> dict[str, Any]:
        return self.parser.tool_message(call_id, output)

    @staticmethod
    def _tool_response(call_id: str, name: str, arguments: dict[str, Any]) -> dict[str, Any]:
        return {
            "choices": [
                {
                    "message": {
                        "role": "assistant",
                        "content": None,
                        "tool_calls": [
                            {
                                "id": call_id,
                                "type": "function",
                                "function": {
                                    "name": name,
                                    "arguments": json.dumps(arguments),
                                },
                            }
                        ],
                    }
                }
            ]
        }


@pytest.mark.asyncio
async def test_openai_agent_runs_mcp_discovery_then_exact_database_tool(monkeypatch):
    monkeypatch.setattr(settings, "llm_provider", "openai")
    await init_db()

    async def fake_list_tools(self, product: str) -> ToolExecution:
        assert product == "database"
        return ToolExecution(
            tool_name="MCPDiscoveryTool",
            status="success",
            input={"product": product},
            output={
                "product": product,
                "tools": [
                    {
                        "name": "query",
                        "description": "Run a read-only SQL query",
                        "input_schema": {"type": "object", "properties": {"sql": {"type": "string"}}},
                    }
                ],
            },
            latency_ms=1,
        )

    async def fake_call_tool(
        self,
        product: str,
        tool_name: str,
        arguments: dict[str, Any],
    ) -> ToolExecution:
        assert product == "database"
        assert tool_name == "query"
        assert arguments == {"sql": "select count(*) as orders from orders"}
        return ToolExecution(
            tool_name="ExternalMCPTool",
            status="success",
            input={"product": product, "tool_name": tool_name, "arguments": arguments},
            output={
                "product": product,
                "tool_name": tool_name,
                "content": [{"type": "text", "text": '[{"orders":"42742"}]'}],
                "structured_content": None,
                "is_error": False,
            },
            latency_ms=1,
        )

    monkeypatch.setattr(ExternalMCPGateway, "list_tools", fake_list_tools)
    monkeypatch.setattr(ExternalMCPGateway, "call_tool", fake_call_tool)

    agent = AgentOrchestrator()
    agent.openai = FakeOpenAIMCPClient()

    async with AsyncSessionLocal() as session:
        user = await session.scalar(select(User).where(User.email == "admin@local.dev"))
        result = await agent.run(
            session,
            "Проверь через MCP что лежит в аналитической базе и посчитай orders",
            user,
            {"screen": "ai-agent"},
        )

    assert result.intent == "mcp"
    assert result.answer == "Через MCP database проверил orders: 42742."
    assert [call.tool_name for call in result.tool_calls] == ["MCPDiscoveryTool", "ExternalMCPTool"]
    assert result.tool_calls[1].input["tool_name"] == "query"
    assert len(agent.openai.calls) == 3


@pytest.mark.asyncio
async def test_openrouter_agent_writes_dag_then_runs_airflow_sandbox(monkeypatch, tmp_path):
    monkeypatch.setattr(settings, "llm_provider", "openrouter")
    monkeypatch.setattr(settings, "artifact_root", str(tmp_path))
    monkeypatch.setattr(settings, "artifact_git_root", str(tmp_path))
    await init_db()

    dag_code = """from datetime import datetime

from airflow import DAG


with DAG(
    dag_id="orders_sync",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    pass
"""
    captured_sandbox: dict[str, Any] = {}

    async def fake_run_artifact(self, **kwargs):
        captured_sandbox.update(kwargs)
        return {
            "sandbox": "agent-debugger",
            "runtime_status": "success",
            "runtime_returncode": 0,
            "validation_status": "valid",
            "runtime_stdout": "DAG import ok\n",
            "runtime_stderr": "",
        }

    monkeypatch.setattr(DebugSandboxClient, "run_artifact", fake_run_artifact)

    agent = AgentOrchestrator()
    agent.openrouter = FakeOpenRouterArtifactClient(dag_code)

    async with AsyncSessionLocal() as session:
        user = await session.scalar(select(User).where(User.email == "admin@local.dev"))
        result = await agent.run(session, "Напиши DAG orders_sync и проверь его", user, {"screen": "ai-agent"})

    assert result.intent == "artifact"
    assert result.answer == "DAG создан и проверен в sandbox."
    assert [call.tool_name for call in result.tool_calls] == ["ArtifactTool", "AirflowSandboxTool"]
    assert result.tool_calls[0].status == "success"
    assert result.tool_calls[0].output["artifact_name"] == "orders_sync.py"
    assert result.tool_calls[0].output["validation_status"] == "valid"
    assert result.tool_calls[1].status == "success"
    assert result.tool_calls[1].output["runtime_status"] == "success"
    assert captured_sandbox["artifact_type"] == "airflow_dag"
    assert captured_sandbox["artifact_name"] == "orders_sync.py"
    assert captured_sandbox["code"] == dag_code
    assert captured_sandbox["user_context"]["email"] == "admin@local.dev"
    assert any(action["type"] == "toast" for action in result.ui_actions)


@pytest.mark.asyncio
async def test_magnitgpt_react_agent_lists_airflow_dags_through_tool(monkeypatch):
    monkeypatch.setattr(settings, "llm_provider", "magnitgpt")
    monkeypatch.setattr(settings, "airflow_base_url", None)
    await init_db()

    agent = AgentOrchestrator()
    agent.magnitgpt = FakeMagnitGPTAirflowClient()
    async with AsyncSessionLocal() as session:
        user = await session.scalar(select(User).where(User.email == "admin@local.dev"))
        result = await agent.run(session, "Так какие даги сейчас подняты у меня?", user, {"screen": "ai-agent"})

    assert result.intent == "airflow"
    assert [call.tool_name for call in result.tool_calls] == ["AirflowTool"]
    assert result.tool_calls[0].output["pipelines"][0]["dag_id"] == "orders_sync"
    assert "orders_sync" in result.answer
    assert "clickstream_aggregation" in result.answer
    assert len(agent.magnitgpt.calls) == 2


@pytest.mark.asyncio
async def test_agent_query_api_returns_tool_calls_ui_actions_and_persists_tool_run(monkeypatch):
    monkeypatch.setattr(settings, "llm_provider", "openai")
    monkeypatch.setattr(agent_route.orchestrator, "openai", FakeOpenAISiteClient())
    await init_db()

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        login = await client.post(
            "/auth/login",
            json={"email": "admin@local.dev", "password": "admin"},
        )
        assert login.status_code == 200
        token = login.json()["access_token"]

        response = await client.post(
            "/agent/query",
            headers={"Authorization": f"Bearer {token}"},
            json={"query": "открой Spark", "app_state": {"screen": "ai-agent"}},
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["intent"] == "site-control"
    assert payload["answer"] == "Открыл Spark через function call."
    assert payload["tool_calls"][0]["tool_name"] == "SiteControlTool"
    assert payload["tool_calls"][0]["status"] == "success"
    assert payload["ui_actions"][0] == {"type": "navigate", "screen": "spark"}

    async with AsyncSessionLocal() as session:
        persisted = await session.scalar(
            select(ToolRun).where(ToolRun.message_id == payload["message_id"])
        )

    assert persisted is not None
    assert persisted.tool_name == "SiteControlTool"
    assert persisted.status == "success"
