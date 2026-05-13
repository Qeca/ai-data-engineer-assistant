import json

from httpx import ASGITransport, AsyncClient
import pytest
from sqlalchemy import select

from app.agent.orchestrator import AgentOrchestrator
from app.api.routes import agent as agent_route
from app.core.config import settings
from app.db.session import AsyncSessionLocal, init_db
from app.main import app
from app.models import User


def test_agent_uses_langgraph_tool_loop():
    agent = AgentOrchestrator()
    assert hasattr(agent.graph, "ainvoke")
    assert "call_model" in agent.graph.nodes
    assert "execute_tools" in agent.graph.nodes
    assert "local_tool_plan" not in agent.graph.nodes


def test_agent_instructions_train_model_for_mcp_tool_loop():
    instructions = AgentOrchestrator()._instructions()

    assert "MCP PLAYBOOK" in instructions
    assert "discovery -> call -> validate" in instructions
    assert "сначала вызови list_mcp_tools" in instructions
    assert "не вызывай call_mcp_tool без discovery" in instructions
    assert "database: use for PostgreSQL" in instructions
    assert "airflow: use for Airflow" in instructions
    assert "spark: use for Spark" in instructions
    assert "artifacts_git: use for Git status" in instructions
    assert "filesystem MCP используй только" in instructions


class FakeOpenAIClient:
    enabled = True

    def __init__(self):
        self.calls = 0

    async def create(self, input_payload, tools, instructions, previous_response_id=None):
        self.calls += 1
        if self.calls == 1:
            return {
                "id": "resp_1",
                "output": [
                    {
                        "type": "function_call",
                        "call_id": "call_1",
                        "name": "navigate_site",
                        "arguments": '{"screen":"spark"}',
                    }
                ],
            }
        return {"id": "resp_2", "output_text": "Открыл Spark экран через function call.", "output": []}

    def get_function_calls(self, response):
        return OpenAILikeParser().get_function_calls(response)

    def get_text(self, response):
        return response.get("output_text", "")


class OpenAILikeParser:
    def get_function_calls(self, response):
        from app.services.openai_responses import OpenAIResponsesClient

        return OpenAIResponsesClient().get_function_calls(response)


@pytest.mark.asyncio
async def test_openai_function_calling_can_control_site(monkeypatch):
    monkeypatch.setattr(settings, "llm_provider", "openai")
    await init_db()
    agent = AgentOrchestrator()
    agent.openai = FakeOpenAIClient()

    async with AsyncSessionLocal() as session:
        user = await session.scalar(select(User).where(User.email == "admin@local.dev"))
        result = await agent.run(session, "Открой Spark", user, {"screen": "ai-agent"})

    assert result.intent == "site-control"
    assert result.answer == "Открыл Spark экран через function call."
    assert result.tool_calls[0].tool_name == "SiteControlTool"
    assert result.ui_actions == [
        {"type": "navigate", "screen": "spark"},
        {"type": "toast", "message": "Открываю экран spark"},
    ]


@pytest.mark.asyncio
async def test_agent_stream_emits_intermediate_tool_events(monkeypatch):
    monkeypatch.setattr(settings, "llm_provider", "openai")
    monkeypatch.setattr(agent_route.orchestrator, "openai", FakeOpenAIClient())
    await init_db()
    transport = ASGITransport(app=app)

    async with AsyncClient(transport=transport, base_url="http://test") as client:
        login = await client.post(
            "/auth/login",
            json={"email": "admin@local.dev", "password": "admin"},
        )
        token = login.json()["access_token"]
        async with client.stream(
            "POST",
            "/agent/query/stream",
            headers={"Authorization": f"Bearer {token}"},
            json={"query": "Открой Spark", "app_state": {"screen": "ai-agent"}},
        ) as response:
            assert response.status_code == 200
            events = [json.loads(line) async for line in response.aiter_lines() if line]
        messages = await client.get(
            f"/sessions/{events[0]['session_id']}/messages",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert [event["type"] for event in events] == [
        "session",
        "tool_call_start",
        "tool_call_result",
        "final",
    ]
    assert events[1]["tool_name"] == "navigate_site"
    assert events[2]["tool_call"]["tool_name"] == "SiteControlTool"
    assert events[3]["response"]["answer"] == "Открыл Spark экран через function call."
    assert messages.status_code == 200
    persisted = messages.json()
    assert [message["role"] for message in persisted[-3:]] == ["user", "tool", "assistant"]
    assert persisted[-2]["metadata_json"]["tool_call"]["tool_name"] == "SiteControlTool"


@pytest.mark.asyncio
async def test_agent_without_llm_returns_configuration_error(monkeypatch):
    monkeypatch.setattr(settings, "llm_provider", "magnitgpt")
    monkeypatch.setattr(settings, "magnitgpt_api_key", None)
    await init_db()
    agent = AgentOrchestrator()

    async with AsyncSessionLocal() as session:
        user = await session.scalar(select(User).where(User.email == "admin@local.dev"))
        result = await agent.run(session, "что вообще лежит в базенке", user, {"screen": "ai-agent"})

    assert result.intent == "configuration-error"
    assert result.tool_calls == []
    assert "Rule-based fallback отключен" in result.answer
