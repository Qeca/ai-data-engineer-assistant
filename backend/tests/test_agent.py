from __future__ import annotations

import pytest
from sqlalchemy import select

from app.agent.orchestrator import AgentOrchestrator
from app.core.config import settings
from app.db.session import AsyncSessionLocal, init_db
from app.models import User
from app.tools.airflow import AirflowTool


def test_agent_intent_routing():
    agent = AgentOrchestrator()
    assert hasattr(agent.graph, "ainvoke")
    assert agent.classify("проанализируй orders за 30 дней") == "sql"
    assert agent.classify("запусти DAG orders_sync") == "airflow"
    assert agent.classify("отправь Spark job для clickstream") == "spark"
    assert agent.classify("покажи каталог таблиц") == "catalog"
    assert agent.classify("покажи все статусы") == "site"
    assert agent.classify("открой Spark") == "navigate:spark"
    assert agent.classify("что вообще лежит в базенке") == "database"
    assert agent.classify("открой даги") == "navigate:airflow"
    assert agent.classify("останови все даги") == "airflow_control"
    assert agent.classify("создай DAG orders_sync") == "artifact_airflow"
    assert agent.classify("напиши Spark скрипт") == "artifact_spark"
    assert agent.classify("покажи историю версий DAG") == "versioning"
    assert agent.classify("отладь Python traceback") == "debug"
    assert agent.classify("какие MCP продукты доступны") == "mcp"


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
async def test_openai_function_calling_can_control_site():
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
async def test_local_fallback_uses_full_tools_for_user_scenarios(monkeypatch):
    monkeypatch.setattr(settings, "llm_provider", "local")
    monkeypatch.setattr(settings, "airflow_base_url", None)
    await init_db()
    agent = AgentOrchestrator()

    async with AsyncSessionLocal() as session:
        user = await session.scalar(select(User).where(User.email == "admin@local.dev"))

        database = await agent.run(session, "что вообще лежит в базенке", user, {"screen": "ai-agent"})
        navigation = await agent.run(session, "открой даги", user, {"screen": "ai-agent"})
        airflow = await agent.run(session, "останови все даги", user, {"screen": "ai-agent"})
        mcp = await agent.run(session, "какие MCP продукты доступны", user, {"screen": "ai-agent"})
        await AirflowTool().manage_dags(session, "unpause_all", "")

    assert database.intent == "database"
    assert database.tool_calls[0].tool_name == "DatabaseInspectorTool"
    assert database.tool_calls[0].output["table_count"] >= 3

    assert navigation.intent == "site-control"
    assert navigation.tool_calls[0].tool_name == "SiteControlTool"
    assert navigation.ui_actions[0] == {"type": "navigate", "screen": "airflow"}

    assert airflow.intent == "airflow-control"
    assert airflow.tool_calls[0].tool_name == "AirflowControlTool"
    assert airflow.tool_calls[0].output["affected_count"] == 4

    assert mcp.intent == "mcp"
    assert mcp.tool_calls[0].tool_name == "MCPDiscoveryTool"
    assert "database" in mcp.answer
