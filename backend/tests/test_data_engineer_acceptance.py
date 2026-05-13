import json
from typing import Any

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import select, text

from app.agent.orchestrator import AgentOrchestrator
from app.api.routes import agent as agent_route
from app.core.config import settings
from app.db.session import AsyncSessionLocal, init_db
from app.main import app
from app.models import Message, SparkJob, ToolRun, User
from app.services.magnitgpt import MagnitGPTToolClient
from app.tools.airflow import AirflowTool
from app.tools.catalog import CatalogTool
from app.tools.spark import SparkTool
from app.tools.sql import SQLTool


class FakeDataEngineerInvestigationClient:
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
        tool_names = {tool["name"] for tool in tools}
        assert {"inspect_database", "execute_sql"}.issubset(tool_names)
        assert "Используй OpenAI function calling" in instructions
        assert "Rule-based" not in instructions

        if len(self.calls) == 1:
            return self._tool_response(
                "call_inspect_database",
                "inspect_database",
                {"sample_limit": 1},
            )

        if len(self.calls) == 2:
            tool_payload = json.loads(messages[-1]["content"])
            assert tool_payload["tool_name"] == "DatabaseInspectorTool"
            assert any(table["name"] == "orders" for table in tool_payload["output"]["tables"])
            return self._tool_response(
                "call_anomaly_query",
                "execute_sql",
                {
                    "query": """
SELECT
  strftime('%Y-%m-%d %H:00:00', created_at) AS hour,
  COUNT(*) AS order_count,
  ROUND(AVG(total_amount), 2) AS avg_amount
FROM orders
GROUP BY 1
ORDER BY order_count DESC
LIMIT 1
""".strip(),
                    "limit": 1,
                },
            )

        tool_payload = json.loads(messages[-1]["content"])
        assert tool_payload["tool_name"] == "SQLTool"
        row = tool_payload["output"]["rows"][0]
        return {
            "choices": [
                {
                    "message": {
                        "role": "assistant",
                        "content": (
                            f"Пик заказов найден через SQL: {row['hour']} — "
                            f"{row['order_count']} заказов, средний чек {row['avg_amount']}."
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


class FakeOperationalRunbookClient:
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
        tool_names = {tool["name"] for tool in tools}
        assert {"list_site_status", "submit_spark_job", "get_spark_job"}.issubset(tool_names)

        if len(self.calls) == 1:
            return self._tool_response("call_site_status", "list_site_status", {})

        if len(self.calls) == 2:
            site_status = json.loads(messages[-1]["content"])
            assert site_status["tool_name"] == "SiteStatusTool"
            assert "orders" in site_status["output"]["catalog"]["tables"]
            return self._tool_response(
                "call_submit_spark",
                "submit_spark_job",
                {
                    "name": "de_acceptance_quality_job",
                    "app_resource": "local:///opt/spark/jobs/sample_job.py",
                    "executor_memory": "1g",
                    "partitions": 8,
                },
            )

        if len(self.calls) == 3:
            spark_submit = json.loads(messages[-1]["content"])
            assert spark_submit["tool_name"] == "SparkTool"
            job_id = spark_submit["output"]["job_id"]
            return self._tool_response("call_get_spark", "get_spark_job", {"job_id": job_id})

        spark_status = json.loads(messages[-1]["content"])
        assert spark_status["output"]["status"] == "success"
        return {
            "choices": [
                {
                    "message": {
                        "role": "assistant",
                        "content": (
                            "Проверил платформу и запустил Spark smoke job: "
                            f"{spark_status['output']['job_id']} завершился success."
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
        return FakeDataEngineerInvestigationClient._tool_response(call_id, name, arguments)


@pytest.mark.asyncio
async def test_demo_warehouse_quality_contract_is_data_engineer_ready(monkeypatch):
    monkeypatch.setattr(settings, "airflow_base_url", None)
    await init_db()

    quality_query = """
SELECT
  COUNT(*) AS total_orders,
  COUNT(DISTINCT o.id) AS distinct_orders,
  SUM(CASE WHEN o.total_amount <= 0 THEN 1 ELSE 0 END) AS non_positive_amounts,
  SUM(CASE WHEN o.status NOT IN ('paid', 'cancelled') THEN 1 ELSE 0 END) AS invalid_statuses,
  SUM(CASE WHEN c.id IS NULL THEN 1 ELSE 0 END) AS orphan_customers
FROM orders o
LEFT JOIN customers c ON c.id = o.user_id
""".strip()

    async with AsyncSessionLocal() as session:
        result = await SQLTool().execute(session, quality_query, limit=1)

    assert result.status == "success"
    row = result.output["rows"][0]
    assert row["total_orders"] >= 5000
    assert row["total_orders"] == row["distinct_orders"]
    assert row["non_positive_amounts"] == 0
    assert row["invalid_statuses"] == 0
    assert row["orphan_customers"] == 0


@pytest.mark.asyncio
async def test_sql_anomaly_and_catalog_tools_profile_orders_like_de_checks(monkeypatch):
    monkeypatch.setattr(settings, "airflow_base_url", None)
    await init_db()

    async with AsyncSessionLocal() as session:
        catalog = await CatalogTool().inspect_database(session, sample_limit=2)
        anomaly = await SQLTool().execute(session, SQLTool().anomaly_query(), limit=10)
        blocked_write = await SQLTool().execute(
            session,
            "select * from orders; delete from orders",
            limit=10,
        )
        order_count_after_blocked_write = await session.scalar(text("SELECT COUNT(*) FROM orders"))

    assert catalog.status == "success"
    tables = {table["name"]: table for table in catalog.output["tables"]}
    assert {"orders", "customers", "events"}.issubset(tables)
    assert tables["orders"]["row_count"] >= 5000
    assert {"id", "created_at", "user_id", "total_amount", "status"}.issubset(
        {column["name"] for column in tables["orders"]["columns"]}
    )
    assert len(tables["orders"]["sample_rows"]) == 2

    assert anomaly.status == "success"
    assert anomaly.output["row_count"] == 10
    peak = anomaly.output["rows"][0]
    assert peak["order_count"] >= 900
    assert peak["avg_amount"] > 0

    assert blocked_write.status == "error"
    assert blocked_write.error == "read_only_violation"
    assert order_count_after_blocked_write == tables["orders"]["row_count"]


@pytest.mark.asyncio
async def test_airflow_topology_and_spark_lifecycle_are_operational_metadata(monkeypatch):
    monkeypatch.setattr(settings, "airflow_base_url", None)
    await init_db()

    async with AsyncSessionLocal() as session:
        airflow = AirflowTool()
        pipelines = await airflow.list_pipelines(session)
        by_id = {pipeline.dag_id: pipeline for pipeline in pipelines}
        orders_tasks = {task["task_id"]: task for task in by_id["orders_sync"].tasks}

        paused = await airflow.manage_dags(session, "pause_all", "")
        unpaused = await airflow.manage_dags(session, "unpause_all", "")
        run = await airflow.trigger_run(session, "orders_sync", {"source": "acceptance_test"})
        run_status = await airflow.get_run(session, "orders_sync", run.run_id)

        spark = SparkTool()
        submitted_job = await spark.submit_job(
            session,
            "acceptance_hourly_anomaly_job",
            "local:///opt/spark/jobs/sample_job.py",
            {"partitions": 8, "source": "acceptance_test"},
        )
        finished_job = await spark.get_job(session, submitted_job.job_id)
        persisted_job = await session.scalar(select(SparkJob).where(SparkJob.job_id == submitted_job.job_id))

    assert list(by_id) == ["orders_sync", "ml_feature_pipeline", "clickstream_aggregation", "dw_nightly_refresh"]
    assert orders_tasks["extract_orders"]["downstream_task_ids"] == ["validate_orders"]
    assert orders_tasks["validate_orders"]["downstream_task_ids"] == ["load_orders"]
    assert by_id["clickstream_aggregation"].status == "failed"

    assert paused.output["affected_count"] == 4
    assert all(pipeline["status"] == "paused" for pipeline in paused.output["pipelines"])
    assert unpaused.output["affected_count"] == 4
    assert all(pipeline["status"] != "paused" for pipeline in unpaused.output["pipelines"])

    assert run.status == "queued"
    assert run_status.status == "running"
    assert finished_job.status == "success"
    assert finished_job.result_sample
    assert persisted_job is not None
    assert persisted_job.status == "success"


@pytest.mark.asyncio
async def test_react_agent_performs_real_database_investigation_with_tools(monkeypatch):
    monkeypatch.setattr(settings, "llm_provider", "magnitgpt")
    monkeypatch.setattr(settings, "airflow_base_url", None)
    await init_db()

    agent = AgentOrchestrator()
    agent.magnitgpt = FakeDataEngineerInvestigationClient()

    async with AsyncSessionLocal() as session:
        user = await session.scalar(select(User).where(User.email == "admin@local.dev"))
        result = await agent.run(
            session,
            "Проанализируй orders за 30 дней и найди самый аномальный час",
            user,
            {"screen": "ai-agent"},
        )

    assert result.intent == "sql"
    assert [call.tool_name for call in result.tool_calls] == ["DatabaseInspectorTool", "SQLTool"]
    assert result.tool_calls[0].output["table_count"] >= 3
    assert result.tool_calls[1].status == "success"
    assert result.tool_calls[1].output["rows"][0]["order_count"] >= 900
    assert "Пик заказов найден через SQL" in result.answer
    assert len(agent.magnitgpt.calls) == 3


@pytest.mark.asyncio
async def test_agent_api_persists_de_runbook_messages_tool_runs_and_spark_result(monkeypatch):
    monkeypatch.setattr(settings, "llm_provider", "magnitgpt")
    monkeypatch.setattr(settings, "airflow_base_url", None)
    monkeypatch.setattr(agent_route.orchestrator, "magnitgpt", FakeOperationalRunbookClient())
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
            json={
                "query": "Проверь платформу как дата инженер и запусти Spark smoke job",
                "app_state": {"screen": "ai-agent"},
            },
        )

        assert response.status_code == 200
        payload = response.json()
        messages = await client.get(
            f"/sessions/{payload['session_id']}/messages",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert payload["intent"] == "spark"
    assert [call["tool_name"] for call in payload["tool_calls"]] == [
        "SiteStatusTool",
        "SparkTool",
        "SparkTool",
    ]
    assert payload["tool_calls"][-1]["output"]["status"] == "success"
    assert "Spark smoke job" in payload["answer"]

    assert messages.status_code == 200
    persisted_messages = messages.json()
    assert [message["role"] for message in persisted_messages[-2:]] == ["user", "assistant"]
    assert persisted_messages[-1]["content"] == payload["answer"]

    async with AsyncSessionLocal() as session:
        tool_runs = (
            await session.scalars(
                select(ToolRun).where(ToolRun.message_id == payload["message_id"]).order_by(ToolRun.created_at)
            )
        ).all()
        assistant_message = await session.get(Message, payload["message_id"])

    assert assistant_message is not None
    assert [tool.tool_name for tool in tool_runs] == ["SiteStatusTool", "SparkTool", "SparkTool"]
    assert tool_runs[-1].output_json["status"] == "success"
