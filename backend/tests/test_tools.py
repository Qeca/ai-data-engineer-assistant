import pytest

from app.core.config import settings
from app.db.session import AsyncSessionLocal, init_db
from app.tools.airflow import AirflowTool
from app.tools.spark import SparkTool
from app.tools.sql import SQLTool


class FakeAirflowResponse:
    def __init__(self, payload: dict) -> None:
        self.payload = payload
        self.headers = {"content-type": "application/json"}
        self.text = str(payload)

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict:
        return self.payload


class FakeAirflowClient:
    def __init__(self, *args, **kwargs) -> None:
        self.args = args
        self.kwargs = kwargs

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, traceback) -> None:
        return None

    async def get(self, url: str, **kwargs) -> FakeAirflowResponse:
        if url.endswith("/api/v1/dags"):
            return FakeAirflowResponse(
                {
                    "dags": [
                        {
                            "dag_id": "remote_orders",
                            "dag_display_name": "Remote Orders",
                            "is_active": True,
                            "is_paused": False,
                            "has_import_errors": False,
                            "owners": ["data-team"],
                            "schedule_interval": {"value": "@hourly"},
                            "next_dagrun": "2026-05-11T12:00:00+00:00",
                        },
                        {
                            "dag_id": "remote_clickstream",
                            "is_active": True,
                            "is_paused": True,
                            "has_import_errors": False,
                            "owners": ["analytics"],
                            "schedule_interval": None,
                        },
                    ]
                }
            )
        if url.endswith("/api/v1/dags/remote_orders/tasks"):
            return FakeAirflowResponse(
                {
                    "tasks": [
                        {
                            "task_id": "extract_orders",
                            "operator_name": "EmptyOperator",
                            "downstream_task_ids": ["load_orders"],
                        },
                        {
                            "task_id": "load_orders",
                            "operator_name": "EmptyOperator",
                            "downstream_task_ids": [],
                        },
                    ]
                }
            )
        if url.endswith("/api/v1/dags/remote_clickstream/tasks"):
            return FakeAirflowResponse(
                {
                    "tasks": [
                        {
                            "task_id": "aggregate_clickstream",
                            "operator_name": "BashOperator",
                            "downstream_task_ids": [],
                        }
                    ]
                }
            )
        if url.endswith("/api/v1/dags/remote_orders/dagRuns/run_1/taskInstances"):
            return FakeAirflowResponse(
                {
                    "task_instances": [
                        {
                            "dag_id": "remote_orders",
                            "dag_run_id": "run_1",
                            "task_id": "extract_orders",
                            "state": "success",
                            "try_number": 1,
                            "operator": "EmptyOperator",
                            "start_date": "2026-05-11T11:00:00+00:00",
                            "end_date": "2026-05-11T11:00:01+00:00",
                            "duration": 1.0,
                        }
                    ]
                }
            )
        if url.endswith("/api/v1/dags/remote_orders/dagRuns/run_1/taskInstances/extract_orders/logs/1"):
            return FakeAirflowResponse({"content": "task log ok"})
        if url.endswith("/api/v1/dags/remote_orders/dagRuns") and kwargs.get("params", {}).get("limit") == 1:
            return FakeAirflowResponse({"dag_runs": [{"state": "success", "end_date": "2026-05-11T11:00:00+00:00"}]})
        if url.endswith("/api/v1/dags/remote_orders/dagRuns"):
            return FakeAirflowResponse(
                {
                    "dag_runs": [
                        {
                            "dag_id": "remote_orders",
                            "dag_run_id": "run_1",
                            "state": "success",
                        }
                    ]
                }
            )
        if "/dagRuns/" in url:
            return FakeAirflowResponse({"state": "success"})
        return FakeAirflowResponse({"dag_runs": [{"state": "success", "end_date": "2026-05-11T11:00:00+00:00"}]})


def test_sql_tool_blocks_write_queries():
    assert SQLTool._is_read_only("select * from orders")
    assert not SQLTool._is_read_only("drop table orders")
    assert not SQLTool._is_read_only("select * from orders; delete from orders")


@pytest.mark.asyncio
async def test_sql_tool_executes_demo_query():
    await init_db()
    async with AsyncSessionLocal() as session:
        tool = SQLTool()
        result = await tool.execute(session, tool.anomaly_query(), limit=5)

    assert result.status == "success"
    assert result.output["row_count"] > 0
    assert "order_count" in result.output["columns"]


@pytest.mark.asyncio
async def test_airflow_and_spark_tools_persist_metadata():
    await init_db()
    async with AsyncSessionLocal() as session:
        airflow_run = await AirflowTool().trigger_run(session, "orders_sync", {"test": True})
        spark_job = await SparkTool().submit_job(
            session,
            "unit_test_job",
            "local:///opt/spark/jobs/sample_job.py",
            {"partitions": 4},
        )
        spark_jobs = await SparkTool().list_jobs(session)

    assert airflow_run.dag_id == "orders_sync"
    assert airflow_run.status in {"queued", "running", "success"}
    assert spark_job.status == "running"
    assert spark_job.result_sample
    assert any(job.job_id == spark_job.job_id for job in spark_jobs)


@pytest.mark.asyncio
async def test_airflow_control_tool_persists_pause_state(monkeypatch):
    monkeypatch.setattr(settings, "airflow_base_url", None)
    await init_db()
    async with AsyncSessionLocal() as session:
        tool = AirflowTool()
        result = await tool.manage_dags(session, "pause_all", "")
        paused = await tool.list_pipelines(session)
        await tool.manage_dags(session, "unpause_all", "")

    assert result.tool_name == "AirflowControlTool"
    assert result.status == "success"
    assert result.output["affected_count"] == 4
    assert all(pipeline.status == "paused" for pipeline in paused)


@pytest.mark.asyncio
async def test_airflow_tool_reads_remote_airflow_api(monkeypatch):
    monkeypatch.setattr(settings, "airflow_base_url", "http://airflow.test")
    monkeypatch.setattr("app.tools.airflow.httpx.AsyncClient", FakeAirflowClient)

    pipelines = await AirflowTool().list_pipelines()

    assert [pipeline.dag_id for pipeline in pipelines] == ["remote_clickstream", "remote_orders"]
    assert pipelines[0].status == "paused"
    assert pipelines[1].status == "active"
    assert pipelines[1].schedule == "@hourly"
    assert pipelines[1].last_run == "success 2026-05-11T11:00:00+00:00"
    assert pipelines[1].tasks[0]["task_id"] == "extract_orders"


@pytest.mark.asyncio
async def test_airflow_get_run_syncs_remote_status(monkeypatch):
    monkeypatch.setattr(settings, "airflow_base_url", None)
    await init_db()

    async with AsyncSessionLocal() as session:
        tool = AirflowTool()
        created = await tool.trigger_run(session, "remote_orders", {"test": True})

        monkeypatch.setattr(settings, "airflow_base_url", "http://airflow.test")
        monkeypatch.setattr("app.tools.airflow.httpx.AsyncClient", FakeAirflowClient)
        synced = await tool.get_run(session, created.dag_id, created.run_id)

    assert created.status == "queued"
    assert synced.status == "success"


@pytest.mark.asyncio
async def test_airflow_tool_reads_runs_tasks_and_logs(monkeypatch):
    monkeypatch.setattr(settings, "airflow_base_url", "http://airflow.test")
    monkeypatch.setattr("app.tools.airflow.httpx.AsyncClient", FakeAirflowClient)
    await init_db()

    async with AsyncSessionLocal() as session:
        tool = AirflowTool()
        runs = await tool.list_runs(session, "remote_orders")
        tasks = await tool.list_task_instances("remote_orders", "run_1")
        log = await tool.get_task_log("remote_orders", "run_1", "extract_orders", 1)

    assert runs[0].run_id == "run_1"
    assert runs[0].status == "success"
    assert tasks[0].task_id == "extract_orders"
    assert tasks[0].state == "success"
    assert log.content == "task log ok"
