from __future__ import annotations

import pytest

from app.core.config import settings
from app.db.session import AsyncSessionLocal, init_db
from app.tools.airflow import AirflowTool
from app.tools.spark import SparkTool
from app.tools.sql import SQLTool


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

    assert airflow_run.dag_id == "orders_sync"
    assert airflow_run.status in {"queued", "running", "success"}
    assert spark_job.status == "running"
    assert spark_job.result_sample


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
