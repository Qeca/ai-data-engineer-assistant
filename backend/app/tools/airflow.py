from __future__ import annotations

import time
from typing import Any
from datetime import UTC, datetime

import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.models import PipelineRun, PipelineState
from app.schemas import AirflowRunRead, PipelineRead
from app.tools.base import ToolExecution


class AirflowTool:
    name = "AirflowTool"

    async def list_pipelines(self, db: AsyncSession | None = None) -> list[PipelineRead]:
        pipelines = [
            PipelineRead(
                dag_id="orders_sync",
                name="Orders sync",
                schedule="@hourly",
                status="success",
                owner="data-team",
                last_run="4m 12s",
                next_run="через 38m",
            ),
            PipelineRead(
                dag_id="ml_feature_pipeline",
                name="ML feature pipeline",
                schedule="@daily",
                status="running",
                owner="ml-platform",
                last_run="18m 04s",
                next_run="06:00 tomorrow",
            ),
            PipelineRead(
                dag_id="clickstream_aggregation",
                name="Clickstream aggregation",
                schedule="*/15 * * * *",
                status="failed",
                owner="analytics",
                last_run="SparkOutOfMemoryError",
                next_run="retry pending",
            ),
            PipelineRead(
                dag_id="dw_nightly_refresh",
                name="DW nightly refresh",
                schedule="0 3 * * *",
                status="success",
                owner="de-platform",
                last_run="12m 44s",
                next_run="03:00 tomorrow",
            ),
        ]
        if db is None:
            return pipelines

        states = await db.scalars(select(PipelineState))
        pause_map = {state.dag_id: state.is_paused for state in states}
        return [
            pipeline.model_copy(update={"status": "paused" if pause_map.get(pipeline.dag_id) else pipeline.status})
            for pipeline in pipelines
        ]

    async def trigger_run(
        self,
        db: AsyncSession,
        dag_id: str,
        conf: dict | None = None,
    ) -> AirflowRunRead:
        conf = conf or {}
        run_id = f"manual__{datetime.now(UTC).strftime('%Y%m%dT%H%M%S')}"
        external_url = None
        status = "queued"

        if settings.airflow_base_url:
            try:
                async with httpx.AsyncClient(timeout=3) as client:
                    response = await client.post(
                        f"{settings.airflow_base_url.rstrip('/')}/api/v1/dags/{dag_id}/dagRuns",
                        auth=(settings.airflow_username, settings.airflow_password),
                        json={"dag_run_id": run_id, "conf": conf},
                    )
                    response.raise_for_status()
                    data = response.json()
                    run_id = data.get("dag_run_id", run_id)
                    status = data.get("state", status)
                    external_url = (
                        f"{settings.airflow_base_url.rstrip('/')}/dags/{dag_id}/grid"
                    )
            except Exception:
                status = "queued"

        run = PipelineRun(dag_id=dag_id, run_id=run_id, status=status, conf_json=conf, external_url=external_url)
        db.add(run)
        await db.commit()
        await db.refresh(run)
        return AirflowRunRead(
            dag_id=run.dag_id,
            run_id=run.run_id,
            status=run.status,
            external_url=run.external_url,
            created_at=run.created_at,
        )

    async def get_run(self, db: AsyncSession, dag_id: str, run_id: str) -> AirflowRunRead:
        run = await db.scalar(
            select(PipelineRun).where(PipelineRun.dag_id == dag_id, PipelineRun.run_id == run_id)
        )
        if run is None:
            return AirflowRunRead(dag_id=dag_id, run_id=run_id, status="not_found")

        if run.status == "queued":
            run.status = "running"
            await db.commit()
            await db.refresh(run)

        return AirflowRunRead(
            dag_id=run.dag_id,
            run_id=run.run_id,
            status=run.status,
            external_url=run.external_url,
            created_at=run.created_at,
        )

    async def execute(self, db: AsyncSession, dag_id: str, action: str = "trigger") -> ToolExecution:
        started = time.perf_counter()
        if action == "trigger":
            run = await self.trigger_run(db, dag_id, {"source": "agent"})
            output = run.model_dump()
        elif action in {"pause", "unpause", "pause_all", "unpause_all"}:
            return await self.manage_dags(db, action, dag_id)
        else:
            output = {"pipelines": [pipeline.model_dump() for pipeline in await self.list_pipelines(db)]}
        return ToolExecution(
            tool_name=self.name,
            status="success",
            input={"dag_id": dag_id, "action": action},
            output=output,
            latency_ms=max(1, int((time.perf_counter() - started) * 1000)),
        )

    async def manage_dags(self, db: AsyncSession, action: str, dag_id: str = "") -> ToolExecution:
        started = time.perf_counter()
        pipelines = await self.list_pipelines(db)
        all_ids = [pipeline.dag_id for pipeline in pipelines]
        targets = all_ids if action.endswith("_all") else [dag_id]
        targets = [target for target in targets if target]
        is_paused = action.startswith("pause")
        remote_results: list[dict[str, Any]] = []

        for target in targets:
            remote_results.append(await self._set_remote_pause_state(target, is_paused))
            state = await db.get(PipelineState, target)
            if state is None:
                db.add(PipelineState(dag_id=target, is_paused=is_paused, source="agent_tool"))
            else:
                state.is_paused = is_paused
                state.source = "agent_tool"

        await db.commit()
        updated = await self.list_pipelines(db)
        return ToolExecution(
            tool_name="AirflowControlTool",
            status="success",
            input={"action": action, "dag_id": dag_id},
            output={
                "action": action,
                "is_paused": is_paused,
                "affected_dags": targets,
                "affected_count": len(targets),
                "remote_results": remote_results,
                "pipelines": [pipeline.model_dump() for pipeline in updated],
            },
            latency_ms=max(1, int((time.perf_counter() - started) * 1000)),
            metadata={
                "ui_actions": [
                    {"type": "navigate", "screen": "airflow"},
                    {
                        "type": "toast",
                        "message": f"{'Остановлено' if is_paused else 'Возобновлено'} DAG: {len(targets)}",
                    },
                ]
            },
        )

    async def _set_remote_pause_state(self, dag_id: str, is_paused: bool) -> dict[str, Any]:
        if not settings.airflow_base_url:
            return {"dag_id": dag_id, "status": "local_only"}
        try:
            async with httpx.AsyncClient(timeout=3) as client:
                response = await client.patch(
                    f"{settings.airflow_base_url.rstrip('/')}/api/v1/dags/{dag_id}",
                    auth=(settings.airflow_username, settings.airflow_password),
                    json={"is_paused": is_paused},
                )
                response.raise_for_status()
                data = response.json()
                return {
                    "dag_id": dag_id,
                    "status": "remote_updated",
                    "is_paused": data.get("is_paused", is_paused),
                }
        except Exception as exc:  # noqa: BLE001
            return {"dag_id": dag_id, "status": "local_fallback", "error": str(exc)}
