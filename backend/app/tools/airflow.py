import time
from typing import Any
from datetime import UTC, datetime
from urllib.parse import quote

import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.models import PipelineRun, PipelineState
from app.schemas import AirflowRunRead, AirflowTaskInstanceRead, AirflowTaskLogRead, PipelineRead
from app.tools.base import ToolExecution


class AirflowTool:
    name = "AirflowTool"

    async def list_pipelines(self, db: AsyncSession | None = None) -> list[PipelineRead]:
        remote_pipelines = await self._remote_pipelines()
        if remote_pipelines is not None:
            return remote_pipelines

        pipelines = self._local_pipelines()
        if db is None:
            return pipelines

        states = await db.scalars(select(PipelineState))
        pause_map = {state.dag_id: state.is_paused for state in states}
        return [
            pipeline.model_copy(update={"status": "paused" if pause_map.get(pipeline.dag_id) else pipeline.status})
            for pipeline in pipelines
        ]

    @staticmethod
    def _local_pipelines() -> list[PipelineRead]:
        return [
            PipelineRead(
                dag_id="orders_sync",
                name="Orders sync",
                schedule="@hourly",
                status="success",
                owner="data-team",
                last_run="4m 12s",
                next_run="через 38m",
                tasks=[
                    {"task_id": "extract_orders", "operator_name": "EmptyOperator", "downstream_task_ids": ["validate_orders"]},
                    {"task_id": "validate_orders", "operator_name": "EmptyOperator", "downstream_task_ids": ["load_orders"]},
                    {"task_id": "load_orders", "operator_name": "EmptyOperator", "downstream_task_ids": []},
                ],
            ),
            PipelineRead(
                dag_id="ml_feature_pipeline",
                name="ML feature pipeline",
                schedule="@daily",
                status="running",
                owner="ml-platform",
                last_run="18m 04s",
                next_run="06:00 tomorrow",
                tasks=[
                    {"task_id": "build_features", "operator_name": "PythonOperator", "downstream_task_ids": ["publish_features"]},
                    {"task_id": "publish_features", "operator_name": "PythonOperator", "downstream_task_ids": []},
                ],
            ),
            PipelineRead(
                dag_id="clickstream_aggregation",
                name="Clickstream aggregation",
                schedule="*/15 * * * *",
                status="failed",
                owner="analytics",
                last_run="SparkOutOfMemoryError",
                next_run="retry pending",
                tasks=[
                    {"task_id": "aggregate_clickstream", "operator_name": "BashOperator", "downstream_task_ids": []},
                ],
            ),
            PipelineRead(
                dag_id="dw_nightly_refresh",
                name="DW nightly refresh",
                schedule="0 3 * * *",
                status="success",
                owner="de-platform",
                last_run="12m 44s",
                next_run="03:00 tomorrow",
                tasks=[
                    {"task_id": "refresh_dimensions", "operator_name": "PythonOperator", "downstream_task_ids": ["refresh_facts"]},
                    {"task_id": "refresh_facts", "operator_name": "PythonOperator", "downstream_task_ids": []},
                ],
            ),
        ]

    async def _remote_pipelines(self) -> list[PipelineRead] | None:
        if not settings.airflow_base_url:
            return None

        base_url = settings.airflow_base_url.rstrip("/")
        try:
            async with httpx.AsyncClient(timeout=5) as client:
                response = await client.get(
                    f"{base_url}/api/v1/dags",
                    auth=(settings.airflow_username, settings.airflow_password),
                    params={"limit": 100},
                )
                response.raise_for_status()
                dags = response.json().get("dags", [])
                pipelines = [
                    await self._pipeline_from_remote_dag(client, base_url, dag)
                    for dag in dags
                    if dag.get("dag_id")
                ]
                return sorted(pipelines, key=lambda pipeline: pipeline.dag_id)
        except Exception:
            return None

    async def _pipeline_from_remote_dag(
        self,
        client: httpx.AsyncClient,
        base_url: str,
        dag: dict[str, Any],
    ) -> PipelineRead:
        dag_id = str(dag["dag_id"])
        latest_run = await self._latest_run_text(client, base_url, dag_id)
        tasks = await self._dag_tasks(client, base_url, dag_id)
        return PipelineRead(
            dag_id=dag_id,
            name=str(dag.get("dag_display_name") or dag_id.replace("_", " ").title()),
            schedule=self._schedule_text(dag),
            status=self._dag_status(dag),
            owner=self._owners_text(dag.get("owners")),
            description=self._value_text(dag.get("description")),
            last_run=latest_run,
            next_run=self._value_text(dag.get("next_dagrun")),
            tasks=tasks,
        )

    async def _dag_tasks(self, client: httpx.AsyncClient, base_url: str, dag_id: str) -> list[dict[str, Any]]:
        try:
            response = await client.get(
                f"{base_url}/api/v1/dags/{dag_id}/tasks",
                auth=(settings.airflow_username, settings.airflow_password),
            )
            response.raise_for_status()
            tasks = response.json().get("tasks", [])
            return [
                {
                    "task_id": task.get("task_id"),
                    "operator_name": task.get("operator_name"),
                    "downstream_task_ids": task.get("downstream_task_ids") or [],
                }
                for task in tasks
                if task.get("task_id")
            ]
        except Exception:
            return []

    async def _latest_run_text(self, client: httpx.AsyncClient, base_url: str, dag_id: str) -> str | None:
        try:
            response = await client.get(
                f"{base_url}/api/v1/dags/{dag_id}/dagRuns",
                auth=(settings.airflow_username, settings.airflow_password),
                params={"limit": 1, "order_by": "-execution_date"},
            )
            response.raise_for_status()
            runs = response.json().get("dag_runs", [])
            if not runs:
                return None
            run = runs[0]
            state = run.get("state") or "unknown"
            timestamp = run.get("end_date") or run.get("start_date") or run.get("logical_date")
            return f"{state} {timestamp}" if timestamp else str(state)
        except Exception:
            return None

    @classmethod
    def _schedule_text(cls, dag: dict[str, Any]) -> str:
        schedule = cls._value_text(dag.get("schedule_interval"))
        if schedule and schedule != "None":
            return schedule
        timetable = cls._value_text(dag.get("timetable_description"))
        return timetable or "manual"

    @staticmethod
    def _dag_status(dag: dict[str, Any]) -> str:
        if dag.get("has_import_errors"):
            return "import_error"
        if dag.get("is_paused"):
            return "paused"
        if dag.get("is_active") is False:
            return "inactive"
        return "active"

    @staticmethod
    def _owners_text(owners: Any) -> str:
        if isinstance(owners, list) and owners:
            return ", ".join(str(owner) for owner in owners)
        if isinstance(owners, str) and owners:
            return owners
        return "airflow"

    @classmethod
    def _value_text(cls, value: Any) -> str | None:
        if value is None:
            return None
        if isinstance(value, dict):
            for key in ("value", "expression", "cron", "description"):
                if value.get(key) is not None:
                    return cls._value_text(value[key])
            return str(value)
        if isinstance(value, list):
            return ", ".join(str(item) for item in value)
        return str(value)

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
        error = None

        if settings.airflow_base_url:
            try:
                async with httpx.AsyncClient(timeout=3) as client:
                    response = await client.post(
                        f"{settings.airflow_base_url.rstrip('/')}/api/v1/dags/{self._path(dag_id)}/dagRuns",
                        auth=(settings.airflow_username, settings.airflow_password),
                        json={"dag_run_id": run_id, "conf": conf},
                    )
                    response.raise_for_status()
                    data = response.json()
                    run_id = data.get("dag_run_id", run_id)
                    status = data.get("state", status)
                    external_url = self._grid_url(dag_id)
            except Exception as exc:
                status = "error"
                error = f"Airflow trigger failed: {exc}"
                external_url = self._grid_url(dag_id)

        stored_conf = {**conf, "_error": error} if error else conf
        run = PipelineRun(dag_id=dag_id, run_id=run_id, status=status, conf_json=stored_conf, external_url=external_url)
        db.add(run)
        await db.commit()
        await db.refresh(run)
        return AirflowRunRead(
            dag_id=run.dag_id,
            run_id=run.run_id,
            status=run.status,
            external_url=run.external_url,
            created_at=run.created_at,
            error=error,
        )

    async def get_run(self, db: AsyncSession, dag_id: str, run_id: str) -> AirflowRunRead:
        run = await db.scalar(
            select(PipelineRun).where(PipelineRun.dag_id == dag_id, PipelineRun.run_id == run_id)
        )
        remote_run = await self._remote_run(dag_id, run_id)
        if run is None:
            if remote_run is not None:
                return AirflowRunRead(
                    dag_id=dag_id,
                    run_id=run_id,
                    status=remote_run["status"],
                    external_url=remote_run["external_url"],
                )
            return AirflowRunRead(
                dag_id=dag_id,
                run_id=run_id,
                status="not_found",
                error="Remote Airflow run was not found.",
            )

        if remote_run is not None:
            run.status = remote_run["status"]
            run.external_url = remote_run["external_url"]
            await db.commit()
            await db.refresh(run)
        elif settings.airflow_base_url and run.status in {"queued", "running"}:
            run.status = "not_found"
            run.conf_json = {**(run.conf_json or {}), "_error": "Remote Airflow run was not found."}
            await db.commit()
            await db.refresh(run)
        elif run.status == "queued":
            run.status = "running"
            await db.commit()
            await db.refresh(run)

        return AirflowRunRead(
            dag_id=run.dag_id,
            run_id=run.run_id,
            status=run.status,
            external_url=run.external_url,
            created_at=run.created_at,
            error=self._run_error(run.conf_json),
        )

    async def list_runs(self, db: AsyncSession, dag_id: str, limit: int = 25) -> list[AirflowRunRead]:
        remote_runs = await self._remote_runs(dag_id, limit)
        if remote_runs is not None:
            return remote_runs

        rows = await db.scalars(
            select(PipelineRun)
            .where(PipelineRun.dag_id == dag_id)
            .order_by(PipelineRun.created_at.desc())
            .limit(limit)
        )
        return [
            AirflowRunRead(
                dag_id=row.dag_id,
                run_id=row.run_id,
                status=row.status,
                external_url=row.external_url,
                created_at=row.created_at,
            )
            for row in rows
        ]

    async def list_task_instances(self, dag_id: str, run_id: str) -> list[AirflowTaskInstanceRead]:
        if not settings.airflow_base_url:
            return []

        base_url = settings.airflow_base_url.rstrip("/")
        try:
            async with httpx.AsyncClient(timeout=5) as client:
                response = await client.get(
                    f"{base_url}/api/v1/dags/{self._path(dag_id)}/dagRuns/{self._path(run_id)}/taskInstances",
                    auth=(settings.airflow_username, settings.airflow_password),
                )
                response.raise_for_status()
                tasks = response.json().get("task_instances", [])
        except Exception:
            return []

        return [
            AirflowTaskInstanceRead(
                dag_id=str(task.get("dag_id") or dag_id),
                run_id=str(task.get("dag_run_id") or run_id),
                task_id=str(task.get("task_id") or ""),
                state=str(task.get("state") or "unknown"),
                try_number=int(task.get("try_number") or 1),
                operator=task.get("operator"),
                start_date=task.get("start_date"),
                end_date=task.get("end_date"),
                duration=task.get("duration"),
            )
            for task in tasks
            if task.get("task_id")
        ]

    async def get_task_log(
        self,
        dag_id: str,
        run_id: str,
        task_id: str,
        try_number: int = 1,
    ) -> AirflowTaskLogRead:
        if not settings.airflow_base_url:
            return AirflowTaskLogRead(
                dag_id=dag_id,
                run_id=run_id,
                task_id=task_id,
                try_number=try_number,
                content="Airflow REST API is not configured.",
                source="local_fallback",
            )

        base_url = settings.airflow_base_url.rstrip("/")
        try:
            async with httpx.AsyncClient(timeout=8) as client:
                response = await client.get(
                    f"{base_url}/api/v1/dags/{self._path(dag_id)}/dagRuns/{self._path(run_id)}/taskInstances/{self._path(task_id)}/logs/{try_number}",
                    auth=(settings.airflow_username, settings.airflow_password),
                )
                response.raise_for_status()
                content = self._log_content(response)
        except Exception as exc:
            content = f"Could not load Airflow log: {exc}"

        return AirflowTaskLogRead(
            dag_id=dag_id,
            run_id=run_id,
            task_id=task_id,
            try_number=try_number,
            content=content,
        )

    async def _remote_runs(self, dag_id: str, limit: int) -> list[AirflowRunRead] | None:
        if not settings.airflow_base_url:
            return None

        base_url = settings.airflow_base_url.rstrip("/")
        try:
            async with httpx.AsyncClient(timeout=5) as client:
                response = await client.get(
                    f"{base_url}/api/v1/dags/{self._path(dag_id)}/dagRuns",
                    auth=(settings.airflow_username, settings.airflow_password),
                    params={"limit": limit, "order_by": "-execution_date"},
                )
                response.raise_for_status()
                runs = response.json().get("dag_runs", [])
        except Exception:
            return None

        return [
            AirflowRunRead(
                dag_id=str(run.get("dag_id") or dag_id),
                run_id=str(run.get("dag_run_id") or ""),
                status=str(run.get("state") or "unknown"),
                external_url=self._grid_url(dag_id),
                created_at=None,
            )
            for run in runs
            if run.get("dag_run_id")
        ]

    async def _remote_run(self, dag_id: str, run_id: str) -> dict[str, str] | None:
        if not settings.airflow_base_url:
            return None

        base_url = settings.airflow_base_url.rstrip("/")
        try:
            async with httpx.AsyncClient(timeout=3) as client:
                response = await client.get(
                    f"{base_url}/api/v1/dags/{self._path(dag_id)}/dagRuns/{self._path(run_id)}",
                    auth=(settings.airflow_username, settings.airflow_password),
                )
                response.raise_for_status()
                data = response.json()
        except Exception:
            return None

        status = str(data.get("state") or "unknown")
        return {
            "status": status,
            "external_url": self._grid_url(dag_id),
        }

    @staticmethod
    def _grid_url(dag_id: str) -> str | None:
        base_url = settings.airflow_public_url or settings.airflow_base_url
        if not base_url:
            return None
        return f"{base_url.rstrip('/')}/dags/{dag_id}/grid"

    @staticmethod
    def _path(value: str) -> str:
        return quote(value, safe="")

    @staticmethod
    def _log_content(response: httpx.Response) -> str:
        content_type = response.headers.get("content-type", "")
        if "application/json" not in content_type:
            return response.text

        data = response.json()
        if isinstance(data, str):
            return data
        if isinstance(data, dict):
            for key in ("content", "message", "log"):
                if data.get(key) is not None:
                    return str(data[key])
        return response.text

    @staticmethod
    def _run_error(conf: dict | None) -> str | None:
        if not isinstance(conf, dict):
            return None
        value = conf.get("_error")
        return str(value) if value else None

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
            status="error" if output.get("status") in {"error", "failed", "not_found"} else "success",
            input={"dag_id": dag_id, "action": action},
            output=output,
            latency_ms=max(1, int((time.perf_counter() - started) * 1000)),
            error=output.get("error"),
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
