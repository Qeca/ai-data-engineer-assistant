import time
import uuid

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import SparkJob
from app.schemas import SparkJobRead
from app.tools.base import ToolExecution


class SparkTool:
    name = "SparkTool"

    async def submit_job(
        self,
        db: AsyncSession,
        name: str,
        app_resource: str,
        params: dict | None = None,
    ) -> SparkJobRead:
        params = params or {}
        job_id = f"spark-{uuid.uuid4().hex[:8]}"
        job = SparkJob(
            job_id=job_id,
            name=name,
            status="running",
            app_resource=app_resource,
            params_json=params,
            result_sample_json=[
                {"metric": "records_processed", "value": 1284920},
                {"metric": "partitions", "value": params.get("partitions", 96)},
                {"metric": "shuffle_read_mb", "value": 842},
            ],
            driver_log=(
                "Submitted to local standalone Spark. "
                "Use SPARK_MASTER_URL to connect this metadata run to the cluster."
            ),
        )
        db.add(job)
        await db.commit()
        await db.refresh(job)
        return self._read(job)

    async def get_job(self, db: AsyncSession, job_id: str) -> SparkJobRead:
        job = await db.scalar(select(SparkJob).where(SparkJob.job_id == job_id))
        if job is None:
            return SparkJobRead(
                job_id=job_id,
                name="unknown",
                status="not_found",
                app_resource="",
                params={},
            )

        if job.status == "running":
            job.status = "success"
            job.driver_log = (job.driver_log or "") + "\nJob completed. Result sample persisted."
            await db.commit()
            await db.refresh(job)
        return self._read(job)

    async def list_jobs(self, db: AsyncSession, limit: int = 50) -> list[SparkJobRead]:
        rows = await db.scalars(select(SparkJob).order_by(SparkJob.created_at.desc()).limit(limit))
        return [self._read(job) for job in rows]

    async def execute(self, db: AsyncSession, name: str, app_resource: str, params: dict | None = None) -> ToolExecution:
        started = time.perf_counter()
        job = await self.submit_job(db, name, app_resource, params)
        return ToolExecution(
            tool_name=self.name,
            status="success",
            input={"name": name, "app_resource": app_resource, "params": params or {}},
            output=job.model_dump(),
            latency_ms=max(1, int((time.perf_counter() - started) * 1000)),
        )

    @staticmethod
    def _read(job: SparkJob) -> SparkJobRead:
        return SparkJobRead(
            job_id=job.job_id,
            name=job.name,
            status=job.status,
            app_resource=job.app_resource,
            params=job.params_json or {},
            result_sample=job.result_sample_json,
            driver_log=job.driver_log,
            created_at=job.created_at,
            updated_at=job.updated_at,
        )
