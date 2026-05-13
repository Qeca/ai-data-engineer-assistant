from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_current_user
from app.db.session import get_db
from app.models import User
from app.schemas import SparkJobRead, SparkJobRequest
from app.tools.spark import SparkTool

router = APIRouter(prefix="/spark", tags=["spark"])
tool = SparkTool()


@router.post("/jobs", response_model=SparkJobRead)
async def submit_job(
    payload: SparkJobRequest,
    _: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> SparkJobRead:
    return await tool.submit_job(db, payload.name, payload.app_resource, payload.params)


@router.get("/jobs", response_model=list[SparkJobRead])
async def list_jobs(
    _: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> list[SparkJobRead]:
    return await tool.list_jobs(db)


@router.get("/jobs/{job_id}", response_model=SparkJobRead)
async def get_job(
    job_id: str,
    _: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> SparkJobRead:
    return await tool.get_job(db, job_id)
