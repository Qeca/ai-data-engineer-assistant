from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_current_user
from app.db.session import get_db
from app.models import User
from app.schemas import PipelineRead
from app.tools.airflow import AirflowTool

router = APIRouter(prefix="/pipelines", tags=["pipelines"])
tool = AirflowTool()


@router.get("", response_model=list[PipelineRead])
async def list_pipelines(
    _: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> list[PipelineRead]:
    return await tool.list_pipelines(db)
