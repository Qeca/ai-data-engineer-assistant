from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_current_user
from app.db.session import get_db
from app.models import User
from app.schemas import AirflowRunRead, AirflowRunRequest
from app.tools.airflow import AirflowTool

router = APIRouter(prefix="/airflow", tags=["airflow"])
tool = AirflowTool()


@router.post("/dags/{dag_id}/runs", response_model=AirflowRunRead)
async def trigger_dag_run(
    dag_id: str,
    payload: AirflowRunRequest,
    _: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> AirflowRunRead:
    return await tool.trigger_run(db, dag_id, payload.conf)


@router.get("/dags/{dag_id}/runs/{run_id}", response_model=AirflowRunRead)
async def get_dag_run(
    dag_id: str,
    run_id: str,
    _: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> AirflowRunRead:
    return await tool.get_run(db, dag_id, run_id)
