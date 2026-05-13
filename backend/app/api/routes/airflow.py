from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_current_user
from app.db.session import get_db
from app.models import User
from app.schemas import AirflowRunRead, AirflowRunRequest, AirflowTaskInstanceRead, AirflowTaskLogRead
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


@router.get("/dags/{dag_id}/runs", response_model=list[AirflowRunRead])
async def list_dag_runs(
    dag_id: str,
    _: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> list[AirflowRunRead]:
    return await tool.list_runs(db, dag_id)


@router.get("/dags/{dag_id}/runs/{run_id}", response_model=AirflowRunRead)
async def get_dag_run(
    dag_id: str,
    run_id: str,
    _: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> AirflowRunRead:
    return await tool.get_run(db, dag_id, run_id)


@router.get("/dags/{dag_id}/runs/{run_id}/tasks", response_model=list[AirflowTaskInstanceRead])
async def list_dag_run_tasks(
    dag_id: str,
    run_id: str,
    _: User = Depends(get_current_user),
) -> list[AirflowTaskInstanceRead]:
    return await tool.list_task_instances(dag_id, run_id)


@router.get("/dags/{dag_id}/runs/{run_id}/tasks/{task_id}/logs/{try_number}", response_model=AirflowTaskLogRead)
async def get_task_log(
    dag_id: str,
    run_id: str,
    task_id: str,
    try_number: int,
    _: User = Depends(get_current_user),
) -> AirflowTaskLogRead:
    return await tool.get_task_log(dag_id, run_id, task_id, try_number)
