from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_current_user
from app.db.session import get_db
from app.models import User
from app.schemas import (
    DatabaseConnectionCreate,
    DatabaseConnectionRead,
    DatabaseConnectionTestRead,
    DatabaseConnectionUpdate,
)
from app.services.connections import DatabaseConnectionService
from app.tasks.connections import refresh_connection_statuses

router = APIRouter(prefix="/connections", tags=["connections"])
service = DatabaseConnectionService()


@router.get("", response_model=list[DatabaseConnectionRead])
async def list_connections(
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> list[dict]:
    connections = await service.list_visible(db, user)
    return [service.serialize(connection) for connection in connections]


@router.post("", response_model=DatabaseConnectionRead, status_code=status.HTTP_201_CREATED)
async def create_connection(
    payload: DatabaseConnectionCreate,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> dict:
    try:
        connection = await service.create(db, user, payload.model_dump())
        return service.serialize(connection)
    except PermissionError as exc:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc


@router.patch("/{connection_id}", response_model=DatabaseConnectionRead)
async def update_connection(
    connection_id: str,
    payload: DatabaseConnectionUpdate,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> dict:
    connection = await service.get_visible(db, user, connection_id)
    if connection is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Connection not found")

    try:
        updated = await service.update(db, user, connection, payload.model_dump(exclude_unset=True))
        return service.serialize(updated)
    except PermissionError as exc:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc


@router.post("/{connection_id}/test", response_model=DatabaseConnectionTestRead)
async def test_connection(
    connection_id: str,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> dict:
    connection = await service.get_visible(db, user, connection_id)
    if connection is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Connection not found")
    return await service.test_connection(db, connection)


@router.post("/health-checks")
async def queue_connection_health_checks(
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> dict:
    connections = await service.list_visible(db, user)
    task = refresh_connection_statuses.delay([connection.id for connection in connections])
    return {"task_id": task.id, "queued": len(connections)}
