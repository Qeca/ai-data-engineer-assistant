import asyncio
from typing import Any

from sqlalchemy import select

from app.db.session import AsyncSessionLocal, close_db
from app.models import DatabaseConnection
from app.services.connections import DatabaseConnectionService
from app.worker import celery_app


@celery_app.task(name="connections.refresh_statuses", queue="connections")
def refresh_connection_statuses(connection_ids: list[str] | None = None) -> dict[str, Any]:
    return asyncio.run(_run_refresh_connection_statuses(connection_ids))


async def _run_refresh_connection_statuses(connection_ids: list[str] | None = None) -> dict[str, Any]:
    try:
        return await _refresh_connection_statuses(connection_ids)
    finally:
        await close_db()


async def _refresh_connection_statuses(connection_ids: list[str] | None = None) -> dict[str, Any]:
    if connection_ids is not None and len(connection_ids) == 0:
        return {"checked": 0, "connections": []}

    service = DatabaseConnectionService()
    async with AsyncSessionLocal() as session:
        query = select(DatabaseConnection).order_by(DatabaseConnection.engine, DatabaseConnection.name)
        if connection_ids is not None:
            query = query.where(DatabaseConnection.id.in_(connection_ids))

        connections = list(await session.scalars(query))
        results: list[dict[str, Any]] = []
        for connection in connections:
            try:
                result = await service.test_connection(session, connection)
                results.append(
                    {
                        "connection_id": connection.id,
                        "name": connection.name,
                        "status": result["status"],
                        "latency_ms": result["latency_ms"],
                        "error": result["error"],
                    }
                )
            except Exception as exc:
                await session.rollback()
                results.append(
                    {
                        "connection_id": connection.id,
                        "name": connection.name,
                        "status": "error",
                        "error": str(exc),
                    }
                )

    return {"checked": len(results), "connections": results}
