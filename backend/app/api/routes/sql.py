from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_current_user
from app.db.session import get_db
from app.models import User
from app.schemas import SqlExecuteRequest, SqlExecuteResponse
from app.tools.sql import SQLTool

router = APIRouter(prefix="/sql", tags=["sql"])
tool = SQLTool()


@router.post("/execute", response_model=SqlExecuteResponse)
async def execute_sql(
    payload: SqlExecuteRequest,
    _: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> SqlExecuteResponse:
    result = await tool.execute(db, payload.query, payload.limit)
    if result.status != "success":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=result.output.get("error", "SQL execution failed"),
        )
    return SqlExecuteResponse(
        columns=result.output["columns"],
        rows=result.output["rows"],
        row_count=result.output["row_count"],
        latency_ms=result.latency_ms,
    )
