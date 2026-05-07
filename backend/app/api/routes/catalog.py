from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_current_user
from app.db.session import get_db
from app.models import User
from app.schemas import CatalogTable
from app.tools.catalog import CatalogTool

router = APIRouter(prefix="/catalog", tags=["catalog"])
tool = CatalogTool()


@router.get("/tables", response_model=list[CatalogTable])
async def list_tables(
    _: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> list[CatalogTable]:
    return await tool.list_tables(db)
