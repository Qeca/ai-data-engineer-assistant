import re
from contextlib import asynccontextmanager
from typing import AsyncIterator

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine

from app.core.config import settings


IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class AgentDataDatabase:
    def __init__(self) -> None:
        self._url: str | None = None
        self._engine: AsyncEngine | None = None
        self._sessionmaker: async_sessionmaker[AsyncSession] | None = None

    @asynccontextmanager
    async def session(self, fallback_db: AsyncSession) -> AsyncIterator[AsyncSession]:
        if not settings.agent_database_url:
            yield fallback_db
            return

        sessionmaker = self._get_sessionmaker()
        async with sessionmaker() as session:
            await self._prepare_session(session)
            yield session

    def url(self) -> str:
        return settings.agent_database_url or settings.database_url

    def is_sqlite(self) -> bool:
        return self.url().startswith("sqlite")

    def schema_filter(self) -> str | None:
        return settings.agent_database_schema

    async def close(self) -> None:
        if self._engine is not None:
            await self._engine.dispose()
        self._engine = None
        self._sessionmaker = None
        self._url = None

    def _get_sessionmaker(self) -> async_sessionmaker[AsyncSession]:
        url = self.url()
        if self._sessionmaker is not None and self._url == url:
            return self._sessionmaker

        connect_args = {"check_same_thread": False} if url.startswith("sqlite") else {}
        self._engine = create_async_engine(url, echo=False, future=True, connect_args=connect_args)
        self._sessionmaker = async_sessionmaker(self._engine, expire_on_commit=False, class_=AsyncSession)
        self._url = url
        return self._sessionmaker

    async def _prepare_session(self, session: AsyncSession) -> None:
        schema = settings.agent_database_schema
        if not schema or not self.url().startswith("postgresql"):
            return

        if not IDENTIFIER.match(schema):
            raise ValueError(f"Invalid AGENT_DATABASE_SCHEMA: {schema}")
        await session.execute(text(f'SET search_path TO "{schema}", public'))


agent_data_db = AgentDataDatabase()
