import asyncio
import socket
import time
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import DatabaseConnection, User


SUPPORTED_DATABASE_ENGINES = {"postgresql", "mysql", "clickhouse", "mongodb", "redis"}


class DatabaseConnectionService:
    demo_connections: list[dict[str, Any]] = [
        {
            "name": "demo-postgres-warehouse",
            "engine": "postgresql",
            "host": "demo-postgres",
            "port": 5432,
            "database": "analytics",
            "username": "demo",
            "password": "demo",
            "options": {"public_host": "localhost", "public_port": 15433, "schemas": ["sales"]},
            "visibility": "shared",
        },
        {
            "name": "demo-mysql-mart",
            "engine": "mysql",
            "host": "demo-mysql",
            "port": 3306,
            "database": "analytics",
            "username": "demo",
            "password": "demo",
            "options": {"public_host": "localhost", "public_port": 13306},
            "visibility": "shared",
        },
        {
            "name": "demo-clickhouse-events",
            "engine": "clickhouse",
            "host": "demo-clickhouse",
            "port": 8123,
            "database": "analytics",
            "username": "demo",
            "password": "demo",
            "options": {"public_host": "localhost", "public_port": 18123, "native_port": 19000},
            "visibility": "shared",
        },
        {
            "name": "demo-mongodb-documents",
            "engine": "mongodb",
            "host": "demo-mongo",
            "port": 27017,
            "database": "analytics",
            "username": "demo",
            "password": "demo",
            "options": {"public_host": "localhost", "public_port": 27018},
            "visibility": "shared",
        },
        {
            "name": "demo-redis-cache",
            "engine": "redis",
            "host": "demo-redis",
            "port": 6379,
            "database": "0",
            "username": None,
            "password": "demo",
            "options": {"public_host": "localhost", "public_port": 16379},
            "visibility": "shared",
        },
    ]

    async def seed_demo_connections(self, db: AsyncSession) -> None:
        for payload in self.demo_connections:
            existing = await db.scalar(
                select(DatabaseConnection).where(
                    DatabaseConnection.name == payload["name"],
                    DatabaseConnection.visibility == "shared",
                )
            )
            if existing:
                continue
            db.add(self._build_connection(payload, owner_user_id=None))
        await db.commit()

    async def list_visible(self, db: AsyncSession, user: User) -> list[DatabaseConnection]:
        query = select(DatabaseConnection).order_by(DatabaseConnection.engine, DatabaseConnection.name)
        if user.role != "admin":
            query = query.where(
                or_(
                    DatabaseConnection.visibility == "shared",
                    DatabaseConnection.owner_user_id == user.id,
                )
            )
        result = await db.scalars(query)
        return list(result)

    async def get_visible(self, db: AsyncSession, user: User, connection_id: str) -> DatabaseConnection | None:
        query = select(DatabaseConnection).where(DatabaseConnection.id == connection_id)
        if user.role != "admin":
            query = query.where(
                or_(
                    DatabaseConnection.visibility == "shared",
                    DatabaseConnection.owner_user_id == user.id,
                )
            )
        return await db.scalar(query)

    async def find_visible_by_name(self, db: AsyncSession, user: User, name: str) -> DatabaseConnection | None:
        query = select(DatabaseConnection).where(DatabaseConnection.name == name)
        if user.role != "admin":
            query = query.where(
                or_(
                    DatabaseConnection.visibility == "shared",
                    DatabaseConnection.owner_user_id == user.id,
                )
            )
        return await db.scalar(query.order_by(DatabaseConnection.visibility.desc()))

    async def create(self, db: AsyncSession, user: User, payload: dict[str, Any]) -> DatabaseConnection:
        self._ensure_can_manage(user)
        self._validate_engine(payload.get("engine"))
        owner_user_id = None if payload.get("visibility") == "shared" else user.id
        connection = self._build_connection(payload, owner_user_id)
        db.add(connection)
        await db.commit()
        await db.refresh(connection)
        return connection

    async def update(
        self,
        db: AsyncSession,
        user: User,
        connection: DatabaseConnection,
        payload: dict[str, Any],
    ) -> DatabaseConnection:
        self._ensure_can_manage(user)
        if user.role != "admin" and connection.visibility == "shared":
            raise PermissionError("Only admin can update shared database connections")
        if payload.get("engine") is not None:
            self._validate_engine(payload["engine"])

        for key in ["name", "engine", "host", "port", "database", "username"]:
            if key in payload and payload[key] is not None:
                setattr(connection, key, payload[key])
        if "password" in payload and payload["password"] is not None:
            connection.password_secret = payload["password"]
        if "options" in payload and payload["options"] is not None:
            connection.options_json = payload["options"]
        if "visibility" in payload and payload["visibility"] is not None:
            connection.visibility = payload["visibility"]
            connection.owner_user_id = None if payload["visibility"] == "shared" else user.id
        await db.commit()
        await db.refresh(connection)
        return connection

    async def upsert(self, db: AsyncSession, user: User, payload: dict[str, Any]) -> DatabaseConnection:
        connection_id = payload.get("id")
        if connection_id:
            existing = await self.get_visible(db, user, connection_id)
            if existing is None:
                raise LookupError("Database connection not found")
            return await self.update(db, user, existing, payload)

        name = payload.get("name")
        if not name:
            raise ValueError("Connection name is required")
        existing = await self.find_visible_by_name(db, user, name)
        if existing:
            return await self.update(db, user, existing, payload)
        return await self.create(db, user, payload)

    async def test_connection(
        self,
        db: AsyncSession,
        connection: DatabaseConnection,
        timeout_seconds: float = 2.0,
    ) -> dict[str, Any]:
        started = time.perf_counter()
        error = None
        status = "online"
        try:
            await asyncio.to_thread(self._open_socket, connection.host, connection.port, timeout_seconds)
        except Exception as exc:
            status = "offline"
            error = str(exc)

        connection.status = status
        connection.last_error = error
        connection.last_tested_at = datetime.now(UTC)
        await db.commit()
        await db.refresh(connection)
        return {
            "connection_id": connection.id,
            "status": status,
            "latency_ms": max(1, int((time.perf_counter() - started) * 1000)),
            "error": error,
        }

    def serialize(self, connection: DatabaseConnection) -> dict[str, Any]:
        return {
            "id": connection.id,
            "name": connection.name,
            "engine": connection.engine,
            "host": connection.host,
            "port": connection.port,
            "database": connection.database,
            "username": connection.username,
            "options_json": connection.options_json or {},
            "visibility": connection.visibility,
            "owner_user_id": connection.owner_user_id,
            "status": connection.status,
            "last_error": connection.last_error,
            "last_tested_at": connection.last_tested_at,
            "created_at": connection.created_at,
            "updated_at": connection.updated_at,
            "has_password": bool(connection.password_secret),
        }

    def serialize_for_tool(self, connection: DatabaseConnection) -> dict[str, Any]:
        item = self.serialize(connection)
        for key in ["created_at", "updated_at", "last_tested_at"]:
            value = item.get(key)
            item[key] = value.isoformat() if value else None
        return item

    def _build_connection(self, payload: dict[str, Any], owner_user_id: str | None) -> DatabaseConnection:
        return DatabaseConnection(
            name=payload["name"],
            engine=payload["engine"],
            host=payload["host"],
            port=int(payload["port"]),
            database=payload.get("database"),
            username=payload.get("username"),
            password_secret=payload.get("password"),
            options_json=payload.get("options") or {},
            visibility=payload.get("visibility") or "private",
            owner_user_id=owner_user_id,
            status="unknown",
        )

    @staticmethod
    def _open_socket(host: str, port: int, timeout_seconds: float) -> None:
        with socket.create_connection((host, port), timeout=timeout_seconds):
            return

    @staticmethod
    def _ensure_can_manage(user: User) -> None:
        if user.role not in {"admin", "engineer"}:
            raise PermissionError("Only admin and engineer users can manage database connections")

    @staticmethod
    def _validate_engine(engine: str | None) -> None:
        if engine not in SUPPORTED_DATABASE_ENGINES:
            raise ValueError(f"Unsupported database engine: {engine}")
