import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import select

from app.agent.tool_registry import AgentToolRegistry
from app.db.session import AsyncSessionLocal, init_db
from app.main import app
from app.models import User


async def login(client: AsyncClient, email: str = "admin@local.dev", password: str = "admin") -> str:
    response = await client.post("/auth/login", json={"email": email, "password": password})
    assert response.status_code == 200
    return response.json()["access_token"]


@pytest.mark.asyncio
async def test_demo_database_connections_are_visible_in_api():
    await init_db()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        token = await login(client)
        response = await client.get("/connections", headers={"Authorization": f"Bearer {token}"})

    assert response.status_code == 200
    connections = response.json()
    engines = {item["engine"] for item in connections}
    names = {item["name"] for item in connections}

    assert {"postgresql", "mysql", "clickhouse", "mongodb", "redis"}.issubset(engines)
    assert "demo-postgres-warehouse" in names
    assert all("password" not in item for item in connections)


@pytest.mark.asyncio
async def test_analyst_can_view_but_cannot_create_database_connections():
    await init_db()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        admin_token = await login(client)
        await client.post(
            "/users",
            headers={"Authorization": f"Bearer {admin_token}"},
            json={
                "email": "readonly@local.dev",
                "full_name": "Read Only",
                "role": "analyst",
                "password": "demo",
            },
        )
        analyst_token = await login(client, "readonly@local.dev", "demo")

        visible = await client.get("/connections", headers={"Authorization": f"Bearer {analyst_token}"})
        created = await client.post(
            "/connections",
            headers={"Authorization": f"Bearer {analyst_token}"},
            json={
                "name": "analyst-db",
                "engine": "postgresql",
                "host": "localhost",
                "port": 5432,
                "database": "analytics",
                "username": "demo",
                "password": "demo",
                "visibility": "private",
            },
        )

    assert visible.status_code == 200
    assert created.status_code == 403


@pytest.mark.asyncio
async def test_agent_can_upsert_and_test_database_connection():
    await init_db()
    async with AsyncSessionLocal() as session:
        user = await session.scalar(select(User).where(User.email == "admin@local.dev"))
        registry = AgentToolRegistry(session, user, {"screen": "settings"})

        saved = await registry.execute(
            "upsert_database_connection",
            {
                "connection_id": "",
                "name": "agent-managed-postgres",
                "engine": "postgresql",
                "host": "127.0.0.1",
                "port": 1,
                "database": "analytics",
                "username": "demo",
                "password": "demo",
                "visibility": "private",
                "options": {},
            },
        )
        checked = await registry.execute(
            "test_database_connection",
            {"connection_id": saved.output["connection"]["id"], "name": ""},
        )

    assert saved.status == "success"
    assert saved.output["connection"]["name"] == "agent-managed-postgres"
    assert saved.ui_actions[0]["type"] == "refresh_connections"
    assert checked.status == "error"
    assert checked.output["status"] == "offline"
