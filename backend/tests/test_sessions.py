from httpx import ASGITransport, AsyncClient
import pytest
from sqlalchemy import select

from app.db.session import AsyncSessionLocal, init_db
from app.main import app
from app.models import AgentSession, Message, ToolRun, User


@pytest.mark.asyncio
async def test_user_can_delete_own_chat_session_with_messages_and_tool_runs():
    await init_db()

    async with AsyncSessionLocal() as session:
        user = await session.scalar(select(User).where(User.email == "admin@local.dev"))
        chat = AgentSession(user_id=user.id, title="delete me")
        session.add(chat)
        await session.flush()
        message = Message(session_id=chat.id, role="assistant", content="answer", metadata_json={})
        session.add(message)
        await session.flush()
        session.add(
            ToolRun(
                session_id=chat.id,
                message_id=message.id,
                tool_name="AirflowTool",
                status="success",
                input_json={},
                output_json={},
                latency_ms=1,
            )
        )
        await session.commit()
        chat_id = chat.id
        message_id = message.id

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        login = await client.post("/auth/login", json={"email": "admin@local.dev", "password": "admin"})
        token = login.json()["access_token"]
        response = await client.delete(f"/sessions/{chat_id}", headers={"Authorization": f"Bearer {token}"})
        messages = await client.get(f"/sessions/{chat_id}/messages", headers={"Authorization": f"Bearer {token}"})

    assert response.status_code == 204
    assert messages.status_code == 404

    async with AsyncSessionLocal() as session:
        assert await session.get(AgentSession, chat_id) is None
        assert await session.get(Message, message_id) is None
        tool_run = await session.scalar(select(ToolRun).where(ToolRun.session_id == chat_id))

    assert tool_run is None
