import logging
import asyncio
import json

from fastapi import APIRouter, Depends
from fastapi.encoders import jsonable_encoder
from starlette.responses import StreamingResponse
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.agent.orchestrator import AgentOrchestrator, AgentResult
from app.api.deps import get_current_user
from app.db.session import get_db
from app.models import AgentSession, Message, ToolRun, User
from app.schemas import AgentQueryRequest, AgentQueryResponse, ToolCallRead
from app.services.tracing import TraceClient

router = APIRouter(prefix="/agent", tags=["agent"])
orchestrator = AgentOrchestrator()
tracing = TraceClient()
logger = logging.getLogger(__name__)


def tool_call_response(call) -> ToolCallRead:
    return ToolCallRead(
        tool_name=call.tool_name,
        status=call.status,
        input=jsonable_encoder(call.input),
        output=jsonable_encoder(call.output),
        latency_ms=call.latency_ms,
    )


def encode_stream_event(event: dict) -> str:
    return json.dumps(jsonable_encoder(event), ensure_ascii=False) + "\n"


async def prepare_session(
    payload: AgentQueryRequest,
    user: User,
    db: AsyncSession,
) -> tuple[AgentSession, dict]:
    if payload.session_id:
        session = await db.get(AgentSession, payload.session_id)
    else:
        session = None

    if session is None:
        session = AgentSession(user_id=user.id, title=payload.query[:80])
        db.add(session)
        await db.flush()

    previous_messages = (
        await db.scalars(
            select(Message)
            .where(Message.session_id == session.id)
            .order_by(Message.created_at.desc())
            .limit(12)
        )
    ).all()
    conversation_history = [
        {
            "role": message.role,
            "content": message.content,
            "metadata": message.metadata_json or {},
        }
        for message in reversed(previous_messages)
    ]
    app_state = dict(payload.app_state or {})
    app_state["conversation_history"] = conversation_history

    user_message = Message(session_id=session.id, role="user", content=payload.query, metadata_json={})
    db.add(user_message)
    await db.flush()
    return session, app_state


async def persist_agent_result(
    db: AsyncSession,
    session: AgentSession,
    result: AgentResult,
    trace,
) -> Message:
    assistant_message = Message(
        session_id=session.id,
        role="assistant",
        content=result.answer,
        metadata_json={"intent": result.intent},
    )
    db.add(assistant_message)
    await db.flush()

    for call in result.tool_calls:
        tracing.tool_span(trace, call.tool_name, call.input, call.output, call.latency_ms)
        db.add(
            ToolRun(
                session_id=session.id,
                message_id=assistant_message.id,
                tool_name=call.tool_name,
                status=call.status,
                input_json=jsonable_encoder(call.input),
                output_json=jsonable_encoder(call.output),
                latency_ms=call.latency_ms,
            )
        )
    await db.commit()
    return assistant_message


def agent_response(session: AgentSession, assistant_message: Message, result: AgentResult) -> AgentQueryResponse:
    return AgentQueryResponse(
        session_id=session.id,
        message_id=assistant_message.id,
        intent=result.intent,
        answer=result.answer,
        tool_calls=[tool_call_response(call) for call in result.tool_calls],
        ui_actions=jsonable_encoder(result.ui_actions),
    )


@router.post("/query", response_model=AgentQueryResponse)
async def query_agent(
    payload: AgentQueryRequest,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> AgentQueryResponse:
    session, app_state = await prepare_session(payload, user, db)

    async with tracing.trace("agent.query", user_id=user.id, input={"query": payload.query}) as trace:
        try:
            result = await orchestrator.run(db, payload.query, user, app_state)
        except Exception as exc:  # noqa: BLE001
            logger.exception("Agent query failed")
            result = AgentResult(
                intent="agent-error",
                answer=(
                    "Агентский запрос завершился ошибкой, но backend остался жив. "
                    f"`{exc.__class__.__name__}`: {exc}"
                ),
                tool_calls=[],
                ui_actions=[],
            )
        assistant_message = await persist_agent_result(db, session, result, trace)

    return agent_response(session, assistant_message, result)


@router.post("/query/stream")
async def stream_agent_query(
    payload: AgentQueryRequest,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> StreamingResponse:
    queue: asyncio.Queue[dict] = asyncio.Queue()

    async def emit(event: dict) -> None:
        await queue.put(event)

    async def run_agent() -> None:
        try:
            session, app_state = await prepare_session(payload, user, db)
            await queue.put({"type": "session", "session_id": session.id})

            async with tracing.trace("agent.query", user_id=user.id, input={"query": payload.query}) as trace:
                try:
                    result = await orchestrator.run(db, payload.query, user, app_state, event_handler=emit)
                except Exception as exc:  # noqa: BLE001
                    logger.exception("Agent stream query failed")
                    result = AgentResult(
                        intent="agent-error",
                        answer=(
                            "Агентский запрос завершился ошибкой, но backend остался жив. "
                            f"`{exc.__class__.__name__}`: {exc}"
                        ),
                        tool_calls=[],
                        ui_actions=[],
                    )
                assistant_message = await persist_agent_result(db, session, result, trace)
                await queue.put(
                    {
                        "type": "final",
                        "response": agent_response(session, assistant_message, result),
                    }
                )
        except Exception as exc:  # noqa: BLE001
            logger.exception("Agent stream failed outside orchestrator")
            await queue.put(
                {
                    "type": "error",
                    "error": f"{exc.__class__.__name__}: {exc}",
                }
            )
        finally:
            await queue.put({"type": "done"})

    async def event_stream():
        task = asyncio.create_task(run_agent())
        while True:
            event = await queue.get()
            if event.get("type") == "done":
                break
            yield encode_stream_event(event)
        await task

    return StreamingResponse(event_stream(), media_type="application/x-ndjson")
