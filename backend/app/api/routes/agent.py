from fastapi import APIRouter, Depends
from fastapi.encoders import jsonable_encoder
from sqlalchemy.ext.asyncio import AsyncSession

from app.agent.orchestrator import AgentOrchestrator
from app.api.deps import get_current_user
from app.db.session import get_db
from app.models import AgentSession, Message, ToolRun, User
from app.schemas import AgentQueryRequest, AgentQueryResponse, ToolCallRead
from app.services.tracing import TraceClient

router = APIRouter(prefix="/agent", tags=["agent"])
orchestrator = AgentOrchestrator()
tracing = TraceClient()


@router.post("/query", response_model=AgentQueryResponse)
async def query_agent(
    payload: AgentQueryRequest,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> AgentQueryResponse:
    if payload.session_id:
        session = await db.get(AgentSession, payload.session_id)
    else:
        session = None

    if session is None:
        session = AgentSession(user_id=user.id, title=payload.query[:80])
        db.add(session)
        await db.flush()

    user_message = Message(session_id=session.id, role="user", content=payload.query, metadata_json={})
    db.add(user_message)
    await db.flush()

    async with tracing.trace("agent.query", user_id=user.id, input={"query": payload.query}) as trace:
        result = await orchestrator.run(db, payload.query, user, payload.app_state)
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
    return AgentQueryResponse(
        session_id=session.id,
        message_id=assistant_message.id,
        intent=result.intent,
        answer=result.answer,
        tool_calls=[
            ToolCallRead(
                tool_name=call.tool_name,
                status=call.status,
                input=jsonable_encoder(call.input),
                output=jsonable_encoder(call.output),
                latency_ms=call.latency_ms,
            )
            for call in result.tool_calls
        ],
        ui_actions=jsonable_encoder(result.ui_actions),
    )
