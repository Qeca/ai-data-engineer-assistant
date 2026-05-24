from dataclasses import dataclass
from time import perf_counter
from typing import Any, Awaitable, Callable, Literal, TypedDict

from langgraph.graph import END, START, StateGraph
from sqlalchemy.ext.asyncio import AsyncSession

from app.agent.mcp_instruction_book import MCPInstructionBook
from app.agent.tool_registry import AgentToolRegistry
from app.core.config import settings
from app.models import User
from app.services.guardrails import AgentGuardrails
from app.services.magnitgpt import MagnitGPTToolClient
from app.services.openai_responses import OpenAIResponsesClient
from app.services.openrouter import OpenRouterToolClient
from app.tools.base import ToolExecution


@dataclass
class AgentResult:
    intent: str
    answer: str
    tool_calls: list[ToolExecution]
    ui_actions: list[dict[str, Any]]
    elapsed_ms: int = 0


class AgentGraphState(TypedDict, total=False):
    query: str
    registry: AgentToolRegistry
    provider: str
    response: dict[str, Any]
    messages: list[dict[str, Any]]
    pending_calls: list[Any]
    openai_outputs: list[dict[str, Any]]
    tool_calls: list[ToolExecution]
    intent: str
    answer: str
    ui_actions: list[dict[str, Any]]
    event_handler: Callable[[dict[str, Any]], Awaitable[None]]


class AgentOrchestrator:
    def __init__(self) -> None:
        self.openai = OpenAIResponsesClient()
        self.openrouter = OpenRouterToolClient()
        self.magnitgpt = MagnitGPTToolClient()
        self.mcp_instruction_book = MCPInstructionBook()
        self.guardrails = AgentGuardrails()
        self.graph = self._build_graph()

    async def run(
        self,
        db: AsyncSession,
        query: str,
        user: User,
        app_state: dict[str, Any] | None = None,
        event_handler: Callable[[dict[str, Any]], Awaitable[None]] | None = None,
    ) -> AgentResult:
        registry = AgentToolRegistry(db, user, app_state)
        started_at = perf_counter()
        decision = self.guardrails.validate_user_query(query)
        if not decision.allowed:
            return AgentResult(
                intent="guardrail-blocked",
                answer=decision.answer(),
                tool_calls=[],
                ui_actions=[],
                elapsed_ms=round((perf_counter() - started_at) * 1000),
            )
        final_state = await self.graph.ainvoke(
            {
                "query": query,
                "registry": registry,
                "tool_calls": [],
                "event_handler": event_handler,
            }
        )
        tool_calls = final_state.get("tool_calls", [])
        return AgentResult(
            intent=final_state.get("intent") or self._infer_intent(tool_calls),
            answer=final_state.get("answer") or self._empty_model_answer(tool_calls),
            tool_calls=tool_calls,
            ui_actions=final_state.get("ui_actions") or self._collect_ui_actions(tool_calls),
            elapsed_ms=round((perf_counter() - started_at) * 1000),
        )

    def _build_graph(self):
        workflow = StateGraph(AgentGraphState)
        workflow.add_node("select_runtime", self._select_runtime_node)
        workflow.add_node("call_model", self._call_model_node)
        workflow.add_node("execute_tools", self._execute_tools_node)
        workflow.add_node("configuration_error", self._configuration_error_node)
        workflow.add_node("finalize", self._finalize_node)

        workflow.add_edge(START, "select_runtime")
        workflow.add_conditional_edges(
            "select_runtime",
            self._route_after_select_runtime,
            {"llm": "call_model", "error": "configuration_error"},
        )
        workflow.add_conditional_edges(
            "call_model",
            self._route_after_model,
            {"tools": "execute_tools", "final": "finalize"},
        )
        workflow.add_conditional_edges(
            "execute_tools",
            self._route_after_tools,
            {"llm": "call_model", "final": "finalize"},
        )
        workflow.add_edge("configuration_error", END)
        workflow.add_edge("finalize", END)
        return workflow.compile()

    async def _select_runtime_node(self, state: AgentGraphState) -> AgentGraphState:
        registry = state["registry"]
        if settings.llm_provider == "magnitgpt" and self.magnitgpt.enabled:
            return {
                "provider": "magnitgpt",
                "messages": self._initial_messages(state["query"], registry.app_state),
            }
        if settings.llm_provider == "openrouter" and self.openrouter.enabled:
            return {
                "provider": "openrouter",
                "messages": self._initial_messages(state["query"], registry.app_state),
            }
        if settings.llm_provider == "openai" and self.openai.enabled:
            return {"provider": "openai"}
        return {"provider": "unconfigured", "intent": "configuration-error", "answer": self._configuration_error()}

    @staticmethod
    def _route_after_select_runtime(state: AgentGraphState) -> Literal["llm", "error"]:
        return "llm" if state.get("provider") in {"openai", "openrouter", "magnitgpt"} else "error"

    async def _call_model_node(self, state: AgentGraphState) -> AgentGraphState:
        provider = state["provider"]
        registry = state["registry"]
        tool_calls = state.get("tool_calls", [])

        if provider == "openai":
            response = await self._call_openai_model(state, registry)
            function_calls = self.openai.get_function_calls(response)
            if not function_calls:
                intent = self._infer_intent(tool_calls)
                answer = self.openai.get_text(response) or self._empty_model_answer(tool_calls)
                return {"response": response, "pending_calls": [], "intent": intent, "answer": answer}
            return {"response": response, "pending_calls": function_calls, "openai_outputs": []}

        chat_client = self._chat_client(provider)
        response = await chat_client.create(
            state.get("messages", []),
            registry.specs(),
            self._instructions(),
        )
        function_calls = chat_client.get_function_calls(response)
        if not function_calls:
            intent = self._infer_intent(tool_calls)
            answer = chat_client.get_text(response) or self._empty_model_answer(tool_calls)
            return {"response": response, "pending_calls": [], "intent": intent, "answer": answer}
        return {"response": response, "pending_calls": function_calls}

    async def _call_openai_model(
        self,
        state: AgentGraphState,
        registry: AgentToolRegistry,
    ) -> dict[str, Any]:
        response = state.get("response")
        if not response:
            return await self.openai.create(
                input_payload=self._initial_messages(state["query"], registry.app_state),
                tools=registry.specs(),
                instructions=self._instructions(),
            )

        return await self.openai.create(
            input_payload=state.get("openai_outputs", []),
            tools=registry.specs(),
            instructions=self._instructions(),
            previous_response_id=response["id"],
        )

    @staticmethod
    def _route_after_model(state: AgentGraphState) -> Literal["tools", "final"]:
        return "tools" if state.get("pending_calls") else "final"

    async def _execute_tools_node(self, state: AgentGraphState) -> AgentGraphState:
        registry = state["registry"]
        provider = state["provider"]
        tool_calls = list(state.get("tool_calls", []))
        openai_outputs: list[dict[str, Any]] = []
        messages = list(state.get("messages", []))

        if provider in {"openrouter", "magnitgpt"}:
            messages.append(self._chat_client(provider).assistant_message_for_history(state["response"]))

        for call in state.get("pending_calls", []):
            await self._emit(
                state,
                {
                    "type": "tool_call_start",
                    "tool_name": call.name,
                    "arguments": call.arguments,
                },
            )
            execution = await registry.execute(call.name, call.arguments)
            tool_calls.append(execution)
            await self._emit(
                state,
                {
                    "type": "tool_call_result",
                    "tool_call": self._tool_event_payload(execution),
                },
            )
            if provider == "openai":
                openai_outputs.append(
                    {
                        "type": "function_call_output",
                        "call_id": call.call_id,
                        "output": registry.tool_output_for_model(execution),
                    }
                )
            else:
                messages.append(
                    self._chat_client(provider).tool_message(
                        call.call_id,
                        registry.tool_output_for_model(execution),
                    )
                )

        result: AgentGraphState = {
            "tool_calls": tool_calls,
            "pending_calls": [],
            "openai_outputs": openai_outputs,
            "messages": messages,
        }
        return result

    def _route_after_tools(self, state: AgentGraphState) -> Literal["llm", "final"]:
        return "llm"

    @staticmethod
    async def _emit(state: AgentGraphState, event: dict[str, Any]) -> None:
        handler = state.get("event_handler")
        if handler:
            await handler(event)

    @staticmethod
    def _tool_event_payload(execution: ToolExecution) -> dict[str, Any]:
        return {
            "tool_name": execution.tool_name,
            "status": execution.status,
            "input": execution.input,
            "output": execution.output,
            "latency_ms": execution.latency_ms,
            "ui_actions": execution.ui_actions,
        }

    async def _configuration_error_node(self, state: AgentGraphState) -> AgentGraphState:
        return {
            "intent": "configuration-error",
            "answer": state.get("answer") or self._configuration_error(),
            "tool_calls": [],
            "ui_actions": [],
        }

    def _finalize_node(self, state: AgentGraphState) -> AgentGraphState:
        tool_calls = state.get("tool_calls", [])
        intent = state.get("intent") or self._infer_intent(tool_calls)
        answer = state.get("answer") or self._empty_model_answer(tool_calls)
        return {
            "intent": intent,
            "answer": answer,
            "ui_actions": self._collect_ui_actions(tool_calls),
        }

    def _instructions(self) -> str:
        return (
            "Ты AI Data Engineer Assistant внутри рабочей платформы. "
            "Используй OpenAI function calling для любых данных и действий. "
            "Не выполняй опасные действия: удаление/очистка данных, чтение секретов, отключение авторизации, destructive shell/Git/Docker-команды. "
            "Если пользователь просит опасное действие, не вызывай tools и объясни безопасную альтернативу. "
            "Для готовых внешних MCP-серверов используй list_mcp_products, list_mcp_tools и call_mcp_tool. "
            "Для обычных действий в UI используй локальные product tools; внешний MCP вызывай только если пользователь явно пишет MCP или просит проверить именно через MCP. "
            "Не дублируй одну и ту же проверку через локальный tool и внешний MCP. "
            "Не выдумывай статусы: вызывай list_site_status, list_pipelines, get_airflow_run или get_spark_job. "
            "Если пользователь спрашивает, что лежит в базе/БД/warehouse, вызывай inspect_database. "
            "Если пользователь спрашивает про подключения к БД или просит настроить БД, вызывай list_database_connections, upsert_database_connection и test_database_connection. "
            "Для SQL вызывай execute_sql. Для Airflow вызывай trigger_airflow_dag/get_airflow_run. "
            "Для просмотра результатов DAG как в Airflow UI вызывай list_airflow_runs, list_airflow_task_instances и get_airflow_task_log. "
            "Для управления DAG как в интерфейсе вызывай manage_airflow_dags: pause, unpause, pause_all, unpause_all или list. "
            "Для Spark вызывай submit_spark_job/get_spark_job. "
            "Для изменения существующего DAG сначала вызывай read_airflow_dag, затем write_airflow_dag с полным обновленным кодом. "
            "Для изменения существующего Spark-скрипта сначала вызывай read_spark_script, затем write_spark_script с полным обновленным кодом. "
            "Для написания нового DAG вызывай write_airflow_dag, для нового Spark-скрипта write_spark_script. "
            "После записи DAG запускай check_airflow_dag_sandbox, после записи Spark-скрипта run_spark_script_sandbox. "
            "Для отладки существующих DAG/скриптов используй sandbox tools, а не просто текстовый совет. "
            "Для выполнения произвольного кода или bash-команд вызывай run_bash_sandbox; это Docker sandbox, не host shell. "
            "Для истории и версий артефактов вызывай list_artifact_versions; версии ведутся в Git. "
            "Для прямой работы с Git вызывай run_git_command и передавай полную команду вида git status --short, git diff или git log --oneline -n 5. "
            "Пользователь не имеет прямого доступа к внутреннему workspace и путям вида /workspace/...; показывай код, логи и результаты в ответе или UI, а не отправляй пользователя открывать такие файлы самому. "
            "Для управления интерфейсом вызывай navigate_site. "
            "Перед SQL учитывай description инструмента execute_sql: там указан текущий SQL dialect. "
            "Отвечай на русском, кратко, указывая какие действия выполнены, без emoji. "
            f"{self.mcp_instruction_book.render()}"
        )

    @staticmethod
    def _initial_messages(query: str, app_state: dict[str, Any]) -> list[dict[str, str]]:
        messages: list[dict[str, str]] = []
        history = app_state.get("conversation_messages")
        if isinstance(history, list):
            for item in history[-8:]:
                if not isinstance(item, dict):
                    continue
                role = item.get("role")
                content = item.get("content")
                if role in {"user", "assistant"} and isinstance(content, str) and content.strip():
                    messages.append({"role": role, "content": content[:4000]})
        messages.append({"role": "user", "content": AgentOrchestrator._user_prompt(query, app_state)})
        return messages

    @staticmethod
    def _user_prompt(query: str, app_state: dict[str, Any]) -> str:
        visible_state = {
            key: value
            for key, value in app_state.items()
            if key not in {"conversation_history", "conversation_messages"}
        }
        return (
            f"Запрос пользователя: {query}\n\n"
            f"Текущее состояние frontend-приложения: {visible_state}\n"
            "Если нужно управлять сайтом, вызови соответствующий function tool."
        )

    @staticmethod
    def _collect_ui_actions(tool_calls: list[ToolExecution]) -> list[dict[str, Any]]:
        actions: list[dict[str, Any]] = []
        for call in tool_calls:
            actions.extend(call.ui_actions)
        return actions

    @staticmethod
    def _infer_intent(tool_calls: list[ToolExecution]) -> str:
        if not tool_calls:
            return "chat"
        names = {call.tool_name for call in tool_calls}
        if "SQLTool" in names:
            return "sql"
        if "DatabaseInspectorTool" in names:
            return "database"
        if "DatabaseConnectionTool" in names:
            return "database-connections"
        if "AirflowControlTool" in names:
            return "airflow-control"
        if any(name in names for name in ["AirflowTool", "AirflowRunTool", "AirflowTaskTool", "AirflowLogTool"]):
            return "airflow"
        if "SparkTool" in names:
            return "spark"
        if "AirflowDAGSourceTool" in names or "SparkScriptSourceTool" in names:
            return "artifact"
        if "ArtifactTool" in names:
            return "artifact"
        if any(name in names for name in ["AirflowSandboxTool", "SparkSandboxTool", "PythonSandboxTool"]):
            return "debug"
        if "ArtifactVersionTool" in names:
            return "versioning"
        if "CatalogTool" in names:
            return "catalog"
        if "MCPDiscoveryTool" in names or "ExternalMCPTool" in names:
            return "mcp"
        if "GuardrailTool" in names:
            return "guardrail-blocked"
        if "SiteControlTool" in names:
            return "site-control"
        if "SiteStatusTool" in names:
            return "site-status"
        return "tools"

    def _chat_client(self, provider: str) -> OpenRouterToolClient | MagnitGPTToolClient:
        if provider == "openrouter":
            return self.openrouter
        if provider == "magnitgpt":
            return self.magnitgpt
        raise ValueError(f"Unsupported chat tool provider: {provider}")

    @staticmethod
    def _configuration_error() -> str:
        return (
            "LLM не настроена. Rule-based fallback отключен: агент не будет подменять модель "
            "захардкоженными сценариями. Для локального запуска задай `LLM_PROVIDER=magnitgpt`, "
            "`MAGNITGPT_API_KEY`, `MAGNITGPT_BASE_URL` и `MAGNITGPT_MODEL`."
        )

    @staticmethod
    def _empty_model_answer(tool_calls: list[ToolExecution]) -> str:
        if tool_calls:
            names = ", ".join(call.tool_name for call in tool_calls)
            return (
                "Модель выполнила tool calls, но не вернула финальный текст. "
                f"Выполненные tools: {names}."
            )
        return "Модель не вернула tool calls или финальный ответ."

    def _tool_observation_summary(self, provider: str, tool_calls: list[ToolExecution]) -> str:
        lines = [
            f"{provider} выполнил tool calls, но не вернул финальный текст. Возвращаю резюме наблюдений.",
            "",
        ]

        airflow_lines = self._airflow_observation_lines(tool_calls)
        if airflow_lines:
            lines.append("**Airflow**")
            lines.extend(airflow_lines)
            lines.append("")

        database_line = self._database_observation_line(tool_calls)
        if database_line:
            lines.append("**База данных**")
            lines.append(database_line)
            lines.append("")

        mcp_line = self._mcp_observation_line(tool_calls)
        if mcp_line:
            lines.append("**MCP**")
            lines.append(mcp_line)
            lines.append("")

        tool_names = ", ".join(dict.fromkeys(call.tool_name for call in tool_calls))
        lines.append(f"Выполненные tools: {tool_names}.")
        return "\n".join(lines).strip()

    @staticmethod
    def _airflow_observation_lines(tool_calls: list[ToolExecution]) -> list[str]:
        pipelines: dict[str, dict[str, Any]] = {}
        triggered: list[str] = []
        run_statuses: list[str] = []

        for call in tool_calls:
            if call.tool_name == "AirflowControlTool":
                for pipeline in call.output.get("pipelines", []):
                    pipelines[pipeline["dag_id"]] = pipeline
            if call.tool_name == "AirflowTool" and "pipelines" in call.output:
                for pipeline in call.output.get("pipelines", []):
                    pipelines[pipeline["dag_id"]] = pipeline
            if call.tool_name == "AirflowTool" and call.output.get("run_id"):
                dag_id = call.output.get("dag_id")
                run_id = call.output.get("run_id")
                status = call.output.get("status")
                item = f"`{dag_id}` run `{run_id}`: `{status}`"
                if call.input.get("action") == "trigger":
                    triggered.append(item)
                else:
                    run_statuses.append(item)

        lines: list[str] = []
        if pipelines:
            lines.append(f"Видно DAG: {len(pipelines)}.")
            for pipeline in sorted(pipelines.values(), key=lambda item: item["dag_id"]):
                task_summary = AgentOrchestrator._task_summary(pipeline.get("tasks", []))
                task_suffix = f" Tasks: {task_summary}" if task_summary else ""
                lines.append(
                    "- "
                    f"`{pipeline.get('dag_id')}`: `{pipeline.get('status')}`, "
                    f"schedule `{pipeline.get('schedule')}`, owner `{pipeline.get('owner')}`."
                    f"{task_suffix}"
                )
        if triggered:
            lines.append(f"Запущено DAG: {len(triggered)}.")
            lines.extend(f"- {item}" for item in dict.fromkeys(triggered).keys())
        if run_statuses:
            lines.append("Проверенные run statuses:")
            lines.extend(f"- {item}" for item in list(dict.fromkeys(run_statuses).keys())[:8])
        return lines

    @staticmethod
    def _task_summary(tasks: Any) -> str:
        if not isinstance(tasks, list) or not tasks:
            return ""
        parts = []
        for task in tasks[:5]:
            task_id = task.get("task_id") if isinstance(task, dict) else None
            operator = task.get("operator_name") if isinstance(task, dict) else None
            if task_id:
                parts.append(f"`{task_id}`" + (f" ({operator})" if operator else ""))
        return " -> ".join(parts)

    @staticmethod
    def _database_observation_line(tool_calls: list[ToolExecution]) -> str | None:
        inspector = next((call for call in reversed(tool_calls) if call.tool_name == "DatabaseInspectorTool"), None)
        connection_tool = next((call for call in reversed(tool_calls) if call.tool_name == "DatabaseConnectionTool"), None)
        if connection_tool is not None:
            connections = connection_tool.output.get("connections")
            if connections is None and isinstance(connection_tool.output.get("connection"), dict):
                connections = [connection_tool.output["connection"]]
            if connections is not None:
                preview = ", ".join(
                    f"`{item.get('name')}`/{item.get('engine')} ({item.get('status')})"
                    for item in connections[:8]
                    if isinstance(item, dict)
                )
                return f"Подключения к БД: {len(connections)}. Основные: {preview}."

        if inspector is None:
            return None

        tables = inspector.output.get("tables", [])
        preview = ", ".join(f"`{table.get('name')}` ({table.get('row_count')} строк)" for table in tables[:6])
        return f"Проверено таблиц: {inspector.output.get('table_count', len(tables))}. Основные: {preview}."

    @staticmethod
    def _mcp_observation_line(tool_calls: list[ToolExecution]) -> str | None:
        discovery = next((call for call in reversed(tool_calls) if call.tool_name == "MCPDiscoveryTool"), None)
        if discovery is None:
            return None

        tools = discovery.output.get("tools", [])
        product = discovery.output.get("product")
        names = ", ".join(f"`{tool.get('name')}`" for tool in tools[:12])
        return f"Для `{product}` найдено MCP tools: {names}."
