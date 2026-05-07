from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Literal, TypedDict

from langgraph.graph import END, START, StateGraph
from sqlalchemy.ext.asyncio import AsyncSession

from app.agent.tool_registry import AgentToolRegistry
from app.core.config import settings
from app.models import User
from app.services.openai_responses import OpenAIResponsesClient
from app.services.openrouter import OpenRouterToolClient
from app.tools.base import ToolExecution
from app.tools.sql import SQLTool


@dataclass
class AgentResult:
    intent: str
    answer: str
    tool_calls: list[ToolExecution]
    ui_actions: list[dict[str, Any]]


class AgentGraphState(TypedDict, total=False):
    query: str
    registry: AgentToolRegistry
    provider: str
    response: dict[str, Any]
    messages: list[dict[str, Any]]
    pending_calls: list[Any]
    openai_outputs: list[dict[str, Any]]
    tool_calls: list[ToolExecution]
    loop_count: int
    intent: str
    answer: str
    ui_actions: list[dict[str, Any]]


class AgentOrchestrator:
    max_tool_steps = 8

    def __init__(self) -> None:
        self.openai = OpenAIResponsesClient()
        self.openrouter = OpenRouterToolClient()
        self.sql_helper = SQLTool()
        self.graph = self._build_graph()

    async def run(
        self,
        db: AsyncSession,
        query: str,
        user: User,
        app_state: dict[str, Any] | None = None,
    ) -> AgentResult:
        registry = AgentToolRegistry(db, user, app_state)
        final_state = await self.graph.ainvoke(
            {
                "query": query,
                "registry": registry,
                "tool_calls": [],
                "loop_count": 0,
            }
        )
        tool_calls = final_state.get("tool_calls", [])
        return AgentResult(
            intent=final_state.get("intent") or self._infer_intent(tool_calls),
            answer=final_state.get("answer") or self._fallback_answer(query, "tools", tool_calls),
            tool_calls=tool_calls,
            ui_actions=final_state.get("ui_actions") or self._collect_ui_actions(tool_calls),
        )

    def _build_graph(self):
        workflow = StateGraph(AgentGraphState)
        workflow.add_node("select_runtime", self._select_runtime_node)
        workflow.add_node("call_model", self._call_model_node)
        workflow.add_node("execute_tools", self._execute_tools_node)
        workflow.add_node("local_tool_plan", self._local_tool_plan_node)
        workflow.add_node("finalize", self._finalize_node)

        workflow.add_edge(START, "select_runtime")
        workflow.add_conditional_edges(
            "select_runtime",
            self._route_after_select_runtime,
            {"llm": "call_model", "local": "local_tool_plan"},
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
        workflow.add_edge("local_tool_plan", "finalize")
        workflow.add_edge("finalize", END)
        return workflow.compile()

    async def _select_runtime_node(self, state: AgentGraphState) -> AgentGraphState:
        registry = state["registry"]
        if settings.llm_provider == "openrouter" and self.openrouter.enabled:
            return {
                "provider": "openrouter",
                "messages": [{"role": "user", "content": self._user_prompt(state["query"], registry.app_state)}],
            }
        if settings.llm_provider == "openai" and self.openai.enabled:
            return {"provider": "openai"}
        return {"provider": "local"}

    @staticmethod
    def _route_after_select_runtime(state: AgentGraphState) -> Literal["llm", "local"]:
        return "llm" if state.get("provider") in {"openai", "openrouter"} else "local"

    async def _call_model_node(self, state: AgentGraphState) -> AgentGraphState:
        provider = state["provider"]
        registry = state["registry"]
        query = state["query"]
        tool_calls = state.get("tool_calls", [])

        if provider == "openai":
            response = await self._call_openai_model(state, registry)
            function_calls = self.openai.get_function_calls(response)
            if not function_calls:
                intent = self._infer_intent(tool_calls)
                answer = self.openai.get_text(response) or self._fallback_answer(query, intent, tool_calls)
                return {"response": response, "pending_calls": [], "intent": intent, "answer": answer}
            return {"response": response, "pending_calls": function_calls, "openai_outputs": []}

        response = await self.openrouter.create(
            state.get("messages", []),
            registry.specs(),
            self._instructions(),
        )
        function_calls = self.openrouter.get_function_calls(response)
        if not function_calls:
            intent = self._infer_intent(tool_calls)
            answer = self.openrouter.get_text(response) or self._fallback_answer(query, intent, tool_calls)
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
                input_payload=self._user_prompt(state["query"], registry.app_state),
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

        if provider == "openrouter":
            messages.append(self.openrouter.assistant_message_for_history(state["response"]))

        for call in state.get("pending_calls", []):
            execution = await registry.execute(call.name, call.arguments)
            tool_calls.append(execution)
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
                    self.openrouter.tool_message(
                        call.call_id,
                        registry.tool_output_for_model(execution),
                    )
                )

        loop_count = state.get("loop_count", 0) + 1
        result: AgentGraphState = {
            "tool_calls": tool_calls,
            "pending_calls": [],
            "openai_outputs": openai_outputs,
            "messages": messages,
            "loop_count": loop_count,
        }
        if loop_count >= self.max_tool_steps:
            result["intent"] = self._infer_intent(tool_calls)
            result["answer"] = f"{provider} tool calling остановлен после максимального числа tool-call шагов."
        return result

    def _route_after_tools(self, state: AgentGraphState) -> Literal["llm", "final"]:
        return "final" if state.get("loop_count", 0) >= self.max_tool_steps else "llm"

    async def _local_tool_plan_node(self, state: AgentGraphState) -> AgentGraphState:
        result = await self._run_local_fallback(state["query"], state["registry"])
        return {
            "intent": result.intent,
            "answer": result.answer,
            "tool_calls": result.tool_calls,
            "ui_actions": result.ui_actions,
        }

    def _finalize_node(self, state: AgentGraphState) -> AgentGraphState:
        tool_calls = state.get("tool_calls", [])
        intent = state.get("intent") or self._infer_intent(tool_calls)
        answer = state.get("answer") or self._fallback_answer(state["query"], intent, tool_calls)
        return {
            "intent": intent,
            "answer": answer,
            "ui_actions": self._collect_ui_actions(tool_calls),
        }

    async def _run_local_fallback(
        self,
        query: str,
        registry: AgentToolRegistry,
    ) -> AgentResult:
        intent = self.classify(query)
        tool_calls: list[ToolExecution] = []

        if intent == "airflow":
            dag_id = self._extract_dag_id(query)
            tool_name = "trigger_airflow_dag" if self._wants_run(query) else "list_pipelines"
            args = {"dag_id": dag_id} if tool_name == "trigger_airflow_dag" else {}
            tool_calls.append(await registry.execute(tool_name, args))
        elif intent == "spark":
            tool_calls.append(
                await registry.execute(
                    "submit_spark_job",
                    {
                        "name": self._extract_spark_name(query),
                        "app_resource": "local:///opt/spark/jobs/sample_job.py",
                        "executor_memory": "6g",
                        "partitions": 96,
                    },
                )
            )
        elif intent == "artifact_airflow":
            dag_id = self._extract_dag_id(query)
            code = self._extract_code_block(query) or self._dag_template(dag_id)
            tool_calls.append(
                await registry.execute(
                    "write_airflow_dag",
                    {
                        "dag_id": dag_id,
                        "code": code,
                        "message": "Created by local agent fallback",
                    },
                )
            )
            tool_calls.append(
                await registry.execute(
                    "check_airflow_dag_sandbox",
                    {"dag_id": dag_id, "code": code, "error_log": ""},
                )
            )
        elif intent == "artifact_spark":
            script_name = self._extract_spark_name(query)
            code = self._extract_code_block(query) or self._spark_template(script_name)
            tool_calls.append(
                await registry.execute(
                    "write_spark_script",
                    {
                        "script_name": script_name,
                        "code": code,
                        "message": "Created by local agent fallback",
                    },
                )
            )
            tool_calls.append(
                await registry.execute(
                    "run_spark_script_sandbox",
                    {"script_name": script_name, "code": code, "error_log": "", "arguments": []},
                )
            )
        elif intent == "database":
            tool_calls.append(await registry.execute("inspect_database", {"sample_limit": 3}))
        elif intent == "airflow_control":
            action = self._airflow_control_action(query)
            dag_id = "" if action.endswith("_all") else self._extract_dag_id(query)
            tool_calls.append(await registry.execute("manage_airflow_dags", {"action": action, "dag_id": dag_id}))
        elif intent == "catalog":
            tool_calls.append(await registry.execute("list_catalog", {}))
        elif intent == "site":
            tool_calls.append(await registry.execute("list_site_status", {}))
        elif intent == "mcp":
            tool_calls.append(await registry.execute("list_mcp_products", {}))
        elif intent.startswith("navigate:"):
            screen = intent.split(":", 1)[1]
            tool_calls.append(await registry.execute("navigate_site", {"screen": screen}))
        elif intent == "versioning":
            tool_calls.append(
                await registry.execute("list_artifact_versions", {"artifact_type": "all", "artifact_name": ""})
            )
        elif intent == "debug":
            tool_calls.append(
                await registry.execute(
                    self._sandbox_tool_for_query(query),
                    {
                        **self._sandbox_args_for_query(query),
                        "error_log": query,
                        "arguments": [],
                    },
                )
            )
        else:
            tool_calls.append(
                await registry.execute(
                    "execute_sql",
                    {"query": self._select_sql(query), "limit": 100},
                )
            )

        final_intent = self._infer_intent(tool_calls) or intent
        return AgentResult(
            intent=final_intent,
            answer=self._fallback_answer(query, final_intent, tool_calls),
            tool_calls=tool_calls,
            ui_actions=self._collect_ui_actions(tool_calls),
        )

    @staticmethod
    def _instructions() -> str:
        return (
            "Ты AI Data Engineer Assistant внутри рабочей платформы. "
            "Используй OpenAI function calling для любых данных и действий. "
            "Для готовых внешних MCP-серверов используй list_mcp_products, list_mcp_tools и call_mcp_tool. "
            "Для БД, Airflow, Spark и артефактов сначала предпочитай готовый MCP tool, если он доступен; локальные tools используй как fallback. "
            "Не выдумывай статусы: вызывай list_site_status, list_pipelines, get_airflow_run или get_spark_job. "
            "Если пользователь спрашивает, что лежит в базе/БД/warehouse, вызывай inspect_database. "
            "Для SQL вызывай execute_sql. Для Airflow вызывай trigger_airflow_dag/get_airflow_run. "
            "Для управления DAG как в интерфейсе вызывай manage_airflow_dags: pause, unpause, pause_all, unpause_all или list. "
            "Для Spark вызывай submit_spark_job/get_spark_job. "
            "Для написания DAG вызывай write_airflow_dag, для Spark-скриптов write_spark_script. "
            "После записи DAG запускай check_airflow_dag_sandbox, после записи Spark-скрипта run_spark_script_sandbox. "
            "Для отладки существующих DAG/скриптов используй sandbox tools, а не просто текстовый совет. "
            "Для истории и версий артефактов вызывай list_artifact_versions; версии ведутся в Git. "
            "Для управления интерфейсом вызывай navigate_site. "
            "Перед SQL учитывай description инструмента execute_sql: там указан текущий SQL dialect. "
            "Отвечай на русском, кратко, указывая какие действия выполнены."
        )

    @staticmethod
    def _user_prompt(query: str, app_state: dict[str, Any]) -> str:
        return (
            f"Запрос пользователя: {query}\n\n"
            f"Текущее состояние frontend-приложения: {app_state}\n"
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
        if "AirflowControlTool" in names:
            return "airflow-control"
        if "AirflowTool" in names:
            return "airflow"
        if "SparkTool" in names:
            return "spark"
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
        if "SiteControlTool" in names:
            return "site-control"
        if "SiteStatusTool" in names:
            return "site-status"
        return "tools"

    @staticmethod
    def classify(query: str) -> str:
        q = query.lower()
        if "mcp" in q or "model context protocol" in q:
            return "mcp"
        navigation = {
            "dashboard": "dashboard",
            "дашборд": "dashboard",
            "ai agent": "ai-agent",
            "агент": "ai-agent",
            "sql workspace": "sql",
            "airflow": "airflow",
            "dag": "airflow",
            "dags": "airflow",
            "даг": "airflow",
            "даги": "airflow",
            "пайплайны": "pipelines",
            "pipelines": "pipelines",
            "spark": "spark",
            "catalog": "catalog",
            "каталог": "catalog",
            "settings": "settings",
            "настрой": "settings",
            "profile": "profile",
            "профил": "profile",
        }
        if any(word in q for word in ["открой", "перейди", "покажи экран", "navigate", "go to"]):
            for marker, screen in navigation.items():
                if marker in q:
                    return f"navigate:{screen}"

        dag_words = ["airflow", "dag", "dags", "даг", "даги", "пайплайн", "pipeline"]
        pause_words = ["останов", "выключ", "pause", "disable", "поставь на пауз", "пауза"]
        resume_words = ["unpause", "resume", "enable", "включ", "возобнов"]
        all_words = ["все", "all", "кажд"]
        wants_resume_all = any(word in q for word in ["запусти", "start", "run"]) and any(
            word in q for word in all_words
        )
        if any(word in q for word in dag_words) and (
            any(word in q for word in pause_words)
            or any(word in q for word in resume_words)
            or wants_resume_all
        ):
            return "airflow_control"

        database_words = ["баз", "бд", "database", "warehouse", "данные"]
        inspect_words = ["что", "лежит", "покажи", "список", "таблиц", "схема", "какие", "внутри"]
        if any(word in q for word in database_words) and any(word in q for word in inspect_words):
            return "database"

        if any(word in q for word in ["статус", "status", "состояни", "что сейчас", "все статусы"]):
            return "site"
        if any(word in q for word in ["верс", "version", "история"]) and any(
            word in q for word in ["даг", "dag", "скрипт", "spark", "артефакт", "artifact"]
        ):
            return "versioning"
        if any(word in q for word in ["отлад", "debug", "ошибка", "traceback"]):
            return "debug"
        wants_write = any(word in q for word in ["напиши", "создай", "сгенерируй", "сохрани", "write", "create"])
        if wants_write and any(word in q for word in ["airflow", "dag", "даг", "spark", "pyspark", "скрипт"]):
            if any(word in q for word in ["spark", "pyspark", "скрипт"]):
                return "artifact_spark"
            return "artifact_airflow"
        if any(word in q for word in ["airflow", "dag", "даг", "пайплайн", "pipeline", "запусти"]):
            return "airflow"
        if any(word in q for word in ["spark", "executor", "shuffle", "oom", "job", "джоб"]):
            return "spark"
        if any(word in q for word in ["select", "sql", "orders", "заказ", "аномал", "выручк"]):
            return "sql"
        if any(word in q for word in ["catalog", "каталог", "схема", "таблиц", "колонк"]):
            return "catalog"
        return "site"

    def _fallback_answer(self, query: str, intent: str, tool_calls: list[ToolExecution]) -> str:
        if not tool_calls:
            return "Нет выполненных tool calls."
        latest = tool_calls[-1]
        if latest.status != "success":
            return f"Инструмент {latest.tool_name} вернул ошибку: {latest.output.get('error', 'unknown error')}."

        if intent == "sql":
            rows = latest.output.get("rows", [])
            if not rows:
                return "SQL выполнен успешно, но результат пустой."
            first = rows[0]
            if "order_count" in first:
                return (
                    "Я выполнил SQL-анализ через function call. Самый высокий часовой объём: "
                    f"{first.get('hour')} — {first.get('order_count')} заказов, "
                    f"средний чек {first.get('avg_amount')}."
                )
            return f"SQL выполнен через function call. Вернулось {latest.output.get('row_count', len(rows))} строк."

        if intent == "airflow":
            if "pipelines" in latest.output:
                failed = [p for p in latest.output["pipelines"] if p.get("status") == "failed"]
                return f"Проверил Airflow через function call: DAG всего {len(latest.output['pipelines'])}, проблемных {len(failed)}."
            return (
                "Airflow DAG запущен через function call. "
                f"`{latest.output.get('dag_id')}` run `{latest.output.get('run_id')}` "
                f"сейчас `{latest.output.get('status')}`."
            )

        if intent == "spark":
            return (
                "Spark job отправлен через function call. "
                f"`{latest.output.get('job_id')}` сейчас `{latest.output.get('status')}`."
            )

        if intent == "artifact":
            artifact_call = next((call for call in tool_calls if call.tool_name == "ArtifactTool"), latest)
            sandbox_call = next(
                (
                    call
                    for call in reversed(tool_calls)
                    if call.tool_name in {"AirflowSandboxTool", "SparkSandboxTool", "PythonSandboxTool"}
                ),
                None,
            )
            git_ref = artifact_call.output.get("git_commit_short_sha") or artifact_call.output.get("git_status")
            runtime_status = sandbox_call.output.get("runtime_status") if sandbox_call else "not_run"
            return (
                "Артефакт сохранен через function call: "
                f"`{artifact_call.output.get('artifact_name')}` версия `{artifact_call.output.get('version')}`, "
                f"валидация `{artifact_call.output.get('validation_status')}`, "
                f"sandbox `{runtime_status}`, Git `{git_ref}`."
            )

        if intent == "debug":
            hints = latest.output.get("hints", [])
            suffix = f" Рекомендации: {'; '.join(hints)}." if hints else ""
            status = (
                latest.output.get("validation_status")
                or latest.output.get("syntax_status")
                or latest.output.get("status")
                or "unknown"
            )
            runtime_status = latest.output.get("runtime_status", "unknown")
            return (
                "Артефакт проверен в sandbox через function call: "
                f"syntax `{status}`, runtime `{runtime_status}`."
                f"{suffix}"
            )

        if intent == "versioning":
            versions = latest.output.get("versions", [])
            return f"История артефактов прочитана через function call: найдено {len(versions)} версий."

        if intent == "database":
            tables = latest.output.get("tables", [])
            preview = ", ".join(
                f"{table.get('name')} ({table.get('row_count')} строк)" for table in tables[:5]
            )
            return (
                "Проверил базу через function call: "
                f"{latest.output.get('table_count', len(tables))} таблиц. "
                f"Основные: {preview}."
            )

        if intent == "airflow-control":
            action = latest.output.get("action")
            affected = latest.output.get("affected_dags", [])
            return (
                "Управление Airflow выполнено через function call: "
                f"`{action}`, DAG затронуто {latest.output.get('affected_count', len(affected))}. "
                f"{', '.join(affected[:5])}."
            )

        if intent == "site-control":
            return f"Управляю сайтом: перехожу на экран `{latest.output.get('screen')}`."

        if intent == "site-status":
            pipelines = latest.output.get("pipelines", [])
            spark_jobs = latest.output.get("spark_jobs", [])
            return (
                "Проверил статусы сайта через function call: "
                f"экран `{latest.output.get('current_screen')}`, "
                f"DAG {len(pipelines)}, Spark jobs {len(spark_jobs)}."
            )

        if intent == "mcp":
            if "products" in latest.output:
                products = latest.output.get("products", [])
                names = ", ".join(product.get("product", "") for product in products)
                return f"Проверил готовые MCP-серверы через function call: доступны {names}."
            tools = latest.output.get("tools", [])
            return f"Проверил готовый MCP-сервер через function call: найдено {len(tools)} tools."

        tables = latest.output.get("tables", [])
        return f"Каталог прочитан через function call: {len(tables)} таблиц. Основные: " + ", ".join(
            table["name"] for table in tables[:5]
        )

    @staticmethod
    def _airflow_control_action(query: str) -> str:
        q = query.lower()
        all_requested = any(word in q for word in ["все", "all", "кажд"])
        wants_pause = any(word in q for word in ["останов", "выключ", "pause", "disable", "поставь на пауз", "пауза"])
        wants_unpause = any(word in q for word in ["unpause", "resume", "enable", "включ", "возобнов", "запусти"])
        if wants_pause:
            return "pause_all" if all_requested else "pause"
        if wants_unpause:
            return "unpause_all" if all_requested else "unpause"
        return "list"

    def _select_sql(self, query: str) -> str:
        q = query.lower()
        if any(word in q for word in ["аномал", "час", "orders", "заказ"]):
            return self.sql_helper.anomaly_query()
        return self.sql_helper.revenue_query()

    @staticmethod
    def _wants_run(query: str) -> bool:
        q = query.lower()
        return any(word in q for word in ["запусти", "trigger", "run", "start", "перезапусти"])

    @staticmethod
    def _extract_dag_id(query: str) -> str:
        known = [
            "orders_sync",
            "ml_feature_pipeline",
            "clickstream_aggregation",
            "dw_nightly_refresh",
            "spark_model_training",
        ]
        q = query.lower()
        for dag_id in known:
            if dag_id in q:
                return dag_id
        return "clickstream_aggregation" if "clickstream" in q else "orders_sync"

    @staticmethod
    def _extract_spark_name(query: str) -> str:
        q = query.lower()
        if "clickstream" in q:
            return "clickstream_aggregation_debug"
        if "feature" in q or "фич" in q:
            return "ml_feature_pipeline"
        return "agent_spark_job"

    def _sandbox_tool_for_query(self, query: str) -> str:
        q = query.lower()
        if any(word in q for word in ["airflow", "dag", "даг"]):
            return "check_airflow_dag_sandbox"
        if any(word in q for word in ["spark", "pyspark"]):
            return "run_spark_script_sandbox"
        return "run_python_script_sandbox"

    def _sandbox_args_for_query(self, query: str) -> dict[str, str]:
        code = self._extract_code_block(query)
        tool = self._sandbox_tool_for_query(query)
        if tool == "check_airflow_dag_sandbox":
            return {"dag_id": self._extract_dag_id(query), "code": code or ""}
        if tool == "run_spark_script_sandbox":
            return {"script_name": self._extract_spark_name(query), "code": code or ""}
        return {"script_name": "agent_debug_script.py", "code": code or "print('debug smoke test')"}

    @staticmethod
    def _extract_code_block(query: str) -> str | None:
        match = re.search(r"```(?:python|py)?\s*(.*?)```", query, flags=re.IGNORECASE | re.DOTALL)
        if not match:
            return None
        return match.group(1).strip()

    @staticmethod
    def _dag_template(dag_id: str) -> str:
        return f'''from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def run() -> None:
    print("DAG {dag_id} executed")


with DAG(
    dag_id="{dag_id}",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["agent"],
) as dag:
    PythonOperator(task_id="run", python_callable=run)
'''

    @staticmethod
    def _spark_template(script_name: str) -> str:
        return f'''from __future__ import annotations

from pyspark.sql import SparkSession


class SparkJob:
    def __init__(self, app_name: str) -> None:
        self.spark = SparkSession.builder.appName(app_name).getOrCreate()

    def run(self) -> None:
        data = [("ok", 1)]
        frame = self.spark.createDataFrame(data, ["status", "count"])
        frame.show(truncate=False)
        self.spark.stop()


if __name__ == "__main__":
    SparkJob("{script_name}").run()
'''
