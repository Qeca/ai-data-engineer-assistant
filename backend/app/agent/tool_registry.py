import json
import time
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.models import SparkJob, User
from app.services.artifacts import ArtifactService
from app.services.connections import DatabaseConnectionService
from app.services.debug_sandbox import DebugSandboxClient
from app.services.external_mcp import ExternalMCPGateway
from app.tools.airflow import AirflowTool
from app.tools.base import ToolExecution
from app.tools.catalog import CatalogTool
from app.tools.spark import SparkTool
from app.tools.sql import SQLTool


class AgentToolRegistry:
    product_tool_names: dict[str, list[str]] = {
        "site": ["list_site_status", "navigate_site"],
        "database": [
            "execute_sql",
            "list_catalog",
            "inspect_database",
            "list_database_connections",
            "upsert_database_connection",
            "test_database_connection",
        ],
        "airflow": [
            "list_pipelines",
            "manage_airflow_dags",
            "trigger_airflow_dag",
            "get_airflow_run",
            "list_airflow_runs",
            "list_airflow_task_instances",
            "get_airflow_task_log",
        ],
        "spark": ["submit_spark_job", "get_spark_job"],
        "external_mcp": ["list_mcp_products", "list_mcp_tools", "call_mcp_tool"],
        "artifacts": [
            "read_airflow_dag",
            "read_spark_script",
            "write_airflow_dag",
            "write_spark_script",
            "check_airflow_dag_sandbox",
            "run_spark_script_sandbox",
            "run_python_script_sandbox",
            "list_artifact_versions",
        ],
    }

    def __init__(self, db: AsyncSession, user: User, app_state: dict[str, Any] | None = None) -> None:
        self.db = db
        self.user = user
        self.app_state = app_state or {}
        self.sql = SQLTool()
        self.catalog = CatalogTool()
        self.connections = DatabaseConnectionService()
        self.airflow = AirflowTool()
        self.spark = SparkTool()
        self.artifacts = ArtifactService()
        self.debug_sandbox = DebugSandboxClient()
        self.external_mcp = ExternalMCPGateway()

    def specs(self) -> list[dict[str, Any]]:
        return [
            self._spec(
                "list_site_status",
                "Return all visible application statuses: current UI screen, user, pipelines, recent Spark jobs, catalog size and backend capabilities. Use this before answering status questions.",
                {},
                [],
            ),
            self._spec(
                "navigate_site",
                "Navigate the web application to a screen. Use it when the user asks to open, show, go to, or manage a section of the site.",
                {
                    "screen": {
                        "type": "string",
                        "enum": [
                            "dashboard",
                            "ai-agent",
                            "sql",
                            "pipelines",
                            "airflow",
                            "spark",
                            "connections",
                            "catalog",
                            "settings",
                            "profile",
                        ],
                        "description": "Target frontend screen.",
                    }
                },
                ["screen"],
            ),
            self._spec(
                "execute_sql",
                f"Run a read-only SQL query against the connected warehouse. Current SQL dialect is {self.sql_dialect()}. The backend rejects writes.",
                {
                    "query": {"type": "string", "description": "Read-only SQL query."},
                    "limit": {"type": "integer", "minimum": 1, "maximum": 500, "description": "Maximum rows."},
                },
                ["query", "limit"],
            ),
            self._spec("list_catalog", "List database tables and columns.", {}, []),
            self._spec(
                "list_mcp_products",
                "List configured ready-made external MCP servers for product integrations. Use this to discover available MCP products.",
                {},
                [],
            ),
            self._spec(
                "list_mcp_tools",
                "Discover tools exposed by a ready-made external MCP server. Always use this before call_mcp_tool for a product unless the user supplied an exact tool name.",
                {
                    "product": {
                        "type": "string",
                        "enum": ["database", "airflow", "spark", "artifacts_git", "artifacts_filesystem"],
                        "description": "External MCP product server.",
                    }
                },
                ["product"],
            ),
            self._spec(
                "call_mcp_tool",
                "Call a tool exposed by a ready-made external MCP server. tool_name must be an exact name discovered from list_mcp_tools, and arguments must match that discovered input schema.",
                {
                    "product": {
                        "type": "string",
                        "enum": ["database", "airflow", "spark", "artifacts_git", "artifacts_filesystem"],
                    },
                    "tool_name": {"type": "string", "description": "Tool name discovered from list_mcp_tools."},
                    "arguments": {
                        "type": "object",
                        "description": "Arguments matching the discovered external MCP tool schema.",
                    },
                },
                ["product", "tool_name", "arguments"],
            ),
            self._spec(
                "inspect_database",
                "Inspect what is stored in the database: tables, columns, row counts and small samples. Use when the user asks what is in the database, warehouse, DB, or база/базенка/БД.",
                {
                    "sample_limit": {
                        "type": "integer",
                        "minimum": 0,
                        "maximum": 10,
                        "description": "Number of sample rows per table.",
                    }
                },
                ["sample_limit"],
            ),
            self._spec(
                "list_database_connections",
                "List database connections visible to the current user. Use this before answering what DBs are connected or before changing DB connection settings.",
                {},
                [],
            ),
            self._spec(
                "upsert_database_connection",
                "Create or update a database connection so it appears in Settings and Connections. Admin and engineer roles can configure connections; analyst can only view.",
                {
                    "connection_id": {
                        "type": "string",
                        "description": "Existing connection id to update, or empty string to upsert by name.",
                    },
                    "name": {"type": "string", "description": "Human-readable connection name."},
                    "engine": {
                        "type": "string",
                        "enum": ["postgresql", "mysql", "clickhouse", "mongodb", "redis"],
                    },
                    "host": {"type": "string", "description": "Backend/container reachable host."},
                    "port": {"type": "integer", "minimum": 1, "maximum": 65535},
                    "database": {"type": "string", "description": "Database/schema name, or empty for Redis."},
                    "username": {"type": "string", "description": "Username, or empty if not needed."},
                    "password": {"type": "string", "description": "Password, or empty string to keep existing password."},
                    "visibility": {"type": "string", "enum": ["private", "shared"]},
                    "options": {
                        "type": "object",
                        "description": "Extra settings such as public_host, public_port, sslmode, schema, warehouse.",
                    },
                },
                [
                    "connection_id",
                    "name",
                    "engine",
                    "host",
                    "port",
                    "database",
                    "username",
                    "password",
                    "visibility",
                    "options",
                ],
            ),
            self._spec(
                "test_database_connection",
                "Check whether a database connection endpoint is reachable from the backend network and persist its status.",
                {
                    "connection_id": {"type": "string", "description": "Connection id, or empty string to search by name."},
                    "name": {"type": "string", "description": "Connection name if connection_id is empty."},
                },
                ["connection_id", "name"],
            ),
            self._spec(
                "list_pipelines",
                "List Airflow DAGs with latest statuses, schedule, owner, next run, and task/operator structure. Use this to answer what DAGs do.",
                {},
                [],
            ),
            self._spec(
                "manage_airflow_dags",
                "List, pause, unpause, pause all, or unpause all Airflow DAGs. Use this for the same DAG controls a user can do in the UI.",
                {
                    "action": {
                        "type": "string",
                        "enum": ["list", "pause", "unpause", "pause_all", "unpause_all"],
                        "description": "Airflow DAG management action.",
                    },
                    "dag_id": {
                        "type": "string",
                        "description": "Target DAG id for single-DAG actions, or empty string for all/list actions.",
                    },
                },
                ["action", "dag_id"],
            ),
            self._spec(
                "trigger_airflow_dag",
                "Trigger an Airflow DAG run.",
                {"dag_id": {"type": "string", "description": "Airflow DAG id."}},
                ["dag_id"],
            ),
            self._spec(
                "get_airflow_run",
                "Get status of an Airflow DAG run.",
                {
                    "dag_id": {"type": "string"},
                    "run_id": {"type": "string"},
                },
                ["dag_id", "run_id"],
            ),
            self._spec(
                "list_airflow_runs",
                "List recent Airflow runs for a DAG, including run ids and states.",
                {"dag_id": {"type": "string", "description": "Airflow DAG id."}},
                ["dag_id"],
            ),
            self._spec(
                "list_airflow_task_instances",
                "List task instances for one Airflow DAG run.",
                {
                    "dag_id": {"type": "string"},
                    "run_id": {"type": "string"},
                },
                ["dag_id", "run_id"],
            ),
            self._spec(
                "get_airflow_task_log",
                "Read an Airflow task log for a DAG run like the Airflow UI log view.",
                {
                    "dag_id": {"type": "string"},
                    "run_id": {"type": "string"},
                    "task_id": {"type": "string"},
                    "try_number": {"type": "integer", "minimum": 1, "maximum": 50},
                },
                ["dag_id", "run_id", "task_id", "try_number"],
            ),
            self._spec(
                "submit_spark_job",
                "Submit a Spark job and persist its metadata and result sample.",
                {
                    "name": {"type": "string"},
                    "app_resource": {"type": "string"},
                    "executor_memory": {"type": "string"},
                    "partitions": {"type": "integer", "minimum": 1, "maximum": 10000},
                },
                ["name", "app_resource", "executor_memory", "partitions"],
            ),
            self._spec(
                "get_spark_job",
                "Get current Spark job status and result sample.",
                {"job_id": {"type": "string"}},
                ["job_id"],
            ),
            self._spec(
                "read_airflow_dag",
                "Read the active Airflow DAG Python source from the runtime DAG folder. Use this before changing an existing DAG.",
                {"dag_id": {"type": "string", "description": "DAG id or DAG filename."}},
                ["dag_id"],
            ),
            self._spec(
                "read_spark_script",
                "Read the active Spark script source from the runtime Spark jobs folder. Use this before changing an existing Spark script.",
                {"script_name": {"type": "string", "description": "Script filename or logical name."}},
                ["script_name"],
            ),
            self._spec(
                "write_airflow_dag",
                "Create or update a Python Airflow DAG file, validate syntax, store a user-scoped Git version, and deploy the valid source into the active Airflow DAG folder.",
                {
                    "dag_id": {"type": "string", "description": "DAG id, used as the Python filename."},
                    "code": {"type": "string", "description": "Complete Python DAG source code."},
                    "message": {"type": "string", "description": "Short version message."},
                },
                ["dag_id", "code", "message"],
            ),
            self._spec(
                "write_spark_script",
                "Create or update a Python Spark script, validate syntax, store a user-scoped Git version, and deploy the valid source into the active Spark jobs folder.",
                {
                    "script_name": {"type": "string", "description": "Script filename or logical name."},
                    "code": {"type": "string", "description": "Complete Python Spark script source code."},
                    "message": {"type": "string", "description": "Short version message."},
                },
                ["script_name", "code", "message"],
            ),
            self._spec(
                "check_airflow_dag_sandbox",
                "Run an Airflow DAG import check in the sandbox. Use after writing a DAG or when debugging DAG errors.",
                {
                    "dag_id": {"type": "string", "description": "DAG id or DAG filename."},
                    "code": {"type": "string", "description": "Complete DAG source code, or empty string to load saved artifact."},
                    "error_log": {"type": "string", "description": "Observed traceback, Airflow log, Spark log, or empty string."},
                },
                ["dag_id", "code", "error_log"],
            ),
            self._spec(
                "run_spark_script_sandbox",
                "Run a Spark/PySpark script in the sandbox. Use after writing a Spark script or when debugging Spark errors.",
                {
                    "script_name": {"type": "string", "description": "Script filename or logical name."},
                    "code": {"type": "string", "description": "Complete script source code, or empty string to load saved artifact."},
                    "error_log": {"type": "string", "description": "Observed Spark log, traceback, or empty string."},
                    "arguments": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Command-line arguments for the script.",
                    },
                },
                ["script_name", "code", "error_log", "arguments"],
            ),
            self._spec(
                "run_python_script_sandbox",
                "Run a generic Python script in the sandbox. Use for non-Airflow and non-Spark script debugging.",
                {
                    "script_name": {"type": "string", "description": "Script filename or logical name."},
                    "code": {"type": "string", "description": "Complete script source code."},
                    "error_log": {"type": "string", "description": "Observed traceback or empty string."},
                    "arguments": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Command-line arguments for the script.",
                    },
                },
                ["script_name", "code", "error_log", "arguments"],
            ),
            self._spec(
                "list_artifact_versions",
                "List saved DB versions and Git history for Airflow DAGs and Spark scripts.",
                {
                    "artifact_type": {
                        "type": "string",
                        "enum": ["airflow_dag", "spark_script", "all"],
                        "description": "Artifact family or all.",
                    },
                    "artifact_name": {
                        "type": "string",
                        "description": "Exact filename to filter by, or empty string for all.",
                    },
                },
                ["artifact_type", "artifact_name"],
            ),
        ]

    async def execute(self, name: str, args: dict[str, Any]) -> ToolExecution:
        if name == "list_site_status":
            return await self.list_site_status()
        if name == "navigate_site":
            return self.navigate_site(args["screen"])
        if name == "execute_sql":
            return await self.sql.execute(self.db, args["query"], int(args["limit"]))
        if name == "list_catalog":
            return await self.catalog.execute(self.db)
        if name == "list_mcp_products":
            return ToolExecution(
                tool_name="MCPDiscoveryTool",
                status="success",
                input={},
                output={"products": self.external_mcp.product_specs()},
                latency_ms=1,
            )
        if name == "list_mcp_tools":
            return await self.external_mcp.list_tools(args["product"])
        if name == "call_mcp_tool":
            return await self.external_mcp.call_tool(args["product"], args["tool_name"], args["arguments"])
        if name == "inspect_database":
            return await self.catalog.inspect_database(self.db, int(args["sample_limit"]))
        if name == "list_database_connections":
            return await self.list_database_connections()
        if name == "upsert_database_connection":
            return await self.upsert_database_connection(args)
        if name == "test_database_connection":
            return await self.test_database_connection(args["connection_id"], args["name"])
        if name == "list_pipelines":
            return await self.airflow.execute(self.db, dag_id="", action="list")
        if name == "manage_airflow_dags":
            action = args["action"]
            dag_id = args["dag_id"]
            return await self.airflow.execute(self.db, dag_id=dag_id, action=action)
        if name == "trigger_airflow_dag":
            return await self.airflow.execute(self.db, dag_id=args["dag_id"], action="trigger")
        if name == "get_airflow_run":
            started = time.perf_counter()
            run = await self.airflow.get_run(self.db, args["dag_id"], args["run_id"])
            return ToolExecution(
                tool_name="AirflowTool",
                status="success",
                input=args,
                output=run.model_dump(),
                latency_ms=self._elapsed_ms(started),
            )
        if name == "list_airflow_runs":
            started = time.perf_counter()
            runs = await self.airflow.list_runs(self.db, args["dag_id"])
            return ToolExecution(
                tool_name="AirflowRunTool",
                status="success",
                input=args,
                output={"runs": [run.model_dump() for run in runs]},
                latency_ms=self._elapsed_ms(started),
            )
        if name == "list_airflow_task_instances":
            started = time.perf_counter()
            tasks = await self.airflow.list_task_instances(args["dag_id"], args["run_id"])
            return ToolExecution(
                tool_name="AirflowTaskTool",
                status="success",
                input=args,
                output={"tasks": [task.model_dump() for task in tasks]},
                latency_ms=self._elapsed_ms(started),
            )
        if name == "get_airflow_task_log":
            started = time.perf_counter()
            log = await self.airflow.get_task_log(
                args["dag_id"],
                args["run_id"],
                args["task_id"],
                int(args["try_number"]),
            )
            return ToolExecution(
                tool_name="AirflowLogTool",
                status="success",
                input=args,
                output=log.model_dump(),
                latency_ms=self._elapsed_ms(started),
            )
        if name == "submit_spark_job":
            return await self.spark.execute(
                self.db,
                name=args["name"],
                app_resource=args["app_resource"],
                params={
                    "executor_memory": args["executor_memory"],
                    "partitions": args["partitions"],
                    "source": "openai_function_call",
                },
            )
        if name == "get_spark_job":
            started = time.perf_counter()
            job = await self.spark.get_job(self.db, args["job_id"])
            return ToolExecution(
                tool_name="SparkTool",
                status="success",
                input=args,
                output=job.model_dump(),
                latency_ms=self._elapsed_ms(started),
            )
        if name == "read_airflow_dag":
            return self.read_runtime_artifact("airflow_dag", args["dag_id"], "AirflowDAGSourceTool")
        if name == "read_spark_script":
            return self.read_runtime_artifact("spark_script", args["script_name"], "SparkScriptSourceTool")
        if name == "write_airflow_dag":
            return await self.write_airflow_dag(args["dag_id"], args["code"], args["message"])
        if name == "write_spark_script":
            return await self.write_spark_script(args["script_name"], args["code"], args["message"])
        if name == "check_airflow_dag_sandbox":
            return await self.check_airflow_dag_sandbox(args["dag_id"], args["code"], args["error_log"])
        if name == "run_spark_script_sandbox":
            return await self.run_spark_script_sandbox(
                args["script_name"],
                args["code"],
                args["error_log"],
                args["arguments"],
            )
        if name == "run_python_script_sandbox":
            return await self.run_python_script_sandbox(
                args["script_name"],
                args["code"],
                args["error_log"],
                args["arguments"],
            )
        if name == "debug_python_artifact":
            return await self.run_python_script_sandbox("artifact.py", args["code"], args["error_log"], [])
        if name == "list_artifact_versions":
            artifact_type = args["artifact_type"]
            if artifact_type == "all":
                artifact_type = None
            return await self.list_artifact_versions(artifact_type, args["artifact_name"] or None)

        return ToolExecution(
            tool_name=name,
            status="error",
            input=args,
            output={"error": f"Unknown tool: {name}"},
            latency_ms=1,
            error="unknown_tool",
        )

    async def list_site_status(self) -> ToolExecution:
        started = time.perf_counter()
        pipelines = await self.airflow.list_pipelines(self.db)
        tables = await self.catalog.list_tables(self.db)
        database_connections = await self.connections.list_visible(self.db, self.user)
        spark_rows = await self.db.scalars(select(SparkJob).order_by(SparkJob.created_at.desc()).limit(10))
        spark_jobs = [
            {
                "job_id": job.job_id,
                "name": job.name,
                "status": job.status,
                "created_at": job.created_at.isoformat() if job.created_at else None,
            }
            for job in spark_rows
        ]
        output = {
            "current_screen": self.app_state.get("screen"),
            "frontend_state": self.app_state,
            "user": {
                "email": self.user.email,
                "role": self.user.role,
                "status": self.user.status,
            },
            "pipelines": [pipeline.model_dump() for pipeline in pipelines],
            "spark_jobs": spark_jobs,
            "catalog": {
                "table_count": len(tables),
                "tables": [table.name for table in tables],
            },
            "database_connections": [
                self.connections.serialize_for_tool(connection) for connection in database_connections
            ],
            "capabilities": [
                "navigate_site",
                "execute_sql",
                "list_database_connections",
                "upsert_database_connection",
                "test_database_connection",
                "trigger_airflow_dag",
                "submit_spark_job",
                "list_catalog",
                "list_mcp_products",
                "list_mcp_tools",
                "call_mcp_tool",
                "inspect_database",
                "manage_airflow_dags",
                "list_airflow_runs",
                "list_airflow_task_instances",
                "get_airflow_task_log",
                "read_airflow_dag",
                "read_spark_script",
                "write_airflow_dag",
                "write_spark_script",
                "check_airflow_dag_sandbox",
                "run_spark_script_sandbox",
                "run_python_script_sandbox",
                "list_artifact_versions",
            ],
            "versioning": {
                "git_enabled": settings.artifact_git_enabled,
                "git_root": settings.artifact_git_root or settings.artifact_root,
            },
            "sql_dialect": self.sql_dialect(),
            "debugger": {
                "enabled": self.debug_sandbox.enabled,
                "url": settings.agent_debugger_url,
            },
        }
        return ToolExecution(
            tool_name="SiteStatusTool",
            status="success",
            input={},
            output=output,
            latency_ms=self._elapsed_ms(started),
        )

    async def list_database_connections(self) -> ToolExecution:
        started = time.perf_counter()
        try:
            connections = await self.connections.list_visible(self.db, self.user)
            return ToolExecution(
                tool_name="DatabaseConnectionTool",
                status="success",
                input={},
                output={
                    "connections": [
                        self.connections.serialize_for_tool(connection) for connection in connections
                    ]
                },
                latency_ms=self._elapsed_ms(started),
            )
        except Exception as exc:
            return self.error_execution("DatabaseConnectionTool", {}, started, exc)

    async def upsert_database_connection(self, args: dict[str, Any]) -> ToolExecution:
        started = time.perf_counter()
        payload = self._connection_payload(args)
        try:
            connection = await self.connections.upsert(self.db, self.user, payload)
            return ToolExecution(
                tool_name="DatabaseConnectionTool",
                status="success",
                input={key: value for key, value in payload.items() if key != "password"},
                output={"connection": self.connections.serialize_for_tool(connection)},
                latency_ms=self._elapsed_ms(started),
                metadata={
                    "ui_actions": [
                        {"type": "refresh_connections"},
                        {"type": "toast", "message": f"Подключение {connection.name} сохранено"},
                    ]
                },
            )
        except Exception as exc:
            return self.error_execution(
                "DatabaseConnectionTool",
                {key: value for key, value in payload.items() if key != "password"},
                started,
                exc,
            )

    async def test_database_connection(self, connection_id: str, name: str) -> ToolExecution:
        started = time.perf_counter()
        args = {"connection_id": connection_id, "name": name}
        try:
            connection = None
            if connection_id:
                connection = await self.connections.get_visible(self.db, self.user, connection_id)
            if connection is None and name:
                connection = await self.connections.find_visible_by_name(self.db, self.user, name)
            if connection is None:
                raise LookupError("Database connection not found")

            result = await self.connections.test_connection(self.db, connection)
            status = "success" if result["status"] == "online" else "error"
            return ToolExecution(
                tool_name="DatabaseConnectionTool",
                status=status,
                input=args,
                output={
                    **result,
                    "connection": self.connections.serialize_for_tool(connection),
                },
                latency_ms=self._elapsed_ms(started),
                error=result["error"] if status == "error" else None,
                metadata={"ui_actions": [{"type": "refresh_connections"}]},
            )
        except Exception as exc:
            return self.error_execution("DatabaseConnectionTool", args, started, exc)

    def navigate_site(self, screen: str) -> ToolExecution:
        return ToolExecution(
            tool_name="SiteControlTool",
            status="success",
            input={"screen": screen},
            output={"screen": screen, "message": f"Navigate frontend to {screen}"},
            latency_ms=1,
            metadata={
                "ui_actions": [
                    {"type": "navigate", "screen": screen},
                    {"type": "toast", "message": f"Открываю экран {screen}"},
                ]
            },
        )

    @staticmethod
    def _connection_payload(args: dict[str, Any]) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "name": args["name"],
            "engine": args["engine"],
            "host": args["host"],
            "port": int(args["port"]),
            "visibility": args["visibility"],
            "options": args.get("options") or {},
        }
        for source_key, target_key in [
            ("connection_id", "id"),
            ("database", "database"),
            ("username", "username"),
            ("password", "password"),
        ]:
            value = args.get(source_key)
            if value not in (None, ""):
                payload[target_key] = value
        return payload

    def read_runtime_artifact(self, artifact_type: str, name: str, tool_name: str) -> ToolExecution:
        started = time.perf_counter()
        artifact_name = name if name.endswith(".py") else f"{name}.py"
        try:
            path, code = self.artifacts.read_runtime_code(artifact_type, artifact_name)
            return ToolExecution(
                tool_name=tool_name,
                status="success",
                input={"artifact_type": artifact_type, "artifact_name": artifact_name},
                output={
                    "artifact_type": artifact_type,
                    "artifact_name": artifact_name,
                    "path": str(path),
                    "code": code,
                },
                latency_ms=self._elapsed_ms(started),
            )
        except Exception as exc:
            return self.error_execution(
                tool_name,
                {"artifact_type": artifact_type, "artifact_name": artifact_name},
                started,
                exc,
            )

    async def write_airflow_dag(self, dag_id: str, code: str, message: str) -> ToolExecution:
        started = time.perf_counter()
        try:
            result = await self.artifacts.write_airflow_dag(
                self.db,
                dag_id=dag_id,
                code=code,
                message=message,
                author_user_id=self.user.id,
            )
            output = result.__dict__
            if result.validation_status == "valid" and self.can_deploy_runtime_artifacts():
                output = {
                    **output,
                    "deployed_to_runtime": True,
                    **self.artifacts.deploy_runtime_copy(
                        "airflow_dag",
                        result.artifact_name,
                        code,
                        message,
                    ),
                }
            return ToolExecution(
                tool_name="ArtifactTool",
                status="success",
                input={"dag_id": dag_id, "message": message},
                output=output,
                latency_ms=self._elapsed_ms(started),
                metadata={
                    "ui_actions": [
                        {"type": "toast", "message": f"DAG {dag_id} сохранен и задеплоен, версия {result.version}"}
                    ]
                },
            )
        except Exception as exc:
            return self.error_execution("ArtifactTool", {"dag_id": dag_id, "message": message}, started, exc)

    async def write_spark_script(self, script_name: str, code: str, message: str) -> ToolExecution:
        started = time.perf_counter()
        try:
            result = await self.artifacts.write_spark_script(
                self.db,
                script_name=script_name,
                code=code,
                message=message,
                author_user_id=self.user.id,
            )
            output = result.__dict__
            if result.validation_status == "valid" and self.can_deploy_runtime_artifacts():
                output = {
                    **output,
                    "deployed_to_runtime": True,
                    **self.artifacts.deploy_runtime_copy(
                        "spark_script",
                        result.artifact_name,
                        code,
                        message,
                    ),
                }
            return ToolExecution(
                tool_name="ArtifactTool",
                status="success",
                input={"script_name": script_name, "message": message},
                output=output,
                latency_ms=self._elapsed_ms(started),
                metadata={
                    "ui_actions": [
                        {"type": "toast", "message": f"Spark script {script_name} сохранен и задеплоен, версия {result.version}"}
                    ]
                },
            )
        except Exception as exc:
            return self.error_execution("ArtifactTool", {"script_name": script_name, "message": message}, started, exc)

    async def check_airflow_dag_sandbox(self, dag_id: str, code: str, error_log: str) -> ToolExecution:
        artifact_name = dag_id if dag_id.endswith(".py") else f"{dag_id}.py"
        started = time.perf_counter()
        try:
            code_to_run = code or self.read_artifact_code("airflow_dag", artifact_name)
        except Exception as exc:
            return self.error_execution(
                "AirflowSandboxTool",
                {"artifact_type": "airflow_dag", "artifact_name": artifact_name, "error_log": error_log},
                started,
                exc,
            )
        return await self.run_artifact_sandbox(
            tool_name="AirflowSandboxTool",
            artifact_type="airflow_dag",
            artifact_name=artifact_name,
            code=code_to_run,
            error_log=error_log,
            arguments=[],
        )

    async def run_spark_script_sandbox(
        self,
        script_name: str,
        code: str,
        error_log: str,
        arguments: list[str],
    ) -> ToolExecution:
        artifact_name = script_name if script_name.endswith(".py") else f"{script_name}.py"
        started = time.perf_counter()
        try:
            code_to_run = code or self.read_artifact_code("spark_script", artifact_name)
        except Exception as exc:
            return self.error_execution(
                "SparkSandboxTool",
                {"artifact_type": "spark_script", "artifact_name": artifact_name, "error_log": error_log},
                started,
                exc,
            )
        return await self.run_artifact_sandbox(
            tool_name="SparkSandboxTool",
            artifact_type="spark_script",
            artifact_name=artifact_name,
            code=code_to_run,
            error_log=error_log,
            arguments=arguments,
        )

    async def run_python_script_sandbox(
        self,
        script_name: str,
        code: str,
        error_log: str,
        arguments: list[str],
    ) -> ToolExecution:
        return await self.run_artifact_sandbox(
            tool_name="PythonSandboxTool",
            artifact_type="python",
            artifact_name=script_name,
            code=code,
            error_log=error_log,
            arguments=arguments,
        )

    async def run_artifact_sandbox(
        self,
        tool_name: str,
        artifact_type: str,
        artifact_name: str,
        code: str,
        error_log: str,
        arguments: list[str],
    ) -> ToolExecution:
        started = time.perf_counter()
        try:
            remote_result = await self.debug_sandbox.run_artifact(
                artifact_type=artifact_type,
                artifact_name=artifact_name,
                code=code,
                error_log=error_log,
                arguments=arguments,
                user_context=self.sandbox_user_context(),
            )
            if not remote_result:
                return ToolExecution(
                    tool_name=tool_name,
                    status="error",
                    input={"artifact_type": artifact_type, "artifact_name": artifact_name, "error_log": error_log},
                    output={
                        "error": "Docker sandbox is not configured. Set AGENT_DEBUGGER_URL and run agent-debugger.",
                        "runtime_status": "sandbox_not_configured",
                        "artifact_type": artifact_type,
                        "artifact_name": artifact_name,
                    },
                    latency_ms=self._elapsed_ms(started),
                    error="sandbox_not_configured",
                )
            result = remote_result
            result["artifact_type"] = artifact_type
            result["artifact_name"] = artifact_name
            return ToolExecution(
                tool_name=tool_name,
                status="success",
                input={"artifact_type": artifact_type, "artifact_name": artifact_name, "error_log": error_log},
                output=result,
                latency_ms=self._elapsed_ms(started),
            )
        except Exception as exc:
            return ToolExecution(
                tool_name=tool_name,
                status="error",
                input={"artifact_type": artifact_type, "artifact_name": artifact_name, "error_log": error_log},
                output={
                    "error": str(exc),
                    "runtime_status": "sandbox_error",
                    "artifact_type": artifact_type,
                    "artifact_name": artifact_name,
                },
                latency_ms=self._elapsed_ms(started),
                error=exc.__class__.__name__,
            )

    def read_artifact_code(self, artifact_type: str, artifact_name: str) -> str:
        return self.artifacts.read_code(
            artifact_type,
            artifact_name,
            self.user.id,
            allow_global_fallback=self.user.role == "admin",
        )

    def sandbox_user_context(self) -> dict[str, str]:
        return {
            "id": self.user.id,
            "email": self.user.email,
            "role": self.user.role,
        }

    def can_deploy_runtime_artifacts(self) -> bool:
        return self.user.role in {"admin", "engineer"}

    async def list_artifact_versions(
        self,
        artifact_type: str | None,
        artifact_name: str | None,
    ) -> ToolExecution:
        started = time.perf_counter()
        try:
            author_user_id = None if self.user.role == "admin" else self.user.id
            versions = await self.artifacts.list_versions(self.db, artifact_type, artifact_name, author_user_id)
            return ToolExecution(
                tool_name="ArtifactVersionTool",
                status="success",
                input={
                    "artifact_type": artifact_type or "all",
                    "artifact_name": artifact_name or "",
                    "scope": "all" if self.user.role == "admin" else "own",
                },
                output={
                    "scope": "all" if self.user.role == "admin" else "own",
                    "versions": [
                        {
                            "artifact_type": item.artifact_type,
                            "artifact_name": item.artifact_name,
                            "path": item.path,
                            "version": item.version,
                            "content_hash": item.content_hash,
                            "message": item.message,
                            "validation_status": item.validation_status,
                            "validation_output": item.validation_output,
                            "git_status": item.git_status,
                            "git_repository": item.git_repository,
                            "git_commit_sha": item.git_commit_sha,
                            "git_commit_short_sha": item.git_commit_short_sha,
                            "git_error": item.git_error,
                            "created_at": item.created_at.isoformat() if item.created_at else None,
                            "git_history": self.artifacts.git_history_for_path(item.path, limit=10),
                        }
                        for item in versions
                    ]
                },
                latency_ms=self._elapsed_ms(started),
            )
        except Exception as exc:
            return self.error_execution(
                "ArtifactVersionTool",
                {"artifact_type": artifact_type or "all", "artifact_name": artifact_name or ""},
                started,
                exc,
            )

    def tool_output_for_model(self, execution: ToolExecution) -> str:
        return json.dumps(
            {
                "status": execution.status,
                "tool_name": execution.tool_name,
                "output": execution.output,
                "ui_actions": execution.ui_actions,
            },
            ensure_ascii=False,
            default=str,
        )

    def mcp_specs(self) -> list[dict[str, Any]]:
        return [
            {
                "name": spec["name"],
                "description": spec["description"],
                "inputSchema": spec["parameters"],
            }
            for spec in self.specs()
        ]

    def mcp_product_specs(self) -> dict[str, dict[str, Any]]:
        specs = {spec["name"]: spec for spec in self.mcp_specs()}
        return {
            product: {
                "product": product,
                "tools": [specs[name] for name in tool_names if name in specs],
            }
            for product, tool_names in self.product_tool_names.items()
        }

    def _spec(
        self,
        name: str,
        description: str,
        properties: dict[str, Any],
        required: list[str],
    ) -> dict[str, Any]:
        return {
            "type": "function",
            "name": name,
            "description": description,
            "parameters": {
                "type": "object",
                "properties": properties,
                "required": required,
                "additionalProperties": False,
            },
            "strict": True,
        }

    def error_execution(
        self,
        tool_name: str,
        args: dict[str, Any],
        started: float,
        exc: Exception,
    ) -> ToolExecution:
        return ToolExecution(
            tool_name=tool_name,
            status="error",
            input=args,
            output={"error": str(exc)},
            latency_ms=self._elapsed_ms(started),
            error=exc.__class__.__name__,
        )

    @staticmethod
    def _elapsed_ms(started: float) -> int:
        return max(1, int((time.perf_counter() - started) * 1000))

    @staticmethod
    def sql_dialect() -> str:
        if settings.database_url.startswith("sqlite"):
            return "sqlite"
        if "postgresql" in settings.database_url:
            return "postgresql"
        return "unknown"
