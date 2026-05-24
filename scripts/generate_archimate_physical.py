"""Generate ArchiMate model (.archimate) for section 3.1 physical architecture.

Produces docs/3_1_physical_architecture.archimate, openable in Archi tool
(https://www.archimatetool.com). Layered like the reference: Motivation/Business
(yellow) -> Application (blue) -> Technology (green) -> Physical (purple/violet).
"""

from pathlib import Path
from xml.dom import minidom
from xml.etree import ElementTree as ET

NS_XSI = "http://www.w3.org/2001/XMLSchema-instance"
NS_ARCH = "http://www.archimatetool.com/archimate"
ET.register_namespace("xsi", NS_XSI)
ET.register_namespace("archimate", NS_ARCH)


def _q(prefix: str, name: str) -> str:
    return f"{{{prefix}}}{name}"


XSI_TYPE = _q(NS_XSI, "type")


class Model:
    def __init__(self, name: str):
        self.root = ET.Element(
            _q(NS_ARCH, "model"),
            {
                "name": name,
                "id": "id-model-aida-physical",
                "version": "5.0.0",
            },
        )
        self.folders: dict[str, ET.Element] = {}
        for folder_name, folder_type in [
            ("Strategy", "strategy"),
            ("Business", "business"),
            ("Application", "application"),
            ("Technology & Physical", "technology"),
            ("Motivation", "motivation"),
            ("Implementation & Migration", "implementation_migration"),
            ("Other", "other"),
            ("Relations", "relations"),
            ("Views", "diagrams"),
        ]:
            f = ET.SubElement(
                self.root,
                "folder",
                {
                    "name": folder_name,
                    "id": f"id-folder-{folder_type}",
                    "type": folder_type,
                },
            )
            self.folders[folder_type] = f
        self._counter = 0
        self._conn_counter = 0

    def add_element(self, folder_type: str, etype: str, name: str, eid: str) -> str:
        ET.SubElement(
            self.folders[folder_type],
            "element",
            {
                XSI_TYPE: f"archimate:{etype}",
                "name": name,
                "id": eid,
            },
        )
        return eid

    def add_relation(
        self,
        rtype: str,
        source: str,
        target: str,
        rid: str | None = None,
        name: str = "",
    ) -> str:
        if rid is None:
            self._counter += 1
            rid = f"id-rel-{self._counter:04d}"
        attrs = {
            XSI_TYPE: f"archimate:{rtype}",
            "id": rid,
            "source": source,
            "target": target,
        }
        if name:
            attrs["name"] = name
        ET.SubElement(self.folders["relations"], "element", attrs)
        return rid

    def new_view(self, name: str, vid: str) -> ET.Element:
        view = ET.SubElement(
            self.folders["diagrams"],
            "element",
            {
                XSI_TYPE: "archimate:ArchimateDiagramModel",
                "name": name,
                "id": vid,
            },
        )
        return view

    def next_conn_id(self) -> str:
        self._conn_counter += 1
        return f"id-conn-{self._conn_counter:04d}"


def make_object(
    parent: ET.Element,
    elem_id: str,
    oid: str,
    x: int,
    y: int,
    w: int,
    h: int,
    fill: str | None = None,
) -> ET.Element:
    obj = ET.SubElement(
        parent,
        "child",
        {
            XSI_TYPE: "archimate:DiagramObject",
            "id": oid,
            "archimateElement": elem_id,
        },
    )
    if fill:
        obj.set("fillColor", fill)
    ET.SubElement(
        obj,
        "bounds",
        {"x": str(x), "y": str(y), "width": str(w), "height": str(h)},
    )
    return obj


def add_target_conn(obj: ET.Element, conn_id: str) -> None:
    existing = obj.get("targetConnections", "").split()
    existing.append(conn_id)
    obj.set("targetConnections", " ".join(filter(None, existing)))


def add_connection(
    model: Model,
    source_obj: ET.Element,
    target_obj: ET.Element,
    rel_id: str,
) -> ET.Element:
    cid = model.next_conn_id()
    conn = ET.SubElement(
        source_obj,
        "sourceConnection",
        {
            XSI_TYPE: "archimate:Connection",
            "id": cid,
            "source": source_obj.get("id"),
            "target": target_obj.get("id"),
            "archimateRelationship": rel_id,
        },
    )
    add_target_conn(target_obj, cid)
    return conn


def pretty(elem: ET.Element) -> str:
    raw = ET.tostring(elem, encoding="utf-8")
    parsed = minidom.parseString(raw)
    return parsed.toprettyxml(indent="  ", encoding="UTF-8").decode("utf-8")


def build() -> Model:
    m = Model("AI Data Engineer Assistant - Physical Architecture")

    # ---------------- Business / Motivation layer ----------------
    A_DE = m.add_element("business", "BusinessActor", "Data Engineer", "id-actor-de")
    A_ADMIN = m.add_element("business", "BusinessActor", "Администратор", "id-actor-admin")
    R_USER = m.add_element("business", "BusinessRole", "Пользователь LLM-агента", "id-role-user")
    BS_MAIN = m.add_element(
        "business", "BusinessService", "AI Data Engineer Assistant", "id-bs-main"
    )
    # Business flow — единый сценарий «Запрос → ответ агента», разбитый на шаги.
    # User-side processes (внутри группы «Взаимодействие с пользователем»):
    BP_SEND = m.add_element(
        "business", "BusinessProcess", "Отправка запроса", "id-bp-send"
    )
    BP_DISPLAY = m.add_element(
        "business", "BusinessProcess", "Получение ответа и\nотображение в UI", "id-bp-display"
    )
    # Internal-service processes (внутри «AI Data Engineer Assistant — внутренний сервис»):
    BP_INTERNAL_RECV = m.add_element(
        "business", "BusinessProcess", "Получение запроса", "id-bp-internal-recv"
    )
    BP_INTENT = m.add_element(
        "business", "BusinessProcess", "Анализ намерения и\nвыбор инструмента", "id-bp-intent"
    )
    BP_LLM_PROC = m.add_element(
        "business",
        "BusinessProcess",
        "Обработка запроса в LLM\n(tool calling)",
        "id-bp-llm-proc",
    )
    BP_EXEC = m.add_element(
        "business",
        "BusinessProcess",
        "Выполнение инструмента\n(SQL / Airflow / Spark / Sandbox / MCP)",
        "id-bp-exec",
    )
    BP_FORM = m.add_element(
        "business", "BusinessProcess", "Формирование ответа", "id-bp-form"
    )

    # Business groupings (visual containers like в референсе)
    GRP_USER_SIDE = m.add_element(
        "other", "Grouping", "Взаимодействие с пользователем", "id-grp-user-side"
    )
    GRP_INTERNAL = m.add_element(
        "other", "Grouping", "AI Data Engineer Assistant  (внутренний сервис)", "id-grp-internal"
    )

    # ---------------- Application layer ----------------
    AC_FE = m.add_element("application", "ApplicationComponent", "Next.js Frontend", "id-ac-frontend")
    AC_BE = m.add_element("application", "ApplicationComponent", "FastAPI Backend", "id-ac-backend")

    AF_CHAT = m.add_element("application", "ApplicationFunction", "AI Chat UI", "id-af-chat")
    AF_SQLUI = m.add_element("application", "ApplicationFunction", "SQL Workspace UI", "id-af-sql-ui")
    AF_PIPE = m.add_element("application", "ApplicationFunction", "Pipelines UI", "id-af-pipe-ui")
    AF_CATUI = m.add_element(
        "application", "ApplicationFunction", "Catalog / Connections UI", "id-af-catalog-ui"
    )
    AF_SETUI = m.add_element("application", "ApplicationFunction", "Settings / Profile UI", "id-af-settings-ui")

    # Backend internal flow — последовательность обработки запроса агента.
    AF_RECV = m.add_element(
        "application", "ApplicationFunction", "Получение запроса\n(REST API)", "id-af-recv"
    )
    AF_AUTH = m.add_element(
        "application", "ApplicationFunction", "Авторизация (JWT)", "id-af-auth"
    )
    AF_AGENT = m.add_element(
        "application", "ApplicationFunction", "Agent Orchestrator\n(LangGraph)", "id-af-agent"
    )
    AF_LLM = m.add_element(
        "application", "ApplicationFunction", "Обработка запроса в LLM\n(tool calling)", "id-af-llm"
    )
    AF_TOOLS = m.add_element("application", "ApplicationFunction", "Tool Registry", "id-af-tools")
    AF_SQLX = m.add_element(
        "application", "ApplicationFunction", "SQL Executor\n(read-only)", "id-af-sql-exec"
    )
    AF_AFCLI = m.add_element("application", "ApplicationFunction", "Airflow Client", "id-af-airflow")
    AF_SPCLI = m.add_element("application", "ApplicationFunction", "Spark Client", "id-af-spark")
    AF_MCP = m.add_element("application", "ApplicationFunction", "MCP Client", "id-af-mcp")
    AF_ART = m.add_element(
        "application", "ApplicationFunction", "Artifact Writer + Git", "id-af-artifact"
    )
    AF_SBX = m.add_element("application", "ApplicationFunction", "Sandbox Client", "id-af-sandbox")
    AF_RESP = m.add_element(
        "application", "ApplicationFunction", "Формирование ответа\n(intent + ui_actions)", "id-af-resp"
    )
    AF_PERSIST = m.add_element(
        "application", "ApplicationFunction", "Сохранение сессии,\nсообщений и tool_runs", "id-af-persist"
    )

    DO_USERS = m.add_element(
        "application", "DataObject", "Пользователи, сессии, сообщения", "id-do-users"
    )
    DO_RUNS = m.add_element(
        "application", "DataObject", "Tool runs, версии артефактов", "id-do-runs"
    )
    DO_AFMETA = m.add_element("application", "DataObject", "Метаданные Airflow", "id-do-airflow-meta")

    # Application service exposed to business
    AS_API = m.add_element(
        "application", "ApplicationService", "AI Data Engineer Service", "id-as-api"
    )

    # ---------------- Technology & Physical layer ----------------
    # Physical (host & networks)
    NODE_HOST = m.add_element(
        "technology", "Node", "Host Machine (Docker Engine)", "id-node-host"
    )
    # Высокоуровневый Node «вся docker-инфраструктура» — для view-B без детализации.
    NODE_PLATFORM = m.add_element(
        "technology", "Node", "Docker", "id-node-platform"
    )
    CN_DOCKER = m.add_element(
        "technology", "CommunicationNetwork", "Docker bridge network", "id-cn-docker"
    )
    # CN "HTTPS / Internet" intentionally removed — Flow arrows carry HTTPS labels
    # already, so a standalone network element added clutter without value.

    # Client
    NODE_BROWSER = m.add_element("technology", "Node", "Browser (Client)", "id-node-browser")

    # Containers (Nodes)
    NODE_FE = m.add_element("technology", "Node", "frontend container", "id-node-frontend")
    NODE_BE = m.add_element("technology", "Node", "backend container", "id-node-backend")
    NODE_PG = m.add_element("technology", "Node", "postgres", "id-node-pg")
    NODE_AFW = m.add_element("technology", "Node", "airflow-webserver", "id-node-af-web")
    NODE_AFS = m.add_element("technology", "Node", "airflow-scheduler", "id-node-af-sch")
    NODE_AFP = m.add_element("technology", "Node", "airflow-postgres", "id-node-af-pg")
    NODE_SPM = m.add_element("technology", "Node", "spark-master", "id-node-sp-m")
    NODE_SPW = m.add_element("technology", "Node", "spark-worker", "id-node-sp-w")
    NODE_DBG = m.add_element("technology", "Node", "agent-debugger", "id-node-dbg")
    # Celery stack: broker (redis), worker, beat
    NODE_REDIS = m.add_element("technology", "Node", "redis (Celery broker)", "id-node-redis")
    NODE_BWORKER = m.add_element("technology", "Node", "backend-worker", "id-node-bworker")
    NODE_BBEAT = m.add_element("technology", "Node", "backend-beat", "id-node-bbeat")

    # demo-* containers (docker-compose: demo-postgres/mysql/clickhouse/mongo/redis)
    # intentionally NOT modeled in the architecture view — they are local seed
    # fixtures for testing, not architectural elements. Any external database
    # connected by users is represented by NODE_USER_DBS (1..N).
    NODE_SBXPY = m.add_element(
        "technology", "Node", "Sandbox: python:3.12-slim", "id-node-sbx-py"
    )
    NODE_SBXAF = m.add_element(
        "technology", "Node", "Sandbox: apache/airflow:2.10.4", "id-node-sbx-af"
    )
    NODE_SBXSP = m.add_element(
        "technology", "Node", "Sandbox: apache/spark:3.5.4", "id-node-sbx-sp"
    )
    NODE_LLM = m.add_element(
        "technology",
        "Node",
        "LLM Provider (MagnitGPT / OpenAI / OpenRouter)",
        "id-node-llm",
    )
    # External user-managed databases — arbitrary number, registered through
    # the in-app DatabaseConnections feature (PostgreSQL, MySQL, ClickHouse,
    # MongoDB, Redis, ...). One node representing 1..N runtime connections.
    NODE_USER_DBS = m.add_element(
        "technology",
        "Node",
        "Внешние БД пользователей  (1..N)\nPostgreSQL / MySQL / ClickHouse / MongoDB / Redis / ...",
        "id-node-user-dbs",
    )

    # System Software
    SS_NODE = m.add_element("technology", "SystemSoftware", "Node.js 20", "id-ss-node")
    SS_PY = m.add_element(
        "technology", "SystemSoftware", "Python 3.12 + Uvicorn", "id-ss-python"
    )
    SS_PG = m.add_element("technology", "SystemSoftware", "PostgreSQL 16", "id-ss-pg")
    SS_AF = m.add_element("technology", "SystemSoftware", "Apache Airflow 2.10", "id-ss-af")
    SS_SP = m.add_element("technology", "SystemSoftware", "Apache Spark 3.5.4", "id-ss-sp")
    SS_DOCKER = m.add_element("technology", "SystemSoftware", "Docker Engine", "id-ss-docker")
    SS_REDIS = m.add_element("technology", "SystemSoftware", "Redis 7", "id-ss-redis")
    SS_CELERY = m.add_element(
        "technology", "SystemSoftware", "Python 3.12 + Celery", "id-ss-celery"
    )
    # Note: demo DB system-software (MySQL/ClickHouse/Mongo/Redis) intentionally not
    # modeled separately — engine type is shown in the demo Node name to avoid clutter.

    # Artifacts
    ART_FE = m.add_element("technology", "Artifact", "Next.js bundle", "id-art-frontend")
    ART_BE = m.add_element(
        "technology", "Artifact", "FastAPI app + LangGraph", "id-art-backend"
    )
    ART_DAGS = m.add_element(
        "technology", "Artifact", "DAG files (infra/airflow/dags)", "id-art-dags"
    )
    ART_JOBS = m.add_element(
        "technology", "Artifact", "Spark jobs (infra/spark/jobs)", "id-art-jobs"
    )
    ART_SOCK = m.add_element("technology", "Artifact", "/var/run/docker.sock", "id-art-sock")
    ART_PGVOL = m.add_element("technology", "Artifact", "Volume: postgres-data", "id-art-pgvol")

    # ---------------- Relations ----------------
    # Business: actors -> role; role assigned to user-side processes
    m.add_relation("AssignmentRelationship", A_DE, R_USER)
    m.add_relation("AssignmentRelationship", A_ADMIN, R_USER)
    m.add_relation("AssignmentRelationship", R_USER, BP_SEND)
    m.add_relation("AssignmentRelationship", R_USER, BP_DISPLAY)

    # Business process flow (Triggering — последовательность шагов)
    m.add_relation("TriggeringRelationship", BP_SEND, BP_INTERNAL_RECV)
    m.add_relation("TriggeringRelationship", BP_INTERNAL_RECV, BP_INTENT)
    m.add_relation("TriggeringRelationship", BP_INTENT, BP_LLM_PROC)
    m.add_relation("TriggeringRelationship", BP_LLM_PROC, BP_EXEC)
    m.add_relation("TriggeringRelationship", BP_EXEC, BP_LLM_PROC)  # ReAct loop
    m.add_relation("TriggeringRelationship", BP_LLM_PROC, BP_FORM)
    m.add_relation("TriggeringRelationship", BP_FORM, BP_DISPLAY)

    # Internal processes realize the business service
    for bp in (BP_INTERNAL_RECV, BP_INTENT, BP_LLM_PROC, BP_EXEC, BP_FORM):
        m.add_relation("RealizationRelationship", bp, BS_MAIN)

    # Application service realizes business service; serves internal processes
    m.add_relation("RealizationRelationship", AS_API, BS_MAIN)
    for bp in (BP_INTERNAL_RECV, BP_INTENT, BP_LLM_PROC, BP_EXEC, BP_FORM):
        m.add_relation("ServingRelationship", AS_API, bp)

    # Grouping composition — visual containers contain business elements
    m.add_relation("CompositionRelationship", GRP_USER_SIDE, BP_SEND)
    m.add_relation("CompositionRelationship", GRP_USER_SIDE, BP_DISPLAY)
    for bp in (BP_INTERNAL_RECV, BP_INTENT, BP_LLM_PROC, BP_EXEC, BP_FORM):
        m.add_relation("CompositionRelationship", GRP_INTERNAL, bp)

    # Application: components assigned to functions
    # (ArchiMate 3.2: Composition between Component & Function is illegal — use Assignment.)
    for af in (AF_CHAT, AF_SQLUI, AF_PIPE, AF_CATUI, AF_SETUI):
        m.add_relation("AssignmentRelationship", AC_FE, af)
    for af in (
        AF_RECV,
        AF_AUTH,
        AF_AGENT,
        AF_LLM,
        AF_TOOLS,
        AF_SQLX,
        AF_AFCLI,
        AF_SPCLI,
        AF_MCP,
        AF_ART,
        AF_SBX,
        AF_RESP,
        AF_PERSIST,
    ):
        m.add_relation("AssignmentRelationship", AC_BE, af)

    # Backend internal flow (Triggering between functions = sequence of execution)
    REL_RECV_AUTH = m.add_relation(
        "TriggeringRelationship", AF_RECV, AF_AUTH, rid="id-rel-recv-auth"
    )
    REL_AUTH_AGENT = m.add_relation(
        "TriggeringRelationship", AF_AUTH, AF_AGENT, rid="id-rel-auth-agent"
    )
    REL_AGENT_LLM = m.add_relation(
        "TriggeringRelationship", AF_AGENT, AF_LLM, rid="id-rel-agent-llm"
    )
    REL_LLM_AGENT = m.add_relation(
        "TriggeringRelationship", AF_LLM, AF_AGENT, rid="id-rel-llm-agent"
    )
    REL_AGENT_TOOLS = m.add_relation(
        "TriggeringRelationship", AF_AGENT, AF_TOOLS, rid="id-rel-agent-tools"
    )
    REL_AGENT_RESP = m.add_relation(
        "TriggeringRelationship", AF_AGENT, AF_RESP, rid="id-rel-agent-resp"
    )
    REL_RESP_PERSIST = m.add_relation(
        "TriggeringRelationship", AF_RESP, AF_PERSIST, rid="id-rel-resp-persist"
    )

    # Backend exposes API
    REL_BE_API = m.add_relation("RealizationRelationship", AC_BE, AS_API, rid="id-rel-be-api")

    # Backend API entry-point serves each frontend screen (granular Serving)
    REL_RECV_CHAT = m.add_relation(
        "ServingRelationship", AF_RECV, AF_CHAT, rid="id-rel-recv-chat"
    )
    REL_RECV_SQLUI = m.add_relation(
        "ServingRelationship", AF_RECV, AF_SQLUI, rid="id-rel-recv-sqlui"
    )
    REL_RECV_PIPE = m.add_relation(
        "ServingRelationship", AF_RECV, AF_PIPE, rid="id-rel-recv-pipe"
    )
    REL_RECV_CATUI = m.add_relation(
        "ServingRelationship", AF_RECV, AF_CATUI, rid="id-rel-recv-catui"
    )
    REL_RECV_SETUI = m.add_relation(
        "ServingRelationship", AF_RECV, AF_SETUI, rid="id-rel-recv-setui"
    )
    # Response goes back to chat UI for display
    REL_RESP_CHAT = m.add_relation(
        "TriggeringRelationship", AF_RESP, AF_CHAT, rid="id-rel-resp-chat"
    )

    # Agent uses tools (Tool Registry serves Agent — once, not per tool)
    m.add_relation("ServingRelationship", AF_TOOLS, AF_AGENT)
    for af in (AF_SQLX, AF_AFCLI, AF_SPCLI, AF_MCP, AF_ART, AF_SBX):
        m.add_relation("CompositionRelationship", AF_TOOLS, af)

    # Data access
    REL_AUTH_USERS = m.add_relation(
        "AccessRelationship", AF_AUTH, DO_USERS, rid="id-rel-auth-users", name="read"
    )
    REL_PERSIST_RUNS = m.add_relation(
        "AccessRelationship", AF_PERSIST, DO_RUNS, rid="id-rel-persist-runs"
    )
    REL_PERSIST_USERS = m.add_relation(
        "AccessRelationship", AF_PERSIST, DO_USERS, rid="id-rel-persist-users"
    )
    REL_AFCLI_META = m.add_relation(
        "AccessRelationship", AF_AFCLI, DO_AFMETA, rid="id-rel-afcli-meta"
    )

    # Technology -> Application realization: per ArchiMate 3.2, realization runs from
    # Artifact to ApplicationComponent / DataObject (not from Node directly).
    REL_ARTFE_AC = m.add_relation(
        "RealizationRelationship", ART_FE, AC_FE, rid="id-rel-artfe-ac"
    )
    REL_ARTBE_AC = m.add_relation(
        "RealizationRelationship", ART_BE, AC_BE, rid="id-rel-artbe-ac"
    )
    REL_PGVOL_USERS = m.add_relation(
        "RealizationRelationship", ART_PGVOL, DO_USERS, rid="id-rel-pgvol-users"
    )
    REL_PGVOL_RUNS = m.add_relation(
        "RealizationRelationship", ART_PGVOL, DO_RUNS, rid="id-rel-pgvol-runs"
    )
    REL_DAGS_META = m.add_relation(
        "RealizationRelationship", ART_DAGS, DO_AFMETA, rid="id-rel-dags-meta"
    )

    # System software composed by nodes (ArchiMate 3.2: Composition Node -> SystemSoftware)
    m.add_relation("CompositionRelationship", NODE_FE, SS_NODE)
    m.add_relation("CompositionRelationship", NODE_BE, SS_PY)
    m.add_relation("CompositionRelationship", NODE_PG, SS_PG)
    m.add_relation("CompositionRelationship", NODE_AFP, SS_PG)
    m.add_relation("CompositionRelationship", NODE_AFW, SS_AF)
    m.add_relation("CompositionRelationship", NODE_AFS, SS_AF)
    m.add_relation("CompositionRelationship", NODE_SPM, SS_SP)
    m.add_relation("CompositionRelationship", NODE_SPW, SS_SP)
    m.add_relation("CompositionRelationship", NODE_DBG, SS_PY)
    m.add_relation("CompositionRelationship", NODE_HOST, SS_DOCKER)
    m.add_relation("CompositionRelationship", NODE_REDIS, SS_REDIS)
    m.add_relation("CompositionRelationship", NODE_BWORKER, SS_CELERY)
    m.add_relation("CompositionRelationship", NODE_BBEAT, SS_CELERY)

    # Artifacts deployed on nodes (ArchiMate 3.2: Assignment Node -> Artifact)
    m.add_relation("AssignmentRelationship", NODE_FE, ART_FE)
    m.add_relation("AssignmentRelationship", NODE_BE, ART_BE)
    # backend-worker and backend-beat share the same FastAPI app artifact
    m.add_relation("AssignmentRelationship", NODE_BWORKER, ART_BE)
    m.add_relation("AssignmentRelationship", NODE_BBEAT, ART_BE)
    m.add_relation("AssignmentRelationship", NODE_AFW, ART_DAGS)
    m.add_relation("AssignmentRelationship", NODE_AFS, ART_DAGS)
    m.add_relation("AssignmentRelationship", NODE_SPM, ART_JOBS)
    m.add_relation("AssignmentRelationship", NODE_SPW, ART_JOBS)
    m.add_relation("AssignmentRelationship", NODE_DBG, ART_SOCK)
    m.add_relation("AssignmentRelationship", NODE_PG, ART_PGVOL)

    # Aggregate-уровень: Platform Node реализует frontend & backend components.
    # Использется на view-B (без детализации технологического слоя).
    m.add_relation(
        "RealizationRelationship", NODE_PLATFORM, AC_FE, rid="id-rel-platform-fe"
    )
    m.add_relation(
        "RealizationRelationship", NODE_PLATFORM, AC_BE, rid="id-rel-platform-be"
    )
    # Docker also hosts persistent storage for DataObjects (volumes, files).
    m.add_relation(
        "AssociationRelationship", NODE_PLATFORM, DO_USERS, rid="id-rel-platform-do-users"
    )
    m.add_relation(
        "AssociationRelationship", NODE_PLATFORM, DO_RUNS, rid="id-rel-platform-do-runs"
    )
    m.add_relation(
        "AssociationRelationship", NODE_PLATFORM, DO_AFMETA, rid="id-rel-platform-do-afmeta"
    )

    # Association: actors operate through the Browser (view-C показывает Browser
    # как клиентский узел, актёры подключены к нему).
    m.add_relation(
        "AssociationRelationship", A_DE, NODE_BROWSER, rid="id-rel-de-browser"
    )
    m.add_relation(
        "AssociationRelationship", A_ADMIN, NODE_BROWSER, rid="id-rel-admin-browser"
    )

    # Host owns the bridge network and spark jobs artifact (visual nesting advice)
    # Node <-> CommunicationNetwork: only Association is allowed in ArchiMate 3.2.
    m.add_relation("AssociationRelationship", NODE_HOST, CN_DOCKER)
    m.add_relation("AssignmentRelationship", NODE_HOST, ART_JOBS)

    # Network paths (flow + serving)
    REL_BROWSER_FE = m.add_relation(
        "FlowRelationship", NODE_BROWSER, NODE_FE, rid="id-rel-browser-fe", name="HTTPS :3000"
    )
    REL_FE_BE_FLOW = m.add_relation(
        "FlowRelationship", NODE_FE, NODE_BE, rid="id-rel-fe-be-flow", name="HTTP :8000"
    )
    REL_BE_PG = m.add_relation(
        "FlowRelationship", NODE_BE, NODE_PG, rid="id-rel-be-pg", name="TCP :5432"
    )
    REL_BE_AFW = m.add_relation(
        "FlowRelationship", NODE_BE, NODE_AFW, rid="id-rel-be-afw", name="HTTP :8080"
    )
    REL_AFW_AFP = m.add_relation(
        "FlowRelationship", NODE_AFW, NODE_AFP, rid="id-rel-afw-afp", name="TCP :5432"
    )
    REL_AFS_AFP = m.add_relation(
        "FlowRelationship", NODE_AFS, NODE_AFP, rid="id-rel-afs-afp", name="TCP :5432"
    )
    REL_BE_SPM = m.add_relation(
        "FlowRelationship", NODE_BE, NODE_SPM, rid="id-rel-be-spm", name="Spark :7077"
    )
    REL_SPW_SPM = m.add_relation(
        "FlowRelationship", NODE_SPW, NODE_SPM, rid="id-rel-spw-spm", name="Spark :7077"
    )
    REL_BE_DBG = m.add_relation(
        "FlowRelationship", NODE_BE, NODE_DBG, rid="id-rel-be-dbg", name="HTTP :8090"
    )
    REL_DBG_SBXPY = m.add_relation(
        "FlowRelationship",
        NODE_DBG,
        NODE_SBXPY,
        rid="id-rel-dbg-sbxpy",
        name="Docker API",
    )
    REL_DBG_SBXAF = m.add_relation(
        "FlowRelationship",
        NODE_DBG,
        NODE_SBXAF,
        rid="id-rel-dbg-sbxaf",
        name="Docker API",
    )
    REL_DBG_SBXSP = m.add_relation(
        "FlowRelationship",
        NODE_DBG,
        NODE_SBXSP,
        rid="id-rel-dbg-sbxsp",
        name="Docker API",
    )
    REL_BE_LLM = m.add_relation(
        "FlowRelationship", NODE_BE, NODE_LLM, rid="id-rel-be-llm", name="HTTPS (tool calling)"
    )
    REL_BE_USER_DBS = m.add_relation(
        "FlowRelationship",
        NODE_BE,
        NODE_USER_DBS,
        rid="id-rel-be-user-dbs",
        name="TCP (engine-specific)",
    )
    # Celery: broker traffic between backend / worker / beat and Redis;
    # worker also reads/writes the app DB.
    REL_BE_REDIS = m.add_relation(
        "FlowRelationship", NODE_BE, NODE_REDIS, rid="id-rel-be-redis", name="enqueue task :6379"
    )
    REL_BW_REDIS = m.add_relation(
        "FlowRelationship", NODE_BWORKER, NODE_REDIS, rid="id-rel-bw-redis", name="consume :6379"
    )
    REL_BB_REDIS = m.add_relation(
        "FlowRelationship", NODE_BBEAT, NODE_REDIS, rid="id-rel-bb-redis", name="schedule :6379"
    )
    REL_BW_PG = m.add_relation(
        "FlowRelationship", NODE_BWORKER, NODE_PG, rid="id-rel-bw-pg", name="TCP :5432"
    )
    # (Demo DB flows removed — see comment above about not modeling demo containers.)

    # Containers composed by host
    for child_node in (
        NODE_FE,
        NODE_BE,
        NODE_PG,
        NODE_AFW,
        NODE_AFS,
        NODE_AFP,
        NODE_SPM,
        NODE_SPW,
        NODE_DBG,
        NODE_SBXPY,
        NODE_SBXAF,
        NODE_SBXSP,
        NODE_REDIS,
        NODE_BWORKER,
        NODE_BBEAT,
    ):
        m.add_relation("CompositionRelationship", NODE_HOST, child_node)

    # ---------------- Views ----------------
    # Layer fills:
    BUS_FILL = "#FFE7AB"
    APP_FILL = "#B5E0E2"
    TECH_FILL = "#C5E0B4"
    DOCKER_FILL = "#A9D08E"

    # Build a lookup once for cc() helpers.
    rel_index: dict[tuple[str, str], str] = {}
    for rel in m.folders["relations"]:
        rel_index[(rel.get("source"), rel.get("target"))] = rel.get("id")

    def make_view_helpers(view: ET.Element):
        """Возвращает (place, cc, objs) для конкретного view."""
        objs: dict[str, ET.Element] = {}
        counter = {"n": 0}

        def place(elem_id: str, x: int, y: int, w: int = 120, h: int = 55,
                  parent=None, fill: str | None = None, register: bool = True):
            counter["n"] += 1
            parent = parent if parent is not None else view
            oid = f"id-do-{elem_id}-v{view.get('id')[-2:]}-{counter['n']}"
            obj = make_object(parent, elem_id, oid, x, y, w, h, fill=fill)
            if register and elem_id not in objs:
                objs[elem_id] = obj
            return obj

        def cc(src_key: str, tgt_key: str) -> None:
            rid = rel_index.get((src_key, tgt_key))
            if rid is None or src_key not in objs or tgt_key not in objs:
                return
            add_connection(m, objs[src_key], objs[tgt_key], rid)

        return place, cc, objs

    # =====================================================================
    # View A — Business + Service bridge
    # =====================================================================
    view_a = m.new_view(
        "3.1.a Бизнес-слой и сервис AI Data Engineer", "id-view-a-business"
    )
    placeA, ccA, _ = make_view_helpers(view_a)

    # Actors and Role
    placeA(A_DE, 20, 30, 130, 60, fill=BUS_FILL)
    placeA(A_ADMIN, 20, 100, 130, 60, fill=BUS_FILL)
    placeA(R_USER, 170, 65, 150, 60, fill=BUS_FILL)

    # Group: user-side BPs
    user_grpA = placeA(GRP_USER_SIDE, 340, 20, 280, 200, fill=BUS_FILL)
    placeA(BP_SEND, 20, 50, 240, 60, parent=user_grpA, fill=BUS_FILL)
    placeA(BP_DISPLAY, 20, 120, 240, 70, parent=user_grpA, fill=BUS_FILL)

    # Group: internal-service BPs
    internal_grpA = placeA(GRP_INTERNAL, 640, 20, 760, 200, fill=BUS_FILL)
    placeA(BP_INTERNAL_RECV, 20, 50, 180, 60, parent=internal_grpA, fill=BUS_FILL)
    placeA(BP_INTENT, 220, 50, 200, 60, parent=internal_grpA, fill=BUS_FILL)
    placeA(BP_LLM_PROC, 440, 50, 200, 60, parent=internal_grpA, fill=BUS_FILL)
    placeA(BP_EXEC, 440, 125, 300, 60, parent=internal_grpA, fill=BUS_FILL)
    placeA(BP_FORM, 220, 125, 200, 60, parent=internal_grpA, fill=BUS_FILL)

    placeA(BS_MAIN, 340, 240, 1060, 50, fill=BUS_FILL)

    # Single Application Service (blue) acts as the bridge to business service.
    placeA(AS_API, 340, 320, 1060, 60, fill=APP_FILL)

    # Connections within View A
    ccA(A_DE, R_USER)
    ccA(A_ADMIN, R_USER)
    ccA(R_USER, BP_SEND)
    ccA(R_USER, BP_DISPLAY)
    ccA(BP_SEND, BP_INTERNAL_RECV)
    ccA(BP_INTERNAL_RECV, BP_INTENT)
    ccA(BP_INTENT, BP_LLM_PROC)
    ccA(BP_LLM_PROC, BP_EXEC)
    ccA(BP_EXEC, BP_LLM_PROC)
    ccA(BP_LLM_PROC, BP_FORM)
    ccA(BP_FORM, BP_DISPLAY)
    for bp in (BP_INTERNAL_RECV, BP_INTENT, BP_LLM_PROC, BP_EXEC, BP_FORM):
        ccA(bp, BS_MAIN)
    ccA(AS_API, BS_MAIN)

    # =====================================================================
    # View B — Application detail + 1 Tech block + 1 Business block
    # =====================================================================
    view_b = m.new_view(
        "3.1.b Слой приложения и платформенный узел", "id-view-b-application"
    )
    placeB, ccB, _ = make_view_helpers(view_b)

    # Reference business service at the top (yellow)
    placeB(BS_MAIN, 20, 30, 1240, 50, fill=BUS_FILL)

    # Application service bridge
    placeB(AS_API, 20, 110, 1240, 50, fill=APP_FILL)

    # Frontend component
    fe_objB = placeB(AC_FE, 20, 200, 300, 260, fill=APP_FILL)
    placeB(AF_CHAT, 20, 50, 130, 55, parent=fe_objB, fill=APP_FILL)
    placeB(AF_SQLUI, 160, 50, 130, 55, parent=fe_objB, fill=APP_FILL)
    placeB(AF_PIPE, 20, 115, 130, 55, parent=fe_objB, fill=APP_FILL)
    placeB(AF_CATUI, 160, 115, 130, 55, parent=fe_objB, fill=APP_FILL)
    placeB(AF_SETUI, 20, 180, 270, 55, parent=fe_objB, fill=APP_FILL)

    # Backend component (full flow detail)
    be_objB = placeB(AC_BE, 340, 200, 700, 460, fill=APP_FILL)
    placeB(AF_RECV, 20, 40, 200, 70, parent=be_objB, fill=APP_FILL)
    placeB(AF_AUTH, 250, 40, 200, 70, parent=be_objB, fill=APP_FILL)
    placeB(AF_AGENT, 20, 130, 200, 80, parent=be_objB, fill=APP_FILL)
    placeB(AF_LLM, 250, 130, 200, 80, parent=be_objB, fill=APP_FILL)
    placeB(AF_TOOLS, 20, 230, 660, 50, parent=be_objB, fill=APP_FILL)
    placeB(AF_SQLX, 20, 290, 105, 55, parent=be_objB, fill=APP_FILL)
    placeB(AF_AFCLI, 130, 290, 105, 55, parent=be_objB, fill=APP_FILL)
    placeB(AF_SPCLI, 240, 290, 105, 55, parent=be_objB, fill=APP_FILL)
    placeB(AF_MCP, 350, 290, 105, 55, parent=be_objB, fill=APP_FILL)
    placeB(AF_ART, 460, 290, 105, 55, parent=be_objB, fill=APP_FILL)
    placeB(AF_SBX, 570, 290, 105, 55, parent=be_objB, fill=APP_FILL)
    placeB(AF_RESP, 20, 365, 320, 70, parent=be_objB, fill=APP_FILL)
    placeB(AF_PERSIST, 360, 365, 320, 70, parent=be_objB, fill=APP_FILL)

    # DataObjects to the right of backend
    placeB(DO_USERS, 1060, 260, 200, 60, fill=APP_FILL)
    placeB(DO_RUNS, 1060, 340, 200, 60, fill=APP_FILL)
    placeB(DO_AFMETA, 1060, 420, 200, 60, fill=APP_FILL)

    # Single Technology block "Docker" — справа, на одной высоте с backend
    # и DataObjects, без внутренней детализации. Цвет — оранжевый, чтобы
    # визуально не сливаться с зелёным технологическим слоем в третьем view.
    DOCKER_FILL_VIEW_B = "#E1A340"
    placeB(NODE_PLATFORM, 1320, 260, 220, 280, fill=DOCKER_FILL_VIEW_B)

    # Connections in View B
    ccB(AC_BE, AS_API)
    ccB(AS_API, BS_MAIN)
    # Intra-backend flow
    ccB(AF_RECV, AF_AUTH)
    ccB(AF_AUTH, AF_AGENT)
    ccB(AF_AGENT, AF_LLM)
    ccB(AF_LLM, AF_AGENT)
    ccB(AF_AGENT, AF_TOOLS)
    ccB(AF_AGENT, AF_RESP)
    ccB(AF_RESP, AF_PERSIST)
    # Tool Registry composition
    for af in (AF_SQLX, AF_AFCLI, AF_SPCLI, AF_MCP, AF_ART, AF_SBX):
        ccB(AF_TOOLS, af)
    # API entry serves frontend
    ccB(AF_RECV, AF_CHAT)
    ccB(AF_RECV, AF_SQLUI)
    ccB(AF_RECV, AF_PIPE)
    ccB(AF_RECV, AF_CATUI)
    ccB(AF_RECV, AF_SETUI)
    ccB(AF_RESP, AF_CHAT)
    # Access to data
    ccB(AF_AUTH, DO_USERS)
    ccB(AF_PERSIST, DO_USERS)
    ccB(AF_PERSIST, DO_RUNS)
    ccB(AF_AFCLI, DO_AFMETA)
    # Platform Node realizes the components (single arrow each)
    ccB(NODE_PLATFORM, AC_FE)
    ccB(NODE_PLATFORM, AC_BE)
    # Docker also hosts persistent storage for DataObjects
    ccB(NODE_PLATFORM, DO_USERS)
    ccB(NODE_PLATFORM, DO_RUNS)
    ccB(NODE_PLATFORM, DO_AFMETA)

    # =====================================================================
    # View C — Technology detail + simplified Application
    # =====================================================================
    view_c = m.new_view(
        "3.1.в Инфраструктура (технологический слой)", "id-view-c-technology"
    )
    placeC, ccC, _ = make_view_helpers(view_c)

    # Actors attached to Browser (yellow on green canvas)
    placeC(A_DE, 20, 30, 130, 60, fill=BUS_FILL)
    placeC(A_ADMIN, 20, 100, 130, 60, fill=BUS_FILL)

    # Browser node (recolored green — all "physical" things are tech in view C)
    placeC(NODE_BROWSER, 170, 60, 160, 70, fill=TECH_FILL)

    # Simplified Application layer — only two boxes
    placeC(AC_FE, 360, 60, 220, 70, fill=APP_FILL)
    placeC(AC_BE, 600, 60, 220, 70, fill=APP_FILL)

    # External LLM and User DBs — recolored green
    placeC(NODE_LLM, 1690, 60, 220, 80, fill=TECH_FILL)
    placeC(NODE_USER_DBS, 1690, 160, 220, 130, fill=TECH_FILL)

    # Host container (technology detail)
    host_objC = placeC(NODE_HOST, 20, 320, 1640, 740, fill=TECH_FILL)

    NODE_W = 230
    NODE_H_TALL = 230
    GAP = 30

    # Row 1
    x = 20
    fe_node_C = placeC(NODE_FE, x, 40, NODE_W, NODE_H_TALL, parent=host_objC, fill=TECH_FILL)
    placeC(SS_NODE, 15, 45, NODE_W - 30, 50, parent=fe_node_C, fill=TECH_FILL)
    placeC(ART_FE, 15, 110, NODE_W - 30, 50, parent=fe_node_C, fill=TECH_FILL)
    x += NODE_W + GAP
    be_node_C = placeC(NODE_BE, x, 40, NODE_W + 20, NODE_H_TALL, parent=host_objC, fill=TECH_FILL)
    placeC(SS_PY, 15, 45, NODE_W - 10, 50, parent=be_node_C, fill=TECH_FILL)
    placeC(ART_BE, 15, 110, NODE_W - 10, 50, parent=be_node_C, fill=TECH_FILL)
    x += NODE_W + 20 + GAP
    pg_node_C = placeC(NODE_PG, x, 40, 200, NODE_H_TALL, parent=host_objC, fill=TECH_FILL)
    placeC(SS_PG, 15, 45, 170, 50, parent=pg_node_C, fill=TECH_FILL)
    placeC(ART_PGVOL, 15, 110, 170, 50, parent=pg_node_C, fill=TECH_FILL)
    x += 200 + GAP
    AF_W = 200
    afw_C = placeC(NODE_AFW, x, 40, AF_W, 100, parent=host_objC, fill=TECH_FILL)
    placeC(SS_AF, 15, 50, AF_W - 30, 35, parent=afw_C, fill=TECH_FILL)
    afs_C = placeC(NODE_AFS, x, 150, AF_W, 120, parent=host_objC, fill=TECH_FILL)
    placeC(SS_AF, 15, 45, AF_W - 30, 35, parent=afs_C, fill=TECH_FILL, register=False)
    placeC(ART_DAGS, 15, 90, AF_W - 30, 25, parent=afs_C, fill=TECH_FILL)
    x += AF_W + GAP
    afp_C = placeC(NODE_AFP, x, 40, 200, NODE_H_TALL, parent=host_objC, fill=TECH_FILL)
    placeC(SS_PG, 15, 45, 170, 50, parent=afp_C, fill=TECH_FILL, register=False)
    x += 200 + GAP
    SP_W = 250
    spm_C = placeC(NODE_SPM, x, 40, SP_W, 100, parent=host_objC, fill=TECH_FILL)
    placeC(SS_SP, 15, 50, SP_W - 30, 35, parent=spm_C, fill=TECH_FILL)
    spw_C = placeC(NODE_SPW, x, 150, SP_W, 80, parent=host_objC, fill=TECH_FILL)
    placeC(SS_SP, 15, 35, SP_W - 30, 35, parent=spw_C, fill=TECH_FILL, register=False)
    placeC(ART_JOBS, x, 240, SP_W, 30, parent=host_objC, fill=TECH_FILL)

    # Row 2: debugger + sandboxes
    dbg_C = placeC(NODE_DBG, 20, 320, 240, 180, parent=host_objC, fill=TECH_FILL)
    placeC(SS_PY, 15, 45, 210, 40, parent=dbg_C, fill=TECH_FILL, register=False)
    placeC(ART_SOCK, 15, 95, 210, 50, parent=dbg_C, fill=TECH_FILL)
    SBX_W = 250
    sbx_x = 290
    placeC(NODE_SBXPY, sbx_x, 320, SBX_W, 80, parent=host_objC, fill=TECH_FILL)
    sbx_x += SBX_W + GAP
    placeC(NODE_SBXAF, sbx_x, 320, SBX_W, 80, parent=host_objC, fill=TECH_FILL)
    sbx_x += SBX_W + GAP
    placeC(NODE_SBXSP, sbx_x, 320, SBX_W, 80, parent=host_objC, fill=TECH_FILL)

    # Row 2b: Celery stack
    bw_C = placeC(NODE_BWORKER, 20, 520, 280, 100, parent=host_objC, fill=TECH_FILL)
    placeC(SS_CELERY, 15, 45, 250, 40, parent=bw_C, fill=TECH_FILL)
    bb_C = placeC(NODE_BBEAT, 330, 520, 280, 100, parent=host_objC, fill=TECH_FILL)
    placeC(SS_CELERY, 15, 45, 250, 40, parent=bb_C, fill=TECH_FILL, register=False)
    redis_C = placeC(NODE_REDIS, 640, 520, 260, 100, parent=host_objC, fill=TECH_FILL)
    placeC(SS_REDIS, 15, 45, 230, 40, parent=redis_C, fill=TECH_FILL)

    # Row 3: docker infra
    placeC(CN_DOCKER, 20, 660, 1200, 40, parent=host_objC, fill=DOCKER_FILL)
    placeC(SS_DOCKER, 1250, 660, 360, 40, parent=host_objC, fill=DOCKER_FILL)

    # Connections in View C
    # Actors → Browser
    ccC(A_DE, NODE_BROWSER)
    ccC(A_ADMIN, NODE_BROWSER)
    # Browser → frontend container (HTTP)
    ccC(NODE_BROWSER, NODE_FE)
    # Tech flows
    ccC(NODE_FE, NODE_BE)
    ccC(NODE_BE, NODE_PG)
    ccC(NODE_BE, NODE_AFW)
    ccC(NODE_AFW, NODE_AFP)
    ccC(NODE_AFS, NODE_AFP)
    ccC(NODE_BE, NODE_SPM)
    ccC(NODE_SPW, NODE_SPM)
    ccC(NODE_BE, NODE_DBG)
    ccC(NODE_DBG, NODE_SBXPY)
    ccC(NODE_DBG, NODE_SBXAF)
    ccC(NODE_DBG, NODE_SBXSP)
    ccC(NODE_BE, NODE_LLM)
    ccC(NODE_BE, NODE_USER_DBS)
    ccC(NODE_BE, NODE_REDIS)
    ccC(NODE_BWORKER, NODE_REDIS)
    ccC(NODE_BBEAT, NODE_REDIS)
    ccC(NODE_BWORKER, NODE_PG)
    # Tech → Application realization
    ccC(ART_FE, AC_FE)
    ccC(ART_BE, AC_BE)

    return m


def main() -> None:
    model = build()
    out_path = Path(__file__).resolve().parent.parent / "docs" / "3_1_physical_architecture.archimate"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    xml = pretty(model.root)
    # Replace ns0/ns1 prefix artifacts from ElementTree if any
    out_path.write_text(xml, encoding="utf-8")
    print(f"Wrote {out_path} ({len(xml)} bytes)")


if __name__ == "__main__":
    main()
