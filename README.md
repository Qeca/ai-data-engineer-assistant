# AI Data Engineer Assistant

Fullstack-приложение для data engineering workflows: AI-агент принимает запрос на естественном языке, выбирает нужный инструмент и выполняет действия в SQL, Airflow, Spark, каталоге данных или sandbox-окружении.

Проект сделан как портфолио/дипломная работа и показывает связку `LLM agent -> tool calling -> production-like data tools`.

## Возможности

- AI Agent с LangGraph orchestration и tool-call loop.
- SQL Workspace с read-only выполнением запросов и инспекцией схемы.
- Airflow orchestration: просмотр DAG, запуск DAG runs, управление пайплайнами.
- Spark jobs: отправка задач, проверка статуса и вывод результата.
- Catalog и Connections для обзора доступных таблиц, продуктов, MCP tools и подключений к БД.
- Demo DB stack для проверки подключений: PostgreSQL, MySQL, ClickHouse, MongoDB и Redis.
- Artifact workflow: генерация DAG/PySpark-скриптов, syntax validation, Git-версии.
- Docker sandbox для проверки Python, Airflow DAG и Spark scripts по пользователям.
- Auth, роли пользователей, история сессий, сообщения и tool runs.

## Состав

- `frontend/` — Next.js App Router, TypeScript, React Query, Zustand, Monaco Editor.
- `backend/` — FastAPI, Pydantic v2, SQLAlchemy async, JWT, OpenAI Responses function calling или OpenRouter chat tool calling, Langfuse tracing adapter.
- `infra/` — sample Airflow DAGs, Spark job и sandbox-сервис для запуска/проверки агентских скриптов.
- `docs/` — описание дипломной работы и проектные материалы.
- `docker-compose.yml` — PostgreSQL, demo DB stack, backend, frontend, Airflow webserver/scheduler, Spark master/worker, optional agent debugger.

## Архитектура

```text
User
  -> Next.js frontend
  -> FastAPI backend
  -> LangGraph agent orchestrator
  -> Tool registry
     -> SQL / Catalog
     -> Database connections
     -> Airflow
     -> Spark
     -> MCP integrations
     -> Artifact writer
     -> Docker sandbox
```

Агент работает через MagnitGPT, OpenAI Responses API или OpenRouter tool calling. Без LLM-ключа rule-based сценарии не запускаются: backend возвращает ошибку конфигурации, чтобы ответы не подменялись хардкодом.

## Быстрый локальный запуск

Используйте Python 3.12.

```bash
cp .env.example .env
```

```bash
cd backend
python3.12 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --reload
```

```bash
cd frontend
npm install
npm run dev
```

Откройте `http://localhost:3000`.

Демо-пользователь:

- email: `admin@local.dev`
- password: `admin`

## Docker Compose

```bash
docker compose up --build
```

Если локальные порты уже заняты, переопределите их через env, например:

```bash
FRONTEND_PORT=3002 POSTGRES_PORT=15432 AIRFLOW_WEBSERVER_PORT=18080 docker compose up --build
```

Для Docker-изолированного sandbox, где агент запускает и проверяет DAG/Spark/Python-скрипты:

```bash
export SANDBOX_HOST_RUNS_DIR=/tmp/ai-de-sandbox-runs
export AGENT_DEBUGGER_PORT=18090
docker compose --profile debug up --build agent-debugger
```

`agent-debugger` сам поднимает одноразовые Docker-контейнеры для каждого запуска:

- `python:3.12-slim` для обычных Python-скриптов;
- `apache/airflow:2.10.4` для import-check DAG;
- `apache/spark:3.5.4` для Spark scripts.

Для этого сервису монтируется Docker socket и общий каталог `SANDBOX_HOST_RUNS_DIR`.
Рабочие каталоги и Docker labels разделяются по пользователям: `SANDBOX_HOST_RUNS_DIR/users/<user_id>/run-*`.

Сервисы:

- Frontend: `http://localhost:3000`
- Backend: `http://localhost:8000`
- Airflow: `http://localhost:8080` (`airflow` / `airflow`)
- Spark master UI: `http://localhost:8081`
- Agent debugger: `http://localhost:${AGENT_DEBUGGER_PORT:-8090}` при profile `debug`
- Demo PostgreSQL: `localhost:15433` (`demo` / `demo`, database `analytics`)
- Demo MySQL: `localhost:13306` (`demo` / `demo`, database `analytics`)
- Demo ClickHouse HTTP/native: `localhost:18123` / `localhost:19000` (`demo` / `demo`, database `analytics`)
- Demo MongoDB: `localhost:27018` (`demo` / `demo`, database `analytics`)
- Demo Redis: `localhost:16379` (password `demo`)

Backend CORS автоматически включает `localhost` и `127.0.0.1` для `FRONTEND_PORT`, плюс стандартные `3000` и `3002`.
Frontend по умолчанию обращается к backend через same-origin proxy `/api/backend/*`, поэтому браузеру не нужен прямой CORS-доступ к backend-порту.

Backend автоматически создает `shared` подключения к этим demo DB. Агент видит их через tool `list_database_connections`, может создавать/обновлять подключения через `upsert_database_connection` и проверять доступность через `test_database_connection`; изменения появляются на экранах Settings и Connections.

## Проверки

```bash
cd backend
pytest
```

Сценарные проверки агентского поведения:

```bash
cd backend
pytest tests/test_agent_scenarios.py
```

```bash
cd frontend
npm run build
```

## Тестовые Postgres-Базы

Для ручной проверки MCP database и SQL-сценариев есть сиды в `infra/postgres/test_databases.sql`.

```bash
docker cp infra/postgres/test_databases.sql postgres:/tmp/test_databases.sql
docker exec postgres psql -U postgres -v ON_ERROR_STOP=1 -f /tmp/test_databases.sql
```

Скрипт создает три отдельные базы:

- `ai_de_playground` — основная аналитическая база: `customers`, `products`, `orders`, `order_items`, `payments`, `events`, views `analytics.hourly_order_anomalies`, `analytics.customer_ltv`, `quality.order_quality_checks`.
- `marketing_playground` — кампании, рекламные расходы, лиды и конверсии.
- `ops_playground` — источники данных, pipeline runs, data quality checks и incidents.

Для MCP database backend должен стартовать с:

```bash
MCP_DATABASE_URL=postgresql://postgres:postgres@host.docker.internal:5432/ai_de_playground
```

Примеры запросов агенту:

- `какие MCP tools умеет database`
- `через MCP database выполни query: select * from analytics.hourly_order_anomalies limit 5`
- `что лежит в базе ai_de_playground`
- `найди аномалии по заказам за последние 30 дней`
- `посчитай LTV по сегментам клиентов`

## API

Основные endpoint'ы:

- `POST /auth/login`, `POST /auth/refresh`, `GET /me`
- `GET/POST /users`
- `POST /agent/query`
- `GET /sessions`, `GET /sessions/{id}/messages`
- `GET /catalog/tables`, `POST /sql/execute`
- `GET /pipelines`
- `POST /airflow/dags/{dag_id}/runs`, `GET /airflow/dags/{dag_id}/runs/{run_id}`
- `POST /spark/jobs`, `GET /spark/jobs/{job_id}`

Function/tool calling включается через env:

- `LLM_PROVIDER=magnitgpt` + `MAGNITGPT_API_KEY` для MagnitGPT через OpenAI-compatible `/chat/completions`.
- `LLM_PROVIDER=openai` + `OPENAI_API_KEY` для OpenAI Responses API.
- `LLM_PROVIDER=openrouter` + `OPENROUTER_API_KEY` для OpenRouter chat tool calling.

Оркестрация агента построена на LangGraph в ReAct-цикле: model call -> function tool call -> observation -> следующий model call до финального ответа. Rule-based fallback без LLM отключен: если ключ модели не настроен, агент возвращает ошибку конфигурации и не подменяет модель захардкоженными сценариями.

Доступные function tools:

- `list_site_status` — все видимые статусы сайта и backend-capabilities.
- `navigate_site` — управление frontend-экраном через `ui_actions`.
- `execute_sql` — read-only SQL.
- `list_catalog` — таблицы и колонки.
- `inspect_database` — полный осмотр БД: таблицы, колонки, row count и небольшие samples.
- `list_mcp_products`, `list_mcp_tools`, `call_mcp_tool` — подключение готовых внешних MCP-серверов.
- `list_pipelines`, `manage_airflow_dags`, `trigger_airflow_dag`, `get_airflow_run`.
- `submit_spark_job`, `get_spark_job`.
- `write_airflow_dag` — запись DAG-файла, syntax validation, запись версии в БД и Git commit.
- `write_spark_script` — запись PySpark-скрипта, syntax validation, запись версии в БД и Git commit.
- `check_airflow_dag_sandbox` — импортирует DAG в user-scoped Docker sandbox и проверяет, что Airflow-код реально загружается.
- `run_spark_script_sandbox` — запускает Spark/PySpark-скрипт в user-scoped Docker sandbox.
- `run_python_script_sandbox` — запускает обычный Python-скрипт в user-scoped Docker sandbox.
- `list_artifact_versions` — история версий DAG/Spark-скриптов из БД с Git history по файлу.

Готовые внешние MCP-серверы подключаются через MCP Python client: stdio для локальных CLI-серверов и streamable HTTP для серверов, которые сами поднимают HTTP endpoint. Backend Docker image устанавливает npm/Python MCP servers заранее и использует их binaries напрямую, поэтому зависимости не скачиваются во время пользовательского tool-call.

- `database` — `@modelcontextprotocol/server-postgres`, schema/read-only query tools.
- `airflow` — `astro-airflow-mcp` от Astronomer, DAG/runs/logs/health tools.
- `spark` — `pyspark-mcp` и streamable HTTP `/mcp`, Spark catalog/query plan tools.
- `artifacts_git` — reference `mcp-server-git`, Git tools для артефактов.
- `artifacts_filesystem` — reference filesystem MCP, ограниченный `ARTIFACT_ROOT`.

MCP-compatible tool schema также доступна из общего registry и разложена по продуктам: `site`, `database`, `airflow`, `spark`, `external_mcp`, `artifacts`.

Модель обучается работать с MCP через отдельный prompt playbook в `MCPInstructionBook`: сначала `list_mcp_products` при неизвестном продукте, затем `list_mcp_tools(product)`, затем `call_mcp_tool(product, exact_tool_name, arguments)` строго по найденной input schema. Если внешний MCP недоступен или не имеет нужного tool, агент использует локальный product tool и сообщает об этом в ответе. Для записи пользовательских DAG/Spark-скриптов модель предпочитает локальные artifact tools, потому что они enforce user scope, sandbox validation и Git versioning; filesystem MCP оставлен для явного чтения, поиска и низкоуровневых операций в `ARTIFACT_ROOT`.

Артефакты сохраняются по пользователям в `infra/users/<user_id>/...`; обычный пользователь видит и отлаживает только свои версии. Admin видит все версии в истории. Git-репозиторий артефактов инициализируется строго в `ARTIFACT_GIT_ROOT`, чтобы не смешивать с git-репозиторием исходного кода.

## Безопасность

- SQL tool принимает только read-only запросы.
- `.env` и локальные базы не публикуются.
- Пользовательские артефакты из `infra/users/` исключены из репозитория.
- Sandbox runs и временные upload-файлы не попадают в Git.
