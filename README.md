# AI Data Engineer Assistant

Fullstack-приложение для data engineering workflows: AI-агент принимает запрос на естественном языке, выбирает нужный инструмент и выполняет действия в SQL, Airflow, Spark, каталоге данных или sandbox-окружении.

Проект сделан как портфолио/дипломная работа и показывает связку `LLM agent -> tool calling -> production-like data tools`.

## Возможности

- AI Agent с LangGraph orchestration и tool-call loop.
- SQL Workspace с read-only выполнением запросов и инспекцией схемы.
- Airflow orchestration: просмотр DAG, запуск DAG runs, управление пайплайнами.
- Spark jobs: отправка задач, проверка статуса и вывод результата.
- Catalog и Connections для обзора доступных таблиц, продуктов и MCP tools.
- Artifact workflow: генерация DAG/PySpark-скриптов, syntax validation, Git-версии.
- Docker sandbox для проверки Python, Airflow DAG и Spark scripts по пользователям.
- Auth, роли пользователей, история сессий, сообщения и tool runs.

## Состав

- `frontend/` — Next.js App Router, TypeScript, React Query, Zustand, Monaco Editor.
- `backend/` — FastAPI, Pydantic v2, SQLAlchemy async, JWT, OpenAI Responses function calling или OpenRouter chat tool calling, Langfuse tracing adapter.
- `infra/` — sample Airflow DAGs, Spark job и sandbox-сервис для запуска/проверки агентских скриптов.
- `docs/` — описание дипломной работы и проектные материалы.
- `docker-compose.yml` — PostgreSQL, backend, frontend, Airflow webserver/scheduler, Spark master/worker, optional agent debugger.

## Архитектура

```text
User
  -> Next.js frontend
  -> FastAPI backend
  -> LangGraph agent orchestrator
  -> Tool registry
     -> SQL / Catalog
     -> Airflow
     -> Spark
     -> MCP integrations
     -> Artifact writer
     -> Docker sandbox
```

Агент может работать через OpenAI Responses API, OpenRouter tool calling или локальный fallback-режим. Даже без LLM-ключей демо-сценарии и тесты используют тот же registry инструментов.

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
- `bitnami/spark:3.5.4` для Spark scripts.

Для этого сервису монтируется Docker socket и общий каталог `SANDBOX_HOST_RUNS_DIR`.
Рабочие каталоги и Docker labels разделяются по пользователям: `SANDBOX_HOST_RUNS_DIR/users/<user_id>/run-*`.

Сервисы:

- Frontend: `http://localhost:3000`
- Backend: `http://localhost:8000`
- Airflow: `http://localhost:8080` (`airflow` / `airflow`)
- Spark master UI: `http://localhost:8081`
- Agent debugger: `http://localhost:${AGENT_DEBUGGER_PORT:-8090}` при profile `debug`

Backend CORS автоматически включает `localhost` и `127.0.0.1` для `FRONTEND_PORT`, плюс стандартные `3000` и `3002`.
Frontend по умолчанию обращается к backend через same-origin proxy `/api/backend/*`, поэтому браузеру не нужен прямой CORS-доступ к backend-порту.

## Проверки

```bash
cd backend
pytest
```

```bash
cd frontend
npm run build
```

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

- `LLM_PROVIDER=openai` + `OPENAI_API_KEY` для OpenAI Responses API.
- `LLM_PROVIDER=openrouter` + `OPENROUTER_API_KEY` для OpenRouter chat tool calling.

Оркестрация агента построена на LangGraph: runtime выбирает OpenAI/OpenRouter или локальный fallback, выполняет tool-call loop, вызывает product tools и финализирует ответ через graph nodes. Без ключа агент работает в локальном fallback-режиме для демо и тестов, но с тем же registry tools и сохранением `tool_runs`.

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

Готовые внешние MCP-серверы подключаются через MCP Python client: stdio для npm/uvx-серверов и streamable HTTP для серверов, которые сами поднимают HTTP endpoint.

- `database` — `@modelcontextprotocol/server-postgres` через `npx`, schema/read-only query tools.
- `airflow` — `astro-airflow-mcp` от Astronomer через `uvx`, DAG/runs/logs/health tools.
- `spark` — `pyspark-mcp` через `uvx` и streamable HTTP `/mcp`, Spark catalog/query plan tools.
- `artifacts_git` — reference `mcp-server-git` через `uvx`, Git tools для артефактов.
- `artifacts_filesystem` — reference filesystem MCP через `npx`, ограниченный `ARTIFACT_ROOT`.

MCP-compatible tool schema также доступна из общего registry и разложена по продуктам: `site`, `database`, `airflow`, `spark`, `external_mcp`, `artifacts`.

Артефакты сохраняются по пользователям в `infra/users/<user_id>/...`; обычный пользователь видит и отлаживает только свои версии. Admin видит все версии в истории. Git-репозиторий артефактов инициализируется строго в `ARTIFACT_GIT_ROOT`, чтобы не смешивать с git-репозиторием исходного кода.

## Безопасность

- SQL tool принимает только read-only запросы.
- `.env` и локальные базы не публикуются.
- Пользовательские артефакты из `infra/users/` исключены из репозитория.
- Sandbox runs и временные upload-файлы не попадают в Git.
