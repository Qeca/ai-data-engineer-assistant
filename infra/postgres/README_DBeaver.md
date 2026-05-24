# Подключение схемы AI DE к DBeaver

## Что находится в `ai_de_schema_normalized.sql`

Нормализованная (3НФ) схема БД для AI Data Engineer Assistant.
**15 таблиц** + **3 представления** + 13 ENUM-типов + триггеры + демо-данные.

| # | Таблица | Назначение |
| --- | --- | --- |
| 1 | `app_users` | Пользователи |
| 2 | `database_connections` | Подключения к внешним БД |
| 3 | `database_connection_secrets` | Зашифрованные пароли (отделены от метаданных) |
| 4 | `agent_sessions` | Сессии чата |
| 5 | `messages` | Сообщения чата (нормализованный history) |
| 6 | `agent_tools` | Справочник tool-функций |
| 7 | `tool_runs` | Каждый вызов tool — отдельная запись |
| 8 | `ui_actions` | UI-действия агента |
| 9 | `pipeline_runs` | Запуски Airflow DAG |
| 10 | `pipeline_states` | Состояние DAG (pause/resume) |
| 11 | `spark_jobs` | Spark-задачи |
| 12 | `artifacts` | Шапка артефакта |
| 13 | `artifact_versions` | Версии артефакта |
| 14 | `artifact_validations` | История sandbox-валидаций |
| 15 | `artifact_git_commits` | Git-коммиты артефактов |

| View | Что показывает |
| --- | --- |
| `v_chat_history` | Полная история чата с агрегатами tool_calls / ui_actions |
| `v_artifact_current` | Текущая версия каждого артефакта со связанным git / validation |
| `v_session_metrics` | Метрики использования агента (токены, латентность, ошибки) |

---

## Шаг 1 — поднять PostgreSQL

Если в проекте уже запущен docker-compose, контейнер `diplom-postgres-1` доступен.
Иначе:

```bash
docker run -d --name pg-aide -p 5432:5432 \
  -e POSTGRES_PASSWORD=postgres \
  postgres:16-alpine
```

## Шаг 2 — создать БД и накатить схему

```bash
# создать пустую БД
docker exec diplom-postgres-1 psql -U postgres \
  -c "CREATE DATABASE ai_de_normalized;"

# скопировать SQL в контейнер
docker cp infra/postgres/ai_de_schema_normalized.sql diplom-postgres-1:/tmp/

# применить
docker exec diplom-postgres-1 psql -U postgres -d ai_de_normalized \
  -v ON_ERROR_STOP=1 -f /tmp/ai_de_schema_normalized.sql
```

Если порт `5432` пробрасывается наружу — можно подключиться напрямую:

```bash
psql -h localhost -p 5432 -U postgres -d ai_de_normalized \
  -f infra/postgres/ai_de_schema_normalized.sql
```

## Шаг 3 — подключение в DBeaver

1. **Database → New Database Connection → PostgreSQL**
2. Параметры:
   - Host: `localhost`
   - Port: `5432` (или твой `${POSTGRES_PORT}` — у меня в проекте `15432`)
   - Database: `ai_de_normalized`
   - Username: `postgres`
   - Password: `postgres`
3. **Test Connection** → должно быть зелёное.
4. **Finish**.

## Шаг 4 — ER-диаграмма прямо из DBeaver (для диплома)

1. В левой панели разверни подключение → `Databases` → `ai_de_normalized` → `Schemas` → `public`.
2. Правый клик на `public` → **View Diagram**.
3. Откроется ERD со всеми 15 таблицами, FK-стрелками и группами ENUM.
4. **File → Export As → PNG / SVG** — готовая картинка для ПЗ.

Альтернатива: правый клик на `Tables` → **ER Diagram** → выбрать конкретные таблицы.

## Шаг 5 — проверка демо-данных

```sql
-- Полная история одного чата
SELECT * FROM v_chat_history;

-- Метрики сессий
SELECT * FROM v_session_metrics;

-- Текущие версии артефактов
SELECT * FROM v_artifact_current;

-- Tool calls с раскрытым input_json
SELECT
    s.title           AS session,
    m.sequence_index  AS msg_idx,
    at.name           AS tool,
    tr.status,
    tr.input_json,
    tr.latency_ms
FROM tool_runs tr
JOIN messages       m  ON m.id = tr.message_id
JOIN agent_sessions s  ON s.id = m.session_id
JOIN agent_tools    at ON at.id = tr.tool_id
ORDER BY tr.started_at;
```

## Что показать в защите ВКР

1. **15 таблиц, 3 НФ** — каждая колонка зависит только от первичного ключа.
2. **ENUM-типы** (`user_role`, `db_engine`, `message_role` и т.д.) вместо строковых литералов — нормализация.
3. **Секреты в отдельной таблице** `database_connection_secrets` с историей ротации — best practice для credentials.
4. **Версионирование артефактов**: 4 таблицы (`artifacts` → `artifact_versions` → `artifact_validations`/`artifact_git_commits`) вместо одного «толстого» row.
5. **Сообщения чата с `sequence_index`** — детерминированный порядок без зависимости от `created_at`.
6. **Токены и латентность** — отдельные числовые поля (`prompt_tokens`, `completion_tokens`, `total_tokens` GENERATED) вместо подмножества JSON.
7. **FK с ON DELETE CASCADE / SET NULL** — корректно поддерживается целостность.
8. **Триггеры `updated_at`** — автоматически без касания приложения.
9. **Композитные UNIQUE-констрейнты** (`uq_artifact_version`, `uq_message_seq`, `uq_conn_name_owner`) — гарантируют уникальность бизнес-ключей.
10. **3 представления** для типовых аналитических запросов — не дублируют логику между UI и SQL.
