-- =========================================================================
-- AI Data Engineer Assistant — Нормализованная схема БД (3НФ)
-- =========================================================================
-- PostgreSQL ≥ 14
-- Применение из DBeaver:
--   1. Создать пустую БД: CREATE DATABASE ai_de_normalized;
--   2. Подключиться к ней
--   3. Выполнить этот файл целиком
--   4. Refresh схемы → видна структура с FK-связями
--
-- Соответствует моделям SQLAlchemy в backend/app/models.py, но приведено
-- к 3-й нормальной форме:
--   - все повторяющиеся string-литералы вынесены в ENUM-типы
--   - tool_call'ы и ui_actions вынесены из messages.metadata_json в отдельные
--     таблицы (нет неатомарных JSONB-полей в стабильной части схемы)
--   - пароли подключений вынесены в отдельную таблицу с алгоритмом шифрования
--   - артефакты разнесены: версия отдельно от git-метаданных и валидации
-- =========================================================================

BEGIN;

-- =========================================================================
-- РАСШИРЕНИЯ POSTGRESQL (должны быть установлены до использования типов)
-- =========================================================================

CREATE EXTENSION IF NOT EXISTS "pgcrypto"; -- gen_random_uuid()
CREATE EXTENSION IF NOT EXISTS "citext";   -- case-insensitive email (CITEXT type)

-- =========================================================================
-- ENUM-типы (исключают повторение string-литералов по таблицам — 1НФ/3НФ)
-- =========================================================================

CREATE TYPE user_role     AS ENUM ('engineer', 'admin', 'viewer');
CREATE TYPE user_status   AS ENUM ('active', 'invited', 'disabled');
CREATE TYPE db_engine     AS ENUM ('postgresql', 'mysql', 'clickhouse', 'mongodb', 'redis', 'sqlite');
CREATE TYPE conn_visibility AS ENUM ('private', 'shared');
CREATE TYPE conn_status   AS ENUM ('unknown', 'active', 'error');
CREATE TYPE message_role  AS ENUM ('user', 'assistant', 'system', 'tool');
CREATE TYPE tool_run_status AS ENUM ('success', 'error');
CREATE TYPE pipeline_status AS ENUM ('queued', 'running', 'success', 'failed', 'skipped');
CREATE TYPE spark_status  AS ENUM ('submitted', 'running', 'success', 'failed');
CREATE TYPE artifact_type AS ENUM ('dag', 'spark');
CREATE TYPE validation_status AS ENUM ('ok', 'error', 'unknown');
CREATE TYPE git_status    AS ENUM ('committed', 'dirty', 'error', 'unknown');
CREATE TYPE encryption_algorithm AS ENUM ('fernet', 'aes_gcm', 'plaintext');

-- =========================================================================
-- 1. ПОЛЬЗОВАТЕЛИ
-- =========================================================================

CREATE TABLE app_users (
    id              UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    email           CITEXT       NOT NULL UNIQUE,
    full_name       VARCHAR(255) NOT NULL,
    role            user_role    NOT NULL DEFAULT 'engineer',
    status          user_status  NOT NULL DEFAULT 'active',
    password_hash   TEXT         NOT NULL,           -- PBKDF2 / argon2 hash
    invite_token    VARCHAR(128),
    last_login_at   TIMESTAMPTZ,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT now()
);
COMMENT ON TABLE  app_users IS 'Пользователи системы (Data Engineer, Admin)';
COMMENT ON COLUMN app_users.password_hash IS 'PBKDF2-SHA256 (390k iterations) — не хранит исходный пароль';
COMMENT ON COLUMN app_users.role IS 'Роль пользователя для авторизации';

CREATE INDEX idx_users_status ON app_users (status) WHERE status <> 'active';

-- =========================================================================
-- 2. ПОДКЛЮЧЕНИЯ К ВНЕШНИМ БД (нормализованы: пароли → отдельно)
-- =========================================================================

CREATE TABLE database_connections (
    id              UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    name            VARCHAR(255) NOT NULL,
    engine          db_engine    NOT NULL,
    host            VARCHAR(255) NOT NULL,
    port            INTEGER      NOT NULL CHECK (port BETWEEN 1 AND 65535),
    database_name   VARCHAR(255),
    username        VARCHAR(255),
    options_json    JSONB,                            -- engine-specific опции (произвольная схема)
    visibility      conn_visibility NOT NULL DEFAULT 'private',
    owner_user_id   UUID         REFERENCES app_users(id) ON DELETE CASCADE,
    status          conn_status  NOT NULL DEFAULT 'unknown',
    last_error      TEXT,
    last_tested_at  TIMESTAMPTZ,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),

    CONSTRAINT uq_conn_name_owner UNIQUE (name, owner_user_id),
    -- Shared соединения должны принадлежать суперпользователю (NULL owner запрещён для private)
    CONSTRAINT chk_private_has_owner CHECK (visibility = 'shared' OR owner_user_id IS NOT NULL)
);
COMMENT ON TABLE database_connections IS 'Подключения пользователей к внешним СУБД (1..N экземпляров)';
COMMENT ON COLUMN database_connections.visibility IS 'private = только владелец; shared = доступно всем';

CREATE INDEX idx_conn_engine     ON database_connections (engine);
CREATE INDEX idx_conn_owner      ON database_connections (owner_user_id);
CREATE INDEX idx_conn_visibility ON database_connections (visibility);


-- Секреты подключений вынесены в отдельную таблицу:
-- (1) можно версионировать ротацию пароля;
-- (2) можно ограничить SELECT-доступ к таблице с секретами через GRANT;
-- (3) убирает password из основной таблицы — снижает риск утечки в логи/дампы.
CREATE TABLE database_connection_secrets (
    id              BIGSERIAL    PRIMARY KEY,
    connection_id   UUID         NOT NULL REFERENCES database_connections(id) ON DELETE CASCADE,
    encrypted_value BYTEA        NOT NULL,
    algorithm       encryption_algorithm NOT NULL DEFAULT 'fernet',
    key_version     SMALLINT     NOT NULL DEFAULT 1,  -- для key rotation
    rotated_from_id BIGINT       REFERENCES database_connection_secrets(id) ON DELETE SET NULL,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    is_current      BOOLEAN      NOT NULL DEFAULT TRUE,

    CONSTRAINT uq_secret_current UNIQUE (connection_id, is_current) DEFERRABLE INITIALLY DEFERRED
);
COMMENT ON TABLE database_connection_secrets IS
    'Зашифрованные секреты подключений (пароли). Один current на connection_id, история ротаций сохраняется.';
COMMENT ON COLUMN database_connection_secrets.algorithm IS
    'Алгоритм шифрования. fernet = AES-128-CBC + HMAC-SHA256 (cryptography.Fernet)';

CREATE INDEX idx_secret_connection_current ON database_connection_secrets (connection_id) WHERE is_current;

-- =========================================================================
-- 3. ДИАЛОГ С АГЕНТОМ (нормализованный чат — основная сущность)
-- =========================================================================

CREATE TABLE agent_sessions (
    id              UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id         UUID         NOT NULL REFERENCES app_users(id) ON DELETE CASCADE,
    title           VARCHAR(255) NOT NULL DEFAULT 'New session',
    llm_provider    VARCHAR(64),                      -- 'magnitgpt' / 'openai' / 'openrouter'
    llm_model       VARCHAR(128),                     -- 'gpt-4o', 'qwen-2.5-coder', и т.п.
    archived_at     TIMESTAMPTZ,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT now()
);
COMMENT ON TABLE agent_sessions IS 'Сессия (тред) диалога между пользователем и агентом';

CREATE INDEX idx_sessions_user_created ON agent_sessions (user_id, created_at DESC);
CREATE INDEX idx_sessions_active        ON agent_sessions (user_id) WHERE archived_at IS NULL;


CREATE TABLE messages (
    id              UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    session_id      UUID         NOT NULL REFERENCES agent_sessions(id) ON DELETE CASCADE,
    sequence_index  INTEGER      NOT NULL,             -- порядковый номер в сессии (1, 2, 3, ...)
    role            message_role NOT NULL,
    content         TEXT         NOT NULL,
    -- token usage — отдельные числовые поля вместо подмножества metadata_json
    prompt_tokens       INTEGER  CHECK (prompt_tokens >= 0),
    completion_tokens   INTEGER  CHECK (completion_tokens >= 0),
    total_tokens        INTEGER  GENERATED ALWAYS AS (
        COALESCE(prompt_tokens, 0) + COALESCE(completion_tokens, 0)
    ) STORED,
    latency_ms          INTEGER  CHECK (latency_ms IS NULL OR latency_ms >= 0),
    finish_reason       VARCHAR(32),                   -- stop / tool_calls / length / content_filter
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),

    CONSTRAINT uq_message_seq UNIQUE (session_id, sequence_index)
);
COMMENT ON TABLE messages IS 'Сообщения в сессии (нормализованный chat history)';
COMMENT ON COLUMN messages.sequence_index IS 'Порядок в сессии — гарантирует упорядочивание без сортировки по created_at';
COMMENT ON COLUMN messages.total_tokens IS 'Вычисляется автоматически (GENERATED) — не нужно поддерживать вручную';

CREATE INDEX idx_messages_session ON messages (session_id, sequence_index);
CREATE INDEX idx_messages_role    ON messages (role);

-- =========================================================================
-- 4. ВЫЗОВЫ ИНСТРУМЕНТОВ АГЕНТА (tool calls)
-- =========================================================================

-- Справочник инструментов — нормализует tool_name (раньше повторялась строкой)
CREATE TABLE agent_tools (
    id              SMALLSERIAL  PRIMARY KEY,
    name            VARCHAR(64)  NOT NULL UNIQUE,
    description     TEXT,
    schema_json     JSONB,                              -- OpenAI function schema
    is_destructive  BOOLEAN      NOT NULL DEFAULT FALSE, -- для аудита write-операций
    introduced_at   TIMESTAMPTZ  NOT NULL DEFAULT now()
);
COMMENT ON TABLE agent_tools IS 'Каталог tool-функций агента (execute_sql, write_airflow_dag и т.д.)';


CREATE TABLE tool_runs (
    id              UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    session_id      UUID         NOT NULL REFERENCES agent_sessions(id) ON DELETE CASCADE,
    message_id      UUID         REFERENCES messages(id) ON DELETE SET NULL,
    tool_id         SMALLINT     NOT NULL REFERENCES agent_tools(id),
    status          tool_run_status NOT NULL DEFAULT 'success',
    -- input/output — JSONB т.к. схема каждого tool своя (валидная нормализация: атом для движка PG = весь объект)
    input_json      JSONB        NOT NULL,
    output_json     JSONB,
    error_message   TEXT,
    latency_ms      INTEGER      NOT NULL DEFAULT 0 CHECK (latency_ms >= 0),
    sequence_index  INTEGER      NOT NULL,              -- порядок tool-вызовов в рамках message
    started_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    finished_at     TIMESTAMPTZ
);
COMMENT ON TABLE tool_runs IS 'Каждый вызов инструмента — отдельная запись (1 message может иметь N tool_runs)';

CREATE INDEX idx_tool_runs_session ON tool_runs (session_id, started_at);
CREATE INDEX idx_tool_runs_tool    ON tool_runs (tool_id, started_at);
CREATE INDEX idx_tool_runs_message ON tool_runs (message_id);
CREATE INDEX idx_tool_runs_status_failed ON tool_runs (status) WHERE status = 'error';

-- =========================================================================
-- 5. UI-ACTIONS — также вынесены из metadata_json (нормализация)
-- =========================================================================

CREATE TABLE ui_actions (
    id              BIGSERIAL    PRIMARY KEY,
    message_id      UUID         NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
    action_type     VARCHAR(64)  NOT NULL,              -- 'navigate', 'highlight', 'open_pane'
    target_route    VARCHAR(255),                       -- '/sql', '/airflow/dags/foo', ...
    payload_json    JSONB,
    sequence_index  INTEGER      NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT now()
);
COMMENT ON TABLE ui_actions IS 'Действия UI, эмитированные агентом (раньше в messages.metadata_json)';

CREATE INDEX idx_ui_actions_message ON ui_actions (message_id, sequence_index);

-- =========================================================================
-- 6. ОРКЕСТРАЦИЯ: Airflow DAG runs + текущее состояние
-- =========================================================================

CREATE TABLE pipeline_runs (
    id              UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    dag_id          VARCHAR(255) NOT NULL,
    run_id          VARCHAR(255) NOT NULL,
    triggered_by_user_id UUID    REFERENCES app_users(id) ON DELETE SET NULL,
    triggered_by_session_id UUID REFERENCES agent_sessions(id) ON DELETE SET NULL,
    status          pipeline_status NOT NULL DEFAULT 'queued',
    conf_json       JSONB,
    external_url    TEXT,
    started_at      TIMESTAMPTZ,
    finished_at     TIMESTAMPTZ,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),

    CONSTRAINT uq_pipeline_dag_run UNIQUE (dag_id, run_id)
);
COMMENT ON TABLE pipeline_runs IS 'Запуски Airflow DAG, инициированные через агента';

CREATE INDEX idx_pipeline_runs_dag      ON pipeline_runs (dag_id, created_at DESC);
CREATE INDEX idx_pipeline_runs_session  ON pipeline_runs (triggered_by_session_id);
CREATE INDEX idx_pipeline_runs_user     ON pipeline_runs (triggered_by_user_id);


CREATE TABLE pipeline_states (
    dag_id          VARCHAR(255) PRIMARY KEY,
    is_paused       BOOLEAN      NOT NULL DEFAULT FALSE,
    source          VARCHAR(64)  NOT NULL DEFAULT 'local',  -- 'local' / 'airflow'
    last_synced_at  TIMESTAMPTZ,
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT now()
);
COMMENT ON TABLE pipeline_states IS 'Текущее состояние DAG (pause / source-of-truth)';

-- =========================================================================
-- 7. SPARK JOBS
-- =========================================================================

CREATE TABLE spark_jobs (
    id              UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id          VARCHAR(255) NOT NULL UNIQUE,
    name            VARCHAR(255) NOT NULL,
    submitted_by_user_id UUID    REFERENCES app_users(id) ON DELETE SET NULL,
    submitted_by_session_id UUID REFERENCES agent_sessions(id) ON DELETE SET NULL,
    status          spark_status NOT NULL DEFAULT 'submitted',
    app_resource    TEXT         NOT NULL,            -- путь к Spark-скрипту
    params_json     JSONB,
    result_sample_json JSONB,
    driver_log      TEXT,
    started_at      TIMESTAMPTZ,
    finished_at     TIMESTAMPTZ,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT now()
);
COMMENT ON TABLE spark_jobs IS 'Spark-задачи, отправленные через агента';

CREATE INDEX idx_spark_jobs_status ON spark_jobs (status);
CREATE INDEX idx_spark_jobs_user   ON spark_jobs (submitted_by_user_id, created_at DESC);

-- =========================================================================
-- 8. ВЕРСИОНИРОВАНИЕ АРТЕФАКТОВ (DAG / Spark scripts)
-- =========================================================================

-- Шапка артефакта (имя + тип + текущая версия) — отдельно от версий (3НФ)
CREATE TABLE artifacts (
    id              BIGSERIAL    PRIMARY KEY,
    artifact_type   artifact_type NOT NULL,
    name            VARCHAR(255) NOT NULL,
    owner_user_id   UUID         NOT NULL REFERENCES app_users(id) ON DELETE CASCADE,
    current_version_id BIGINT,                          -- FK добавим позже (циклическая зависимость)
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),

    CONSTRAINT uq_artifact_user_type_name UNIQUE (owner_user_id, artifact_type, name)
);
COMMENT ON TABLE artifacts IS 'Метаданные артефакта (DAG/Spark скрипт) пользователя';


CREATE TABLE artifact_versions (
    id              BIGSERIAL    PRIMARY KEY,
    artifact_id     BIGINT       NOT NULL REFERENCES artifacts(id) ON DELETE CASCADE,
    version         INTEGER      NOT NULL CHECK (version > 0),
    path            TEXT         NOT NULL,
    content_hash    VARCHAR(64)  NOT NULL,             -- sha256(content)
    commit_message  TEXT         NOT NULL DEFAULT '',
    author_user_id  UUID         REFERENCES app_users(id) ON DELETE SET NULL,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),

    CONSTRAINT uq_artifact_version UNIQUE (artifact_id, version)
);
COMMENT ON TABLE artifact_versions IS 'История версий артефакта (новая строка = новая версия)';

ALTER TABLE artifacts
    ADD CONSTRAINT fk_artifacts_current_version
    FOREIGN KEY (current_version_id) REFERENCES artifact_versions(id) ON DELETE SET NULL;

CREATE INDEX idx_artifact_versions_artifact ON artifact_versions (artifact_id, version DESC);
CREATE INDEX idx_artifact_versions_hash     ON artifact_versions (content_hash);


-- Валидация (sandbox check) — отдельная таблица, потому что одна версия может валидироваться несколько раз
CREATE TABLE artifact_validations (
    id              BIGSERIAL    PRIMARY KEY,
    version_id      BIGINT       NOT NULL REFERENCES artifact_versions(id) ON DELETE CASCADE,
    status          validation_status NOT NULL,
    output          TEXT,
    duration_ms     INTEGER      CHECK (duration_ms >= 0),
    sandbox_image   VARCHAR(128),                       -- 'apache/airflow:2.10.4' / 'python:3.12-slim'
    validated_at    TIMESTAMPTZ  NOT NULL DEFAULT now()
);
COMMENT ON TABLE artifact_validations IS 'История валидаций артефакта в Docker sandbox';

CREATE INDEX idx_artifact_validations_version ON artifact_validations (version_id, validated_at DESC);


-- Git-коммиты — отдельная таблица, т.к. версия может быть локальной (нет коммита) или иметь несколько коммитов при ребейзе
CREATE TABLE artifact_git_commits (
    id              BIGSERIAL    PRIMARY KEY,
    version_id      BIGINT       NOT NULL REFERENCES artifact_versions(id) ON DELETE CASCADE,
    repository      TEXT         NOT NULL,
    status          git_status   NOT NULL,
    commit_sha      VARCHAR(64)  NOT NULL,
    commit_short_sha VARCHAR(16) GENERATED ALWAYS AS (SUBSTRING(commit_sha, 1, 7)) STORED,
    error_message   TEXT,
    committed_at    TIMESTAMPTZ  NOT NULL DEFAULT now(),

    CONSTRAINT uq_git_commit UNIQUE (version_id, commit_sha)
);
COMMENT ON TABLE artifact_git_commits IS 'Git-коммиты, связанные с версиями артефактов';

CREATE INDEX idx_git_commits_sha ON artifact_git_commits (commit_sha);

-- =========================================================================
-- 9. ПРЕДСТАВЛЕНИЯ (views) — удобные срезы для UI и аналитики
-- =========================================================================

-- Полный chat history с количеством tool calls и UI actions на каждое сообщение
CREATE VIEW v_chat_history AS
SELECT
    m.id              AS message_id,
    m.session_id,
    s.user_id,
    u.email           AS user_email,
    m.sequence_index,
    m.role,
    m.content,
    m.prompt_tokens,
    m.completion_tokens,
    m.total_tokens,
    m.latency_ms,
    (SELECT COUNT(*) FROM tool_runs   tr WHERE tr.message_id = m.id) AS tool_call_count,
    (SELECT COUNT(*) FROM ui_actions  ua WHERE ua.message_id = m.id) AS ui_action_count,
    m.created_at
FROM messages m
JOIN agent_sessions s ON s.id = m.session_id
JOIN app_users     u ON u.id = s.user_id
ORDER BY m.session_id, m.sequence_index;

COMMENT ON VIEW v_chat_history IS 'Полная история чата с аггрегатами по tool_calls/ui_actions';


-- Текущие версии артефактов (склейка artifacts + последний artifact_versions)
CREATE VIEW v_artifact_current AS
SELECT
    a.id              AS artifact_id,
    a.artifact_type,
    a.name,
    a.owner_user_id,
    u.email           AS owner_email,
    av.id             AS version_id,
    av.version        AS version_number,
    av.path,
    av.content_hash,
    av.created_at     AS version_created_at,
    (SELECT MAX(commit_sha) FROM artifact_git_commits gc WHERE gc.version_id = av.id)  AS latest_commit_sha,
    (SELECT status    FROM artifact_validations vv WHERE vv.version_id = av.id ORDER BY vv.validated_at DESC LIMIT 1) AS last_validation
FROM artifacts a
JOIN app_users     u  ON u.id = a.owner_user_id
LEFT JOIN artifact_versions av ON av.id = a.current_version_id;

COMMENT ON VIEW v_artifact_current IS 'Текущая версия каждого артефакта со связанным git/validation';


-- Метрики использования агента (для дашборда — токены, латентность, ошибки)
CREATE VIEW v_session_metrics AS
SELECT
    s.id              AS session_id,
    s.user_id,
    s.title,
    s.llm_provider,
    s.llm_model,
    COUNT(m.id)                                          AS message_count,
    SUM(m.total_tokens)                                  AS tokens_used,
    AVG(m.latency_ms)::INTEGER                           AS avg_message_latency_ms,
    COUNT(DISTINCT tr.id)                                AS tool_runs_total,
    COUNT(DISTINCT tr.id) FILTER (WHERE tr.status='error') AS tool_runs_failed,
    MIN(m.created_at)                                    AS first_message_at,
    MAX(m.created_at)                                    AS last_message_at
FROM agent_sessions s
LEFT JOIN messages  m  ON m.session_id = s.id
LEFT JOIN tool_runs tr ON tr.session_id = s.id
GROUP BY s.id;

COMMENT ON VIEW v_session_metrics IS 'Агрегированные метрики по сессиям для дашборда';

-- =========================================================================
-- 10. ТРИГГЕРЫ updated_at
-- =========================================================================

-- Триггер для автоматического обновления updated_at
CREATE OR REPLACE FUNCTION touch_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_users_updated         BEFORE UPDATE ON app_users
    FOR EACH ROW EXECUTE FUNCTION touch_updated_at();
CREATE TRIGGER trg_connections_updated   BEFORE UPDATE ON database_connections
    FOR EACH ROW EXECUTE FUNCTION touch_updated_at();
CREATE TRIGGER trg_sessions_updated      BEFORE UPDATE ON agent_sessions
    FOR EACH ROW EXECUTE FUNCTION touch_updated_at();
CREATE TRIGGER trg_artifacts_updated     BEFORE UPDATE ON artifacts
    FOR EACH ROW EXECUTE FUNCTION touch_updated_at();
CREATE TRIGGER trg_pipeline_runs_updated BEFORE UPDATE ON pipeline_runs
    FOR EACH ROW EXECUTE FUNCTION touch_updated_at();
CREATE TRIGGER trg_pipeline_states_updated BEFORE UPDATE ON pipeline_states
    FOR EACH ROW EXECUTE FUNCTION touch_updated_at();
CREATE TRIGGER trg_spark_jobs_updated    BEFORE UPDATE ON spark_jobs
    FOR EACH ROW EXECUTE FUNCTION touch_updated_at();

-- =========================================================================
-- 11. ДЕМО-ДАННЫЕ (минимум для проверки в DBeaver)
-- =========================================================================

-- Базовые tool-функции
INSERT INTO agent_tools (name, description, is_destructive) VALUES
    ('execute_sql',                  'Read-only SQL execution',          FALSE),
    ('list_catalog',                 'Список таблиц и колонок',          FALSE),
    ('inspect_database',             'Полный осмотр БД',                 FALSE),
    ('trigger_airflow_dag',          'Запуск Airflow DAG',               TRUE),
    ('manage_airflow_dags',          'Pause/resume DAG',                 TRUE),
    ('submit_spark_job',             'Отправка Spark job',               TRUE),
    ('write_airflow_dag',            'Запись DAG-файла + commit',        TRUE),
    ('write_spark_script',           'Запись Spark-скрипта + commit',    TRUE),
    ('check_airflow_dag_sandbox',    'Sandbox-валидация DAG',            FALSE),
    ('run_spark_script_sandbox',     'Sandbox-запуск Spark скрипта',     FALSE),
    ('list_mcp_products',            'Список MCP-серверов',              FALSE),
    ('list_mcp_tools',               'Список tool-функций MCP-сервера',  FALSE),
    ('call_mcp_tool',                'Вызов внешнего MCP-инструмента',   TRUE);

-- Демо-пользователь
INSERT INTO app_users (id, email, full_name, role, status, password_hash) VALUES
    ('00000000-0000-0000-0000-000000000001', 'admin@local.dev', 'Admin User', 'admin', 'active',
     '$pbkdf2-sha256$390000$placeholder$placeholder');

-- Демо-сессия с историей
INSERT INTO agent_sessions (id, user_id, title, llm_provider, llm_model) VALUES
    ('aa000000-0000-0000-0000-000000000001',
     '00000000-0000-0000-0000-000000000001',
     'Анализ продаж за неделю',
     'openrouter', 'qwen-2.5-coder-32b');

INSERT INTO messages (session_id, sequence_index, role, content,
                      prompt_tokens, completion_tokens, latency_ms, finish_reason) VALUES
    ('aa000000-0000-0000-0000-000000000001', 1, 'user',
     'Покажи топ-10 заказов за последнюю неделю по сумме', NULL, NULL, NULL, NULL),
    ('aa000000-0000-0000-0000-000000000001', 2, 'assistant',
     'Запросил данные через execute_sql. Топ-10 заказов прикреплён ниже.',
     1240, 87, 2400, 'tool_calls'),
    ('aa000000-0000-0000-0000-000000000001', 3, 'tool',
     '[{"order_id": 1234, "amount": 99.50}, ...]', NULL, NULL, NULL, NULL),
    ('aa000000-0000-0000-0000-000000000001', 4, 'assistant',
     'Топ-10 заказов: ...', 1410, 120, 1900, 'stop');

-- Привязать tool_run к assistant-сообщению
WITH msg AS (
    SELECT id FROM messages
    WHERE session_id = 'aa000000-0000-0000-0000-000000000001' AND sequence_index = 2
)
INSERT INTO tool_runs (session_id, message_id, tool_id, status, input_json, output_json,
                       latency_ms, sequence_index, started_at, finished_at)
SELECT
    'aa000000-0000-0000-0000-000000000001',
    (SELECT id FROM msg),
    (SELECT id FROM agent_tools WHERE name = 'execute_sql'),
    'success',
    '{"query": "SELECT order_id, amount FROM sales.orders ORDER BY amount DESC LIMIT 10"}'::jsonb,
    '{"rows": 10, "elapsed_ms": 120}'::jsonb,
    120, 1, now() - interval '5 minutes', now() - interval '4 minutes';

INSERT INTO ui_actions (message_id, action_type, target_route, payload_json)
SELECT id, 'navigate', '/sql', '{"highlight": "orders"}'::jsonb
FROM messages
WHERE session_id = 'aa000000-0000-0000-0000-000000000001' AND sequence_index = 4;

COMMIT;

-- =========================================================================
-- ПРОВЕРКА: открой v_chat_history и v_session_metrics в DBeaver
-- =========================================================================
-- SELECT * FROM v_chat_history;
-- SELECT * FROM v_session_metrics;
-- SELECT * FROM v_artifact_current;
