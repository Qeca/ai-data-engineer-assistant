# Тестовый датасет ВКР v3 — реальный код из открытых репозиториев

Главное отличие от v1/v2: **все 20 эталонных артефактов (10 DAG + 10 Spark)
взяты verbatim из публичных open-source проектов**, а не написаны автором ВКР.
Это снимает претензию к субъективности эталонов и переводит валидацию
генерации агента в категорию сравнения с production-кодом Apache Software
Foundation и Microsoft.

## Структура

```
test_dataset_v3/
├── README.md                                Этот файл
├── SOURCES.md                               Полный provenance с SHA коммитов и лицензиями
│
├── Категории тест-кейсов:
├── D1_intent_classifier.csv                 43 кейса (полный — 390)
├── D2_1_bird_financial.csv                  106 NL→SQL пар (BIRD-Bench)
├── D2_2_chinook_music.csv                   20 NL→SQL пар (Chinook)
├── D2_3_sakila_rental.csv                   15 NL→SQL пар (Sakila)
├── D2_4_pg_demo_sales.csv                   7 NL→SQL пар
├── D2_5_mysql_retail.csv                    6 NL→SQL пар
├── D2_6_clickhouse_events.csv               7 NL→SQL пар
├── D2_7_mongo_aggregation.json              6 NL→pipeline пар
├── D3_airflow_dags.json                     ★ Спецификации, привязанные к РЕАЛЬНОМУ коду
├── D4_pyspark_tasks.json                    ★ Спецификации, привязанные к РЕАЛЬНОМУ коду
├── D5_mcp_discovery.json                    10 MCP сценариев
├── D6_sandbox_corpus.csv                    20 sandbox семплов
├── D7_prompt_injection.csv                  15 атак
├── D8_session_continuity.json               10 многоходовых сценариев
├── D9_edge_cases.csv                        15 граничных случаев
├── D10_connection_management.csv            30 кейсов CRUD подключений
├── D11_bash_git_security.csv                40 кейсов sandbox + git
│
├── Реальные базы данных:
├── financial.sqlite                         BIRD-Bench (PKDD'99, 1M+ транзакций)
├── chinook.sqlite                           Цифровой магазин музыки
├── sakila.sqlite                            Прокат DVD (MySQL sample)
├── bird_financial_schema/                   Описания таблиц BIRD
│
├── ★ РЕАЛЬНЫЙ КОД из open-source:
├── D3_real_airflow_dags/                    10 DAG из apache/airflow
│   ├── 01_example_postgres.py
│   ├── 02_example_s3_to_sql.py
│   ├── 03_example_http.py
│   ├── 04_example_complex.py
│   ├── 05_tutorial.py
│   ├── 06_example_slack_webhook.py
│   ├── 07_example_spark_dag.py
│   ├── 08_example_dynamic_task_mapping.py
│   ├── 09_tutorial_taskflow_api.py
│   └── 10_example_simplest_dag.py
│
├── D4_real_spark_scripts/                   10 скриптов из dotnet/spark + apache/spark
│   ├── 01_tpch_functional_queries.py        ← Microsoft TPC-H functional (22 queries)
│   ├── 02_tpch_sql_queries.py               ← Microsoft TPC-H SQL (22 queries)
│   ├── 03_tpch_base.py                      ← Microsoft TPC-H base
│   ├── 04_tpch_runner.py                    ← Microsoft TPC-H driver
│   ├── 05_wordcount.py                      ← Apache Spark Python examples
│   ├── 06_sort.py
│   ├── 07_sql_basic.py
│   ├── 08_sql_jdbc.py
│   ├── 09_structured_kafka_streaming.py
│   └── 10_structured_sessionization.py
│
├── D4_spark_test_data/                      Тестовые Parquet/CSV для smoke
│   └── (sales, clickstream, orders, customers, products, events, lineitem, …)
│
└── D3_D4_validators/                        Инструменты валидации
    ├── invariants.py                        Regex/AST предикаты (legacy)
    ├── run_real_validation.py               Запуск regex-валидатора
    ├── llm_judge.py                         ★ LLM-as-a-Judge (главный)
    └── judge_report_dry_run.json            Пример отчёта (без LLM)
```

## Главный результат — LLM-as-a-Judge

Валидация генерации DAG/Spark теперь основана на методе LLM-as-a-Judge
(Zheng L. et al., NeurIPS 2023, MT-Bench). Regex-валидатор оставлен как
быстрый sanity-check для CI, но **основные метрики M-07 и M-08
рассчитываются именно через LLM-судью**.

```bash
# Без codex CLI — dry-run с mock судьями (для документации)
python3 D3_D4_validators/llm_judge.py

# Боевой запуск через OpenAI Codex CLI (`codex exec`)
npm install -g @openai/codex   # установка CLI (один раз)
codex login                    # OAuth-логин или OPENAI_API_KEY
python3 D3_D4_validators/llm_judge.py --live
```

### ★ Судья — `codex exec` (OpenAI Codex CLI)

Валидация выполняется через subcommand **`codex exec`** официального
[OpenAI Codex CLI](https://developers.openai.com/codex/cli). Это
non-interactive режим Codex-агента: на stdin промпт, на stdout — JSON c
рубрикой. Преимущества:

- **Никакого HTTP-клиента** — `subprocess.run(["codex", "exec", prompt])`
- **Аутентификация по аккаунту** — через `~/.codex/auth.json`, общая с
  обычным интерактивным Codex CLI у разработчика
- **Sandbox `read-only`** — судья не может изменить файлы проекта
- **Модель** — `gpt-5-codex`, оптимизированная для code review через
  RL-обучение на реальных pull-request данных открытых репозиториев

Полный путь оценки (один codex exec на пару):

```
codex exec --model gpt-5-codex --sandbox read-only \
    --skip-git-repo-check  "<prompt>"
        ↓ stdout
    {"semantic": 9, "api": 9, "robustness": 8, "style": 8,
     "verdict": "accept", "critique": "uses canonical PostgresHook..."}
```

### Рубрика судьи (4 аспекта × 0-10)

| Аспект | Вес | Что оценивается |
|---|---|---|
| `semantic` | 0.45 | Достигает ли код заявленной бизнес-цели |
| `api` | 0.30 | Корректные ли API Airflow/Spark использованы |
| `robustness` | 0.15 | Retries, error handling, идемпотентность |
| `style` | 0.10 | Именование, докстринги, структура |
| `verdict` | — | accept / accept_with_minors / reject |
| `critique` | — | 1–3 предложения конкретной аргументации |

```
overall = 0.45·semantic + 0.30·api + 0.15·robustness + 0.10·style
```

### Целевые значения метрик

| Метрика | Цель | Описание |
|---|---|---|
| M-07 | mean_overall ≥ 7.5 | Качество DAG (10 артефактов D3) |
| M-08 | mean_overall ≥ 7.0 | Качество Spark (10 артефактов D4) |
| non_reject_rate | ≥ 0.90 | Доля артефактов без вердикта «reject» |

### Стоимость прогона

При запуске через `codex exec` стоимость определяется тарифом OpenAI
Codex (одинаковый с GPT-5). Полный прогон D3+D4 = 20 артефактов
расходует ~50K input + ~6K output токенов, что на момент написания
работы составляет ≈ $0.75–1.50.

## Legacy regex-валидатор (для CI / smoke)

## Legacy regex-валидатор (для CI / smoke)

`run_real_validation.py` оставлен как быстрый pre-commit check. На эталонной
выборке показывает 71/75 (94.7%) — этого достаточно для blocking-проверки
синтаксиса и наличия ключевых API вызовов перед более дорогой LLM-оценкой:

```
D3 — apache/airflow DAGs:           38/39 (97.4%)
D4 — apache/spark + dotnet/spark:   33/36 (91.7%)
Overall:                            71/75 (94.7%)
```

## Почему это важно для защиты

| Аргумент комиссии | Ответ |
|---|---|
| «Эталоны субъективны» | Файлы — verbatim из apache/airflow и apache/spark, SHA коммитов в SOURCES.md, лицензии Apache-2.0 и MIT |
| «Откуда такие инварианты?» | Они отражают принятые в этих фреймворках паттерны: `@dag`/`@task` (modern TaskFlow API), `SparkSession.builder` (стандартный entrypoint), `readStream`/`writeStream` (Structured Streaming) |
| «Почему именно эти DAG-и?» | Они официальные example DAG из репозитория apache/airflow — то же самое запускает любой `astro dev start` |
| «Почему TPC-H именно от Microsoft?» | dotnet/spark — единственный полный PySpark TPC-H, поддерживаемый одной из крупнейших технологических компаний; MIT-лицензия, public benchmark |

## Спецификации D3/D4 (новые)

В `D3_airflow_dags.json` и `D4_pyspark_tasks.json` каждая спецификация
теперь содержит:

```json
{
  "id": "DAG-01",
  "real_source_file": "01_example_postgres.py",
  "source_repo": "apache/airflow",
  "source_path": "providers/postgres/tests/system/postgres/example_postgres.py",
  "source_commit": "ea7481d7d59b0eb129f8b39c848a24aa111e7ca3",
  "source_license": "Apache-2.0",
  "spec": "ETL pipeline that creates a PostgreSQL table…",
  "invariants": [...]
}
```

Это обеспечивает воспроизводимость: можно в любой момент проверить,
что код соответствует указанному коммиту.

## Метод оценки execution accuracy (BIRD-EX) для D2.x

```python
import sqlite3

def execution_accuracy(gold_sql: str, predicted_sql: str, db_path: str) -> bool:
    con = sqlite3.connect(db_path)
    try:
        gold = sorted(con.execute(gold_sql).fetchall())
        pred = sorted(con.execute(predicted_sql).fetchall())
        return gold == pred
    finally:
        con.close()
```

Валидация gold SQL на реальных БД (выполнено локально):
- BIRD financial: 106/106 ✓
- Chinook: 20/20 ✓
- Sakila: 15/15 ✓

## Лицензионная совместимость

Все артефакты в этом архиве распространяются под совместимыми
open-source лицензиями: Apache-2.0, MIT, BSD-New, CC BY-SA 4.0. Полный
список — в файле SOURCES.md.
