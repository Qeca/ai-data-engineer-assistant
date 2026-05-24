# Sources & Provenance

All real DAG and Spark reference artifacts in this dataset are taken
verbatim from public open-source repositories. SHAs are pinned for
reproducibility.

## D3 — Airflow DAGs

All 10 DAGs come from **apache/airflow** @ commit `ea7481d7d59b0eb129f8b39c848a24aa111e7ca3`
(snapshot 2026-05-24). License: **Apache-2.0**. Original
repository: https://github.com/apache/airflow

| File | Source path in apache/airflow |
|---|---|
| 01_example_postgres.py | providers/postgres/tests/system/postgres/example_postgres.py |
| 02_example_s3_to_sql.py | providers/amazon/tests/system/amazon/aws/example_s3_to_sql.py |
| 03_example_http.py | providers/http/tests/system/http/example_http.py |
| 04_example_complex.py | airflow-core/src/airflow/example_dags/example_complex.py |
| 05_tutorial.py | airflow-core/src/airflow/example_dags/tutorial.py |
| 06_example_slack_webhook.py | providers/slack/tests/system/slack/example_slack_webhook.py |
| 07_example_spark_dag.py | providers/apache/spark/tests/system/apache/spark/example_spark_dag.py |
| 08_example_dynamic_task_mapping.py | airflow-core/src/airflow/example_dags/example_dynamic_task_mapping.py |
| 09_tutorial_taskflow_api.py | airflow-core/src/airflow/example_dags/tutorial_taskflow_api.py |
| 10_example_simplest_dag.py | airflow-core/src/airflow/example_dags/example_simplest_dag.py |

## D4 — PySpark scripts

Two sources:

**dotnet/spark** (Microsoft / .NET Foundation) @ commit `dabe85b685886901da9707f728da1974a33d44e7`
(2026-05-14). License: **MIT**. Original repository:
https://github.com/dotnet/spark

| File | Source path |
|---|---|
| 01_tpch_functional_queries.py | benchmark/python/tpch_functional_queries.py |
| 02_tpch_sql_queries.py | benchmark/python/tpch_sql_queries.py |
| 03_tpch_base.py | benchmark/python/tpch_base.py |
| 04_tpch_runner.py | benchmark/python/tpch.py |

**apache/spark** @ commit `b2c2a8d68dcbbaca715adc74c0dd543582c9ff02`
(snapshot 2026-05-23). License: **Apache-2.0**. Original repository:
https://github.com/apache/spark

| File | Source path |
|---|---|
| 05_wordcount.py | examples/src/main/python/wordcount.py |
| 06_sort.py | examples/src/main/python/sort.py |
| 07_sql_basic.py | examples/src/main/python/sql/basic.py |
| 08_sql_jdbc.py | examples/src/main/python/sql/jdbc.py |
| 09_structured_kafka_streaming.py | examples/src/main/python/sql/streaming/structured_kafka_wordcount.py |
| 10_structured_sessionization.py | examples/src/main/python/sql/streaming/structured_sessionization.py |

## Other datasets

- **D2.1 BIRD financial**: https://github.com/niklaswretblad/the-effects-of-noise-in-text-to-SQL (corrected). License: CC BY-SA 4.0.
- **D2.2 Chinook**: https://github.com/lerocha/chinook-database @ `7f67772503d71ba90f19283c38e93923addb43fa`. License: MIT.
- **D2.3 Sakila**: https://github.com/jOOQ/sakila @ `e089a5b1ec9af0df7a9c6a5d47d49fa1736a4e84`. License: BSD-New.
- **TPC-H specification**: https://www.tpc.org/tpch/

All artifacts are redistributable under their respective open-source
licenses. Modifications are limited to numbering prefixes (e.g. `01_…`)
to align with diploma categories D3-01…D3-10 / D4-01…D4-10.

## D7_extended — Prompt injection (расширенный корпус)

Импортирован публичный датасет **deepset/prompt-injections** для повышения
статистической значимости метрики M-16. Источник:

- HuggingFace: https://huggingface.co/datasets/deepset/prompt-injections
- License: CC BY 4.0
- Download date: 2026-05-24
- Splits: test (116 rows) + train (546 rows) = 662 rows total
- Filter: 5 < len(text) < 2000 → 261 attacks + 399 benign
- Используются как D7_extended_attacks.csv / D7_extended_benign.csv
- Сэмплинг для прогона: 100 атак + 50 benign (seed=42)

