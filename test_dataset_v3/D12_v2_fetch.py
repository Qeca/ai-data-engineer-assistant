"""Скачивание 30+ real-world DAG'ов из apache/airflow с пинированным коммитом.
Все DAG распространяются под Apache-2.0; SHA пинируется в SOURCES.md.
"""

import json
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent
OUT = ROOT / "D12_real_dags"
OUT.mkdir(exist_ok=True)

# pinned commit, same as D3 batch (apache/airflow @ ea7481d7d59b0eb129f8b39c848a24aa111e7ca3)
SHA = "ea7481d7d59b0eb129f8b39c848a24aa111e7ca3"
BASE = f"https://raw.githubusercontent.com/apache/airflow/{SHA}/"

# 30 сложных multi-step DAG из официальных example_dags и providers/system.
DAGS = [
    # ---- airflow-core example_dags ----
    "airflow-core/src/airflow/example_dags/example_complex.py",
    "airflow-core/src/airflow/example_dags/example_task_group.py",
    "airflow-core/src/airflow/example_dags/example_task_group_decorator.py",
    "airflow-core/src/airflow/example_dags/example_dynamic_task_mapping.py",
    "airflow-core/src/airflow/example_dags/example_nested_branch_dag.py",
    "airflow-core/src/airflow/example_dags/example_setup_teardown.py",
    "airflow-core/src/airflow/example_dags/example_setup_teardown_taskflow.py",
    "airflow-core/src/airflow/example_dags/example_branch_labels.py",
    "airflow-core/src/airflow/example_dags/example_branch_python_dop_operator_3.py",
    "airflow-core/src/airflow/example_dags/example_trigger_target_dag.py",
    "airflow-core/src/airflow/example_dags/example_xcom.py",
    "airflow-core/src/airflow/example_dags/example_xcomargs.py",
    "airflow-core/src/airflow/example_dags/tutorial.py",
    "airflow-core/src/airflow/example_dags/tutorial_dag.py",
    "airflow-core/src/airflow/example_dags/tutorial_taskflow_api.py",
    "airflow-core/src/airflow/example_dags/tutorial_taskflow_templates.py",
    "airflow-core/src/airflow/example_dags/example_dag_decorator.py",
    "airflow-core/src/airflow/example_dags/example_dynamic_task_mapping_with_no_taskflow_operators.py",
    "airflow-core/src/airflow/example_dags/example_assets.py",
    "airflow-core/src/airflow/example_dags/example_asset_decorator.py",
    # ---- providers/system: realistic ETL DAGs touching external systems ----
    "providers/postgres/tests/system/postgres/example_postgres.py",
    "providers/amazon/tests/system/amazon/aws/example_s3_to_sql.py",
    "providers/http/tests/system/http/example_http.py",
    "providers/slack/tests/system/slack/example_slack_webhook.py",
    "providers/apache/spark/tests/system/apache/spark/example_spark_dag.py",
    "providers/google/tests/system/google/cloud/bigquery/example_bigquery_queries.py",
    "providers/google/tests/system/google/cloud/gcs/example_gcs_to_bigquery.py",
    "providers/google/tests/system/google/cloud/bigquery/example_bigquery_tables.py",
    "providers/microsoft/azure/tests/system/azure/example_local_to_wasb.py",
    "providers/snowflake/tests/system/snowflake/example_snowflake.py",
    "providers/databricks/tests/system/databricks/example_databricks.py",
    "providers/sftp/tests/system/sftp/example_sftp.py",
    "providers/mysql/tests/system/mysql/example_mysql.py",
    "providers/apache/kafka/tests/system/apache/kafka/example_dag_event_listener.py",
    "providers/redis/tests/system/redis/example_redis_publish.py",
]


def fetch(path: str) -> tuple[str, str | None]:
    name = Path(path).name
    out_path = OUT / f"{len(list(OUT.glob('*.py'))) + 1:02d}_{name}"
    url = BASE + path
    try:
        with urllib.request.urlopen(url, timeout=20) as r:
            data = r.read().decode("utf-8", "replace")
        out_path.write_text(data, encoding="utf-8")
        return name, str(out_path.relative_to(ROOT))
    except Exception as e:
        return name, f"ERROR: {e}"


def main() -> None:
    log: list[dict] = []
    for path in DAGS:
        name, result = fetch(path)
        log.append({"source_path": path, "local": result})
        print(f"  {name:60s} → {result}")
    (OUT / "manifest.json").write_text(json.dumps({"sha": SHA, "files": log},
                                                  ensure_ascii=False, indent=2))
    print(f"\nFetched {sum(1 for x in log if not str(x['local']).startswith('ERROR'))}/{len(DAGS)} into {OUT}")


if __name__ == "__main__":
    main()
