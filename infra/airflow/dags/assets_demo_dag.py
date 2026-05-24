from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.assets import Asset
from datetime import datetime, timedelta

# Определение активов (Assets)
data_asset = Asset("s3://bucket/data/processed")
config_asset = Asset("s3://bucket/config/latest")

# DAG с демонстрацией Assets feature
# schedule может быть строкой (cron) или списком активов для asset-based scheduling
dag = DAG(
    dag_id="assets_demo_dag",
    default_args={
        "owner": "demo",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    description="Example DAG demonstrating Assets feature with conditional and asset expression-based scheduling",
    # Asset expression для условного планирования - DAG запускается при обновлении любого из активов
    schedule=[data_asset, config_asset],
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["assets", "demo", "scheduling"],
)

# Задача проверки наличия данных
check_data = BashOperator(
    task_id="check_data_asset",
    bash_command="echo 'Checking data asset' && echo 'Data asset is ready for processing'",
    dag=dag,
)

# Задача обработки конфигурации
process_config = BashOperator(
    task_id="process_config_asset",
    bash_command="echo 'Processing config asset' && echo 'Configuration loaded successfully'",
    dag=dag,
)

# Задача основной обработки с зависимостью от активов
main_processing = BashOperator(
    task_id="main_processing",
    bash_command="""
    echo 'Starting main processing task'
    echo 'Assets triggered this DAG run:'
    echo '- Data asset: s3://bucket/data/processed'
    echo '- Config asset: s3://bucket/config/latest'
    echo 'Processing completed successfully'
    """,
    dag=dag,
)

# Задача генерации отчета
generate_report = BashOperator(
    task_id="generate_report",
    bash_command="echo 'Generating daily report based on processed assets' && date",
    dag=dag,
)

# Задача очистки временных файлов
cleanup = BashOperator(
    task_id="cleanup",
    bash_command="echo 'Cleaning up temporary files' && echo 'Cleanup completed'",
    dag=dag,
)

# Определение зависимостей между задачами
check_data >> process_config >> main_processing >> generate_report >> cleanup

# Альтернативный путь для обработки ошибок
process_config >> cleanup
