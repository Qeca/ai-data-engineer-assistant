from airflow import DAG
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

dag = DAG(
    'slack_notification_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['slack', 'notification'],
)

task1 = SlackWebhookOperator(
    task_id='send_slack_message_1',
    slack_webhook_conn_id='slack_webhook_default',
    message='Привет! Это первое уведомление от Airflow DAG.',
    dag=dag,
)

task2 = SlackWebhookOperator(
    task_id='send_slack_message_2',
    slack_webhook_conn_id='slack_webhook_default',
    message='Второе уведомление: DAG успешно выполнен!',
    dag=dag,
)

task1 >> task2