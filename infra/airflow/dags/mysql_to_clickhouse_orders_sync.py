from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta
import logging
import json
import os

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'mysql_to_clickhouse_orders_sync',
    default_args=default_args,
    description='Синхронизация новых заказов из MySQL retail_db.orders в ClickHouse analytics.orders каждые 15 минут',
    schedule_interval='*/15 * * * *',
    start_date=datetime(2026, 5, 24),
    catchup=False,
    tags=['sync', 'mysql', 'clickhouse', 'orders'],
)

def get_last_run_ts(**context):
    """Получить last_run_ts из Airflow Variable"""
    last_ts = Variable.get('mysql_orders_last_run_ts', default_var=None)
    if last_ts is None:
        # Если нет переменной, используем дату 30 дней назад
        last_ts = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d %H:%M:%S')
    logger.info(f"Last run timestamp: {last_ts}")
    return last_ts

def extract_and_load_orders(**context):
    """Извлечь новые заказы из MySQL и загрузить в ClickHouse"""
    import subprocess
    import json
    
    last_ts = context['ti'].xcom_pull(task_ids='get_last_run_ts')
    
    # FIX: Используем Airflow Connection вместо переменных окружения
    # Connection ID: mysql_retail_db (проверено, статус: online)
    mysql_conn = BaseHook.get_connection('mysql_retail_db')
    
    mysql_host = mysql_conn.host or 'demo-mysql'
    mysql_port = mysql_conn.port or 3306
    mysql_user = mysql_conn.login or 'demo'
    mysql_password = mysql_conn.password or 'demo'
    mysql_db = mysql_conn.schema or 'retail_db'
    
    # ClickHouse connection params (используем существующее подключение)
    ch_conn = BaseHook.get_connection('clickhouse_analytics')
    
    ch_host = ch_conn.host or 'demo-clickhouse'
    ch_port = ch_conn.port or 8123
    ch_user = ch_conn.login or 'demo'
    ch_password = ch_conn.password or 'demo'
    ch_db = ch_conn.schema or 'analytics'
    
    # Извлекаем данные из MySQL
    mysql_query = f"""
    SELECT order_id, customer_id, created_at, amount, status
    FROM orders
    WHERE created_at > '{last_ts}'
    ORDER BY created_at
    """
    
    mysql_cmd = [
        'mysql',
        '-h', mysql_host,
        '-P', str(mysql_port),
        '-u', mysql_user,
        f'-p{mysql_password}',
        mysql_db,
        '-N',  # No column names
        '-B',  # Batch mode (tab-separated)
        '-e', mysql_query
    ]
    
    logger.info(f"Executing MySQL query: {mysql_query}")
    logger.info(f"Using connection: mysql_retail_db (host={mysql_host}, db={mysql_db})")
    result = subprocess.run(mysql_cmd, capture_output=True, text=True)
    
    if result.returncode != 0:
        error_msg = result.stderr.strip()
        logger.error(f"MySQL error: {error_msg}")
        # Добавляем понятное сообщение об ошибке
        if 'Access denied' in error_msg:
            raise Exception(f"MySQL access denied: проверьте права пользователя {mysql_user} на базу {mysql_db}. Ошибка: {error_msg}")
        raise Exception(f"MySQL query failed: {error_msg}")
    
    lines = result.stdout.strip().split('\n') if result.stdout.strip() else []
    logger.info(f"Extracted {len(lines)} new orders from MySQL")
    
    if not lines:
        logger.info("No new orders to load")
        return 0
    
    # Парсим данные и готовим для ClickHouse
    orders = []
    max_ts = None
    for line in lines:
        parts = line.split('\t')
        if len(parts) >= 5:
            order_id, customer_id, created_at, amount, status = parts[:5]
            orders.append({
                'order_id': order_id,
                'customer_id': customer_id,
                'created_at': created_at,
                'amount': amount,
                'status': status
            })
            if max_ts is None or created_at > max_ts:
                max_ts = created_at
    
    if not orders:
        return 0
    
    # Создаем VALUES для ClickHouse
    values = []
    for order in orders:
        values.append(f"({order['order_id']}, {order['customer_id']}, '{order['created_at']}', {order['amount']}, '{order['status']}')")
    
    ch_insert_query = f"""
    INSERT INTO orders (order_id, customer_id, created_at, amount, status)
    VALUES {', '.join(values)}
    """
    
    # Используем clickhouse-client для вставки
    ch_cmd = [
        'clickhouse-client',
        '--host', ch_host,
        '--port', str(ch_port),
        '--user', ch_user,
        '--password', ch_password,
        '--database', ch_db,
        '--query', ch_insert_query
    ]
    
    logger.info(f"Executing ClickHouse insert")
    ch_result = subprocess.run(ch_cmd, capture_output=True, text=True)
    
    if ch_result.returncode != 0:
        logger.error(f"ClickHouse error: {ch_result.stderr}")
        raise Exception(f"ClickHouse insert failed: {ch_result.stderr}")
    
    # Обновляем last_run_ts
    if max_ts:
        Variable.set('mysql_orders_last_run_ts', max_ts)
        logger.info(f"Updated last_run_ts to: {max_ts}")
    
    logger.info(f"Loaded {len(orders)} orders to ClickHouse")
    return len(orders)

get_last_run_ts_task = PythonOperator(
    task_id='get_last_run_ts',
    python_callable=get_last_run_ts,
    dag=dag,
)

extract_and_load_task = PythonOperator(
    task_id='extract_and_load_orders',
    python_callable=extract_and_load_orders,
    dag=dag,
)

get_last_run_ts_task >> extract_and_load_task
