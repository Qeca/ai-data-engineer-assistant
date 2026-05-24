from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import logging
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
    'mysql_retail_orders_to_clickhouse',
    default_args=default_args,
    description='Синхронизация новых заказов из MySQL retail_db.orders в ClickHouse analytics.orders каждые 15 минут',
    schedule_interval='*/15 * * * *',
    start_date=datetime(2026, 5, 24),
    catchup=False,
    tags=['sync', 'mysql', 'clickhouse', 'orders', 'retail'],
)

def get_last_run_ts(**context):
    """Получить last_run_ts из Airflow Variable"""
    last_ts = Variable.get('mysql_retail_orders_last_run_ts', default_var=None)
    if last_ts is None:
        # Если нет переменной, используем дату 30 дней назад
        last_ts = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d %H:%M:%S')
    logger.info(f"Last run timestamp: {last_ts}")
    return last_ts

def extract_and_load_orders(**context):
    """Извлечь новые заказы из MySQL retail_db.orders и загрузить в ClickHouse analytics.orders"""
    import subprocess
    
    last_ts = context['ti'].xcom_pull(task_ids='get_last_run_ts')
    
    # MySQL connection params (retail_db)
    mysql_host = os.environ.get('MYSQL_HOST', 'demo-mysql')
    mysql_port = os.environ.get('MYSQL_PORT', '3306')
    mysql_user = os.environ.get('MYSQL_USER', 'demo')
    mysql_password = os.environ.get('MYSQL_PASSWORD', 'demo')
    mysql_db = 'retail_db'
    
    # ClickHouse connection params (analytics)
    ch_host = os.environ.get('CLICKHOUSE_HOST', 'demo-clickhouse')
    ch_port = os.environ.get('CLICKHOUSE_PORT', '9000')
    ch_user = os.environ.get('CLICKHOUSE_USER', 'demo')
    ch_password = os.environ.get('CLICKHOUSE_PASSWORD', 'demo')
    ch_db = 'analytics'
    
    # Извлекаем данные из MySQL retail_db.orders где created_at > last_run_ts
    mysql_query = f"""
    SELECT order_id, customer_id, created_at, amount, status
    FROM orders
    WHERE created_at > '{last_ts}'
    ORDER BY created_at
    """
    
    mysql_cmd = [
        'mysql',
        '-h', mysql_host,
        '-P', mysql_port,
        '-u', mysql_user,
        f'-p{mysql_password}',
        mysql_db,
        '-N',  # No column names
        '-B',  # Batch mode (tab-separated)
        '-e', mysql_query
    ]
    
    logger.info(f"Executing MySQL query: {mysql_query}")
    result = subprocess.run(mysql_cmd, capture_output=True, text=True)
    
    if result.returncode != 0:
        logger.error(f"MySQL error: {result.stderr}")
        raise Exception(f"MySQL query failed: {result.stderr}")
    
    lines = result.stdout.strip().split('\n') if result.stdout.strip() else []
    logger.info(f"Extracted {len(lines)} new orders from MySQL retail_db.orders")
    
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
    
    # Создаем VALUES для ClickHouse INSERT
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
        '--port', ch_port,
        '--user', ch_user,
        '--password', ch_password,
        '--database', ch_db,
        '--query', ch_insert_query
    ]
    
    logger.info(f"Executing ClickHouse insert into analytics.orders")
    ch_result = subprocess.run(ch_cmd, capture_output=True, text=True)
    
    if ch_result.returncode != 0:
        logger.error(f"ClickHouse error: {ch_result.stderr}")
        raise Exception(f"ClickHouse insert failed: {ch_result.stderr}")
    
    # Обновляем last_run_ts в Airflow Variable
    if max_ts:
        Variable.set('mysql_retail_orders_last_run_ts', max_ts)
        logger.info(f"Updated last_run_ts to: {max_ts}")
    
    logger.info(f"Loaded {len(orders)} orders to ClickHouse analytics.orders")
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
