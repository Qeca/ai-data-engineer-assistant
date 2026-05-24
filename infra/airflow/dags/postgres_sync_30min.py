from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import psycopg2
import json

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'postgres_sync_30min',
    default_args=default_args,
    description='Синхронизация данных с PostgreSQL каждые 30 минут',
    schedule_interval='*/30 * * * *',
    start_date=datetime(2026, 5, 24),
    catchup=False,
    tags=['sync', 'postgresql', 'etl'],
)

def extract_customers(**context):
    """Извлечение данных из таблицы customers"""
    conn = psycopg2.connect(
        host='demo-postgres',
        port=5432,
        database='analytics',
        user='demo',
        password='demo'
    )
    cur = conn.cursor()
    cur.execute("SELECT customer_id, email, segment, created_at FROM sales.customers")
    rows = cur.fetchall()
    columns = [desc[0] for desc in cur.description]
    data = [dict(zip(columns, row)) for row in rows]
    cur.close()
    conn.close()
    
    # Сохраняем в XCom для последующих задач
    context['ti'].xcom_push(key='customers', value=json.dumps(data, default=str))
    print(f"Извлечено {len(data)} записей customers")
    return len(data)

def extract_orders(**context):
    """Извлечение данных из таблицы orders"""
    conn = psycopg2.connect(
        host='demo-postgres',
        port=5432,
        database='analytics',
        user='demo',
        password='demo'
    )
    cur = conn.cursor()
    cur.execute("SELECT order_id, customer_id, order_ts, amount, status FROM sales.orders")
    rows = cur.fetchall()
    columns = [desc[0] for desc in cur.description]
    data = [dict(zip(columns, row)) for row in rows]
    cur.close()
    conn.close()
    
    # Сохраняем в XCom для последующих задач
    context['ti'].xcom_push(key='orders', value=json.dumps(data, default=str))
    print(f"Извлечено {len(data)} записей orders")
    return len(data)

def transform_data(**context):
    """Трансформация данных: валидация и обогащение"""
    ti = context['ti']
    customers_json = ti.xcom_pull(key='customers', task_ids='extract_customers')
    orders_json = ti.xcom_pull(key='orders', task_ids='extract_orders')
    
    customers = json.loads(customers_json) if customers_json else []
    orders = json.loads(orders_json) if orders_json else []
    
    # Создаем маппинг customer_id -> segment
    customer_segments = {c['customer_id']: c['segment'] for c in customers}
    
    # Обогащаем заказы информацией о сегменте клиента
    enriched_orders = []
    for order in orders:
        enriched_order = order.copy()
        enriched_order['customer_segment'] = customer_segments.get(order['customer_id'], 'unknown')
        enriched_orders.append(enriched_order)
    
    ti.xcom_push(key='enriched_orders', value=json.dumps(enriched_orders, default=str))
    print(f"Обогащено {len(enriched_orders)} заказов")
    return {'customers': len(customers), 'orders': len(enriched_orders)}

def load_data(**context):
    """Загрузка синхронизированных данных (логирование)"""
    ti = context['ti']
    customers_json = ti.xcom_pull(key='customers', task_ids='extract_customers')
    enriched_orders_json = ti.xcom_pull(key='enriched_orders', task_ids='transform_data')
    
    customers = json.loads(customers_json) if customers_json else []
    orders = json.loads(enriched_orders_json) if enriched_orders_json else []
    
    # Здесь можно добавить логику загрузки в целевую систему
    # Например, в data warehouse, S3, или другую БД
    print(f"Синхронизация завершена:")
    print(f"  - Customers: {len(customers)} записей")
    print(f"  - Orders: {len(orders)} записей (обогащенных)")
    
    return {'status': 'success', 'customers_count': len(customers), 'orders_count': len(orders)}

extract_customers_task = PythonOperator(
    task_id='extract_customers',
    python_callable=extract_customers,
    dag=dag,
)

extract_orders_task = PythonOperator(
    task_id='extract_orders',
    python_callable=extract_orders,
    dag=dag,
)

transform_data_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)

load_data_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    dag=dag,
)

# Определение зависимостей
[extract_customers_task, extract_orders_task] >> transform_data_task >> load_data_task
