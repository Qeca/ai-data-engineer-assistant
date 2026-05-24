from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='daily_top_customers_russia_report',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    description='Ежедневный отчёт: топ-10 клиентов по выручке из России',
)

def get_top_customers_russia(**context):
    """Выполняет SQL-запрос и логирует результаты"""
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    
    # Примечание: для фильтрации по стране необходима колонка country в таблице customers
    # Если колонка существует, раскомментируйте WHERE c.country = 'Russia'
    query = """
    SELECT 
        c.customer_id, 
        c.email, 
        c.segment,
        c.country,
        SUM(o.amount) as total_revenue, 
        COUNT(o.order_id) as order_count
    FROM sales.customers c
    JOIN sales.orders o ON c.customer_id = o.customer_id
    WHERE o.status = 'paid'
    -- AND c.country = 'Russia'  -- Раскомментировать при наличии колонки country
    GROUP BY c.customer_id, c.email, c.segment, c.country
    ORDER BY total_revenue DESC
    LIMIT 10
    """
    
    hook = PostgresHook(postgres_conn_id='default')
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    cursor.close()
    conn.close()
    
    # Логирование результатов
    ti = context['ti']
    ti.xcom_push(key='top_customers_russia', value={'columns': columns, 'data': results})
    
    print("=" * 100)
    print("ТОП-10 КЛИЕНТОВ ПО ВЫРУЧКЕ (РОССИЯ)")
    print("=" * 100)
    print(f"{'customer_id':<12} {'email':<25} {'segment':<10} {'country':<15} {'total_revenue':<15} {'order_count':<12}")
    print("-" * 100)
    for row in results:
        country = row[3] if row[3] else 'N/A'
        print(f"{row[0]:<12} {row[1]:<25} {row[2]:<10} {country:<15} {float(row[4]):<15.2f} {row[5]:<12}")
    print("=" * 100)
    
    return {'columns': columns, 'data': results}

report_task = PythonOperator(
    task_id='generate_top_customers_russia_report',
    python_callable=get_top_customers_russia,
    provide_context=True,
    dag=dag,
)