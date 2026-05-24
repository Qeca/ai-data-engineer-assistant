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
    dag_id='daily_top_customers_report',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    description='Ежедневный отчёт: топ-10 клиентов по выручке',
)

def get_top_customers(**context):
    """Выполняет SQL-запрос и логирует результаты"""
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    
    query = """
    SELECT 
        c.customer_id, 
        c.email, 
        c.segment, 
        SUM(o.amount) as total_revenue, 
        COUNT(o.order_id) as order_count
    FROM sales.customers c
    JOIN sales.orders o ON c.customer_id = o.customer_id
    WHERE o.status = 'paid'
    GROUP BY c.customer_id, c.email, c.segment
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
    ti.xcom_push(key='top_customers', value={'columns': columns, 'data': results})
    
    print("=" * 80)
    print("ТОП-10 КЛИЕНТОВ ПО ВЫРУЧКЕ")
    print("=" * 80)
    print(f"{'customer_id':<12} {'email':<25} {'segment':<10} {'total_revenue':<15} {'order_count':<12}")
    print("-" * 80)
    for row in results:
        print(f"{row[0]:<12} {row[1]:<25} {row[2]:<10} {float(row[3]):<15.2f} {row[4]:<12}")
    print("=" * 80)
    
    return {'columns': columns, 'data': results}

report_task = PythonOperator(
    task_id='generate_top_customers_report',
    python_callable=get_top_customers,
    provide_context=True,
    dag=dag,
)