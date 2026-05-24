from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2026, 5, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='sales_daily_aggregate',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    description='Ежедневная агрегация продаж из sales.orders с учётом новой колонки discount',
    tags=['sales', 'daily', 'aggregate'],
)

def aggregate_sales_daily(**context):
    """
    Выполняет ежедневную агрегацию продаж с учётом колонки discount.
    
    discount - новая колонка в sales.orders, может быть NULL для старых записей.
    Агрегирует:
    - total_orders: количество заказов
    - total_amount: общая сумма заказов
    - total_discount: сумма скидок (COALESCE для NULL)
    - net_revenue: выручка за вычетом скидок
    - avg_discount_pct: средний процент скидки
    """
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    
    query = """
    INSERT INTO analytics.daily_sales_aggregate (
        sale_date,
        total_orders,
        total_amount,
        total_discount,
        net_revenue,
        avg_discount_pct
    )
    SELECT 
        DATE(order_ts) as sale_date,
        COUNT(order_id) as total_orders,
        SUM(amount) as total_amount,
        COALESCE(SUM(discount), 0) as total_discount,
        SUM(amount) - COALESCE(SUM(discount), 0) as net_revenue,
        CASE 
            WHEN SUM(amount) > 0 
            THEN COALESCE(SUM(discount), 0) / SUM(amount) * 100 
            ELSE 0 
        END as avg_discount_pct
    FROM sales.orders
    WHERE DATE(order_ts) = DATE(%s) - INTERVAL '1 day'
    GROUP BY DATE(order_ts)
    ON CONFLICT (sale_date) DO UPDATE SET
        total_orders = EXCLUDED.total_orders,
        total_amount = EXCLUDED.total_amount,
        total_discount = EXCLUDED.total_discount,
        net_revenue = EXCLUDED.net_revenue,
        avg_discount_pct = EXCLUDED.avg_discount_pct
    """
    
    execution_date = context['execution_date']
    target_date = execution_date - timedelta(days=1)
    
    hook = PostgresHook(postgres_conn_id='demo-postgres-warehouse')
    conn = hook.get_conn()
    cursor = conn.cursor()
    
    try:
        cursor.execute(query, (target_date,))
        conn.commit()
        
        cursor.execute("""
            SELECT sale_date, total_orders, total_amount, total_discount, net_revenue, avg_discount_pct
            FROM analytics.daily_sales_aggregate
            WHERE sale_date = %s
        """, (target_date.date(),))
        
        result = cursor.fetchone()
        if result:
            print(f"Агрегация за {result[0]}: заказов={result[1]}, сумма={result[2]}, скидка={result[3]}, нетто={result[4]}, средний дисконт={result[5]:.2f}%")
        else:
            print(f"Нет данных для агрегации за {target_date.date()}")
            
    finally:
        cursor.close()
        conn.close()

aggregate_task = PythonOperator(
    task_id='aggregate_daily_sales',
    python_callable=aggregate_sales_daily,
    provide_context=True,
    dag=dag,
)

aggregate_task