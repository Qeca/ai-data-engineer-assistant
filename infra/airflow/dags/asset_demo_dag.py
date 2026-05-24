from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Определяем assets для демонстрации функциональности
# В Airflow 2.4+ Assets позволяют запускать DAGs на основе изменений в данных
# Asset представляет собой данные (файл, таблицу, etc.) которые могут быть произведены или потреблены

# Концептуальные assets (в production используются airflow.assets.Asset)
# input_data_asset = Asset("s3://demo-bucket/input/data.csv", name="input_data")
# processed_data_asset = Asset("s3://demo-bucket/processed/data.parquet", name="processed_data")
# report_asset = Asset("s3://demo-bucket/reports/report.html", name="report")

# Asset expression для условного планирования:
# combined_asset_expression = input_data_asset & processed_data_asset
# Это означает что DAG запустится только когда ОБА assets будут обновлены

with DAG(
    dag_id="asset_demo_dag",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["assets", "demo", "conditional", "bash"],
    doc_md="""
    ## Asset Demo DAG
    
    Этот DAG демонстрирует концепцию Assets в Airflow:
    
    ### Что такое Assets?
    Assets - это объекты данных (файлы, таблицы, etc.) которые могут:
    - **Производиться** tasks (outlets)
    - **Потребляться** tasks (inlets)
    - **Триггерить** запуск DAGs при обновлении
    
    ### Asset-based scheduling:
    - DAG может запускаться по расписанию (@daily) ИЛИ
    - При обновлении определенных assets (asset-triggered)
    
    ### Asset expressions:
    - Логические выражения для условного планирования
    - Пример: `asset1 & asset2` - запуск когда оба assets обновлены
    - Пример: `asset1 | asset2` - запуск когда любой asset обновлен
    
    ### Assets в этом DAG:
    - `input_data`: Входные данные (S3 файл)
    - `processed_data`: Обработанные данные (Parquet)
    - `report`: Финальный отчет (HTML)
    
    ### Inlets/Outlets:
    - Tasks могут объявлять какие assets они потребляют (inlets)
    - Tasks могут объявлять какие assets они производят (outlets)
    - Airflow автоматически отслеживает зависимости между assets
    """,
    params={
        "asset_path": "s3://demo-bucket/",
        "enable_asset_triggers": True,
    },
) as dag:
    
    # Task 1: Проверка входных данных
    # В production: inlets=[input_data_asset]
    check_input_data = BashOperator(
        task_id="check_input_data",
        bash_command="""
            echo "=========================================="
            echo "=== Task: Check Input Data Asset ==="
            echo "=========================================="
            echo "Timestamp: $(date)"
            echo ""
            echo "Asset: input_data (s3://demo-bucket/input/data.csv)"
            echo "Checking for new input data..."
            echo ""
            echo "Simulating asset check:"
            echo "  - Verifying file exists..."
            echo "  - Checking file size..."
            echo "  - Validating schema..."
            echo ""
            echo "✓ Input data asset check completed successfully!"
            echo ""
            # XCom для передачи статуса
            echo "{{ task_instance_key_str }}"
        """,
    )
    
    # Task 2: Обработка данных
    # В production: inlets=[input_data_asset], outlets=[processed_data_asset]
    # Этот task ПОТРЕБЛЯЕТ input_data и ПРОИЗВОДИТ processed_data
    process_data = BashOperator(
        task_id="process_data",
        bash_command="""
            echo "=========================================="
            echo "=== Task: Process Data ==="
            echo "=========================================="
            echo "Timestamp: $(date)"
            echo ""
            echo "Inlet Asset: input_data"
            echo "Outlet Asset: processed_data"
            echo ""
            echo "Processing steps:"
            echo "  1. Reading from input asset (s3://demo-bucket/input/data.csv)..."
            echo "  2. Applying transformations..."
            echo "  3. Validating data quality..."
            echo "  4. Writing to output asset (s3://demo-bucket/processed/data.parquet)..."
            echo ""
            echo "✓ Data processing completed!"
            echo "✓ Produced asset: processed_data"
        """,
    )
    
    # Task 3: Генерация отчета с условной логикой
    # В production: inlets=[processed_data_asset], outlets=[report_asset]
    # Запускается только когда processed_data asset готов
    generate_report = BashOperator(
        task_id="generate_report",
        bash_command="""
            echo "=========================================="
            echo "=== Task: Generate Report ==="
            echo "=========================================="
            echo "Timestamp: $(date)"
            echo ""
            echo "Inlet Asset: processed_data"
            echo "Outlet Asset: report"
            echo ""
            echo "Checking asset expression conditions..."
            echo "  - processed_data asset: READY ✓"
            echo "  - input_data asset: READY ✓"
            echo ""
            echo "Asset expression (input_data & processed_data): SATISFIED"
            echo ""
            echo "Generating HTML report..."
            echo "  - Aggregating metrics..."
            echo "  - Creating visualizations..."
            echo "  - Writing to s3://demo-bucket/reports/report.html..."
            echo ""
            echo "✓ Report generation completed!"
            echo "✓ Produced asset: report"
        """,
    )
    
    # Task 4: Уведомление о завершении
    send_notification = BashOperator(
        task_id="send_notification",
        bash_command="""
            echo "=========================================="
            echo "=== Task: Send Notification ==="
            echo "=========================================="
            echo "Timestamp: $(date)"
            echo ""
            echo "All assets have been processed successfully!"
            echo ""
            echo "Asset lineage:"
            echo "  input_data --> process_data --> processed_data"
            echo "  processed_data --> generate_report --> report"
            echo ""
            echo "Report available at: s3://demo-bucket/reports/report.html"
            echo ""
            echo "Sending notification to stakeholders..."
            echo "✓ Notification sent!"
        """,
    )
    
    # Task 5: Очистка временных файлов
    cleanup = BashOperator(
        task_id="cleanup",
        bash_command="""
            echo "=========================================="
            echo "=== Task: Cleanup ==="
            echo "=========================================="
            echo "Timestamp: $(date)"
            echo ""
            echo "Removing temporary files..."
            echo "  - Cleaning /tmp/asset_demo_*..."
            echo "  - Removing cache files..."
            echo ""
            echo "✓ Cleanup completed!"
        """,
    )
    
    # Task 6: Валидация asset lineage
    validate_lineage = BashOperator(
        task_id="validate_lineage",
        bash_command="""
            echo "=========================================="
            echo "=== Task: Validate Asset Lineage ==="
            echo "=========================================="
            echo "Timestamp: $(date)"
            echo ""
            echo "Validating asset dependencies..."
            echo ""
            echo "Expected lineage:"
            echo "  [input_data] --(produces)--> [processed_data]"
            echo "  [processed_data] --(produces)--> [report]"
            echo ""
            echo "Asset expression evaluation:"
            echo "  input_data & processed_data = TRUE"
            echo ""
            echo "✓ Asset lineage validation passed!"
        """,
    )
    
    # Определение зависимостей между tasks
    check_input_data >> process_data >> generate_report >> send_notification
    generate_report >> cleanup
    generate_report >> validate_lineage
