from airflow import DAG
from airflow.decorators import dag, task, asset
from datetime import datetime

# Определение активов (assets) для lineage
input_data = asset("s3://data-lake/input/", name="input_data")
output_data = asset("s3://data-lake/output/", name="output_data")


@dag(
    dag_id="decorator_asset_dag",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["decorator", "asset", "demo"],
    description="DAG с использованием декораторов @dag, @task, @asset",
)
def decorator_asset_dag():
    """
    Пример DAG с использованием современных декораторов Airflow:
    - @dag для определения DAG
    - @task для определения задач
    - @asset для определения активов (lineage)
    """

    @task(outlets=[output_data])
    def extract():
        """Извлечение данных из источника"""
        print("Extracting data from source...")
        data = [
            {"id": 1, "name": "Alice", "value": 100},
            {"id": 2, "name": "Bob", "value": 200},
            {"id": 3, "name": "Charlie", "value": 300},
        ]
        return data

    @task(inlets=[input_data], outlets=[output_data])
    def transform(data):
        """Трансформация данных"""
        print(f"Transforming {len(data)} records...")
        transformed = []
        for record in data:
            transformed_record = {
                "id": record["id"],
                "name": record["name"].upper(),
                "value": record["value"] * 2,
                "processed_at": datetime.now().isoformat(),
            }
            transformed.append(transformed_record)
        return transformed

    @task(inlets=[input_data])
    def load(data):
        """Загрузка данных в целевую систему"""
        print(f"Loading {len(data)} records to destination...")
        for record in data:
            print(f"  - Loaded: {record}")
        return {"status": "success", "loaded_count": len(data)}

    # Определение потока задач
    raw_data = extract()
    transformed_data = transform(raw_data)
    result = load(transformed_data)

    return result


# Инстанциация DAG
dag_instance = decorator_asset_dag()
