from airflow.decorators import dag, task, asset
from airflow.sdk.definitions.asset import Asset
from datetime import datetime
from typing import List

# Определяем активы (assets)
@asset
def input_data_asset():
    """Создает входной актив с данными"""
    return {"data": [1, 2, 3, 4, 5]}

@asset
def processed_data_asset(input_data: dict):
    """Обрабатывает входные данные"""
    processed = [x * 2 for x in input_data["data"]]
    return {"processed_data": processed}

@dag(
    dag_id="asset_decorators_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["demo", "assets", "decorators"],
    default_args={
        "owner": "data_engineer",
        "retries": 1,
    }
)
def asset_decorators_pipeline():
    """DAG с использованием декораторов @asset, @dag, @task"""
    
    @task
    def extract_data():
        """Задача извлечения данных"""
        data = {"records": [{"id": 1, "value": "A"}, {"id": 2, "value": "B"}]}
        print(f"Extracted data: {data}")
        return data
    
    @task
    def transform_data(extracted: dict):
        """Задача трансформации данных"""
        transformed = {
            "transformed_records": [
                {**record, "value_upper": record["value"].upper()}
                for record in extracted["records"]
            ]
        }
        print(f"Transformed data: {transformed}")
        return transformed
    
    @task
    def load_data(transformed: dict):
        """Задача загрузки данных"""
        print(f"Loading {len(transformed['transformed_records'])} records")
        return {"status": "success", "loaded_count": len(transformed["transformed_records"])}
    
    # Определяем зависимости между задачами
    extract = extract_data()
    transform = transform_data(extract)
    load = load_data(transform)
    
    return {"extract": extract, "transform": transform, "load": load}

# Регистрируем DAG
dag = asset_decorators_pipeline()
