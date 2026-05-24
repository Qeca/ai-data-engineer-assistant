from airflow import DAG
from airflow.models.baseoperator import BaseOperator
from datetime import datetime

# Custom operators
class AddOneOperator(BaseOperator):
    """Operator that adds 1 to the input value."""
    
    def __init__(self, value, **kwargs):
        super().__init__(**kwargs)
        self.value = value
    
    def execute(self, context):
        result = self.value + 1
        print(f"AddOneOperator: {self.value} + 1 = {result}")
        return result


class SumItOperator(BaseOperator):
    """Operator that sums a list of values."""
    
    def __init__(self, values, **kwargs):
        super().__init__(**kwargs)
        self.values = values
    
    def execute(self, context):
        result = sum(self.values)
        print(f"SumItOperator: sum({self.values}) = {result}")
        return result


# Default arguments
default_args = {
    'owner': 'user',
    'start_date': datetime(2024, 1, 1),
    'retries': 0,
}

# DAG definition
with DAG(
    'dynamic_task_mapping_example',
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=['example', 'dynamic-mapping', 'demo'],
    doc_md=__doc__,
) as dag:
    
    # Input values for dynamic mapping
    input_values = [1, 2, 3, 4, 5]
    
    # Dynamic task mapping with AddOneOperator
    # This creates 5 parallel tasks, one for each value
    add_one_tasks = AddOneOperator.partial(
        task_id='add_one',
    ).expand(value=input_values)
    
    # Sum all results using SumItOperator
    # This task waits for all add_one_tasks to complete
    sum_task = SumItOperator(
        task_id='sum_results',
        values=add_one_tasks.output,
    )
    
    # Set dependency
    add_one_tasks >> sum_task
