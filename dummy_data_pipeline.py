from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

# Default arguments
default_args = {
    "owner": "Manali Gawde",
    "depends_on_past": False,
    "email": ["manali.gawde95@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,  # Retry once in case of failure
}

# Define the DAG
with DAG(
    dag_id="dummy_data_pipeline",
    default_args=default_args,
    description="Dummy DAG ",
    schedule_interval=None,  # None for manual runs
    start_date=days_ago(1),
    catchup=False,
    tags=["dummy_pipeline"],
) as dag:

    # Task 1: Start
    Data_ingestion = DummyOperator(task_id="Data_ingestion")

    # Task 2: Middle
    Data_transformation = DummyOperator(task_id="Data_transformation")

    # Task 3: End
    Data_loading = DummyOperator(task_id="Data_loading")

    # Define the task dependencies
    Data_ingestion >> Data_transformation >> Data_loading
