from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash_operator import BashOperator

# Define default arguments for the Airflow DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),  # Modify this based on when you want the DAG to start
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Define the DAG
with DAG(
    'profit_cost_pipeline',
    default_args=default_args,
    description='DAG for running a Python script for ingestion and ETL',
    schedule_interval=None,  # Set to `None` for manual runs, or define a cron schedule here
    catchup=False,
) as dag:

    # Task 1: Run the ingestion Python script
    run_ingestion_script = BashOperator(
        task_id='run_ingestion_script',
        bash_command='python C:\desktop\JLR_Technical_Interview\data_ingestion.py'  # Full path to your Python script
    )

    # Task 2: Run the ETL Python script (if separate)
    # If you have a separate ETL script, you can create another BashOperator
    run_etl_script = BashOperator(
        task_id='run_etl_script',
        bash_command='python C:\desktop\JLR_Technical_Interview\Vehicle_analysis.py'  # Full path to ETL script, if different
    )

    # Define task dependencies
    run_ingestion_script >> run_etl_script 
