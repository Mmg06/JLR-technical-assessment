from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.utils.dates import days_ago

default_args = {
    "owner": "Manali Gawde",
    "depends_on_past": False,
    "email_on_failure": True,
    "retries": 1,
}

with DAG(
    dag_id="car_pipeline",
    default_args=default_args,
    description="Run Docker tasks using KubernetesPodOperator in Cloud Composer",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:

    ingestion_task = KubernetesPodOperator(
        task_id="data_ingestion",
        name="data-ingestion",
        namespace="default",  # Use "default" or the namespace Composer is using
        image="europe-west2-docker.pkg.dev/jlr-interview-442609/car-sales-pipeline/my-app:latest",
        cmds=["python"],
        arguments=["data_ingestion.py"],
        is_delete_operator_pod=True,
        in_cluster=True,  # This ensures it uses the in-cluster Kubernetes config
        config_file=None,  # This prevents Airflow from looking for an external kubeconfig
    )



    # Task 2: Data Transformation
    transformation_task = KubernetesPodOperator(
        task_id="data_transformation",
        name="data-transformation",
        namespace="default",
        image="europe-west2-docker.pkg.dev/jlr-interview-442609/car-sales-pipeline/my-app:latest",
        cmds=["python"],
        arguments=["data_transformation.py"],
        is_delete_operator_pod=True,
        in_cluster=True,
        config_file=None,
    )

    # Task 3: Data Loading
    loading_task = KubernetesPodOperator(
        task_id="data_loading",
        name="data-loading",
        namespace="default",
        image="europe-west2-docker.pkg.dev/jlr-interview-442609/car-sales-pipeline/my-app:latest",
        cmds=["python"],
        arguments=["data_loading.py"],
        is_delete_operator_pod=True,
        in_cluster=True,
        config_file=None,
    )

    ingestion_task >> transformation_task >> loading_task
