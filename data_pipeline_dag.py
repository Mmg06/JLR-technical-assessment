from datetime import timedelta
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.utils.dates import days_ago

# Bucket for GCS bucket
env_bucket = ""

# Default arguments for the DAG
default_args = {
    "owner": "Manali Gawde",
    "depends_on_past": False,
    "email": ["manali.gawde95@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
}

# Define the DAG
with DAG(
    dag_id="data_pipeline_dag",
    default_args=default_args,
    description="Data pipeline DAG for JLR sales data",
    schedule_interval=None,  # None for manual runs
    start_date=days_ago(1),
    tags=["Data_pipeline"],
    catchup=False,
) as dag:

    # Task 1: Ingestion step
    ingestion_task = KubernetesPodOperator(
        kubernetes_conn_id="kubernetes_default",
        namespace="default",
        image_pull_policy="Always",
        image="europe-west2-docker.pkg.dev/jlr-interview-442609/car-sales-pipeline/my-app:latest",
        cmds=["python", "scripts/data_ingestion.py"],
        name="data-ingestion",
        task_id="ingestion_task",
        startup_timeout_seconds=1200,
        get_logs=True,
        affinity={
            "nodeAffinity": {
                "requiredDuringSchedulingIgnoredDuringExecution": {
                    "nodeSelectorTerms": [
                        {
                            "matchExpressions": [
                                {
                                    "key": "cloud.google.com/gke-nodepool",
                                    "operator": "In",
                                    "values": [
                                        "default-pool",
                                    ],
                                }
                            ]
                        }
                    ]
                }
            }
        },
    )

    # Task 2: Transformation step
    transformation_task = KubernetesPodOperator(
        kubernetes_conn_id="kubernetes_default",
        namespace="default",
        image_pull_policy="Always",
        image="europe-west2-docker.pkg.dev/jlr-interview-442609/car-sales-pipeline/my-app:latest",
        cmds=["python", "scripts/data_transformation.py"],
        name="data-transformation",
        task_id="transformation_task",
        startup_timeout_seconds=1200,
        get_logs=True,
        affinity={
            "nodeAffinity": {
                "requiredDuringSchedulingIgnoredDuringExecution": {
                    "nodeSelectorTerms": [
                        {
                            "matchExpressions": [
                                {
                                    "key": "cloud.google.com/gke-nodepool",
                                    "operator": "In",
                                    "values": [
                                        "default-pool",
                                    ],
                                }
                            ]
                        }
                    ]
                }
            }
        },
    )

    # Task 3: Loading step
    loading_task = KubernetesPodOperator(
        kubernetes_conn_id="kubernetes_default",
        namespace="default",
        image_pull_policy="Always",
        image="europe-west2-docker.pkg.dev/jlr-interview-442609/car-sales-pipeline/my-app:latest",
        cmds=["python", "scripts/data_loading.py"],
        name="data-loading",
        task_id="loading_task",
        startup_timeout_seconds=1200,
        get_logs=True,
        affinity={
            "nodeAffinity": {
                "requiredDuringSchedulingIgnoredDuringExecution": {
                    "nodeSelectorTerms": [
                        {
                            "matchExpressions": [
                                {
                                    "key": "cloud.google.com/gke-nodepool",
                                    "operator": "In",
                                    "values": [
                                        "default-pool",
                                    ],
                                }
                            ]
                        }
                    ]
                }
            }
        },
    )

    # Task Dependencies
    ingestion_task >> transformation_task >> loading_task
