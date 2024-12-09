from datetime import timedelta
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'Manali',
    'depends_on_past': False,
    'email_on_failure': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG('test_kubernetes',
         default_args=default_args,
         description='A simple DAG to test KubernetesPodOperator.',
         schedule_interval=timedelta(days=1),
         start_date=days_ago(2),
         catchup=False) as dag:

    test_pod = KubernetesPodOperator(
        namespace='default',
        image="alpine",
        cmds=["echo", "Hello Kubernetes!"],
        name="test-pod",
        task_id="pod_task",
        is_delete_operator_pod=True,
        in_cluster=False,  # Set to False if not running Airflow inside a Kubernetes cluster
        config_file="/path/to/kube_config",  # Ensure this is the correct path to your kubeconfig
        get_logs=True
    )

test_pod
