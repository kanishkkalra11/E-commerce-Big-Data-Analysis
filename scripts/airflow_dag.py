from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
import datetime

# Define environment variables
GCP_PROJECT_ID = "data-management-project-452400"
REGION = "us-west1"
CLUSTER_NAME = "utkarshlalcluster"
BUCKET_NAME = "data-mgmt-bucket"

# Define ETL script paths in GCS
ETL_SCRIPTS = [
    "gs://data-mgmt-bucket/pipelines/ETL.py",
    "gs://data-mgmt-bucket/pipelines/Aggregation.py",
    "gs://data-mgmt-bucket/pipelines/churn_ETL.py",
    "gs://data-mgmt-bucket/pipelines/vis.py",
    "gs://data-mgmt-bucket/pipelines/churn_prediction.py"
]

# Define default arguments for DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,  # Ensures past failures donâ€™t affect retries
    "start_date": days_ago(1),
    "retries": 3,  # Retries only the failed task
    "retry_delay": datetime.timedelta(minutes=5),  # Wait 5 minutes before retrying
}

# Define DAG
dag = DAG(
    "dataproc_etl_pipeline",
    default_args=default_args,
    schedule_interval="0 11 * * *",  # Runs every day at 11 AM UTC
    catchup=False,  # Prevents rerunning all tasks from past failed runs
    tags=["dataproc", "etl"],
)

# Function to create Dataproc tasks
def create_dataproc_task(script_path, task_id, dag):
    return DataprocSubmitJobOperator(
        task_id=task_id,
        project_id=GCP_PROJECT_ID,
        region=REGION,
        gcp_conn_id="google_cloud_default",
        retries=2,  # Retry only this task twice
        retry_delay=datetime.timedelta(minutes=5),  # 5-minute retry delay
        trigger_rule="all_success",  # Ensures only failed tasks retry
        job={
            "placement": {"cluster_name": CLUSTER_NAME},
            "pyspark_job": {
                "main_python_file_uri": script_path,
                "file_uris": [f"gs://{BUCKET_NAME}/pipelines/authkey.json"],
                "properties": {
                    "spark.executorEnv.GOOGLE_APPLICATION_CREDENTIALS": "authkey.json",
                    "spark.driverEnv.GOOGLE_APPLICATION_CREDENTIALS": "authkey.json",
                },
            },
        },
        dag=dag,
    )

# Define ETL tasks sequentially
etl_tasks = []
for index, script in enumerate(ETL_SCRIPTS):
    task_id = f"run_{script.split('/')[-1].replace('.py', '')}"
    etl_task = create_dataproc_task(script, task_id, dag)

    # Ensure sequential execution
    if index > 0:
        etl_tasks[-1] >> etl_task

    etl_tasks.append(etl_task)

# The final task runs after all ETL jobs complete
etl_tasks[-1]