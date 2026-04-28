from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from datetime import datetime

with DAG(
    dag_id="pipeline_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    run_glue_pipeline = GlueJobOperator(
        task_id="run_pipeline",
        job_name="customer-etl-job",
        script_location="s3://airflow-intro-dags/plugins/scripts/main.py",
        region_name="us-east-1",
        aws_conn_id="aws_default",
    # )
