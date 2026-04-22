from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import logging

# Import your pipeline runner from main.py
from main import run_pipeline

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("pipeline_dag")

# Define the DAG
with DAG(
    dag_id="pipeline_dag",             # Name shown in Airflow UI
    start_date=datetime(2024, 1, 1),   # When scheduling starts
    schedule_interval="@daily",        # Run once per day
    catchup=False,                     # Do not backfill old runs
) as dag:

    # Single task: run your pipeline
    run_pipeline_task = PythonOperator(
        task_id="run_pipeline",
        python_callable=run_pipeline
    )
