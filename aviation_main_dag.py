from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def check_azure_connection():
    print("Connecting to Azure Data Factory...")
    # This is where your ADF Trigger code will go later!
    return "Success"

with DAG(
    dag_id='aviation_safety_pipeline',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False
) as dag:

    task_ingest = PythonOperator(
        task_id='ingest_from_api',
        python_callable=check_azure_connection
    )