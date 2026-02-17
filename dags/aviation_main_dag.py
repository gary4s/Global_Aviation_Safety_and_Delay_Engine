from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.providers.microsoft.azure.operators.data_factory import AzureDataFactoryRunPipelineOperator

#defualt args for all tasks
default_args = {
    'owner':'airflow',
    'depends_on_past':False,
    'email': 'some_email@example.com',
    'retries':1,
    'retry_delay':timedelta(minutes=5),
}

with DAG(
    dag_id = 'aviation_medallion_pipeline',
    default_args = default_args,
    description = 'Automated Bronze to Gold pipeline',
    schedule_interval = None,
    start_date = datetime(2024, 1, 1),
    catchup = False,
    tags = ['aviation'],
) as dag:
    
# Task 1: Ingest (Fixing the path and the filename)
 #  ingest_bronze = BashOperator(
 #       task_id='ingest_bronze',
 #       bash_command='python3 /opt/airflow/code/bronze_ingestion.py'
 #   )
    #Task 1: code for AirFlow to orchestrate ADF
    ingest_bronze = AzureDataFactoryRunPipelineOperator(
        task_id = 'ingest_bronze_via_adf',
        pipeline_name = 'p_aviation_ingestion',
        azure_data_factory_conn_id = 'azure_data_factory_conn',
        wait_for_termination = True,
        dag=dag,
    )

    # Task 2: Silver (Using the /opt/airflow/ path)
    transform_silver = BashOperator(
        task_id='transform_silver',
        bash_command='python3 /opt/airflow/code/silver_transformation.py'
    )

    # Task 3: Gold
    transform_gold = BashOperator(
        task_id='transform_gold',
        bash_command='python3 /opt/airflow/code/gold_transform.py'
    )

    #seting the low: Bronze -> Silver -> Gold
    ingest_bronze >> transform_silver >> transform_gold