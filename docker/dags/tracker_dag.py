from asyncio import Task
from datetime import datetime
from airflow import DAG
import os
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.transfers import local_to_s3
from airflow.providers.postgres.operators.postgres import PostgresOperator
from satellite_tracker.tracker import download_satellite_data,merge_jsonfiles

path = r'/opt/airflow/satellite_data/'

if os.path.isdir(path):
    os.chdir(path)
else:
    os.mkdir(path)
    os.chdir(path)

get_api_data = DAG(
dag_id="api_ingestion",
start_date=datetime(2022, 1, 1),
catchup=False,
schedule_interval='* * * * *'    
)

for run_instance in range(1,5):
    group_1_task = PythonOperator(
        task_id=f"get_api_data_minute_{run_instance}",
        python_callable=download_satellite_data,
        dag=get_api_data,
        provide_context=True
    )

merge_api_data = PythonOperator(
    task_id="merge_api_data",
    python_callable=merge_jsonfiles,
    dag=get_api_data
)

group_1_task >> merge_api_data