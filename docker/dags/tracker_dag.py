from datetime import datetime
from tabnanny import check
from airflow import DAG
import os
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.transfers import local_to_s3
from airflow.providers.postgres.operators.postgres import PostgresOperator
from satellite_tracker.tracker import download_satellite_data,upload_to_s3

path = r'/opt/airflow/satellite_data/'

if os.path.isdir(path):
    os.chdir(path)
else:
    os.mkdir(path)
    os.chdir(path)

dag = DAG(
dag_id="api_ingestion",
start_date=datetime(2022, 1, 1),
catchup=False,
schedule_interval='* * * * *'    
)

download_api_data = PythonOperator(
    task_id="download_api_data",
    python_callable=download_satellite_data,
    dag=dag
)

upload_file_to_s3 = PythonOperator(
    task_id="upload_file_to_s3",
    python_callable=upload_to_s3,
    dag=dag
)



# check_file = BashOperator(
#         task_id="check_file",
#         bash_command="ls",
#         cwd='/opt/airflow/satellite_data/',
#         dag=dag
#     )

# create_table_dag = DAG(
#     dag_id="postgres_operator_dag",
#     start_date=datetime(2022,1,1),
#     schedule_interval="@once",
#     catchup=False
# )

download_api_data >> upload_file_to_s3