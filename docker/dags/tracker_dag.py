from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from sqlalchemy import false


from satellite_tracker.tracker import download_satellite_data,upload_to_s3



dag = DAG(
dag_id="api_ingestion",
start_date=datetime(2022, 1, 1),
catchup=False,
schedule_interval='* * * * *'    
)

create_table_dag = DAG(
    dag_id="postgres_operator_dag",
    start_date=datetime(2022,1,1),
    schedule_interval="@once",
    catchup=False
)

create_satellite_tracker_table = PostgresOperator(
    task_id="create_satellite_tracker_table",
    sql="""
        CREATE TABLE IF NOT EXISTS satellite_tracker_raw (
        pet_id SERIAL PRIMARY KEY,
        name VARCHAR NOT NULL,
        pet_type VARCHAR NOT NULL,
        birth_date DATE NOT NULL,
        OWNER VARCHAR NOT NULL);
    
    """
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
#         bash_command="echo Hi > /usr/local/airflow/satellite_tracking/data/example.json ",
#         dag=dag
#     )


download_api_data >> upload_file_to_s3