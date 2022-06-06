import boto3
import botocore.exceptions
from datetime import datetime
from io import BytesIO
import json
import logging
import os
import pandas as pd
from pathlib import Path
import requests
import re

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


def download_satellite_data(norad_id: int = 25544, units: str = "miles", is_tle:bool = False) -> dict:
    '''_summary_

    Parameters
    ----------
    norad_id : int, optional
        _description_, by default 25544. This is the NORAD ID for the ISS.

    units : str, optional
        _description_,
         by default "miles". Can alternativly be 'kilometers'.

    is_tle : bool, optional
        _description_,
        by default False. TLE data is used for plotting the orbit.
        When this parameter is False we instead return positioning data (lat, long, altitude, etc..)
    '''

    # If we want to return positioning data
    if not is_tle:
        # Get position data from API
        api_url = f"https://api.wheretheiss.at/v1/satellites/{norad_id}?units={units}&?timestamp"
        iss_data = requests.get(api_url).json()
        satellite_data_type = "position"

    # If we want to return orbital data
    elif is_tle:
        # Get tle data from API
        api_url_tle = f"https://api.wheretheiss.at/v1/satellites/{norad_id}/tles"
        iss_data = requests.get(api_url).json()
        satellite_data_type = "tle"
    
    timestamp_value = datetime.now().strftime("%m-%d-%Y-%H-%M-%S")
    
    file_path = f"/usr/local/airflow/satellite_tracking/data/{satellite_data_type}_raw-{timestamp_value}.json"
    file_path_2 = Path(r'/usr/local/airflow/satellite_tracking/data',satellite_data_type+'-'+timestamp_value+'.json')

    with open(file_path_2,'w') as f:
        json.dump(iss_data,f)

def upload_to_s3(filename: str, key: str, bucket_name: str) -> None:
    hook = S3Hook('s3_conn_id')
    hook.load_file(
        filename=filename,
        key=key,
        bucket_name=bucket_name
    )

# def pre_process():
#     f = open("/usr/local/airflow/satellite_tracking/data/example.json", "w")
#     f.write("key':'value'")
#     f.close()

dag = DAG(
dag_id="my_dag",
start_date=datetime(2022, 1, 1),
catchup=False,
schedule_interval='*/10 * * * * *'    
)

# check_file = BashOperator(
#         task_id="check_file",
#         bash_command="echo Hi > /usr/local/airflow/satellite_tracking/data/example.json ",
#         dag=dag
#     )

pre_processing = PythonOperator(
    task_id="pre_process",
    python_callable=download_satellite_data,
    dag=dag
)

pre_processing

# load_raw_position_data = PythonOperator(
#     task_id="download_satellite_data",
#     python_callable=download_satellite_data,
#     dag=dag    
# )

# load_raw_position_data = PythonOperator(
#     task_id="load_raw_iss_position_data_to_s3",
#     python_callable=load_raw_iss_data_to_s3,
#     dag=dag
    
# )

# load_raw_tle_data = PythonOperator(
#     task_id="load_raw_iss_tle_data_to_s3",
#     python_callable=load_raw_iss_data_to_s3,
#     op_kwargs={'is_tle': True},
#     dag=dag
# )

# get_data_from_api >> api_data_to_dataframe >> raw_data_to_s3