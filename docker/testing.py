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

def download_satellite_data(ti, norad_id: int = 25544, units: str = "miles", is_tle:bool = False) -> dict:
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
    
    file_path_2 = Path(r'/opt/airflow/satellite_data',satellite_data_type+'-'+timestamp_value+'.json')
    xcomm_value = fr"/opt/airflow/satellite_data/{satellite_data_type}-{timestamp_value}.json"

    with open(file_path_2,'w') as f:
        json.dump(iss_data,f)
    
    ti.xcom_push(key="download_file_path_str",value=str(xcomm_value))

def upload_to_s3(ti) -> None:
    fetched_download_file = ti.xcom_pull(key="download_file_path_str",task_ids=['download_api_data'])
    hook = S3Hook(aws_conn_id='s3_conn_id',)
    hook.load_file(
        filename=fetched_download_file,
        key='satellite-tracker-raw-data/position-data/',
        bucket_name='satellite-tracker-raw-data'
    )

def upload_to_s3_testing() -> None:
    testing_folder = r'C:\Users\Matt\Desktop\satellite_tracker\satellite_tracker\docker\data\position-06-09-2022-06-53-00.json'
    s3 = boto3.Session('s3')
    s3object = s3.Object('satellite-tracker-raw-data', testing_folder)

    s3object.put(
        Body=(bytes(json.dumps(testing_folder).encode('UTF-8')))
    )


upload_to_s3_testing()