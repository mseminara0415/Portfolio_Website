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
from airflow.providers.amazon.aws.transfers import local_to_s3

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
        api_url = f'https://api.wheretheiss.at/v1/satellites/{norad_id}?units={units}&?timestamp'
        iss_data = requests.get(api_url).json()
        satellite_data_type = 'position'

    # If we want to return orbital data
    elif is_tle:
        # Get tle data from API
        api_url_tle = f'https://api.wheretheiss.at/v1/satellites/{norad_id}/tles'
        iss_data = requests.get(api_url).json()
        satellite_data_type = 'tle'
    
    timestamp_value = datetime.fromtimestamp(iss_data['timestamp'])
    
    file_path_2 = Path(r'/opt/airflow/satellite_data',satellite_data_type+'-'+timestamp_value+'.json')
    

    # Write json data to docker container  filepath
    with open(file_path_2,'w') as f:
        json.dump(iss_data,f)
    
    # Get docker filepath and filename
    xcomm_full_filepath = os.path.abspath(file_path_2)
    xcom_filename = os.path.basename(file_path_2)

    # Push docker filepath and filename from dag task 
    ti.xcom_push(key='download_file_path_str',value=str(xcomm_full_filepath))
    ti.xcom_push(key='filename',value=str(xcom_filename))

def upload_to_s3(ti) -> None:
    fetched_download_file = ti.xcom_pull(key='download_file_path_str',task_ids=['download_api_data'])[0]
    filename = ti.xcom_pull(key='filename',task_ids=['download_api_data'])[0]
    hook = S3Hook('s3_conn_id')
    hook.load_file(
        filename=fetched_download_file,
        key=f'position-data/{filename}',
        bucket_name='satellite-tracker-raw-data',
        replace=True
    )
