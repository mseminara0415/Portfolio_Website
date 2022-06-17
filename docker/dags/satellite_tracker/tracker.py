from time import sleep
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
import time
from queue import Queue
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.transfers import local_to_s3
from airflow.models import TaskInstance
from airflow.models import BaseOperator
from typing import List, Set

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
        api_url = f'https://api.wheretheiss.at/v1/satellites/{norad_id}?units={units}&?timestamp'
        iss_data = requests.get(api_url).json()
        satellite_data_type = 'position'

    # If we want to return orbital data
    elif is_tle:
        # Get tle data from API
        api_url_tle = f'https://api.wheretheiss.at/v1/satellites/{norad_id}/tles'
        iss_data = requests.get(api_url).json()
        satellite_data_type = 'tle'

    # Get timestamp value from API and format as string
    timestamp_value_string = datetime.fromtimestamp(iss_data['timestamp']).strftime('%Y-%m-%d-%H-%M-%S')
    
    # Create filepath for our API data. This is where we'll want to write it to
    file_path = Path(r'/opt/airflow/satellite_data',satellite_data_type,satellite_data_type+'-'+timestamp_value_string+'.json')    

    # Write json data to file within docker container
    with open(file_path,'w') as f:
        json.dump(iss_data,f)
    
    # Get docker filepath and filename of json file we just created
    xcomm_full_filepath = str(os.path.abspath(file_path))
    xcom_filename = str(os.path.basename(file_path))

    # Get task id of current task being run
    task_id = str(context['task'])

    # Push docker filepath, filename, and satellite_data_type from dag task 
    ti.xcom_push(key='download_file_path_str', value=xcomm_full_filepath)
    ti.xcom_push(key='filename', value=xcom_filename)
    ti.xcom_push(key='satellite_data_type', value=satellite_data_type)
    ti.xcom_push(key='timestamp_value_string', value=timestamp_value_string)
    ti.xcom_push(key='current_task_id', value=task_id)

    # Run this once a minute
    #time.sleep(60)
 
def validate_api_data(ti):
    None

def merge_jsonfiles(ti, **context):

    # get previous task instance id
    parent_task_id = list(context['task'].upstream_task_ids)[0]

    # get satellite_data_type
    satellite_data_type = ti.xcom_pull(key='satellite_data_type',task_ids=[f'{parent_task_id}'])[0]

    # get timestamp value provided from API
    timestamp_value_string = ti.xcom_pull(key='timestamp_value_string',task_ids=[f'{parent_task_id}'])[0]

    # get list of files in the directory
    home_directory = fr'/opt/airflow/satellite_data/{satellite_data_type}/'
    file_list = [Path(home_directory,file) for file in os.listdir(f'{home_directory}')]

    # get time range of merge files
    pattern = re.compile(r"position-|tle-|.json")

    earliest_file = min(file_list,key=os.path.getctime)    
    earliest_time = pattern.split(str(earliest_file))[1]

    latest_file = max(file_list, key=os.path.getctime)
    latest_time = pattern.split(str(latest_file))[1]

    # create merged filepath to write combined
    merged_file_path = fr'/opt/airflow/satellite_data/{satellite_data_type}/-merged-{earliest_time}-through-{latest_time}.json'

    combined_json = []
    for file in file_list:
        with open(file,'r') as infile:
            file_contents = json.load(infile)
            combined_json.append(file_contents)
    with open(merged_file_path,'w') as output_file:
        json.dump(combined_json,output_file,indent=2)

    ti.xcom_push(key='merged_filepath',value=str(merged_file_path))

def upload_to_s3(ti) -> None:
    fetched_download_file = ti.xcom_pull(key='merged_filepath',task_ids=['download_api_data'])[0]
    filename = ti.xcom_pull(key='filename',task_ids=['download_api_data'])[0]
    hook = S3Hook('s3_conn_id')
    hook.load_file(
        filename=fetched_download_file,
        key=f'position-data/{filename}',
        bucket_name='satellite-tracker-raw-data',
        replace=True
    )
