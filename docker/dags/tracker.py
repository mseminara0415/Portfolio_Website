import boto3
import botocore.exceptions
from datetime import datetime
from io import BytesIO
import json
import logging
import pandas as pd
import requests
import re

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


def load_raw_iss_data_to_s3(norad_id: int = 25544, units: str = "miles", is_tle:bool = False) -> dict:
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

    # Set s3 client
    s3 = boto3.client(
        's3'
    )

    # Get todays data, which will be used for partitioning folders within bucket
    todays_date = datetime.today().strftime(r"%Y-%m-%d")

    # Set destination bucket for raw satallite tracking data
    bucket_name = "satellite-tracker-raw"
    

    # If we want to return positioning data
    if not is_tle:
        # Get position data from API
        api_url = f"https://api.wheretheiss.at/v1/satellites/{norad_id}?units={units}&?timestamp"
        iss_data = requests.get(api_url).json()

        # Get the s3 objects timestamp based on the requested datatype
        s3.put_object(
            Bucket=bucket_name,
            Key=f"position-data/{todays_date}/{datetime.now()}-position.json",
            Body=json.dumps(iss_data)
        )

    # If we want to return orbital data
    elif is_tle:
        # Get tle data from API
        api_url_tle = f"https://api.wheretheiss.at/v1/satellites/{norad_id}/tles"
        iss_data = requests.get(api_url_tle).json()

        # Get the s3 objects timestamp based on the requested datatype
        s3.put_object(
            Bucket=bucket_name,
            Key=f"tle-data/{todays_date}/{datetime.now()}-tle.json",
            Body=json.dumps(iss_data)
        )

dag = DAG(
dag_id="my_dag",
start_date=datetime(2022, 1, 1),
catchup=False,
schedule_interval='*/10 * * * * *'    
)

load_raw_position_data = PythonOperator(
    task_id="load_raw_iss_position_data_to_s3",
    python_callable=load_raw_iss_data_to_s3,
    dag=dag
    
)

# load_raw_tle_data = PythonOperator(
#     task_id="load_raw_iss_tle_data_to_s3",
#     python_callable=load_raw_iss_data_to_s3,
#     op_kwargs={'is_tle': True},
#     dag=dag
# )

# get_data_from_api >> api_data_to_dataframe >> raw_data_to_s3


# Steps of datapipeline:
'''
Steps of datapipline:

1. Pull data from API using requests. Save as json file in s3 bucket.


2.


3.


4.


'''