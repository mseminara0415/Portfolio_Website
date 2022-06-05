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


def load_raw_iss_to_s3(norad_id: int = 25544, units: str = "miles", is_tle:bool = False) -> dict:
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
    s3 = boto3.client('s3')

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

def request_to_dataframe(ti:dict) -> pd.DataFrame:
    '''_summary_
    Takes input data in the form of a dictionary and returns
    a Pandas dataframe object.

    Parameters
    ----------
    input_data : dict
        _description_
        input data in the form of a dictionary datatype.

    Returns
    -------
    pd.DataFrame
        _description_
    '''
    input_data = ti.xcom_pull(task_ids=['get_data_from_api'])

    df = pd.DataFrame.from_dict(data=[input_data])

    # Buffer data frame
    out_buffer = BytesIO() 
    df.to_parquet(out_buffer, index=False)



    return pd.DataFrame.from_dict(data=[input_data])

def dataframe_to_s3(ti, satellite_data_type:str = "position"):
    '''_summary_
    Convert pandas dataframe object to Apache Parquet file format and upload data
    to AWS s3 bucket.

    Parameters
    ----------
    input_datafame : pd.DataFrame
        _description_
    satellite_data_type : str, optional
        _description_, by default "position"
        Two options:
        - position (default)
        - tle
    '''

    input_parquet_file = ti.xcom_pull(task_ids=['put_data_into_dataframe'])
    
    s3 = boto3.client('s3')
    todays_date = datetime.today().strftime(r"%Y-%m-%d")
    bucket_name = f"satellite_tracker-{satellite_data_type}-raw"

    # Try and create a bucket in S3
    try:        
        s3.create_bucket(Bucket=f"satellite_tracker-{satellite_data_type}-raw", CreateBucketConfiguration={
        'LocationConstraint': 'us-west-1'})
    # If the bucket already exists, continue process
    except (botocore.exceptions.ClientError):
        print(f"This Bucket Already Exists! Uploading data to folder within existing bucket 'satellite_tracker-{satellite_data_type}-raw`.")    

    # Get the s3 objects timestamp based on the requested datatype
    s3.put_object(Bucket=bucket_name, Key=f"{todays_date}/{datetime.datetime.now()}-{satellite_data_type}.parquet", Body=input_parquet_file.getvalue())

# dag = DAG(
# dag_id="my_dag",
# start_date=datetime(2022, 1, 1),
# catchup=False,
# schedule_interval='*/10 * * * * *'    
# )

# get_data_from_api = PythonOperator(
#     task_id="get_data_from_api",
#     python_callable=get_iss_location,
#     dag=dag
# )

# api_data_to_dataframe = PythonOperator(
#     task_id="put_data_into_dataframe",
#     python_callable=request_to_dataframe,
#     dag=dag
# )

# raw_data_to_s3 = PythonOperator(
#     task_id="ingest_raw_data_to_s3",
#     python_callable=dataframe_to_s3,
#     op_kwargs={'satellite_data_type':'position'},
#     dag=dag
# )

# get_data_from_api >> api_data_to_dataframe >> raw_data_to_s3

load_raw_iss_to_s3(is_tle=False)



# Steps of datapipeline:
'''
Steps of datapipline:

1. Pull data from API using requests. Save as json file in s3 bucket.


2.


3.


4.


'''