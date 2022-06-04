import boto3
import botocore.exceptions
from io import BytesIO
import datetime
import logging
import pandas as pd
import requests
import re



def get_iss_location(norad_id: int = 25544, units: str = "miles", is_tle:bool = False) -> dict:
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

    Returns
    -------
    dict
        _description_
    '''
    # If we want to return positioning data
    if not is_tle:
        api_url = f"https://api.wheretheiss.at/v1/satellites/{norad_id}?units={units}&?timestamp"
        iss_data = requests.get(api_url).json()

    # If we want to return orbital data
    elif is_tle:
        api_url_tle = f"https://api.wheretheiss.at/v1/satellites/{norad_id}/tles"
        iss_data = requests.get(api_url_tle).json()
        
    return iss_data

def request_to_dataframe(input_data:dict) -> pd.DataFrame:
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

    return pd.DataFrame.from_dict(data=[input_data])

def dataframe_to_s3(input_datafame:pd.DataFrame, satellite_data_type:str = "position"):
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
    
    s3 = boto3.client('s3')
    todays_date = datetime.datetime.today().strftime(r"%Y-%m-%d")
    bucket_name = f"satellite_tracker-{satellite_data_type}-raw"

    # Try and create a bucket in S3
    try:        
        s3.create_bucket(Bucket=f"satellite_tracker-{satellite_data_type}-raw", CreateBucketConfiguration={
        'LocationConstraint': 'us-west-1'})
    # If the bucket already exists, continue process
    except (botocore.exceptions.ClientError):
        print(f"This Bucket Already Exists! Uploading data to folder within existing bucket 'satellite_tracker-{satellite_data_type}-raw`.")    
        
    # Buffer data frame
    out_buffer = BytesIO() 
    input_datafame.to_parquet(out_buffer, index=False)

    # Get the s3 objects timestamp based on the requested datatype
    s3.put_object(Bucket=bucket_name, Key=f"{todays_date}/{datetime.datetime.now()}-{satellite_data_type}.parquet", Body=out_buffer.getvalue())

def raw_iss_position_ingestion():
    '''_summary_
    This function builds a datapipeline to get data from the API
    and insert it into an AWS s3 bucket.
    '''

    # Get ISS position data from the API
    iss_position_data = get_iss_location(is_tle=False)

    # Put data into Pandas Dataframe object
    iss_position_dataframe = request_to_dataframe(iss_position_data)

    # Transform dataframe to parquet format and insert into s3 bucket
    dataframe_to_s3(
        iss_position_dataframe,
        "position"
    )
