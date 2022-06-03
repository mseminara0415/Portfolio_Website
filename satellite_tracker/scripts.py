import boto3
import botocore.exceptions
from io import BytesIO
import datetime
import logging
from more_itertools import bucket
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

def dataframe_to_s3(input_datafame:pd.DataFrame, app_name:str, satellite_data_type:str):
    '''_summary_
    Create an s3 bucket if one does not exist. 

    Parameters
    ----------
    input_datafame : pd.DataFrame
        _description_
    app_name : str
        _description_
    satellite_data_type : str
        _description_
    '''
    
    s3 = boto3.client('s3')
    todays_date = datetime.datetime.today().strftime(r"%Y-%m-%d")
    bucket_name = f"{app_name}-{satellite_data_type}"

    # Try and create a bucket in S3
    try:        
        s3.create_bucket(Bucket=f"{app_name}-{satellite_data_type}", CreateBucketConfiguration={
        'LocationConstraint': 'us-west-1'})
    # If the bucket already exists, continue process
    except (botocore.exceptions.ClientError):
        print(f"This Bucket Already Exists! Uploading data to folder within existing bucket '{app_name}-{satellite_data_type}`.")    
        
    # Buffer data frame
    out_buffer = BytesIO() 
    input_datafame.to_parquet(out_buffer, index=False)

    # Get the s3 objects timestamp based on the requested datatype
    s3.put_object(Bucket=bucket_name, Key=f"{todays_date}/{datetime.datetime.now()}-{satellite_data_type}.parquet", Body=out_buffer.getvalue())

test = get_iss_location(is_tle=False)

df = pd.DataFrame.from_dict(data=[test])

dataframe_to_s3(
    input_datafame=df,
    app_name="satellitetracker",
    satellite_data_type="position")  
