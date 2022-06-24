import boto3
import json

def get_s3_object_data(bucket_name:str, key:str) -> dict:
    '''_summary_
    Get contents from s3 file.

    Parameters
    ----------
    bucket_name : str
        _description_
        Bucket name where the file object is located.
    key : str
        _description_
        key (s3 filepath).

    Returns
    -------
    dict
        _description_
        s3 file contents.
    '''
    
    # Create s3 resource
    s3 = boto3.resource('s3')
    
    # Get bucket and key information for the file you want to get
    destination_bucket_name = bucket_name
    key = key
    
    # Find s3 file object and get its contents
    s3_object = s3.Object(bucket_name=destination_bucket_name, key=key)
    response = s3_object.get()['Body'].read().decode('utf-8')
    
    # Load data into json object (dict)
    json_data = json.loads(response)
    
    return json_data
    