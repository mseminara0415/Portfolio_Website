import boto3
from io import BytesIO
import json
import jsonschema
from lib import database
import psycopg2
from psycopg2 import sql
import requests
import time



def get_rds_db_secrets(secret_name:str, region_name:str) -> dict:
        '''_summary_
        Retrieve n2yo API key and info from AWS secrets.

        Parameters
        ----------
        secret_name : str
            _description_
            Secrets name as identified in aws

        region_name : str
            _description_
            Region where your secrets are located (i.e. 'us-east-1')

        Returns
        -------
        dict
            _description_
            Will return the following
            **API_KEY**
        '''

        # Create aws session
        session = boto3.session.Session()

        client = session.client(
            service_name='secretsmanager',
            region_name=region_name
        )

        # Get secret response. This returns a dictionary as string type.
        response = client.get_secret_value(
            SecretId=secret_name
        )

        # Turn response into dictionary
        secret_dictionary = json.loads(response['SecretString'])

        return secret_dictionary
        
def json_validation_checker(json_schema:dict, json_to_validate:dict) -> bool:
    '''_summary_

    Parameters
    ----------
    json_schema : dict
        _description_
        Json schema used to validate against

    json_to_validate : dict
        _description_
        Json used to validate against the schema

    Returns
    -------
    bool
        _description_
        returns True if json is valid and False if not valid
    '''

    # Validate Schema
    schema_validator = jsonschema.Draft202012Validator(json_schema)

    # Using the above schema, check if our json is valid
    is_valid_json = schema_validator.is_valid(instance=json_to_validate)
    
    if not is_valid_json:
        for error in sorted(schema_validator.iter_errors(json_to_validate), key=str):
            print(f"Data is invalid. Please look into the following: {error.message}")


    return is_valid_json

def flatten_dictionary(dictionary:dict) -> dict:
    '''_summary_.
    Flattens a dictionary that contains values that are dictionaries
    or list of dictionaries.

    Parameters
    ----------
    dictionary : dict
        _description_

    Returns
    -------
    dict
        _description_
        Flattened dictionary
    '''
    out = {}
    for key, val in dictionary.items():
        if isinstance(val, dict):
            val = [val]
        if isinstance(val, list):
            for subdict in val:
                deeper = flatten_dictionary(subdict).items()
                out.update({key2: val2 for key2, val2 in deeper})
        else:
            out[key] = val
    return out

def change_dictionary_key_name(dictionary:dict, original_key:str, new_key: str) -> None:
    '''_summary_
    Change dictionary key names.
    **NOTE**
    This only works with flat dictionaries.
    Parameters
    ----------
    dictionary : dict
        _description_
        Flat dictionary that you want to change key names in
    original_key : str
        _description_
        original key name you want to change
    new_key : str
        _description_
        New key name you want to change to
    '''
    dictionary[new_key] = dictionary.pop(original_key)

def download_satellite_data(n2yo_secrets:dict, norad_id: int = 25544, is_tle:bool = False ) -> dict:
    '''_summary_

    Parameters
    ----------
    n2yo_secrets : dict
        _description_
        Secrets needed to access n2yo API
    norad_id : int, optional
        _description_, by default 25544
        Satellite norad id. By default this is set to the ISS (International Space Station)
    is_tle : bool, optional
        _description_, by default False
        If false, we access satellite position data. If true, we access satellite orbital data.

    Returns
    -------
    dict
        _description_
        Depending on value set for is_tle, returns a dictionary with either satellite position or satellite
        orbital data.
    '''

    # Get secrets for n2yo website
    API_KEY = n2yo_secrets['API_KEY']
    LATITUDE = n2yo_secrets['LATITUDE']
    LONGITUDE = n2yo_secrets['LONGITUDE']

    # If we want to return satellite positioning data
    if not is_tle:
        api_url = f'https://api.n2yo.com/rest/v1/satellite/positions/{norad_id}/{LATITUDE}/{LONGITUDE}/0/1/&apiKey={API_KEY}'
        satellite_data = requests.get(api_url).json()
        satellite_data['source'] = 'https://www.n2yo.com/'
        satellite_data['units'] = 'miles'

        # Flatten dictionary
        flattened_satellite_data = flatten_dictionary(satellite_data)

        # Rename keys to match columns in database
        change_dictionary_key_name(flattened_satellite_data, original_key='satname', new_key='name')
        change_dictionary_key_name(flattened_satellite_data, original_key='satid', new_key='id')
        change_dictionary_key_name(flattened_satellite_data, original_key='satlatitude', new_key='latitude')
        change_dictionary_key_name(flattened_satellite_data, original_key='satlongitude', new_key='longitude')
        change_dictionary_key_name(flattened_satellite_data, original_key='sataltitude', new_key='altitude')
        change_dictionary_key_name(flattened_satellite_data, original_key='eclipsed', new_key='visibility')

        # Drop unwanted key/value(s)
        del flattened_satellite_data['transactionscount']
        del flattened_satellite_data['azimuth']
        del flattened_satellite_data['ra']
        del flattened_satellite_data['dec']
        del flattened_satellite_data['elevation']

        # Convert altitude from kilometers to miles
        flattened_satellite_data['altitude'] = flattened_satellite_data['altitude'] * 0.62137

        # Change 'vibility' value from boolean to string
        if not flattened_satellite_data['visibility']:
            flattened_satellite_data['visibility'] = 'eclipsed'
        else:
            flattened_satellite_data['visibility'] = 'daylight'      

        return flattened_satellite_data
        

    # If we want to return satellite orbital data
    elif is_tle:

        # Get tle data from API
        api_url_tle = f' https://api.n2yo.com/rest/v1/satellite/tle/{norad_id}&apiKey={API_KEY}'
        satellite_data = requests.get(api_url_tle).json()

        # Add source key / value
        satellite_data['source'] = 'https://www.n2yo.com/'
        satellite_data['requested_timestamp'] = int(time.time())

        # Flatten tle dictionary
        flattened_satellite_data_tle = flatten_dictionary(satellite_data)

        # Rename keys to match columns in database
        change_dictionary_key_name(flattened_satellite_data_tle, original_key='satname', new_key='name')
        change_dictionary_key_name(flattened_satellite_data_tle, original_key='satid', new_key='id')

        # Split tle into two keys / values
        flattened_satellite_data_tle['line1'] = flattened_satellite_data_tle['tle'].split('\r\n')[0]
        flattened_satellite_data_tle['line2'] = flattened_satellite_data_tle['tle'].split('\r\n')[0]

        # Drop unwanted key/value(s)
        del flattened_satellite_data_tle['transactionscount']
        del flattened_satellite_data_tle['tle']
                
        return flattened_satellite_data_tle
        
def upload_to_s3(data:dict, bucket_name:str, key:str):
    '''_summary_
    Upload fileobject to desired s3 bucket.

    Parameters
    ----------
    data : dict
        _description_
        data to be uploaded. In our case this is
        most likely a json API response.
    bucket_name : str
        _description_
        Desired bucket location to put the fileobj
    key : str
        _description_
        path/name of fileobject. Example (path/filename.json)
    '''

    # Create s3 client
    s3 = boto3.client('s3')
    
    # Write data to json object
    data_as_json_object = json.dumps(data).encode('utf-8')   

    # Write to IO buffer
    fileobj = BytesIO(data_as_json_object)

    # Upload file to s3
    s3.upload_fileobj(fileobj, bucket_name, key)

def lambda_handler(event, context):

    # Get secrets for n2yo API
    n2yo_secrets = get_rds_db_secrets(secret_name='prod/portfolio-website/n2yo_API', region_name='us-east-1')
    
    # Create Database object
    satellite_database = database.PostgreSQL_Database(secret_name='prod/portfolio-website/postgre', region='us-east-1')
    
    # Create query to select all satellites in detail dimension table
    select_query = """
    SELECT
        satellite_id
    FROM raw.satellite_detail_dm;
    """
    
    records = satellite_database.execute_query(
        query=select_query,
        result_retrieval_method=satellite_database.query_result_retrieval_method.FETCH_ALL
    )
    
    returned_satellites = [satellite for sublist in records for satellite in sublist]
    
    for satellite_id in returned_satellites:
        # Get satellite data
        satellite_data = download_satellite_data(n2yo_secrets=n2yo_secrets, norad_id=satellite_id, is_tle=True)
    
        # Get Json schema to validate against
        with open('2y2o_data_validation_schema_tle.json') as file:
            iss_validation_schema = json.load(file)
        
        # Using the above schema, check if our json is valid
        is_valid = json_validation_checker(json_schema=iss_validation_schema,json_to_validate=satellite_data)
        
        print(f"IS DATA VALID?: {is_valid}")
        
        if is_valid:
            # Upload to s3
            upload_to_s3(
                data=satellite_data,
                bucket_name='satellite-tracker',
                key=f'tle/{satellite_data["name"]}-{satellite_data["id"]}-{satellite_data["requested_timestamp"]}.json'
            )
        else:
            pass