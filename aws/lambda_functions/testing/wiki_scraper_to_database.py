import boto3
import itertools
import json
import psycopg2
from psycopg2 import sql
from lib import database

def get_s3_object_data(bucket_name:str, key:str) -> dict:
    '''_summary_
    Get contents from s3 json file.

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

def flatten(list_to_flatten:list) -> list:
    '''_summary_
    Flatten a list of lists.

    Parameters
    ----------
    list_to_flatten : list
        _description_

    Returns
    -------
    _type_
        _description_
    '''
    flattened_list = []
    for item in list_to_flatten:

        # If item in list is of type list, if so, extend the list, else append the item
        if isinstance(item, list):
            flattened_list.extend(flatten(item))
        else:
            flattened_list.append(item)
    return flattened_list

def get_all_keys(dictionary_to_search:dict) -> list:
    '''_summary_
    Recursivly go through a dictionary and extract all keys

    Parameters
    ----------
    dictionary_to_search : dict
        _description_

    Returns
    -------
    _type_
        _description_
    '''
    # Final Key List
    key_list = []

    for outer_key, outer_value in dictionary_to_search.items():
        # Append all outer keys
        key_list.append(outer_key)

        # Using recursion, check if the outer value is of type dictionary
        if isinstance(outer_value, dict):
            key_list.append(get_all_keys(outer_value))

    # flatten any potential list of lists
    flattend_key_list = flatten(key_list)

    return flattend_key_list

def find_item(satellite_obj, key) -> bool:
    '''_summary_
    Recursively search dictionary for the provided key, if found, return the value.
    Parameters
    ----------
    satellite_obj : _type_
        _description_
        Dictionary to search through

    key : _type_
        _description_
        Key to search dictionary for

    Returns
    -------
    bool
        _description_
        If the key is found this returns the value, if not, returns False
    '''
    if key in satellite_obj:
        return satellite_obj[key]
    for v in filter(dict.__instancecheck__, satellite_obj.values()):
        if (found := find_item(v, key)) is not None:  
            return found

def lambda_handler(event, context):
    
    portfolio_website_db = database.PostgreSQL_Database(secret_name='prod/portfolio-website/postgre', region='us-east-1')
    portfolio_website_db_conn = portfolio_website_db.conn    

    wiki_to_db_column_map ={
        'name':'name',
        'description':'description',
        'application':'application',
        'operator':'operator',
        'cospar_id':'cospar_id',
        'call_sign':'call_sign',
        'manufacturer':'manufacturer',
        'mission_duration':'mission_duration',
        'mission_type':'mission_type',
        'spacecraft_type':'spacecraft_type',
        'satcat_number':'satellite_id',
        'bus': 'satellite_bus',
        'launch_mass':'launch_mass_lbs',
        'launch_date':'launch_date',
        'launch_site':'launch_site',
        'reference_system':'reference_system',
        'rocket':'rocket',
        'website':'website',
        'wikipedia_link':'wikipedia_link'
    }

    # Parse EventBridge message
    for record in event['Records']:
        body_info = record["body"]
        body_json = json.loads(body_info)
      
        # Get bucket and key from event
        s3_bucket = body_json["detail"]["bucket"]["name"]
        s3_key = body_json["detail"]["object"]["key"]
        
        # Get s3 fileobject data
        json_data = get_s3_object_data(bucket_name=s3_bucket, key=s3_key)

        for satellite in json_data:
            
            # Check if satellite has the necessary primary key
            satellite_has_primary_key = find_item(satellite, 'satcat_number')

            if satellite_has_primary_key:
        
                # Parse SQS message and return list of tuple(s). Example [(column_1, column_2, ...)]
                db_columns = [column for column in wiki_to_db_column_map.values()]
                db_values = [find_item(satellite, k) for k in wiki_to_db_column_map.keys()]
                excluded_columns = ['excluded' for item in db_values]
                db_columns_without_primary = [column for column in db_columns if column != 'satellite_id']
                arguments = [
                        tuple(db_values)
                ]
            
                # Create query to insert data into Postgre DB
                insert_data_query = sql.SQL(
                    """
                    INSERT INTO raw.satellite_detail_dm ({})
                    VALUES ({})
                    ON CONFLICT (satellite_id)
                    DO UPDATE SET ({}) = ({})
                    """
                    ).format(
                    sql.SQL(', ').join(map(sql.Identifier, db_columns)),
                    sql.SQL(', ').join(sql.Placeholder() * len(db_values)),
                    sql.SQL(', ').join(map(sql.Identifier, db_columns_without_primary)),
                    sql.SQL(', ').join(map(sql.Identifier, excluded_columns, db_columns_without_primary))
                ).as_string(portfolio_website_db_conn)     
            
                # Insert data
                portfolio_website_db.execute_query(
                    query=insert_data_query,
                    argslist=arguments
                )
            else:
                pass
                