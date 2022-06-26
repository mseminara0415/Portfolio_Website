import boto3
import json
import psycopg2
from psycopg2 import sql, errors
from lib import database
import re
import sys
import traceback

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

def lambda_handler(event, context):
    
    portfolio_website_db = database.PostgreSQL_Database(secret_name='prod/portfolio-website/postgre', region='us-east-1')
    portfolio_website_db_conn = portfolio_website_db.conn
    
    for record in event['Records']:
      
      # Parse EventBridge message
      body_info = record["body"]
      body_json = json.loads(body_info)
      
      # Get bucket and key from event
      s3_bucket = body_json["detail"]["bucket"]["name"]
      s3_key = body_json["detail"]["object"]["key"]
      
      # Get s3 fileobject data
      json_data = get_s3_object_data(bucket_name=s3_bucket, key=s3_key)
    
      # Parse SQS message and return list of tuple(s). Example [(column_1, column_2, ...)]
      columns = [k for k in json_data.keys()]
      values = [v for v in json_data.values()]
      arguments = [
              tuple([v for v in json_data.values()])
      ]
      
      # Create query to insert data into Postgre DB
      insert_data_query = sql.SQL("INSERT INTO raw.satellite_position_ft ({}) VALUES ({})").format(
          sql.SQL(', ').join(map(sql.Identifier, columns)),
          sql.SQL(', ').join(sql.Placeholder() * len(values))
      ).as_string(portfolio_website_db_conn) 
      
      try:
          # Try inserting original query
          portfolio_website_db.execute_query(
              query=insert_data_query,
              argslist=arguments
          )
      except errors.UndefinedColumn:
          # Get error message          
          err_type, err_object, traceback = sys.exc_info()
            
          # Find column name that caused the UndefinedColumn error
          column_patter = re.compile('(?<=column\s")(\w+)')
          new_column = str(re.search(pattern=column_patter,string=str(err_object)).group())
          new_column_type = type(json_data[new_column]).__name__
            
          # Python to Postgre datatypes
          python_to_postgre_datatype_mapping = {
              "str": "varchar",
              "int": "numeric",
              "float": "number",
              "bool": "boolean",
              "None": "null"
          }
            
          # Convert Python datatype to Postgre
          new_column_datatype = python_to_postgre_datatype_mapping[new_column_type]
            
          try:
              # Create query to add new column that threw the error
              insert_column_query = sql.SQL("ALTER TABLE raw.satellite_position_ft ADD COLUMN {} {}").format(
                  sql.Identifier(new_column),
                  sql.Identifier(new_column_datatype)
              ).as_string(portfolio_website_db_conn)
        
              # Run query to add column
              portfolio_website_db.execute_query(
                  query=insert_column_query
              )
        
              # Run original query to insert data
              portfolio_website_db.execute_query(
                  query=insert_data_query,
                  argslist=arguments
              )
          except errors.UndefinedObject:
              pass