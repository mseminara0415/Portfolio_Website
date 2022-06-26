import traceback
from psycopg2 import sql
from psycopg2 import errorcodes
from psycopg2 import errors
import psycopg2
import postgre_db
import sys
import re

testing = {
    "name": "iss",
    "id": 25544,
    "latitude": -29.787590705794,
    "longitude": 74.603349955916,
    "altitude": 264.59285942623,
    "velocity": 17125.544516521,
    "visibility": "eclipsed",
    "footprint": 2819.1357683387,
    "timestamp": 1655077139,
    "daynum": 2459743.4854051,
    "solar_lat": 23.19555500706,
    "solar_lon": 185.25272450007,
    "units": "miles",
    "testing": 123,
    "example_string": "helloTESTING"
}

portfolio_website_db = postgre_db.PostgreSQL_Database(secret_name='prod/portfolio-website/postgre', region='us-east-1')
portfolio_website_db_conn = portfolio_website_db.conn

example_columns = [k for k in testing.keys()]
example_values = [v for v in testing.values()]

arguments = [
              tuple(example_values)
      ]

insert_data_query = sql.SQL("INSERT INTO raw.satellite_position_ft ({}) VALUES ({})").format(
    sql.SQL(', ').join(map(sql.Identifier, example_columns)),
    sql.SQL(', ').join(sql.Placeholder() * len(example_values))
).as_string(portfolio_website_db_conn)

try:
    portfolio_website_db.execute_query(
        query=insert_data_query,
        argslist=arguments
    )
except errors.UndefinedColumn as e:

    # Get error message          
    err_type, err_object, traceback = sys.exc_info()

    # Find column name that caused the UndefinedColumn error
    column_patter = re.compile('(?<=column\s")(\w+)')
    new_column = str(re.search(pattern=column_patter,string=str(err_object)).group())
    new_column_type = type(testing[new_column]).__name__

    # print(f"The new column is: {new_column}")
    # print(f"new column datatype is: {new_column_type}")
    # print(f"new column datatype type is: {type(new_column_type)}")
    # print(f"new column datatype type is: {type(new_column)}")

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
        

    
