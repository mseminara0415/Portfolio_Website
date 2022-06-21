import boto3
import json
import psycopg2
from enum import Enum

class rds_pg_database():

    class query_result_retrieval_method(Enum):
        FETCH_NONE = 0
        FETCH_ONE = 1
        FETCH_MANY = 2
        FETCH_ALL = 3

    def __init__(self, secret_name:str, region:str, read_only:bool = False):

        # Get DB connection secrets from aws secrets manager
        self.secrets_dict = self.get_rds_db_secrets(secret_name=secret_name,region_name=region)

        # Create connection to RDS PostgreSQL DB
        self.conn = psycopg2.connect(
            host=self.secrets_dict["host"],
            port=self.secrets_dict["port"],
            dbname=self.secrets_dict["dbInstanceIdentifier"],
            user=self.secrets_dict["username"],
            password=self.secrets_dict["password"]
        )

    def get_rds_db_secrets(self, secret_name:str, region_name:str) -> dict:
        '''_summary_
        Retrieve rds secrets from AWS and return as a dictionary.

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
            **username**
            **password**
            **engine**
            **host**
            **port**
            **dbInstanceIdentifier** (database name)
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

    def execute_query(
        self,
        query: str,
        argslist:list = None,
        result_retrieval:query_result_retrieval_method = query_result_retrieval_method.FETCH_NONE,
        fetch_many_record_count:int = None,
        ):

        with self.conn as conn:
            with conn.cursor() as cursor:
                # If we are passing variables
                if argslist is not None:
                    cursor.executemany(query,argslist)
                # If we are not passing variables
                else:                    
                    cursor.execute(query)

                # Result retrieval methods
                if self.query_result_retrieval_method.FETCH_NONE:
                    pass

                elif self.query_result_retrieval_method.FETCH_ONE:
                    cursor.fetchone()

                elif self.query_result_retrieval_method.FETCH_MANY:
                    # For the fetchmany method, the default result size is 1, unless otherwise specificed
                    if fetch_many_record_count is None:
                        cursor.fetchmany(size=cursor.arraysize)
                    else:
                        cursor.fetchmany(size=fetch_many_record_count)

                elif self.query_result_retrieval_method.FETCH_ALL:
                    cursor.fetchall()

    def close(self):
        '''_summary_
        Closes database connection.
        '''
        self.conn.close()


portfolio_website = rds_pg_database(secret_name='prod/portfolio-website/postgre',region='us-east-1')
# portfolio_website.query(
#     """
#     CREATE TABLE IF NOT EXISTS testing (
#         username varchar(45) NOT NULL,
#         password varchar(45) NOT NULL,
#         enabled integer NOT NULL DEFAULT '1',
#         PRIMARY KEY (username)
#     )
#     """
# )

# portfolio_website.query(
#     '''INSERT INTO testing (username, password, enabled) VALUES (%s, %s, %s);'''
#     ,[("HELLOA???","WHOOOAOAOAOAA","65494")]
# )

portfolio_website.execute_query(
    """
    CREATE TABLE IF NOT EXISTS pw_iss_position
    """
)
