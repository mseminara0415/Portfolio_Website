import boto3
import json
import psycopg2

psycopg2.connect()



class rds_Database():

    def __init__(self, secret_name:str, region:str ) -> None:
        self.secrets_dict = self.get_rds_db_secrets(secret_name=secret_name,region_name=region)
        self.conn = psycopg2.connect(
            host=self.secrets_dict["host"],
            port=self.secrets_dict["port"],
            dbname=self.secrets_dict["dbInstanceIdentifier"],
            user=self.secrets_dict["username"],
            password=self.secrets_dict["password"]
        )

    def get_rds_db_secrets(self, secret_name, region_name) -> dict:
        '''_summary_
        Retrieve rds secrets from AWS and return as a dictionary.

        Parameters
        ----------
        secret_name : _type_
            _description_
            Secrets name as identified in aws
        region_name : _type_
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

    def query(self):
        pass

    def close(self):
        pass

    