from aws.lambda_functions.database import PostgreSQL_Database
from psycopg2 import sql


pg_db = PostgreSQL_Database(secret_name='prod/portfolio-website/postgre', region='us-east-1')
pg_db_conn = pg_db.conn

example_query = sql.SQL("""
SELECT
    satellite_id
FROM raw.satellite_detail_dm;
""").as_string(pg_db_conn)


test = pg_db.execute_query(
    query=example_query,
    result_retrieval_method=pg_db.query_result_retrieval_method.FETCH_ALL
)

example_list = [item for sublist in test for item in sublist]
print(example_list)
