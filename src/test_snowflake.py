import os

import snowflake.connector
from dotenv import load_dotenv


load_dotenv()

connection = snowflake.connector.connect(
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    role=os.getenv("SNOWFLAKE_ROLE"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    schema=os.getenv("SNOWFLAKE_SCHEMA"),
)

cursor = connection.cursor()

cursor.execute(
    "SELECT CURRENT_USER(), CURRENT_DATABASE(), CURRENT_SCHEMA()"
)

print(cursor.fetchone())

cursor.close()
connection.close()