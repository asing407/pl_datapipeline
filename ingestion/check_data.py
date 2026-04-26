import snowflake.connector
import os
from dotenv import load_dotenv

load_dotenv()

conn = snowflake.connector.connect(
    user=os.getenv('SNOWFLAKE_USER'),
    password=os.getenv('SNOWFLAKE_PASSWORD'),
    account=os.getenv('SNOWFLAKE_ACCOUNT'),
    warehouse='PL_WH'
)

cur = conn.cursor()
cur.execute("USE DATABASE PL_PIPELINE")
cur.execute("USE SCHEMA RAW")
cur.execute("DESCRIBE TABLE RAW_MATCHES")

print("Column names:")
for row in cur:
    print(f"  {row[0]}")

conn.close()