import snowflake.connector
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

def load_to_snowflake():
    try:
        # Read CSV first
        csv_path = "ingestion/matches_raw.csv"
        df = pd.read_csv(csv_path)
        print(f"✅ CSV loaded — {len(df)} matches found")
        
        # Connect to Snowflake
        conn = snowflake.connector.connect(
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            role='ACCOUNTADMIN',
            warehouse='PL_WH'
        )
        
        cur = conn.cursor()
        print("✅ Connected to Snowflake!")

        # Setup database and schema
        cur.execute("CREATE DATABASE IF NOT EXISTS PL_PIPELINE")
        cur.execute("USE DATABASE PL_PIPELINE")
        cur.execute("CREATE SCHEMA IF NOT EXISTS RAW")
        cur.execute("USE SCHEMA RAW")
        print("✅ Database and schema ready")

        # Create table
        cur.execute("""
            CREATE OR REPLACE TABLE RAW_MATCHES (
                match_id INT,
                date STRING,
                matchday INT,
                home_team STRING,
                away_team STRING,
                home_goals INT,
                away_goals INT,
                status STRING,
                venue STRING,
                referee STRING
            )
        """)
        print("✅ Table created")

        # Upload CSV
        file_path = os.path.abspath(csv_path)
        print(f"Uploading {file_path}...")
        cur.execute(f"PUT file://{file_path} @%RAW_MATCHES OVERWRITE = TRUE")
        
        # Copy data
        print("Copying data into table...")
        cur.execute("""
            COPY INTO RAW_MATCHES
            FROM @%RAW_MATCHES
            FILE_FORMAT = (
                TYPE = CSV 
                PARSE_HEADER = TRUE 
                FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            )
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            PURGE = TRUE
        """)
        
        # Verify
        result = cur.execute("SELECT COUNT(*) FROM RAW_MATCHES").fetchone()
        print(f"✅ SUCCESS! {result[0]} rows loaded to Snowflake")
        
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    load_to_snowflake()