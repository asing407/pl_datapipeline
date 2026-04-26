import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

load_dotenv()

# PostgreSQL connection
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "pl_pipeline")

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

def load_matches():
    # Read CSV
    df = pd.read_csv("ingestion/matches_raw.csv")
    print(f"✅ CSV loaded — {len(df)} matches found")
    
    # Connect to PostgreSQL
    engine = create_engine(DATABASE_URL)
    
    # Truncate table first (doesn't drop it)
    with engine.connect() as conn:
        conn.execute(text("TRUNCATE TABLE raw_matches"))
        conn.commit()
        print("✅ Table truncated")
    
    # Load fresh data
    df.to_sql(
        name="raw_matches",
        con=engine,
        if_exists="append",
        index=False
    )
    
    print(f"✅ {len(df)} matches loaded to PostgreSQL — raw_matches table")
    
    # Verify
    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM raw_matches"))
        count = result.scalar()
        print(f"✅ Verified — {count} rows in database")

if __name__ == "__main__":
    print("=" * 50)
    print("Loading matches to PostgreSQL")
    print("=" * 50 + "\n")
    load_matches()