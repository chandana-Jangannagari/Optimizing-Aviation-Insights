import snowflake.connector
from dotenv import load_dotenv
import os

# -------------------------------
# 1. Load Environment Variables
# -------------------------------
load_dotenv()

SF_USER = os.getenv("SF_USER", "USERNAEM")
SF_PASS = os.getenv("SF_PASS", "PASSWORD")
SF_ACCOUNT = os.getenv("SF_ACCOUNT", "YOUR -ACCOUNTNAME")

# -------------------------------
# 2. Connect to Snowflake
# -------------------------------
conn = snowflake.connector.connect(
    user=SF_USER,
    password=SF_PASS,
    account=SF_ACCOUNT,
    role='ACCOUNTADMIN'
)

cur = conn.cursor()

try:
    print("🚀 Starting Snowflake setup & ingestion...")

    # -------------------------------
    # 3. Create Warehouse, DB, Schema (SAFE SETUP)
    # -------------------------------
    cur.execute("""
    CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
    WITH WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE;
    """)

    cur.execute("CREATE DATABASE IF NOT EXISTS AVIATION_DB;")
    cur.execute("CREATE SCHEMA IF NOT EXISTS AVIATION_DB.PUBLIC;")

    # -------------------------------
    # 4. Set Context (VERY IMPORTANT)
    # -------------------------------
    cur.execute("USE WAREHOUSE COMPUTE_WH;")
    cur.execute("USE DATABASE AVIATION_DB;")
    cur.execute("USE SCHEMA PUBLIC;")

    print("✅ Warehouse, Database, Schema ready.")

    # -------------------------------
    # 5. Create External Stage (Azure)
    # -------------------------------
    cur.execute("""
    CREATE OR REPLACE STAGE gold_azure_stage
    URL = 'azure://flightaviation.blob.core.windows.net/sowmya/gold/'
    CREDENTIALS = (
        AZURE_SAS_TOKEN = '?sp=rwl&st=2026-03-17T08:39:31Z&se=2026-04-04T16:54:31Z&spr=https&sv=2024-11-04&sr=c&sig=Lzg066sEmZS19avCCWVuNZKw6LkYEcD8yxuyfqLxy34%3D'
    )
    FILE_FORMAT = (TYPE = 'PARQUET');
    """)

    print("✅ Azure Gold Stage created.")

    # -------------------------------
    # 6. Create Tables
    # -------------------------------

    cur.execute("""
    CREATE OR REPLACE TABLE dim_airlines (
        airline_id STRING,
        airline_name STRING
    );
    """)

    cur.execute("""
    CREATE OR REPLACE TABLE dim_airports (
        airport_id STRING,
        airport_name STRING,
        city STRING,
        state STRING,
        latitude DOUBLE,
        longitude DOUBLE
    );
    """)

    cur.execute("""
    CREATE OR REPLACE TABLE dim_date (
        flight_date DATE,
        year INT,
        month INT,
        day INT,
        day_of_week INT
    );
    """)

    cur.execute("""
    CREATE OR REPLACE TABLE fact_flights (
        flight_date DATE,
        airline_id STRING,
        origin_airport_id STRING,
        dest_airport_id STRING,
        departure_delay DOUBLE,
        arrival_delay DOUBLE,
        distance DOUBLE,
        air_time DOUBLE,
        cancelled INT,
        diverted INT
    );
    """)

    print("✅ Tables created.")

    # -------------------------------
    # 7. Load Data (COPY INTO)
    # -------------------------------
    load_mapping = {
        "dim_airlines": "dim_airlines",
        "dim_airports": "dim_airports",
        "dim_date": "dim_date",
        "fact_flights": "fact_flights"
    }

    for folder, table in load_mapping.items():
        print(f"📦 Loading {table}...")

        cur.execute(f"""
        COPY INTO {table}
        FROM @gold_azure_stage/{folder}/
        FILE_FORMAT = (TYPE = 'PARQUET')
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
        ON_ERROR = 'CONTINUE';
        """)

        cur.execute(f"SELECT COUNT(*) FROM {table};")
        count = cur.fetchone()[0]

        print(f"✅ {table} loaded successfully → {count} rows")

    print("🎉 Data ingestion completed successfully!")

# -------------------------------
# 8. Error Handling
# -------------------------------
except Exception as e:
    print(f"❌ Error occurred: {str(e)}")

# -------------------------------
# 9. Cleanup
# -------------------------------
finally:
    cur.close()
    conn.close()
    print("🔒 Snowflake connection closed.")