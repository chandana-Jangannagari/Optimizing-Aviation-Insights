from utils.spark_session import create_spark_session
from pyspark.sql.functions import col, count, when, isnan
from pyspark.sql.types import NumericType
from dotenv import load_dotenv
import os

# =========================
# CONFIGURATION
# =========================
load_dotenv()
STORAGE_ACCOUNT = os.getenv("ACCOUNT_NAME")
STORAGE_KEY = os.getenv("ACCOUNT_KEY")
CONTAINER = os.getenv("CONTAINER_NAME")
GOLD_PATH = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/gold/"

# Initialize Spark
spark = create_spark_session("GoldLayerValidation")
spark.conf.set(f"fs.azure.account.key.{STORAGE_ACCOUNT}.dfs.core.windows.net", STORAGE_KEY)

# Disable ANSI to prevent the script from crashing if it encounters 
# a malformed string while checking for NaNs
spark.conf.set("spark.sql.ansi.enabled", "false")

def perform_validation(df, table_name, primary_keys):
    """
    Checks for:
    1. Total row count
    2. Duplicates based on Primary Keys
    3. Nulls/NaNs across all columns (Type-aware)
    """
    print(f"\n{'='*20}")
    print(f"VALIDATING TABLE: {table_name.upper()}")
    print(f"{'='*20}")

    # --- 1. COUNT CHECK ---
    total_rows = df.count()
    print(f"Total Row Count: {total_rows}")
    if total_rows == 0:
        print(f"❌ FAILED: {table_name} is empty!")
        return

    # --- 2. DUPLICATE CHECK ---
    # Ensure all specified primary keys exist in this specific dataframe
    actual_pks = [pk for pk in primary_keys if pk in df.columns]
    
    unique_rows = df.dropDuplicates(actual_pks).count()
    duplicate_count = total_rows - unique_rows
    
    if duplicate_count > 0:
        print(f"❌ FAILED: Found {duplicate_count} duplicates based on keys {actual_pks}")
    else:
        print(f"✅ PASSED: No duplicates found.")

    # --- 3. NULL & NaN CHECK ---
    # Logic: Only check for NaNs on Numeric columns to avoid Casting Errors
    check_exprs = []
    for field in df.schema.fields:
        # Check for standard NULLs
        condition = col(field.name).isNull()
        
        # Check for NaNs only if column is Numeric (Double/Float/Int)
        if isinstance(field.dataType, NumericType):
            condition = condition | isnan(col(field.name))
            
        check_exprs.append(count(when(condition, field.name)).alias(field.name))

    null_results = df.select(check_exprs).collect()[0].asDict()
    
    # Filter only columns that have at least 1 null/NaN
    failed_columns = {col_name: val for col_name, val in null_results.items() if val > 0}
    
    if failed_columns:
        print(f"⚠️  WARNING: Null/NaN values detected: {failed_columns}")
    else:
        print(f"✅ PASSED: No Null or NaN values found.")

# =========================
# EXECUTE VALIDATIONS
# =========================

try:
    # 1. DIM_AIRLINES
    print("\n[1/4] Reading dim_airlines...")
    dim_airlines = spark.read.parquet(GOLD_PATH + "dim_airlines")
    perform_validation(dim_airlines, "dim_airlines", ["airline_id"])

    # 2. DIM_AIRPORTS
    print("\n[2/4] Reading dim_airports...")
    dim_airports = spark.read.parquet(GOLD_PATH + "dim_airports")
    perform_validation(dim_airports, "dim_airports", ["airport_id"])

    # 3. DIM_DATE
    print("\n[3/4] Reading dim_date...")
    dim_date = spark.read.parquet(GOLD_PATH + "dim_date")
    perform_validation(dim_date, "dim_date", ["flight_date"])

    # 4. FACT_FLIGHTS
    print("\n[4/4] Reading fact_flights...")
    fact_flights = spark.read.parquet(GOLD_PATH + "fact_flights")
    # For a flight fact table, a "primary key" is usually the combination of date and airline
    perform_validation(fact_flights, "fact_flights", ["flight_date", "airline_id", "origin_airport_id"])

except Exception as e:
    print(f"❌ DATA LOAD ERROR: {str(e)}")

print("\nValidation Suite Completed.")
spark.stop()