from utils.spark_session import create_spark_session
from pyspark.sql.functions import col, concat_ws, to_date, lpad, year, month, dayofmonth, dayofweek
from dotenv import load_dotenv
import os

print("Starting Silver → Gold Pipeline")

# =========================
# LOAD ENV VARIABLES
# =========================
load_dotenv()

STORAGE_ACCOUNT = os.getenv("ACCOUNT_NAME")
STORAGE_KEY = os.getenv("ACCOUNT_KEY")
CONTAINER = os.getenv("CONTAINER_NAME")

SILVER_PATH = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/silver/"
GOLD_PATH = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/gold/"

# =========================
# CREATE SPARK SESSION
# =========================
spark = create_spark_session("SilverToGold")

# Ensure Azure access
spark.conf.set(
    f"fs.azure.account.key.{STORAGE_ACCOUNT}.dfs.core.windows.net",
    STORAGE_KEY
)

# =========================
# READ SILVER DATA
# =========================
print("Reading Silver Layer Data")

# Note: Silver data has lowercase column names from our previous cleanup
flights = spark.read.parquet(SILVER_PATH + "flights")
airlines = spark.read.parquet(SILVER_PATH + "airlines")
airports = spark.read.parquet(SILVER_PATH + "airports")

# =========================
# CREATE DIMENSION TABLES
# =========================
print("Creating Dimension Tables")

# DIM AIRLINES: Using 'iata_code' as the unique key
dim_airlines = airlines.select(
    col("iata_code").alias("airline_id"),
    col("airline").alias("airline_name")
).dropDuplicates(["airline_id"])

# DIM AIRPORTS: Using 'iata_code' as the unique key
dim_airports = airports.select(
    col("iata_code").alias("airport_id"),
    col("airport").alias("airport_name"),
    col("city"),
    col("state"),
    col("latitude"),
    col("longitude")
).dropDuplicates(["airport_id"])

# DIM DATE: Derived from the 'flight_date' created in Silver
dim_date = flights.select("flight_date").distinct() \
    .withColumn("year", year(col("flight_date"))) \
    .withColumn("month", month(col("flight_date"))) \
    .withColumn("day", dayofmonth(col("flight_date"))) \
    .withColumn("day_of_week", dayofweek(col("flight_date"))) \
    .dropna(subset=["flight_date"])

# =========================
# CREATE FACT TABLE
# =========================
print("Creating Fact Table")

# We join on airline and origin_airport to ensure the data is referentially sound
fact_flights = flights.select(
    col("flight_date"),
    col("airline").alias("airline_id"),
    col("origin_airport").alias("origin_airport_id"),
    col("destination_airport").alias("dest_airport_id"),
    col("departure_delay"),
    col("arrival_delay"),
    col("distance"),
    col("air_time"),
    col("cancelled"),
    col("diverted")
)

# =========================
# WRITE TO GOLD LAYER
# =========================
print("Writing to Gold Layer (Star Schema)")

# Using 'overwrite' to ensure fresh dimensions and facts
dim_airlines.write.mode("overwrite").parquet(GOLD_PATH + "dim_airlines")
dim_airports.write.mode("overwrite").parquet(GOLD_PATH + "dim_airports")
dim_date.write.mode("overwrite").parquet(GOLD_PATH + "dim_date")
fact_flights.write.mode("overwrite").parquet(GOLD_PATH + "fact_flights")

print("Silver → Gold Pipeline Completed Successfully")

spark.stop()