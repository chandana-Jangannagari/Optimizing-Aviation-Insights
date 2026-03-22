import os
os.environ["JAVA_HOME"] = r"C:\Program Files\Eclipse Adoptium\jdk-11.0.30.7-hotspot"
os.environ["PATH"] = os.environ["JAVA_HOME"] + r"\bin;" + os.environ["PATH"]

# =========================
# IMPORTS
# =========================


import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# =========================
# JAVA FIX
# =========================
os.environ["JAVA_HOME"] = r"C:\Program Files\Eclipse Adoptium\jdk-11.0.30.7-hotspot"
os.environ["PATH"] = os.environ["JAVA_HOME"] + r"\bin;" + os.environ["PATH"]

print("🚀 Bronze → Silver (WINDOWS FIX)")

# =========================
# AZURE CONFIG
# =========================
ACCOUNT_NAME = "flightaviation"
ACCOUNT_KEY = os.getenv("AZURE_STORAGE_KEY")
CONTAINER = "chandana"

BRONZE = f"wasbs://{CONTAINER}@{ACCOUNT_NAME}.blob.core.windows.net/bronze/"
SILVER = f"wasbs://{CONTAINER}@{ACCOUNT_NAME}.blob.core.windows.net/silver/"

# =========================
# SPARK SESSION
# =========================
spark = SparkSession.builder \
    .appName("Azure-Windows-Pipeline") \
    .master("local[*]") \
    .config("spark.jars.packages",
            "org.apache.hadoop:hadoop-azure:3.3.4,"
            "com.microsoft.azure:azure-storage:8.6.6") \
    .config(f"spark.hadoop.fs.azure.account.key.{ACCOUNT_NAME}.blob.core.windows.net", ACCOUNT_KEY) \
    .config("spark.sql.sources.commitProtocolClass",
            "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol") \
    .getOrCreate()

print("✅ Spark Started")

# =========================
# AIRLINES
# =========================
airlines = spark.read.option("header", True).csv(BRONZE + "airlines.csv")

airlines_clean = airlines.select(
    col("IATA_CODE").alias("airline_code"),
    col("AIRLINE").alias("airline_name")
).dropDuplicates()

airlines_clean.coalesce(1).write.mode("overwrite").parquet(SILVER + "airlines")

print("✅ Airlines Done")

# =========================
# AIRPORTS
# =========================
airports = spark.read.option("header", True).csv(BRONZE + "airports.csv")

airports_clean = airports.select(
    col("IATA_CODE").alias("airport_code"),
    col("AIRPORT").alias("airport_name"),
    col("CITY"),
    col("STATE"),
    col("COUNTRY"),
    col("LATITUDE").cast("float"),
    col("LONGITUDE").cast("float")
).dropDuplicates()

airports_clean.coalesce(1).write.mode("overwrite").parquet(SILVER + "airports")

print("✅ Airports Done")

# =========================
# FLIGHTS
# =========================
flights = spark.read.option("header", True).csv(BRONZE + "flights_batch.csv")

flights_clean = flights.select(
    col("YEAR").cast("int"),
    col("MONTH").cast("int"),
    col("DAY").cast("int"),
    col("AIRLINE"),
    col("ORIGIN_AIRPORT"),
    col("DESTINATION_AIRPORT"),
    col("DEPARTURE_DELAY").cast("float"),
    col("ARRIVAL_DELAY").cast("float"),
    col("DISTANCE").cast("float")
).fillna(0)

flights_clean.coalesce(1).write.mode("overwrite").parquet(SILVER + "flights")

print("🎉 SUCCESS DONE")

spark.stop()