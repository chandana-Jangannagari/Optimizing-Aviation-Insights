from pyspark.sql import SparkSession

def create_spark_session(app_name="FlightPipeline"):

    spark = SparkSession.builder \
        .appName(app_name) \
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-azure:3.3.4,"
            "com.microsoft.azure:azure-storage:8.6.6"
        ) \
        .getOrCreate()

    return spark