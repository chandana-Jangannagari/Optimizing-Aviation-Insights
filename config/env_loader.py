# Azure Storage Config

STORAGE_ACCOUNT = "yourstorageaccount"
STORAGE_KEY = "your-storage-key"

BRONZE_CONTAINER = "bronze"
SILVER_CONTAINER = "silver"

BRONZE_PATH = f"abfss://{BRONZE_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/"
SILVER_PATH = f"abfss://{SILVER_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/"