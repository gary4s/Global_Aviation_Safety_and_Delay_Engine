from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime, current_timestamp, trim
import os
from dotenv import load_dotenv


# Config
STORAGE_ACCOUNT = os.getenv("STORAGE_ACCOUNT")
CLIENT_ID       = os.getenv("CLIENT_ID")
TENANT_ID       = os.getenv("TENANT_ID")
CLIENT_SECRET   = os.getenv("CLIENT_SECRET")

spark = SparkSession.builder \
    .appName("Aviation_Silver_Transformation") \
    .config(f"fs.azure.account.auth.type.{STORAGE_ACCOUNT}.dfs.core.windows.net", "OAuth") \
    .config(f"fs.azure.account.oauth.provider.type.{STORAGE_ACCOUNT}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider") \
    .config(f"fs.azure.account.oauth2.client.id.{STORAGE_ACCOUNT}.dfs.core.windows.net", CLIENT_ID) \
    .config(f"fs.azure.account.oauth2.client.secret.{STORAGE_ACCOUNT}.dfs.core.windows.net", CLIENT_SECRET) \
    .config(f"fs.azure.account.oauth2.client.endpoint.{STORAGE_ACCOUNT}.dfs.core.windows.net", f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/token") \
    .getOrCreate()    

# Read data from bronze

bronze_path = f"abfss://broze@{STORAGE_ACCOUNT}.dfs.core.windows.net/raw_flights"
print("=" *40 + "\n")
print(f"Reading data from: {bronze_path}")
print("=" *40 + "\n")

df_bronze = spark.read.parquet(bronze_path)

# transformations

df_silver = df_bronze \
    .filter(col("callsign").isNotNull()) \
    .withColumn("callsign", trim(col("callsign"))) \
    .withColumn("flight_time", from_unixtime(col("time_position"))) \
    .withColumn("processed_at", current_timestamp()) \
    .dropDuplicates(["icao24", "time_position"])

# Write to silver

silver_path = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net/cleaned_flights"

print("=" *40 + "\n")
print(f"Writing cleaned data to: {silver_path}")
print("=" *40 + "\n")

df_silver.write.mode("overwrite").parquet(silver_path)

print("=" *40 + "\n")
print("Silver Success: Data cleaned and stored")
print("=" *40 + "\n")

spark.stop()