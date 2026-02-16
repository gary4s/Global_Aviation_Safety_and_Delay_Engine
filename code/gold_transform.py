import os
import sys
from pathlib import Path
from dotenv import load_dotenv
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

#Load credentials
possible_paths = [
    Path.cwd() / '.env',
    Path(__file__).resolve().parent / '.env',
    Path(__file__).resolve().parent.parent / '.env',
    Path('/opt/spark/work-dir/.env') # Direct Docker path
]

env_loaded = False

for path in possible_paths:
    if load_dotenv(dotenv_path=path):
        print("\n" + "=" *40)    
        print(f"\n .env file found and loaded from: {path}")
        print("\n" + "=" *40)
        env_loaded = True
        break

if not env_loaded:
    print("\n" + "=" *40)
    print("\n FATAL ERROR: Could not find .env file in any of these locations:")
    print("\n" + "=" *40)
    for p in possible_paths: print(f"  - {p}")
    sys.exit(1)

STORAGE_ACCOUNT = os.getenv("STORAGE_ACCOUNT")
CLIENT_ID       = os.getenv("CLIENT_ID")
TENANT_ID       = os.getenv("TENANT_ID")
CLIENT_SECRET   = os.getenv("CLIENT_SECRET")

#BUILD THE SPARK SESSION
spark = SparkSession.builder \
    .appName("Aviation_EDA") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-azure:3.3.4,com.microsoft.azure:azure-storage:8.6.6") \
    .config(f"fs.azure.account.auth.type.{STORAGE_ACCOUNT}.dfs.core.windows.net", "OAuth") \
    .config(f"fs.azure.account.oauth.provider.type.{STORAGE_ACCOUNT}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider") \
    .config(f"fs.azure.account.oauth2.client.id.{STORAGE_ACCOUNT}.dfs.core.windows.net", CLIENT_ID) \
    .config(f"fs.azure.account.oauth2.client.secret.{STORAGE_ACCOUNT}.dfs.core.windows.net", CLIENT_SECRET) \
    .config(f"fs.azure.account.oauth2.client.endpoint.{STORAGE_ACCOUNT}.dfs.core.windows.net", f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/token") \
    .getOrCreate()

def run_gold():
    print("=" *40)
    print("Reading Silver data....")
    print("=" *40)

    silver_path = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net/cleaned_flights"
    df_silver = spark.read.parquet(silver_path)

    #Metric 1 - Traffic by country
    gold_countries = df_silver.groupBy("origin_country").agg(
        F.count("icao24").alias("flight_count"),
        F.avg("velocity").alias("avg_speed_ms"),
        F.avg("baro_altitude").alias("avg_altitude_m")
    ).orderBy(F.col("flight_count").desc())

    #Metric 2 - High Flyer Summary
    gold_summary = df_silver.select(
        F.current_timestamp().alias("processed_at"),
        F.count("*").alias("total_global_flights")
    )

    #Write to gold
    print("=" *40)
    print("Writing to gold....")
    print("=" *40)

    gold_base = f"abfss://gold@{STORAGE_ACCOUNT}.dfs.core.windows.net"

    gold_countries.write.mode("overwrite").parquet(f"{gold_base}/country_stats")
    gold_summary.write.mode("overwrite").parquet(f"{gold_base}/daily_summary")

    print("=" *40)
    print("Gold layer complete")
    print("=" *40)

if __name__ == "__main__":
    run_gold()