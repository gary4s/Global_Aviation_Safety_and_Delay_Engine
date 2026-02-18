import os
import sys
from pathlib import Path
from dotenv import load_dotenv
from pyspark.sql import SparkSession, Window
import pyspark.sql.functions as F
from azure.identity import ClientSecretCredential
from azure.keyvault.secrets import SecretClient

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

VAULT_URL = f"https://aviation-vault-gary.vault.azure.net/"

try:
    credential = ClientSecretCredential(
        tenant_id=TENANT_ID,
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET
    )

    client = SecretClient(vault_url=VAULT_URL, credential=credential)

    #fetch secret from the vault instead of using local .env file
    VAULT_CLIENT_SECRET = client.get_secret("CLIENT-SECRET").value
    print("\n[SUCCESS] Connected to Key Vault and retrieved secrets.")

except Exception as e:
    print("\n" + "!"*40)
    print(f"KEY VAULT ERROR: {str(e)}") # Using str(e) ensures the full error prints
    print("!"*40)
    sys.exit(1)

#BUILD THE SPARK SESSION
spark = SparkSession.builder \
    .appName("Aviation_Gold_Transformation") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-azure:3.3.4,com.microsoft.azure:azure-storage:8.6.6") \
    .config(f"spark.hadoop.fs.azure.account.auth.type.{STORAGE_ACCOUNT}.dfs.core.windows.net", "OAuth") \
    .config(f"spark.hadoop.fs.azure.account.oauth.provider.type.{STORAGE_ACCOUNT}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider") \
    .config(f"spark.hadoop.fs.azure.account.oauth2.client.id.{STORAGE_ACCOUNT}.dfs.core.windows.net", CLIENT_ID) \
    .config(f"spark.hadoop.fs.azure.account.oauth2.client.secret.{STORAGE_ACCOUNT}.dfs.core.windows.net", VAULT_CLIENT_SECRET) \
    .config(f"spark.hadoop.fs.azure.account.oauth2.client.endpoint.{STORAGE_ACCOUNT}.dfs.core.windows.net", f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/token") \
    .getOrCreate()

def run_gold():
    print("=" *40)
    print("Reading Silver data....")
    print("=" *40)

    # Create a reference list of major airports
    airports_data = [
    ("LHR", 51.4700, -0.4543, "London Heathrow"),
    ("JFK", 40.6413, -73.7781, "New York JFK"),
    ("DXB", 25.2532, 55.3657, "Dubai International"),
    ("SIN", 1.3644, 103.9915, "Singapore Changi")
    ]

    airports_df = spark.createDataFrame(airports_data, ["code", "lat_ref", "lon_ref", "airport_name"])

    silver_path = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net/cleaned_flights"
    df_silver = spark.read.parquet(silver_path)

    # Metric 1. WINDOW FUNCTION: Track altitude changes per aircraft over time
    # This requires looking at the Silver history
    window_spec = Window.partitionBy("icao24").orderBy("flight_time")

    gold_flight = df_silver.withColumn("prev_alt", F.lag("baro_altitude").over(window_spec)) \
        .withColumn("flight_phase", 
            F.when(F.col("prev_alt").isNull(), "Initializing")
             .when(F.col("baro_altitude") > F.col("prev_alt") + 15, "Climbing")
             .when(F.col("baro_altitude") < F.col("prev_alt") - 15, "Descending")
             .otherwise("Cruising")
        )\
        .withColumn("processed_at", F.current_timestamp())

    # Metric 2. GEOFENCING: Define airport Coordinates
    # Join every flight with every airport to calculate distances
    # Warning: Only do this with a small list of airports!
    df_joined = df_silver.crossJoin(F.broadcast(airports_df))

    # Calculate distance to EACH airport in the list
    df_dist = df_joined.withColumn("dist", 
       F.sqrt(F.pow(F.col("latitude") - F.col("lat_ref"), 2) + 
              F.pow(F.col("longitude") - F.col("lon_ref"), 2))
    )

    # Filter for planes within the "Zone" (0.2 degrees is ~22km)
    # Then determine which airport they are actually near
    df_nearby = df_dist.filter(F.col("dist") < 0.2) \
       .withColumn("proximity_status", 
           F.when(F.col("baro_altitude") < 3000, "Landing/Taking Off")
            .otherwise("Passing Over"))

    # Final analytical table
    gold_distance = df_nearby.select(
       "icao24", "callsign", "airport_name", "dist", "proximity_status", "flight_time", "processed_at"
    )

    # Metric 3. Aggregations (Updating your existing metrics)
    gold_countries = gold_flight.groupBy("origin_country", "processed_at").agg(
        F.count("icao24").alias("flight_count"),
        F.avg("velocity").alias("avg_speed_ms"),
        F.count(F.when(F.col("flight_phase") == "Climbing", 1)).alias("count_climbing")
    ).orderBy(F.col("flight_count").desc())

    # Metric 4 - High Flyer Summary
    gold_summary = df_silver.select(
        F.current_timestamp().alias("processed_at"),
        F.count("*").alias("total_global_flights")
    )

    #Write to gold
    print("=" *40)
    print("Writing to gold....")
    print("=" *40)

    gold_base = f"abfss://gold@{STORAGE_ACCOUNT}.dfs.core.windows.net"

    gold_countries.write.mode("append").parquet(f"{gold_base}/country_stats")
    gold_summary.write.mode("overwrite").parquet(f"{gold_base}/daily_summary")
    gold_flight.write.mode("append").parquet(f"{gold_base}/flight_analytics")
    gold_distance.write.mode("append").parquet(f"{gold_base}/airport_distance")

    print("=" *40)
    print("Gold layer complete")
    print("=" *40)

if __name__ == "__main__":
    run_gold()