import os
import sys
import requests
from pathlib import Path # Required for the Path() call
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType, LongType

# --- CONFIG LOADING ---
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
CONTAINER       = "bronze"

# Safety check: Stop if variables are still empty
if not all([STORAGE_ACCOUNT, CLIENT_ID, TENANT_ID, CLIENT_SECRET]):
    print("=" *40 + "\n")
    print("ERROR: One or more environment variables are missing. Check .env keys.")
    print("=" *40 + "\n")
    sys.exit(1)

def main():
    #initialise spark with azure drivers
    #these act as a handshake between spark and azure data lake
    spark = SparkSession.builder\
        .appName("Aviation_Bronze_Ingestion") \
        .master("local[*]") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-azure:3.3.4,com.microsoft.azure:azure-storage:8.6.6") \
        .config(f"fs.azure.account.auth.type.{STORAGE_ACCOUNT}.dfs.core.windows.net", "OAuth") \
        .config(f"fs.azure.account.oauth.provider.type.{STORAGE_ACCOUNT}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider") \
        .config(f"fs.azure.account.oauth2.client.id.{STORAGE_ACCOUNT}.dfs.core.windows.net", CLIENT_ID) \
        .config(f"fs.azure.account.oauth2.client.secret.{STORAGE_ACCOUNT}.dfs.core.windows.net", CLIENT_SECRET) \
        .config(f"fs.azure.account.oauth2.client.endpoint.{STORAGE_ACCOUNT}.dfs.core.windows.net", f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/token") \
        .getOrCreate()
    
    print("\n" + "=" *40)
    print("Step 1: Fetching Live Data from OpenSky....")
    print("="*40 + "\n")

    #Fetch live data 
    url = "https://opensky-network.org/api/states/all"
    try:
        response = requests.get(url, timeout = 15)
        response.raise_for_status()
        #the api returns states as a list of lists
        raw_data = response.json().get('states', [])

        #take a 200 slice to keep the test quick
        sample_data = raw_data[:200]

        clean_data = []

        for row in sample_data:
            new_row = list(row)
            # These are the column indices for long, lat, velocity, etc.
            # We force them to be floats to avoid the Int vs Double conflict
            for i in [5, 6, 7, 9, 10, 11, 13]:
                if new_row[i] is not None:
                    new_row[i] = float(new_row[i])
            clean_data.append(tuple(new_row))

        print("="*40 + "\n")
        print(f"Successfully pulled {len(clean_data)} flights.")
        print("="*40 + "\n")

    except Exception as e:
        print("="*40 + "\n")
        print(f"API Error: {e}")
        print("="*40 + "\n")
        spark.stop()
        sys.exit(1)

    #create dataframe with explicit schema
    schema = StructType([
        StructField("icao24", StringType(), True),
        StructField("callsign", StringType(), True),
        StructField("origin_country", StringType(), True),
        StructField("time_position", LongType(), True),
        StructField("last_contact", LongType(), True),
        StructField("long", DoubleType(), True),
        StructField("lat", DoubleType(), True),
        StructField("baro_altitude", DoubleType(), True),
        StructField("on_ground", BooleanType(), True),
        StructField("velocity", DoubleType(), True),
        StructField("true_track", DoubleType(), True),
        StructField("vertical_rate", DoubleType(), True),
        StructField("sensors", StringType(), True), # Often null, so String is safest
        StructField("geo_altitude", DoubleType(), True),
        StructField("squawk", StringType(), True),
        StructField("spi", BooleanType(), True),
        StructField("position_source", LongType(), True)
    ])
    
    df = spark.createDataFrame(clean_data, schema = schema)

    # Write to data lake (bronze layer)
    #use the 'abfss' protocol which is optimized for Azure Data Lake Gen2
    output_path = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/raw_flights"
    print("="*40 + "\n")
    print(f"Step 2: Writing data to Azure Data Lake.....")
    print(f"Path: {output_path}")
    print("="*40 + "\n")
    
    try:
        #overwrite ensure replacing previous data
        df.write.mode("overwrite").parquet(output_path)
        print("="*40 + "\n")
        print("Success. Files save in Azure Bronze container")
        print("="*40 + "\n")
    except Exception as e:
        print("="*40 + "\n")
        print("THE WRITE FAILED! \n")
        print(f"Error Details: {e}") # This will finally tell us the truth
        print("="*40 + "\n")
        spark.stop()

if __name__ == "__main__":
    main()
