import os
import sys
from pathlib import Path
from dotenv import load_dotenv
from pyspark.sql import SparkSession
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

# --- PLACE THE DEBUG LINES HERE ---
#print("\n" + "-"*30)
#print(f"DEBUG: CLIENT_ID is '{CLIENT_ID}'")
#print(f"DEBUG: TENANT_ID is '{TENANT_ID}'")
#print(f"DEBUG: STORAGE_ACCOUNT is '{STORAGE_ACCOUNT}'")
#print("-"*30 + "\n")

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
    print("=" *40)
    print("\n[SUCCESS] Connected to Key Vault and retrieved secrets.")
    print("=" *40 + "\n")

except Exception as e:
    print("\n" + "!"*40)
    print(f"KEY VAULT ERROR: {str(e)}") # Using str(e) ensures the full error prints
    print("!"*40)
    sys.exit(1)

#print("=" *40 + "\n")
## Temporary Debug - check the last 3 characters to see if they match your new secret
#print(f"DEBUG: Secret from Vault ends with: ...{VAULT_CLIENT_SECRET[-3:]}")
#print(f"DEBUG: Secret from .env ends with:  ...{CLIENT_SECRET[-3:]}")
#print("=" *40 + "\n")

spark = SparkSession.builder \
    .appName("Aviation_Silver_Transformation") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-azure:3.3.4,com.microsoft.azure:azure-storage:8.6.6") \
    .config(f"spark.hadoop.fs.azure.account.auth.type.{STORAGE_ACCOUNT}.dfs.core.windows.net", "OAuth") \
    .config(f"spark.hadoop.fs.azure.account.oauth.provider.type.{STORAGE_ACCOUNT}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider") \
    .config(f"spark.hadoop.fs.azure.account.oauth2.client.id.{STORAGE_ACCOUNT}.dfs.core.windows.net", CLIENT_ID) \
    .config(f"spark.hadoop.fs.azure.account.oauth2.client.secret.{STORAGE_ACCOUNT}.dfs.core.windows.net", VAULT_CLIENT_SECRET) \
    .config(f"spark.hadoop.fs.azure.account.oauth2.client.endpoint.{STORAGE_ACCOUNT}.dfs.core.windows.net", f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/token") \
    .getOrCreate()

# Read data from bronze

bronze_path = f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net/raw_data"
print("=" *40 + "\n")
print(f"Reading data from: {bronze_path}")
print("=" *40 + "\n")

df_raw = spark.read.json(bronze_path)

df_exploded = df_raw.select(F.explode("states").alias("s"))

# 3. Map the array indices to real names
# OpenSky uses fixed positions in the array: 0=icao24, 1=callsign, etc.
df_bronze = df_exploded.select(
    F.col("s")[0].alias("icao24"),
    F.col("s")[1].alias("callsign"),
    F.col("s")[2].alias("origin_country"),
    F.col("s")[5].alias("longitude"),
    F.col("s")[6].alias("latitude"),
    F.col("s")[7].alias("baro_altitude"),
    F.col("s")[9].alias("velocity"),
    F.col("s")[10].alias("true_track"),
    F.col("s")[11].alias("vertical_rate")
)

# transformations

df_silver = df_bronze \
    .filter(F.col("callsign").isNotNull()) \
    .withColumn("callsign", F.trim(F.col("callsign"))) \
    .withColumn("latitude", F.col("lat").cast("double")) \
    .withColumn("longitude", F.col("long").cast("double")) \
    .withColumn("flight_time", F.from_unixtime(F.col("time_position"))) \
    .withColumn("processed_at", F.current_timestamp()) \
    .dropDuplicates(["icao24", "time_position"])

# Write to silver

silver_path = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net/cleaned_flights"

print("=" *40 + "\n")
print(f"Writing cleaned data to: {silver_path}")
print("=" *40 + "\n")

#df_silver.write.mode("overwrite").parquet(silver_path)
df_silver.write.mode("append").parquet(silver_path)

print("=" *40 + "\n")
print("Silver Success: Data cleaned and stored")
print("=" *40 + "\n")

spark.stop()