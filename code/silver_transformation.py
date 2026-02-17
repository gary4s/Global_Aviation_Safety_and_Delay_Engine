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
    print("\n[SUCCESS] Connected to Key Vault and retrieved secrets.")

except Exception as e:
    print("\n" + "!"*40)
    print(f"KEY VAULT ERROR: {str(e)}") # Using str(e) ensures the full error prints
    print("!"*40)
    sys.exit(1)

spark = SparkSession.builder \
    .appName("Aviation_Silver_Transformation") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-azure:3.3.4,com.microsoft.azure:azure-storage:8.6.6") \
    .config(f"fs.azure.account.auth.type.{STORAGE_ACCOUNT}.dfs.core.windows.net", "OAuth") \
    .config(f"fs.azure.account.oauth.provider.type.{STORAGE_ACCOUNT}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider") \
    .config(f"fs.azure.account.oauth2.client.id.{STORAGE_ACCOUNT}.dfs.core.windows.net", CLIENT_ID) \
    .config(f"fs.azure.account.oauth2.client.secret.{STORAGE_ACCOUNT}.dfs.core.windows.net", VAULT_CLIENT_SECRET) \
    .config(f"fs.azure.account.oauth2.client.endpoint.{STORAGE_ACCOUNT}.dfs.core.windows.net", f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/token") \
    .getOrCreate()    

# Read data from bronze

bronze_path = f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net/raw_flights"
print("=" *40 + "\n")
print(f"Reading data from: {bronze_path}")
print("=" *40 + "\n")

df_bronze = spark.read.parquet(bronze_path)

# transformations

df_silver = df_bronze \
    .filter(F.col("callsign").isNotNull()) \
    .withColumn("callsign", F.trim(F.col("callsign"))) \
    .withColumn("latitude", F.col("latitude").cast("double")) \
    .withColumn("longitude", F.col("longitude").cast("double")) \
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