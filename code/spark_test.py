from pyspark.sql import SparkSession

# Initialize Spark
spark = SparkSession.builder \
    .appName("AviationProjectTest") \
    .master("local[*]") \
    .getOrCreate()

# Create a small dataset of airplane types
data = [("Boeing 747", 410), ("Airbus A380", 853), ("Cessna 172", 4)]
columns = ["Model", "Max_Passengers"]

df = spark.createDataFrame(data, columns)

print("\n" + "="*30)
print("--- AVIATION DATA TEST ---")
df.show()
print("--- SPARK IS WORKING! ---")
print("="*30 + "\n")

spark.stop()