from pyspark.sql.functions import col, sum as spark_sum

# Read from Bronze Delta table
df_bronze = spark.read.format("delta").load(
    "/Volumes/workspace/default/taxi-data/bronze/taxi_raw"
)
print(f"Bronze rows loaded: {df_bronze.count():,}")

# Remove nulls
df_cleaned = df_bronze.dropna(subset=[
    "passenger_count",
    "RatecodeID",
    "store_and_fwd_flag",
    "congestion_surcharge",
    "airport_fee"
])

# Remove bad data
df_cleaned = df_cleaned.filter(
    (col("fare_amount") > 0) &
    (col("trip_distance") > 0) &
    (col("passenger_count") > 0) &
    (col("total_amount") > 0)
)

# Write Silver Delta table
df_cleaned.write \
    .format("delta") \
    .mode("overwrite") \
    .save("/Volumes/workspace/default/taxi-data/silver/taxi_cleaned")

print(f"Silver rows written: {df_cleaned.count():,}")
print("Silver layer written successfully!")

# Confirm Silver table
df_silver = spark.read.format("delta").load(
    "/Volumes/workspace/default/taxi-data/silver/taxi_cleaned"
)
print(f"Silver table rows: {df_silver.count():,}")
df_silver.show(3)
