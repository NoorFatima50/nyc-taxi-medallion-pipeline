# ============================================
# 02 - SILVER TRANSFORMATION
# NYC Taxi Medallion Pipeline - Project 1
# ============================================

from pyspark.sql.functions import col, sum as spark_sum

# ─────────────────────────────────────────────
# Cell 1 - Read from Bronze Delta table
# ─────────────────────────────────────────────
df_bronze = spark.read.format("delta").load(
    "/Volumes/workspace/default/taxi-data/bronze/taxi_raw"
)
print(f"Bronze rows loaded: {df_bronze.count():,}")

# ─────────────────────────────────────────────
# Cell 2 - Check nulls before cleaning
# ─────────────────────────────────────────────
null_counts = df_bronze.select([
    spark_sum(col(c).isNull().cast("int")).alias(c)
    for c in df_bronze.columns
])
null_counts.show(vertical=True)

# ─────────────────────────────────────────────
# Cell 3 - Remove nulls and bad data
# ─────────────────────────────────────────────
df_cleaned = df_bronze.dropna(subset=[
    "passenger_count",
    "RatecodeID",
    "store_and_fwd_flag",
    "congestion_surcharge",
    "airport_fee"
])

df_cleaned = df_cleaned.filter(
    (col("fare_amount") > 0) &
    (col("trip_distance") > 0) &
    (col("passenger_count") > 0) &
    (col("total_amount") > 0)
)

total_before = df_bronze.count()
total_after = df_cleaned.count()
removed = total_before - total_after

print(f"Before cleaning : {total_before:,}")
print(f"After cleaning  : {total_after:,}")
print(f"Rows removed    : {removed:,}")
print(f"Data kept       : {round((total_after/total_before)*100, 2)}%")

# ─────────────────────────────────────────────
# Cell 4 - Write Silver Delta table
# ─────────────────────────────────────────────
df_cleaned.write \
    .format("delta") \
    .mode("overwrite") \
    .save("/Volumes/workspace/default/taxi-data/silver/taxi_cleaned")

print(f"Silver rows written: {df_cleaned.count():,}")
print("Silver layer written successfully!")

# ─────────────────────────────────────────────
# Cell 5 - Confirm Silver table
# ─────────────────────────────────────────────
df_silver = spark.read.format("delta").load(
    "/Volumes/workspace/default/taxi-data/silver/taxi_cleaned"
)
print(f"Silver table rows: {df_silver.count():,}")
df_silver.show(3)
