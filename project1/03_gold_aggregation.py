from pyspark.sql.functions import hour, round as spark_round, avg, col, count, sum as spark_sum

# Read from Silver Delta table
df_silver = spark.read.format("delta").load(
    "/Volumes/workspace/default/taxi-data/silver/taxi_cleaned"
)
print(f"Silver rows loaded: {df_silver.count():,}")

# Gold Table 1 — Average fare by hour
df_fare_by_hour = df_silver \
    .withColumn("pickup_hour", hour("tpep_pickup_datetime")) \
    .groupBy("pickup_hour") \
    .agg(
        spark_round(avg("fare_amount"), 2).alias("avg_fare"),
        spark_round(avg("trip_distance"), 2).alias("avg_distance"),
        spark_round(avg("tip_amount"), 2).alias("avg_tip")
    ) \
    .orderBy("pickup_hour")

# Gold Table 2 — Payment summary
df_payment = df_silver \
    .groupBy("payment_type") \
    .agg(
        count("*").alias("total_trips"),
        spark_round(spark_sum("total_amount"), 2).alias("total_revenue"),
        spark_round(avg("total_amount"), 2).alias("avg_trip_value")
    ) \
    .orderBy("payment_type")

# Gold Table 3 — Top pickup locations
df_top_pickups = df_silver \
    .groupBy("PULocationID") \
    .agg(
        count("*").alias("total_pickups"),
        spark_round(avg("fare_amount"), 2).alias("avg_fare"),
        spark_round(avg("trip_distance"), 2).alias("avg_distance")
    ) \
    .orderBy(col("total_pickups").desc()) \
    .limit(10)

# Write all three Gold tables
df_fare_by_hour.write \
    .format("delta") \
    .mode("overwrite") \
    .save("/Volumes/workspace/default/taxi-data/gold/fare_by_hour")

df_payment.write \
    .format("delta") \
    .mode("overwrite") \
    .save("/Volumes/workspace/default/taxi-data/gold/payment_summary")

df_top_pickups.write \
    .format("delta") \
    .mode("overwrite") \
    .save("/Volumes/workspace/default/taxi-data/gold/top_pickups")

print("Gold layer written successfully!")
print("✅ fare_by_hour")
print("✅ payment_summary")
print("✅ top_pickups")

# Confirm all Gold tables
df_gold_fares = spark.read.format("delta").load(
    "/Volumes/workspace/default/taxi-data/gold/fare_by_hour"
)
df_gold_payment = spark.read.format("delta").load(
    "/Volumes/workspace/default/taxi-data/gold/payment_summary"
)
df_gold_pickups = spark.read.format("delta").load(
    "/Volumes/workspace/default/taxi-data/gold/top_pickups"
)

print(f"fare_by_hour rows: {df_gold_fares.count():,}")
print(f"payment_summary rows: {df_gold_payment.count():,}")
print(f"top_pickups rows: {df_gold_pickups.count():,}")
