# ============================================
# 03 - GOLD AGGREGATION
# NYC Taxi Medallion Pipeline - Project 1
# ============================================

from pyspark.sql.functions import hour, round as spark_round, avg, col, count, sum as spark_sum

# ─────────────────────────────────────────────
# Cell 1 - Read from Silver Delta table
# ─────────────────────────────────────────────
df_silver = spark.read.format("delta").load(
    "/Volumes/workspace/default/taxi-data/silver/taxi_cleaned"
)
print(f"Silver rows loaded: {df_silver.count():,}")

# ─────────────────────────────────────────────
# Cell 2 - Gold Table 1 — Average fare by hour
# ─────────────────────────────────────────────
df_fare_by_hour = df_silver \
    .withColumn("pickup_hour", hour("tpep_pickup_datetime")) \
    .groupBy("pickup_hour") \
    .agg(
        spark_round(avg("fare_amount"), 2).alias("avg_fare"),
        spark_round(avg("trip_distance"), 2).alias("avg_distance"),
        spark_round(avg("tip_amount"), 2).alias("avg_tip")
    ) \
    .orderBy("pickup_hour")

df_fare_by_hour.show(24)

# ─────────────────────────────────────────────
# Cell 3 - Gold Table 2 — Payment summary
# ─────────────────────────────────────────────
df_payment = df_silver \
    .groupBy("payment_type") \
    .agg(
        count("*").alias("total_trips"),
        spark_round(spark_sum("total_amount"), 2).alias("total_revenue"),
        spark_round(avg("total_amount"), 2).alias("avg_trip_value")
    ) \
    .orderBy("payment_type")

df_payment.show()

# ─────────────────────────────────────────────
# Cell 4 - Gold Table 3 — Top pickup locations
# ─────────────────────────────────────────────
df_top_pickups = df_silver \
    .groupBy("PULocationID") \
    .agg(
        count("*").alias("total_pickups"),
        spark_round(avg("fare_amount"), 2).alias("avg_fare"),
        spark_round(avg("trip_distance"), 2).alias("avg_distance")
    ) \
    .orderBy(col("total_pickups").desc()) \
    .limit(10)

df_top_pickups.show()

# ─────────────────────────────────────────────
# Cell 5 - Write all three Gold tables
# ─────────────────────────────────────────────
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

# ─────────────────────────────────────────────
# Cell 6 - Confirm all Gold tables
# ─────────────────────────────────────────────
df_gold_fares = spark.read.format("delta").load(
    "/Volumes/workspace/default/taxi-data/gold/fare_by_hour"
)
df_gold_payment = spark.read.format("delta").load(
    "/Volumes/workspace/default/taxi-data/gold/payment_summary"
)
df_gold_pickups = spark.read.format("delta").load(
    "/Volumes/workspace/default/taxi-data/gold/top_pickups"
)

print(f"fare_by_hour rows    : {df_gold_fares.count():,}")
print(f"payment_summary rows : {df_gold_payment.count():,}")
print(f"top_pickups rows     : {df_gold_pickups.count():,}")
