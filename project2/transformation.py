import dlt
from pyspark.sql.functions import col, hour, avg, count, sum as spark_sum, round as spark_round
from pyspark.sql.types import TimestampType

# ── BRONZE ──────────────────────────────────────────────
@dlt.table(
    name="bronze_taxi",
    comment="Raw NYC taxi data ingested from parquet file",
    table_properties={
        "delta.feature.timestampNtz": "supported"
    }
)
def bronze_taxi():
    df = spark.read.parquet(
        "/Volumes/workspace/default/taxi-data/yellow_tripdata_2023-01.parquet"
    )
    return df \
        .withColumn("tpep_pickup_datetime",
            col("tpep_pickup_datetime").cast(TimestampType())) \
        .withColumn("tpep_dropoff_datetime",
            col("tpep_dropoff_datetime").cast(TimestampType()))


# ── SILVER ──────────────────────────────────────────────
@dlt.table(
    name="silver_taxi",
    comment="Cleaned and validated NYC taxi data"
)
@dlt.expect("valid_fare", "fare_amount > 0")
@dlt.expect("valid_distance", "trip_distance > 0")
@dlt.expect("valid_passengers", "passenger_count > 0")
@dlt.expect("valid_total", "total_amount > 0")
@dlt.expect("valid_vendor", "VendorID IS NOT NULL")
def silver_taxi():
    return (
        dlt.read("bronze_taxi")
        .dropna(subset=[
            "passenger_count",
            "RatecodeID",
            "store_and_fwd_flag",
            "congestion_surcharge",
            "airport_fee"
        ])
        .filter(
            (col("fare_amount") > 0) &
            (col("trip_distance") > 0) &
            (col("passenger_count") > 0) &
            (col("total_amount") > 0)
        )
    )


# ── GOLD TABLE 1 — Fare by hour ─────────────────────────
@dlt.table(
    name="gold_fare_by_hour",
    comment="Average fare, distance and tip by hour of day"
)
def gold_fare_by_hour():
    return (
        dlt.read("silver_taxi")
        .withColumn("pickup_hour", hour("tpep_pickup_datetime"))
        .groupBy("pickup_hour")
        .agg(
            spark_round(avg("fare_amount"), 2).alias("avg_fare"),
            spark_round(avg("trip_distance"), 2).alias("avg_distance"),
            spark_round(avg("tip_amount"), 2).alias("avg_tip")
        )
        .orderBy("pickup_hour")
    )


# ── GOLD TABLE 2 — Payment summary ──────────────────────
@dlt.table(
    name="gold_payment_summary",
    comment="Total trips and revenue by payment type"
)
def gold_payment_summary():
    return (
        dlt.read("silver_taxi")
        .groupBy("payment_type")
        .agg(
            count("*").alias("total_trips"),
            spark_round(spark_sum("total_amount"), 2).alias("total_revenue"),
            spark_round(avg("total_amount"), 2).alias("avg_trip_value")
        )
        .orderBy("payment_type")
    )


# ── GOLD TABLE 3 — Top pickup locations ─────────────────
@dlt.table(
    name="gold_top_pickups",
    comment="Top 10 busiest pickup zones in NYC"
)
def gold_top_pickups():
    return (
        dlt.read("silver_taxi")
        .groupBy("PULocationID")
        .agg(
            count("*").alias("total_pickups"),
            spark_round(avg("fare_amount"), 2).alias("avg_fare"),
            spark_round(avg("trip_distance"), 2).alias("avg_distance")
        )
        .orderBy(col("total_pickups").desc())
        .limit(10)
    )
