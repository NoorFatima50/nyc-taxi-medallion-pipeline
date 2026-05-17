# ============================================
# 01 - BRONZE INGESTION
# NYC Taxi Medallion Pipeline - Project 1
# ============================================

# Cell 1 - Read raw parquet and write as Bronze Delta table
df_raw = spark.read.parquet(
    "/Volumes/workspace/default/taxi-data/yellow_tripdata_2023-01.parquet"
)
print(f"Total rows: {df_raw.count():,}")
df_raw.printSchema()

df_raw.write \
    .format("delta") \
    .mode("overwrite") \
    .save("/Volumes/workspace/default/taxi-data/bronze/taxi_raw")

print("Bronze layer written successfully!")

# ─────────────────────────────────────────────
# Cell 2 - Confirm Bronze table
# ─────────────────────────────────────────────
df_bronze = spark.read.format("delta").load(
    "/Volumes/workspace/default/taxi-data/bronze/taxi_raw"
)
print(f"Bronze table rows: {df_bronze.count():,}")
df_bronze.show(3)

# ─────────────────────────────────────────────
# Cell 3 - Delta table history — time travel
# ─────────────────────────────────────────────
from delta.tables import DeltaTable

delta_table = DeltaTable.forPath(
    spark,
    "/Volumes/workspace/default/taxi-data/bronze/taxi_raw"
)
delta_table.history().show(truncate=False)
