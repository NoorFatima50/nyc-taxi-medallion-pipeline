# 🚕 NYC Taxi Medallion Pipeline
### End-to-End Data Engineering Pipeline on Databricks

![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-E25A1C?style=for-the-badge&logo=apache-spark&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta_Lake-003366?style=for-the-badge&logo=delta&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)

---

## 📌 Project Overview

A production-grade **ETL data pipeline** built on **Databricks** that processes over **3 million NYC Yellow Taxi trips** from raw ingestion through to analytics-ready Gold tables — using the **Medallion Architecture (Bronze → Silver → Gold)**.

This project demonstrates real-world data engineering skills including:
- Incremental data ingestion with **Auto Loader**
- Data cleaning and validation with **PySpark**
- Business aggregations with **Delta Lake**
- Pipeline orchestration with **Databricks Workflows**

---

## 🏗️ Architecture

```
Raw Parquet File
      │
      ▼
┌─────────────────┐
│   BRONZE LAYER  │  ← Raw data preserved as Delta table
│  3,066,766 rows │    No transformations — raw is raw
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   SILVER LAYER  │  ← Cleaned and validated data
│  2,884,228 rows │    Nulls removed, bad data filtered
└────────┬────────┘
         │
         ▼
┌─────────────────────────────────────────┐
│              GOLD LAYER                 │  ← Analytics-ready tables
│  ├── fare_by_hour    (24 rows)          │
│  ├── payment_summary  (4 rows)          │
│  └── top_pickups     (10 rows)          │
└─────────────────────────────────────────┘
         │
         ▼
  Databricks Workflow
  (Scheduled daily at midnight)
```

---

## 📂 Project Structure

```
nyc-taxi-medallion-pipeline/
│
├── 01_bronze_ingestion.py       # Raw data ingestion to Delta
├── 02_silver_transformation.py  # Data cleaning and validation
├── 03_gold_aggregation.py       # Business aggregations
└── README.md
```
<img width="1895" height="910" alt="notebooks" src="https://github.com/user-attachments/assets/42c63e97-d219-4640-86eb-dea9beb5f03c" />

---

## 🔧 Tech Stack

| Tool | Purpose |
|------|---------|
| **Databricks** | Cloud data platform |
| **Apache Spark / PySpark** | Distributed data processing |
| **Delta Lake** | ACID transactions and time travel |
| **Auto Loader** | Incremental file ingestion |
| **Databricks Workflows** | Pipeline orchestration and scheduling |
| **Python** | Primary programming language |
| **SQL** | Data aggregations and queries |

---

## 📊 Dataset

**Source:** NYC Yellow Taxi Trip Data — January 2023  
**Format:** Parquet  
**Size:** ~45MB / 3,066,766 rows  
**Columns:** 19 fields including pickup/dropoff timestamps, fare amounts, trip distance, passenger count, payment type, and location IDs

---

## 🔄 Pipeline Details

### 🥉 Bronze Layer — `01_bronze_ingestion.py`

**What it does:**
- Reads raw parquet file into a PySpark DataFrame
- Writes data as a **Delta table** with no transformations
- Preserves raw data exactly as received
- Records full **Delta history** for time travel and auditing

**Key concepts demonstrated:**
- `spark.read.parquet()` — reading raw files
- `df.write.format("delta")` — writing Delta tables
- `DeltaTable.history()` — time travel and lineage
- Auto Loader with `cloudFiles` for incremental ingestion

```python
df_raw = spark.read.parquet("/Volumes/.../yellow_tripdata_2023-01.parquet")
df_raw.write.format("delta").mode("overwrite").save(".../bronze/taxi_raw")
```
<img width="1887" height="908" alt="bronze table" src="https://github.com/user-attachments/assets/9877950d-423d-4512-a225-5ed218b722d5" />

---

### 🥈 Silver Layer — `02_silver_transformation.py`

**What it does:**
- Reads from Bronze Delta table
- Removes **71,743 null rows** across 5 columns
- Filters logically invalid data — negative fares, zero distance trips
- Writes clean data as Silver Delta table

**Data quality results:**

| Metric | Value |
|--------|-------|
| Raw rows (Bronze) | 3,066,766 |
| Rows removed | 182,538 |
| Clean rows (Silver) | 2,884,228 |
| Data quality score | **94.05%** |

**Cleaning rules applied:**
- Dropped nulls in `passenger_count`, `RatecodeID`, `store_and_fwd_flag`, `congestion_surcharge`, `airport_fee`
- Filtered `fare_amount > 0`
- Filtered `trip_distance > 0`
- Filtered `passenger_count > 0`
- Filtered `total_amount > 0`

```python
df_cleaned = df_bronze.dropna(subset=["passenger_count", "RatecodeID", ...])
df_cleaned = df_cleaned.filter((col("fare_amount") > 0) & (col("trip_distance") > 0))
```
<img width="1917" height="921" alt="silver table" src="https://github.com/user-attachments/assets/b75d1d0a-e413-4f2b-b6a0-eed8ed150ab8" />

---

### 🥇 Gold Layer — `03_gold_aggregation.py`

**What it does:**
- Reads from Silver Delta table
- Builds **3 analytics-ready aggregation tables**
- Writes each as a separate Gold Delta table

#### Table 1 — `fare_by_hour`
Average fare, distance, and tip amount grouped by hour of day.

**Key insight:** 5 AM has the highest average fare ($26.46) due to airport runs — 6.42 miles average distance vs 1.8 miles during midday.

#### Table 2 — `payment_summary`
Total trips, revenue, and average trip value by payment type.

**Key insight:** Credit card payments (81% of trips) generate $4.53 more per trip than cash payments. Total January 2023 revenue: **$78.2M**.

#### Table 3 — `top_pickups`
Top 10 busiest pickup zones by trip count.

**Key insight:** Zone 132 (JFK Airport) has the highest average fare ($60.74) — 4x more than Midtown Manhattan zones due to longer distance trips.

---
<img width="1857" height="932" alt="gold table" src="https://github.com/user-attachments/assets/cdcf8c80-ca8b-4b4b-be12-fa75e99c7869" />


## ⚙️ Databricks Workflow

The pipeline is fully orchestrated using **Databricks Workflows** with task dependencies:

```
bronze_ingestion
      │
      ▼ (depends on bronze)
silver_transformation
      │
      ▼ (depends on silver)
gold_aggregation
```

**Schedule:** Daily at 12:00 AM (Asia/Karachi timezone)

If any task fails, downstream tasks automatically stop — preventing bad data from propagating through the pipeline.

---
<img width="1347" height="906" alt="jobs" src="https://github.com/user-attachments/assets/0e1aab0f-e8c7-4a55-a529-ba32f68c6e20" />



## 📈 Key Business Insights

| Insight | Finding |
|---------|---------|
| Most expensive hour | **5 AM** — $26.46 avg fare (airport runs) |
| Cheapest hour | **11 AM** — $17.41 avg fare |
| Busiest zone | **Zone 132 (JFK Airport)** — 152,122 trips |
| Payment preference | **81%** of riders pay by credit card |
| Credit vs cash | Credit card riders spend **$4.53 more** per trip |
| Total Jan revenue | **$78.2 million** |
| Data quality | **94.05%** clean data after validation |

---

## 🚀 How to Run

### Prerequisites
- Databricks account (Community Edition works)
- NYC Taxi dataset uploaded to your Volume

### Steps

1. **Clone this repo**
```bash
git clone https://github.com/NoorFatima50/nyc-taxi-medallion-pipeline.git
```

2. **Upload to Databricks Workspace**
   - Import each notebook into your Databricks workspace

3. **Update file paths** in each notebook to match your Volume path:
```python
# Replace this path with your actual Volume path
"/Volumes/workspace/default/taxi-data/..."
```

4. **Run notebooks in order:**
```
01_bronze_ingestion → 02_silver_transformation → 03_gold_aggregation
```

5. **Or set up Databricks Workflow** to run automatically with task dependencies

---

## 🎓 Skills Demonstrated

- ✅ Medallion Architecture (Bronze → Silver → Gold)
- ✅ Delta Lake — ACID transactions, time travel, schema evolution
- ✅ PySpark DataFrames — transformations, aggregations, window functions
- ✅ Auto Loader — incremental file ingestion with checkpointing
- ✅ Data quality validation — null handling, business rule filtering
- ✅ Pipeline orchestration — Databricks Workflows with task dependencies
- ✅ Production practices — proper folder separation, Delta history, error handling

---


