# 🚕 NYC Taxi Medallion Pipeline
### End-to-End Data Engineering Pipeline on Databricks

![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-E25A1C?style=for-the-badge&logo=apache-spark&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta_Lake-003366?style=for-the-badge&logo=delta&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)

---

## 📌 Project Overview

A production-grade **ETL data pipeline** built on **Databricks** that processes over **3 million NYC Yellow Taxi trips** from raw ingestion through to analytics-ready Gold tables.

This repository contains **two implementations** of the same medallion architecture — first built manually with PySpark, then rebuilt using **Delta Live Tables (DLT)** — demonstrating both foundational and modern data engineering approaches.

---

## 🏗️ Architecture

```
Raw Parquet File (45MB — 3,066,766 rows)
           │
           ▼
┌─────────────────────┐
│    BRONZE LAYER     │  Raw data preserved as Delta table
│   3,066,766 rows    │  No transformations — raw is raw
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│    SILVER LAYER     │  Cleaned and validated data
│   2,884,228 rows    │  Nulls removed, bad data filtered
│   94.05% quality    │  182,538 rows removed
└──────────┬──────────┘
           │
           ▼
┌──────────────────────────────────────┐
│             GOLD LAYER               │  Analytics-ready tables
│  ├── fare_by_hour      (24 rows)     │  Avg fare by hour of day
│  ├── payment_summary    (4 rows)     │  Revenue by payment type
│  └── top_pickups       (10 rows)     │  Busiest pickup zones
└──────────────────────────────────────┘
           │
           ▼
  Databricks Workflow / DLT Pipeline
  (Scheduled — runs automatically)
```

---

## 📂 Repository Structure

```
nyc-taxi-medallion-pipeline/
│
├── project1/                        # Manual PySpark implementation
│   ├── 01_bronze_ingestion.py       # Raw data ingestion to Delta
│   ├── 02_silver_transformation.py  # Data cleaning and validation
│   └── 03_gold_aggregation.py       # Business aggregations
│
├── project2/                        # Delta Live Tables implementation
│   └── transformation.py            # Full DLT pipeline with quality checks
│
└── README.md
```

---

## 🔧 Tech Stack

| Tool | Purpose |
|------|---------|
| **Databricks** | Cloud data platform and pipeline orchestration |
| **Apache Spark / PySpark** | Distributed data processing |
| **Delta Lake** | ACID transactions, time travel, schema evolution |
| **Delta Live Tables** | Declarative pipeline framework with quality monitoring |
| **Auto Loader** | Incremental file ingestion with checkpointing |
| **Databricks Workflows** | Pipeline scheduling and task orchestration |
| **Python** | Primary programming language |
| **SQL** | Data aggregations and queries |

---

## 📊 Dataset

| Property | Value |
|----------|-------|
| Source | NYC Yellow Taxi Trip Data |
| Period | January 2023 |
| Format | Parquet |
| Size | ~45MB |
| Rows | 3,066,766 |
| Columns | 19 |
| Download | [NYC TLC Trip Data](https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet) |

---

## 🥉 Project 1 — Manual PySpark Pipeline

### Bronze Layer — `01_bronze_ingestion.py`

**What it does:**
- Reads raw parquet file into PySpark DataFrame
- Writes data as Delta table with zero transformations
- Records full Delta history for time travel and auditing
- Demonstrates Auto Loader for incremental file ingestion

**Key concepts:**
- `spark.read.parquet()` — reading raw files
- `df.write.format("delta")` — writing Delta tables
- `DeltaTable.history()` — time travel and lineage tracking
- Auto Loader with `cloudFiles` for incremental processing

---
<img width="1843" height="908" alt="bronze table" src="https://github.com/user-attachments/assets/c114976f-989b-4016-b2a1-ff4c4c76ca2a" />



### Silver Layer — `02_silver_transformation.py`

**What it does:**
- Reads from Bronze Delta table
- Detects and removes null rows across 5 columns
- Filters logically invalid records
- Writes clean validated data as Silver Delta table

**Data quality results:**

| Metric | Value |
|--------|-------|
| Raw rows (Bronze) | 3,066,766 |
| Null rows removed | 71,743 |
| Invalid rows removed | 110,795 |
| Clean rows (Silver) | 2,884,228 |
| Data quality score | **94.05%** |

**Cleaning rules applied:**
- Dropped nulls: `passenger_count`, `RatecodeID`, `store_and_fwd_flag`, `congestion_surcharge`, `airport_fee`
- Filtered: `fare_amount > 0`
- Filtered: `trip_distance > 0`
- Filtered: `passenger_count > 0`
- Filtered: `total_amount > 0`

---
<img width="1864" height="921" alt="silver table" src="https://github.com/user-attachments/assets/64b01606-01a7-41e8-92e5-f5058c150c76" />


### Gold Layer — `03_gold_aggregation.py`

**What it does:**
- Reads from Silver Delta table
- Builds 3 analytics-ready aggregation tables
- Writes each as a separate Gold Delta table

#### `fare_by_hour` — 24 rows
Average fare, distance, and tip grouped by hour of day.

| Finding | Value |
|---------|-------|
| Most expensive hour | 5 AM — $26.46 avg fare |
| Reason | Airport runs — 6.42 miles avg distance |
| Cheapest hour | 11 AM — $17.41 avg fare |

#### `payment_summary` — 4 rows
Total trips and revenue grouped by payment type.

| Finding | Value |
|---------|-------|
| Credit card trips | 2,350,772 (81% of all trips) |
| Cash trips | 507,944 (19% of all trips) |
| Credit vs cash spend | Credit card riders spend $4.53 more per trip |
| Total January revenue | $78.2 million |

#### `top_pickups` — 10 rows
Top 10 busiest pickup zones by trip volume.

| Finding | Value |
|---------|-------|
| Busiest zone | Zone 132 — JFK Airport (152,122 trips) |
| Highest avg fare | Zone 132 — $60.74 avg fare |
| Why airports dominate | 15+ mile trips vs 1.8 miles in Midtown |

---
<img width="1857" height="932" alt="gold table" src="https://github.com/user-attachments/assets/15189418-c5b7-4568-b6ec-e3ef93391fec" />


### Databricks Workflow

Pipeline orchestrated with task dependencies:

```
bronze_ingestion
      │
      ▼ (depends on bronze)
silver_transformation
      │
      ▼ (depends on silver)
gold_aggregation
```

**Schedule:** Daily at 12:00 AM — fully automated.
If any task fails, downstream tasks automatically stop.

---

<img width="1347" height="906" alt="jobs" src="https://github.com/user-attachments/assets/5d1ca3b7-54ee-4463-85ca-95641238fd8e" />


## 🔵 Project 2 — Delta Live Tables Pipeline

### `transformation.py`

A complete rewrite of the Project 1 pipeline using **Delta Live Tables** — Databricks' modern declarative pipeline framework.

**What's different from Project 1:**

| | Project 1 | Project 2 |
|--|-----------|-----------|
| Approach | Imperative — you write HOW | Declarative — you define WHAT |
| Dependencies | Managed manually | Managed automatically by DLT |
| Data quality | Manual filtering | `@dlt.expect` rules with dashboard |
| Lineage | Manual tracking | Automatic lineage graph |
| Retries | Manual | Automatic |

**5 Data Quality Expectations:**

```python
@dlt.expect("valid_fare",       "fare_amount > 0")
@dlt.expect("valid_distance",   "trip_distance > 0")
@dlt.expect("valid_passengers", "passenger_count > 0")
@dlt.expect("valid_total",      "total_amount > 0")
@dlt.expect("valid_vendor",     "VendorID IS NOT NULL")
```

DLT automatically tracks pass/fail counts for each rule and displays them in a quality dashboard.

<img width="1317" height="648" alt="dlt graphs" src="https://github.com/user-attachments/assets/786d7086-1557-439a-89d5-673d56c38b47" />


**Pipeline results:**

| Table | Rows | Duration |
|-------|------|----------|
| bronze_taxi | 3,066,766 | 6s |
| silver_taxi | 2,884,228 | 9s |
| gold_fare_by_hour | 24 | 3s |
| gold_payment_summary | 4 | 3s |
| gold_top_pickups | 10 | 3s |
| **Total pipeline** | | **34s** |

---

<img width="1903" height="876" alt="dlt tables" src="https://github.com/user-attachments/assets/30b57133-c84b-4983-ba3d-bd512531711c" />


## 📈 Key Business Insights

| Insight | Finding |
|---------|---------|
| Most expensive hour | 5 AM — $26.46 avg fare |
| Cheapest hour | 11 AM — $17.41 avg fare |
| Busiest pickup zone | Zone 132 — JFK Airport (152,122 trips) |
| Highest revenue zone | JFK Airport — $60.74 avg fare per trip |
| Payment preference | 81% of riders pay by credit card |
| Credit vs cash | Credit card riders spend $4.53 more per trip |
| Total January revenue | $78.2 million |
| Data quality score | 94.05% clean after validation |

---

## 🚀 How to Run This Project

### Prerequisites
- Databricks account — [Community Edition is free](https://community.cloud.databricks.com)
- Download the dataset:
```
https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet
```

### Setup

**Step 1 — Upload dataset to Databricks**
- Log into Databricks
- Go to Catalog → Add Data → Upload files to Volume
- Create volume named: `taxi-data`
- Upload the parquet file
- Your volume path will be:
```
/Volumes/workspace/default/taxi-data/yellow_tripdata_2023-01.parquet
```

**Step 2 — Update file paths**

In every file replace this path with your own volume path:
```python
/Volumes/workspace/default/taxi-data/
```

### Running Project 1 — Manual Pipeline

**Option A — Run notebooks individually:**
```
1. Import 01_bronze_ingestion.py into Databricks workspace
2. Import 02_silver_transformation.py
3. Import 03_gold_aggregation.py
4. Run in order: Bronze → Silver → Gold
```

**Option B — Set up Databricks Workflow:**
```
1. Go to Jobs & Pipelines → Create Job
2. Add three tasks pointing to each notebook
3. Set dependencies:
   Silver depends on Bronze
   Gold depends on Silver
4. Click Run Now
```

### Running Project 2 — DLT Pipeline

```
1. Go to Jobs & Pipelines → Create Pipeline → ETL Pipeline
2. Upload transformation.py
3. Name the pipeline: nyc-taxi-dlt-pipeline
4. Click Run Pipeline
5. View quality dashboard and pipeline graph
```

---

## 🎓 Skills Demonstrated

**Data Engineering:**
- ✅ Medallion Architecture — Bronze → Silver → Gold
- ✅ Delta Lake — ACID transactions, time travel, schema evolution
- ✅ PySpark — DataFrames, transformations, aggregations
- ✅ Auto Loader — incremental ingestion with checkpointing
- ✅ Delta Live Tables — declarative pipelines with quality monitoring
- ✅ Data quality validation — null handling, business rule filtering
- ✅ Pipeline orchestration — Databricks Workflows with task dependencies

**Best Practices:**
- ✅ Proper folder separation — raw input vs output
- ✅ Delta history tracking for full data lineage
- ✅ Incremental processing — avoids reprocessing old data
- ✅ Fail-safe pipeline — downstream tasks stop if upstream fails
- ✅ Clean documented code with clear section separation

---

