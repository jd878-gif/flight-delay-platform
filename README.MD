# ✈ US Flight Delay Intelligence Platform

A production-style distributed data pipeline built with **Apache PySpark** that processes 10 years of US airline on-time performance data (171,666 records across 23 airlines and 395 airports) using a **Bronze → Silver → Gold Medallion architecture**.

---

## Key Insights Discovered

| Finding | Value |
|---|---|
| Best airline (2013–2023) | Alaska Airlines — 6.92 avg delay mins/flight |
| Worst airline (2013–2023) | JetBlue Airways — 13+ avg delay mins/flight |
| Worst airport | Newark (EWR) — 16.6 avg delay mins/flight |
| Best airport | Hilo, HI (ITO) — 6.03 avg delay mins/flight |
| #1 delay cause | Late Aircraft (not weather as commonly assumed) |
| COVID impact | 2020 saw -56.47% delay improvement (fewest flights) |
| Post-COVID surge | 2021 rebounded +90.86% as travel demand spiked |
| Worst year on record | 2023 — 15.02 avg delay mins/flight |

---

## Architecture

```
Raw CSV
   │
   ▼
┌─────────────────────────────────────────────────────┐
│  BRONZE LAYER  (data/bronze/)                       │
│  • Read raw CSV with explicit StructType schema     │
│  • Add ingested_at audit timestamp                  │
│  • Write Parquet — mode("overwrite") idempotent     │
│  • 171,666 rows, 22 columns                         │
└─────────────────────────────────────────────────────┘
   │
   ▼
┌─────────────────────────────────────────────────────┐
│  SILVER LAYER  (data/silver/flights_enriched/)      │
│  • Fill nulls (intentional: no flights = 0)         │
│  • Filter zero-flight rows (240 removed)            │
│  • Compute: on_time_pct, cancellation_rate          │
│  • Compute: avg_delay_per_flight, delay_category    │
│  • Compute: dominant_cause, pct_* delay breakdowns  │
│  • Write Parquet partitioned by year                │
│  • 171,426 rows, 32 columns                         │
└─────────────────────────────────────────────────────┘
   │
   ▼
┌─────────────────────────────────────────────────────┐
│  GOLD LAYER  (data/gold/)                           │
│  • gold_airline_performance  — RANK() window fn     │
│  • gold_delay_causes         — cause % breakdown    │
│  • gold_airport_rankings     — DENSE_RANK() window  │
│  • gold_yearly_trends        — LAG() YoY change     │
└─────────────────────────────────────────────────────┘
```

---

## Project Structure

```
flight-delay-platform/
│
├── data/
│   ├── raw/                        # Source CSV (not committed to git)
│   ├── bronze/flights/             # Raw Parquet — permanent archive
│   ├── silver/flights_enriched/    # Cleaned Parquet — partitioned by year
│   └── gold/                       # 4 KPI tables
│       ├── airline_performance/
│       ├── delay_causes/
│       ├── airport_rankings/
│       └── yearly_trends/
│
├── src/
│   ├── utils.py          # SparkSession factory, paths, logger
│   ├── bronze_layer.py   # Raw ingestion with explicit schema
│   ├── silver_layer.py   # Cleaning, enrichment, computed columns
│   └── gold_layer.py     # 4 KPI tables with window functions
│
├── notebooks/
│   └── exploration.ipynb # Data profiling and schema analysis
│
├── pipeline.py           # Main entry point — runs all layers
└── requirements.txt
```

---

## Gold Tables

### 1. `gold_airline_performance`
Ranks all 23 airlines by average delay per flight using `RANK()` window function.

```
+----------------+------------------------+--------------------+---------------+
|performance_rank|carrier_name            |avg_delay_per_flight|avg_on_time_pct|
+----------------+------------------------+--------------------+---------------+
|1               |Alaska Airlines Inc.    |6.92                |84.91          |
|4               |Delta Air Lines Inc.    |8.33                |86.35          |
|15              |United Air Lines Inc.   |12.97               |80.2           |
|16              |American Airlines Inc.  |13.13               |79.0           |
+----------------+------------------------+--------------------+---------------+
```

### 2. `gold_delay_causes`
Shows what percentage of each airline's delays come from carrier issues, weather, NAS, late aircraft, or security.

```
+------------------------+---------------+---------------+-----------+---------------------+
|carrier_name            |avg_pct_carrier|avg_pct_weather|avg_pct_nas|avg_pct_late_aircraft|
+------------------------+---------------+---------------+-----------+---------------------+
|Hawaiian Airlines Inc.  |75.8           |1.85           |1.12       |18.36                |
|Southwest Airlines Co.  |36.42          |2.73           |12.44      |47.98                |
+------------------------+---------------+---------------+-----------+---------------------+
```

### 3. `gold_airport_rankings`
Ranks 130 major US airports (50,000+ flights) by arrival delay using `DENSE_RANK()`.

```
+----------+-------+------------------------------------------+--------------------+
|delay_rank|airport|airport_name                              |avg_delay_per_flight|
+----------+-------+------------------------------------------+--------------------+
|1         |SFB    |Sanford, FL: Orlando Sanford International|20.75               |
|3         |EWR    |Newark, NJ: Newark Liberty International  |16.6                |
|4         |LGA    |New York, NY: LaGuardia                   |15.92               |
+----------+-------+------------------------------------------+--------------------+
```

### 4. `gold_yearly_trends`
Year-over-year delay performance 2013–2023 using `LAG()` window function.

```
+----+--------------------+--------------------+------------------+
|year|avg_delay_per_flight|yoy_delay_change_pct|yoy_on_time_change|
+----+--------------------+--------------------+------------------+
|2019|13.07               |4.81                |-0.1              |
|2020|5.69                |-56.47              |9.85              |
|2021|10.86               |90.86               |-7.37             |
|2022|13.52               |24.49               |-3.79             |
|2023|15.02               |11.09               |-1.6              |
+----+--------------------+--------------------+------------------+
```

---

## Spark Concepts Demonstrated

| Concept | Where used |
|---|---|
| SparkSession + local cluster | `utils.py` |
| Explicit StructType schema | `bronze_layer.py` |
| Lazy evaluation + DAG | All transformations |
| Narrow transformations | `filter()`, `withColumn()`, `fillna()` |
| Wide transformations + shuffle | `groupBy()`, `join()`, window functions |
| Partitioning | Silver layer — `partitionBy("year")` |
| DataFrame API | All layers |
| Spark SQL | Gold layer — `createOrReplaceTempView` |
| Window functions | `RANK()`, `DENSE_RANK()`, `LAG()` |
| Parquet read/write | All layers |
| Idempotent writes | `mode("overwrite")` throughout |
| Medallion architecture | Bronze → Silver → Gold |
| Audit columns | `ingested_at`, `pipeline_ts` |
| AQE (Adaptive Query Execution) | Enabled in SparkSession config |

---

## Setup and Usage

### Prerequisites
- Python 3.11–3.13
- Java 11 or 17
- PySpark 3.5.x
- Windows: winutils 3.3.5 in `C:\hadoop\bin\`

### Install
```bash
pip install -r requirements.txt
```

### Download Data
Get the dataset from BTS (Bureau of Transportation Statistics):
```
https://www.transtats.bts.gov/OT_Delay/OT_DelayCause1.asp
```
Or search **"Airline Delay Cause"** on Kaggle. Place as `data/raw/Airline_Delay_Cause.csv`.

### Run the full pipeline
```bash
python pipeline.py
```

### Run individual layers
```bash
python pipeline.py --bronze    # Bronze only
python pipeline.py --silver    # Silver only
python pipeline.py --gold      # Gold only
```

### Expected output
```
PIPELINE COMPLETE in ~21s
Bronze ✓  Silver ✓  Gold ✓
171,666 rows ingested → 171,426 clean rows → 4 Gold KPI tables
```

---

## Dataset

**Source:** US Bureau of Transportation Statistics (BTS) — Airline On-Time Performance  
**Coverage:** 2013–2023 (11 years)  
**Records:** 171,666 (one row per carrier + airport + month)  
**Airlines:** 23 US carriers  
**Airports:** 395 US airports  
**Size:** ~28MB CSV  

---

## Tech Stack

- **Apache PySpark 3.5.4** — distributed data processing
- **Python 3.13** — pipeline orchestration
- **Parquet** — columnar storage format
- **Medallion architecture** — Bronze/Silver/Gold data layers

---

## What I Would Add Next

- Delta Lake integration for ACID transactions and time travel
- Incremental load pattern using watermark on ingestion date
- Databricks deployment with scheduled Jobs
- Data quality checks using Great Expectations
- Structured Streaming version for real-time delay monitoring