"""
silver_layer.py — Silver Layer: Cleaning & Enrichment
======================================================
Rule: Read Bronze → clean → validate → enrich → write Silver.
This is where most engineering work lives.

What this file does:
  1. Reads Bronze Parquet
  2. Fills nulls (intentional nulls = no flights that month → 0)
  3. Computes derived columns:
       - on_time_pct        : % of flights arriving on time
       - cancellation_rate  : % of flights cancelled
       - avg_delay_per_flight: total delay mins / total flights
       - delay_category     : Low / Medium / High / Severe
       - dominant_cause     : which delay type caused most minutes
       - pct_carrier_delay  : % of delay minutes from carrier issues
       - pct_weather_delay  : % of delay minutes from weather
       - pct_nas_delay      : % of delay minutes from NAS
       - pct_late_aircraft  : % of delay minutes from late aircraft
  4. Filters rows with zero flights (not useful for analysis)
  5. Writes partitioned by year — fast for year-filtered Gold queries

Topics demonstrated:
  - fillna() for null handling
  - withColumn() + when().otherwise() for classification
  - Complex column expressions
  - partitionBy() for query optimization
  - Audit columns (pipeline_ts)
  - Reading from Bronze (never from raw in Silver)
"""

from pyspark.sql.functions import (
    col, when, round, lit, current_timestamp,
    greatest
)

import sys, os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from utils import get_spark, get_logger, BRONZE_FLIGHTS, SILVER_FLIGHTS


# ── Silver function ──────────────────────────────────────────────────────────

def run_silver(spark, bronze_path: str, silver_path: str) -> int:
    """
    Reads Bronze, cleans and enriches, writes Silver partitioned by year.

    Args:
        spark:       active SparkSession
        bronze_path: path to Bronze Parquet
        silver_path: output path for Silver Parquet

    Returns:
        row_count: number of rows written to Silver
    """
    log = get_logger(__name__)
    log.info("=" * 55)
    log.info("SILVER LAYER — starting")
    log.info(f"  Source : {bronze_path}")
    log.info(f"  Output : {silver_path}")

    # ── Read from Bronze ───────────────────────────────────
    # Always read from the upstream layer, never from raw CSV
    df = spark.read.parquet(bronze_path)
    raw_count = df.count()
    log.info(f"  Rows from Bronze : {raw_count:,}")

    # ── Step 1: Fill nulls ─────────────────────────────────
    # From exploration: nulls mean "carrier didn't serve this
    # airport that month" — not missing data. Fill with 0.
    numeric_cols = [
        "arr_flights", "arr_del15", "carrier_ct", "weather_ct",
        "nas_ct", "security_ct", "late_aircraft_ct",
        "arr_cancelled", "arr_diverted", "arr_delay",
        "carrier_delay", "weather_delay", "nas_delay",
        "security_delay", "late_aircraft_delay"
    ]
    df = df.fillna(0, subset=numeric_cols)

    # ── Step 2: Filter rows with zero flights ──────────────
    # Rows where arr_flights = 0 carry no useful information.
    # We keep them in Bronze (faithful archive) but exclude from Silver.
    df_before_filter = df.count()
    df = df.filter(col("arr_flights") > 0)
    df_after_filter = df.count()
    log.info(f"  Rows after filtering zero-flight rows : "
             f"{df_after_filter:,} (removed {df_before_filter - df_after_filter:,})")

    # ── Step 3: Compute performance metrics ───────────────
    # These are the core KPIs every Gold table will use.
    # We compute once in Silver so Gold never duplicates logic.

    df = df.withColumn(
        "on_time_pct",
        # Flights that were NOT delayed 15+ mins, as a percentage
        round(
            ((col("arr_flights") - col("arr_del15")) / col("arr_flights")) * 100,
            2
        )
    ).withColumn(
        "cancellation_rate",
        round((col("arr_cancelled") / col("arr_flights")) * 100, 2)
    ).withColumn(
        "avg_delay_per_flight",
        # Total delay minutes divided by total flights
        # Measures delay burden per flight — better than raw totals
        round(col("arr_delay") / col("arr_flights"), 2)
    )

    # ── Step 4: Delay category ─────────────────────────────
    # Classifies each carrier+airport+month record by severity.
    # Based on avg_delay_per_flight (minutes of delay per flight).
    df = df.withColumn(
        "delay_category",
        when(col("avg_delay_per_flight") >= 30, "Severe")
        .when(col("avg_delay_per_flight") >= 20, "High")
        .when(col("avg_delay_per_flight") >= 10, "Medium")
        .otherwise("Low")
    )

    # ── Step 5: Dominant delay cause ──────────────────────
    # Which delay type caused the most minutes for this record?
    # Uses greatest() to find the max across the 5 cause columns,
    # then maps it back to a readable label.
    df = df.withColumn(
        "dominant_cause",
        when(
            col("late_aircraft_delay") == greatest(
                col("carrier_delay"), col("weather_delay"),
                col("nas_delay"), col("security_delay"),
                col("late_aircraft_delay")
            ), "Late Aircraft"
        ).when(
            col("carrier_delay") == greatest(
                col("carrier_delay"), col("weather_delay"),
                col("nas_delay"), col("security_delay"),
                col("late_aircraft_delay")
            ), "Carrier"
        ).when(
            col("nas_delay") == greatest(
                col("carrier_delay"), col("weather_delay"),
                col("nas_delay"), col("security_delay"),
                col("late_aircraft_delay")
            ), "NAS"
        ).when(
            col("weather_delay") == greatest(
                col("carrier_delay"), col("weather_delay"),
                col("nas_delay"), col("security_delay"),
                col("late_aircraft_delay")
            ), "Weather"
        ).otherwise("Security")
    )

    # ── Step 6: Delay cause percentages ───────────────────
    # What fraction of delay minutes came from each cause?
    # Used in Gold delay_causes table.
    # Guard against division by zero with when().otherwise(0)
    df = df.withColumn(
        "pct_carrier_delay",
        round(
            when(col("arr_delay") > 0,
                 (col("carrier_delay") / col("arr_delay")) * 100
            ).otherwise(0), 2
        )
    ).withColumn(
        "pct_weather_delay",
        round(
            when(col("arr_delay") > 0,
                 (col("weather_delay") / col("arr_delay")) * 100
            ).otherwise(0), 2
        )
    ).withColumn(
        "pct_nas_delay",
        round(
            when(col("arr_delay") > 0,
                 (col("nas_delay") / col("arr_delay")) * 100
            ).otherwise(0), 2
        )
    ).withColumn(
        "pct_late_aircraft_delay",
        round(
            when(col("arr_delay") > 0,
                 (col("late_aircraft_delay") / col("arr_delay")) * 100
            ).otherwise(0), 2
        )
    )

    # ── Step 7: Audit column ───────────────────────────────
    df = df.withColumn("pipeline_ts", current_timestamp())

    # ── Step 8: Final validation ───────────────────────────
    silver_count = df.count()
    log.info(f"  Rows in Silver     : {silver_count:,}")
    log.info(f"  New columns added  : on_time_pct, cancellation_rate, "
             f"avg_delay_per_flight, delay_category, dominant_cause, "
             f"pct_* (4 cause percentages)")

    # ── Step 9: Write Silver partitioned by year ───────────
    # partitionBy("year") creates folders: silver/flights_enriched/year=2023/
    # When Gold queries only 2023, Spark skips all other year folders.
    # This is partition pruning — makes Gold queries much faster.
    (
        df.write
        .mode("overwrite")
        .partitionBy("year")
        .parquet(silver_path)
    )

    log.info(f"  Written to Silver (partitioned by year) : {silver_path}")
    log.info("SILVER LAYER — complete ✓")
    log.info("=" * 55)

    return silver_count


# ── Entry point ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    spark = get_spark("FlightDelay_Silver")
    log   = get_logger(__name__)

    try:
        count = run_silver(spark, BRONZE_FLIGHTS, SILVER_FLIGHTS)
        log.info(f"Silver layer finished. {count:,} rows written.")

        # Quick verification
        log.info("Verifying Silver output...")
        df_verify = spark.read.parquet(SILVER_FLIGHTS)
        log.info(f"  Verification read : {df_verify.count():,} rows, "
                 f"{len(df_verify.columns)} columns")
        df_verify.printSchema()

        # Sample a few rows to confirm computed columns look right
        df_verify.select(
            "carrier_name", "airport", "month",
            "arr_flights", "avg_delay_per_flight",
            "delay_category", "dominant_cause",
            "pct_carrier_delay", "pct_weather_delay",
            "pct_late_aircraft_delay"
        ).orderBy("avg_delay_per_flight", ascending=False).show(5, truncate=False)

    finally:
        spark.stop()
        log.info("SparkSession stopped.")