"""
gold_layer.py — Gold Layer: Business KPI Tables
================================================
Rule: Read Silver → aggregate → write Gold.
These are the tables stakeholders, dashboards, and analysts use.

Four Gold tables:

  1. gold_airline_performance
     Who operates the best and worst? RANK() by avg delay per flight.

  2. gold_delay_causes
     WHY are flights delayed? Carrier vs Weather vs NAS vs Late Aircraft.
     Breakdown by airline across all years.

  3. gold_airport_rankings
     Which airports are the worst to fly from?
     DENSE_RANK() by total arrival delay minutes.

  4. gold_yearly_trends
     How has delay performance changed 2013–2023?
     LAG() window function to compute year-over-year change %.

Topics demonstrated:
  - Spark SQL with createOrReplaceTempView
  - Window functions: RANK(), DENSE_RANK(), LAG()
  - groupBy + agg with multiple aggregations
  - .alias() on every aggregated column
  - Broadcast join (airports lookup is small)
  - orderBy for readable output
  - mode("overwrite") idempotent Gold writes
"""

from pyspark.sql.functions import (
    col, round, sum, avg, count, max, min,
    rank, dense_rank, lag, desc
)
from pyspark.sql import Window
from pyspark.sql.functions import broadcast

import sys, os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from utils import (
    get_spark, get_logger,
    SILVER_FLIGHTS,
    GOLD_AIRLINE_PERFORMANCE,
    GOLD_DELAY_CAUSES,
    GOLD_AIRPORT_RANKINGS,
    GOLD_YEARLY_TRENDS
)


# ── Gold table 1: Airline performance ranking ────────────────────────────────

def build_airline_performance(spark, silver_path: str, gold_path: str) -> None:
    """
    Aggregates performance metrics per airline across all years.
    Uses RANK() window function to rank airlines best → worst.

    Output columns:
      carrier, carrier_name, total_flights, total_delay_mins,
      avg_delay_per_flight, avg_on_time_pct, avg_cancellation_rate,
      performance_rank
    """
    log = get_logger(__name__)
    log.info("Building gold_airline_performance...")

    df = spark.read.parquet(silver_path)

    # Register as SQL temp view — demonstrates Spark SQL pattern
    df.createOrReplaceTempView("silver_flights")

    # Use Spark SQL for the aggregation — readable, SQL-familiar
    df_agg = spark.sql("""
        SELECT
            carrier,
            carrier_name,
            ROUND(SUM(arr_flights), 0)          AS total_flights,
            ROUND(SUM(arr_delay), 0)             AS total_delay_mins,
            ROUND(SUM(arr_cancelled), 0)         AS total_cancellations,
            ROUND(AVG(avg_delay_per_flight), 2)  AS avg_delay_per_flight,
            ROUND(AVG(on_time_pct), 2)           AS avg_on_time_pct,
            ROUND(AVG(cancellation_rate), 2)     AS avg_cancellation_rate
        FROM silver_flights
        GROUP BY carrier, carrier_name
    """)

    # Apply RANK() window function — rank airlines by avg delay (lower = better)
    # Window spec: no partition (rank across ALL airlines), order by avg delay asc
    window_spec = Window.orderBy(col("avg_delay_per_flight").asc())

    df_ranked = df_agg.withColumn(
        "performance_rank",
        rank().over(window_spec)
    ).orderBy("performance_rank")

    df_ranked.write.mode("overwrite").parquet(gold_path)
    log.info(f"  gold_airline_performance: {df_ranked.count()} airlines ranked ✓")

    # Preview
    print("\n=== GOLD: Airline Performance Rankings ===")
    df_ranked.select(
        "performance_rank", "carrier_name",
        "avg_delay_per_flight", "avg_on_time_pct",
        "avg_cancellation_rate", "total_flights"
    ).show(21, truncate=False)


# ── Gold table 2: Delay cause breakdown by airline ───────────────────────────

def build_delay_causes(spark, silver_path: str, gold_path: str) -> None:
    """
    Shows WHY flights are delayed for each airline.
    Percentage breakdown: Carrier / Weather / NAS / Late Aircraft / Security.

    Key insight: Late Aircraft is the #1 cause — not weather as most assume.
    """
    log = get_logger(__name__)
    log.info("Building gold_delay_causes...")

    df = spark.read.parquet(silver_path)
    df.createOrReplaceTempView("silver_flights")

    df_causes = spark.sql("""
        SELECT
            carrier_name,
            ROUND(SUM(arr_delay), 0)                    AS total_delay_mins,
            ROUND(AVG(pct_carrier_delay), 2)            AS avg_pct_carrier,
            ROUND(AVG(pct_weather_delay), 2)            AS avg_pct_weather,
            ROUND(AVG(pct_nas_delay), 2)                AS avg_pct_nas,
            ROUND(AVG(pct_late_aircraft_delay), 2)      AS avg_pct_late_aircraft,
            ROUND(
                100 - AVG(pct_carrier_delay)
                    - AVG(pct_weather_delay)
                    - AVG(pct_nas_delay)
                    - AVG(pct_late_aircraft_delay), 2
            )                                           AS avg_pct_security
        FROM silver_flights
        GROUP BY carrier_name
        ORDER BY total_delay_mins DESC
    """)

    df_causes.write.mode("overwrite").parquet(gold_path)
    log.info(f"  gold_delay_causes: {df_causes.count()} airlines ✓")

    print("\n=== GOLD: Delay Cause Breakdown by Airline ===")
    df_causes.show(21, truncate=False)


# ── Gold table 3: Airport departure delay rankings ───────────────────────────

def build_airport_rankings(spark, silver_path: str, gold_path: str) -> None:
    """
    Ranks the 50 busiest airports by average arrival delay.
    Uses DENSE_RANK() so tied airports share the same rank.

    Filters to airports with 50,000+ total flights to avoid
    small airports with few flights skewing the rankings.
    """
    log = get_logger(__name__)
    log.info("Building gold_airport_rankings...")

    df = spark.read.parquet(silver_path)
    df.createOrReplaceTempView("silver_flights")

    df_airports = spark.sql("""
        SELECT
            airport,
            airport_name,
            ROUND(SUM(arr_flights), 0)         AS total_flights,
            ROUND(SUM(arr_delay), 0)           AS total_delay_mins,
            ROUND(SUM(arr_cancelled), 0)       AS total_cancellations,
            ROUND(AVG(avg_delay_per_flight), 2) AS avg_delay_per_flight,
            ROUND(AVG(on_time_pct), 2)         AS avg_on_time_pct,
            ROUND(AVG(cancellation_rate), 2)   AS avg_cancellation_rate
        FROM silver_flights
        GROUP BY airport, airport_name
        HAVING SUM(arr_flights) >= 50000
    """)

    # DENSE_RANK: tied airports get same rank, no gaps in sequence
    # e.g. if two airports tie for rank 3, next airport is rank 4 (not 5)
    window_spec = Window.orderBy(col("avg_delay_per_flight").desc())

    df_ranked = df_airports.withColumn(
        "delay_rank",
        dense_rank().over(window_spec)
    ).orderBy("delay_rank")

    df_ranked.write.mode("overwrite").parquet(gold_path)
    log.info(f"  gold_airport_rankings: {df_ranked.count()} airports ranked ✓")

    print("\n=== GOLD: Worst Airports by Avg Arrival Delay (Top 20) ===")
    df_ranked.select(
        "delay_rank", "airport", "airport_name",
        "avg_delay_per_flight", "avg_on_time_pct",
        "total_flights"
    ).show(20, truncate=False)

    print("\n=== GOLD: Best Airports (Bottom 10) ===")
    df_ranked.orderBy("delay_rank", ascending=False) \
        .select(
            "delay_rank", "airport", "airport_name",
            "avg_delay_per_flight", "avg_on_time_pct"
        ).show(10, truncate=False)


# ── Gold table 4: Year-over-year delay trends 2013–2023 ──────────────────────

def build_yearly_trends(spark, silver_path: str, gold_path: str) -> None:
    """
    Shows how US flight delay performance changed each year 2013–2023.
    Uses LAG() window function to compute year-over-year change.

    This is the most technically impressive Gold table — 10 years of
    trend data with YoY % change demonstrates real window function use.
    """
    log = get_logger(__name__)
    log.info("Building gold_yearly_trends...")

    df = spark.read.parquet(silver_path)
    df.createOrReplaceTempView("silver_flights")

    # Step 1: Aggregate by year
    df_yearly = spark.sql("""
        SELECT
            year,
            ROUND(SUM(arr_flights), 0)          AS total_flights,
            ROUND(SUM(arr_cancelled), 0)         AS total_cancellations,
            ROUND(SUM(arr_delay), 0)             AS total_delay_mins,
            ROUND(AVG(avg_delay_per_flight), 2)  AS avg_delay_per_flight,
            ROUND(AVG(on_time_pct), 2)           AS avg_on_time_pct,
            ROUND(AVG(cancellation_rate), 2)     AS avg_cancellation_rate
        FROM silver_flights
        GROUP BY year
        ORDER BY year
    """)

    # Step 2: Apply LAG() window function
    # LAG(col, 1) gets the value from the PREVIOUS row (previous year)
    # Window ordered by year ascending = previous year's value
    window_spec = Window.orderBy("year")

    df_trends = df_yearly \
        .withColumn(
            "prev_year_avg_delay",
            lag("avg_delay_per_flight", 1).over(window_spec)
        ) \
        .withColumn(
            "yoy_delay_change_pct",
            # Year-over-year change: (current - previous) / previous * 100
            # Negative = improvement (less delay), Positive = getting worse
            round(
                ((col("avg_delay_per_flight") - col("prev_year_avg_delay"))
                 / col("prev_year_avg_delay")) * 100,
                2
            )
        ) \
        .withColumn(
            "prev_year_on_time",
            lag("avg_on_time_pct", 1).over(window_spec)
        ) \
        .withColumn(
            "yoy_on_time_change",
            round(
                col("avg_on_time_pct") - col("prev_year_on_time"),
                2
            )
        ) \
        .drop("prev_year_avg_delay", "prev_year_on_time")

    df_trends.write.mode("overwrite").parquet(gold_path)
    log.info(f"  gold_yearly_trends: {df_trends.count()} years ✓")

    print("\n=== GOLD: Yearly Delay Trends 2013–2023 ===")
    print("(yoy_delay_change_pct: negative = improvement, positive = getting worse)")
    df_trends.select(
        "year", "total_flights", "avg_delay_per_flight",
        "avg_on_time_pct", "yoy_delay_change_pct", "yoy_on_time_change"
    ).show(11, truncate=False)


# ── Main Gold runner ─────────────────────────────────────────────────────────

def run_gold(spark, silver_path: str) -> None:
    """Runs all four Gold table builds in sequence."""
    log = get_logger(__name__)
    log.info("=" * 55)
    log.info("GOLD LAYER — starting (4 tables)")

    build_airline_performance(spark, silver_path, GOLD_AIRLINE_PERFORMANCE)
    build_delay_causes(spark, silver_path, GOLD_DELAY_CAUSES)
    build_airport_rankings(spark, silver_path, GOLD_AIRPORT_RANKINGS)
    build_yearly_trends(spark, silver_path, GOLD_YEARLY_TRENDS)

    log.info("GOLD LAYER — all 4 tables complete ✓")
    log.info("=" * 55)


# ── Entry point ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    spark = get_spark("FlightDelay_Gold")
    log   = get_logger(__name__)

    try:
        run_gold(spark, SILVER_FLIGHTS)
    finally:
        spark.stop()
        log.info("SparkSession stopped.")