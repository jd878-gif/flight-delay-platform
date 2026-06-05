"""
bronze_layer.py — Bronze Layer: Raw Ingestion
=============================================
Rule: Read raw CSV exactly as-is → write to Parquet.
No transformations. No cleaning. No business logic.
This layer is your permanent raw archive.

What this file does:
  1. Reads Airline_Delay_Cause.csv with an explicit schema
  2. Adds a pipeline audit timestamp
  3. Writes to data/bronze/flights/ as Parquet
  4. Idempotent: mode("overwrite") — safe to re-run

Topics demonstrated:
  - Explicit StructType schema (never inferSchema in production)
  - spark.read.csv with options
  - mode("overwrite") idempotency
  - Parquet write
  - Medallion Bronze layer pattern
"""

from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, StringType, DoubleType
)
from pyspark.sql.functions import current_timestamp, lit

# Import shared utilities
import sys, os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from utils import get_spark, get_logger, RAW_FLIGHTS, BRONZE_FLIGHTS


# ── Schema ──────────────────────────────────────────────────────────────────
# We define this explicitly instead of using inferSchema=True because:
#   1. inferSchema reads the file TWICE (slow on large files)
#   2. It can guess types wrong on messy data
#   3. Explicit schema = self-documenting code
#
# Note: In Bronze we keep types close to raw.
# String dates, nullable everything — Bronze is a faithful copy.

FLIGHTS_SCHEMA = StructType([
    StructField("year",                IntegerType(), True),
    StructField("month",               IntegerType(), True),
    StructField("carrier",             StringType(),  True),
    StructField("carrier_name",        StringType(),  True),
    StructField("airport",             StringType(),  True),
    StructField("airport_name",        StringType(),  True),
    StructField("arr_flights",         DoubleType(),  True),
    StructField("arr_del15",           DoubleType(),  True),   # arrivals delayed 15+ mins
    StructField("carrier_ct",          DoubleType(),  True),   # delay count: carrier cause
    StructField("weather_ct",          DoubleType(),  True),   # delay count: weather cause
    StructField("nas_ct",              DoubleType(),  True),   # delay count: NAS cause
    StructField("security_ct",         DoubleType(),  True),   # delay count: security cause
    StructField("late_aircraft_ct",    DoubleType(),  True),   # delay count: late aircraft
    StructField("arr_cancelled",       DoubleType(),  True),
    StructField("arr_diverted",        DoubleType(),  True),
    StructField("arr_delay",           DoubleType(),  True),   # total delay minutes
    StructField("carrier_delay",       DoubleType(),  True),   # delay mins: carrier cause
    StructField("weather_delay",       DoubleType(),  True),   # delay mins: weather cause
    StructField("nas_delay",           DoubleType(),  True),   # delay mins: NAS cause
    StructField("security_delay",      DoubleType(),  True),   # delay mins: security cause
    StructField("late_aircraft_delay", DoubleType(),  True),   # delay mins: late aircraft
])


# ── Bronze function ──────────────────────────────────────────────────────────

def run_bronze(spark, raw_path: str, bronze_path: str) -> int:
    """
    Reads raw CSV and writes to Bronze Parquet layer.

    Args:
        spark:       active SparkSession
        raw_path:    path to Airline_Delay_Cause.csv
        bronze_path: output path for bronze Parquet files

    Returns:
        row_count: number of rows written
    """
    log = get_logger(__name__)
    log.info("=" * 55)
    log.info("BRONZE LAYER — starting")
    log.info(f"  Source : {raw_path}")
    log.info(f"  Output : {bronze_path}")

    # ── Read raw CSV with explicit schema ──────────────────
    df_raw = (
        spark.read
        .schema(FLIGHTS_SCHEMA)          # explicit schema — no guessing
        .option("header", "true")        # first row is the header
        .option("mode", "PERMISSIVE")    # bad rows → null instead of crash
        .option("nullValue", "")         # treat empty strings as null
        .csv(raw_path)
    )

    row_count = df_raw.count()
    log.info(f"  Rows read from CSV : {row_count:,}")
    log.info(f"  Columns            : {len(df_raw.columns)}")

    # ── Add audit column ───────────────────────────────────
    # pipeline_ts tells you exactly when this record was ingested.
    # Essential for debugging production issues.
    df_bronze = df_raw.withColumn("ingested_at", current_timestamp())

    # ── Write to Bronze as Parquet ─────────────────────────
    # mode("overwrite") = idempotent: re-running this replaces
    # the output cleanly. Same input → same output every time.
    (
        df_bronze.write
        .mode("overwrite")
        .parquet(bronze_path)
    )

    log.info(f"  Rows written to Bronze : {row_count:,}")
    log.info("BRONZE LAYER — complete ✓")
    log.info("=" * 55)

    return row_count


# ── Entry point ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    spark = get_spark("FlightDelay_Bronze")
    log   = get_logger(__name__)

    try:
        count = run_bronze(spark, RAW_FLIGHTS, BRONZE_FLIGHTS)
        log.info(f"Bronze layer finished. {count:,} rows written.")

        # Quick verification — read back what we wrote
        log.info("Verifying Bronze output...")
        df_verify = spark.read.parquet(BRONZE_FLIGHTS)
        log.info(f"  Verification read: {df_verify.count():,} rows, "
                 f"{len(df_verify.columns)} columns")
        df_verify.printSchema()
        df_verify.show(3, truncate=False)

    finally:
        spark.stop()
        log.info("SparkSession stopped.")