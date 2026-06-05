"""
utils.py — Shared utilities for the Flight Delay Intelligence Platform
Provides: SparkSession factory, logger, and path configuration
"""

import os
import logging
from pyspark.sql import SparkSession


# ── Path configuration ─────────────────────────────────────────────────────
# All paths are relative to the project root.
# Change BASE_DIR if you move the project folder.

BASE_DIR   = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

RAW_DIR    = os.path.join(BASE_DIR, "data", "raw")
BRONZE_DIR = os.path.join(BASE_DIR, "data", "bronze")
SILVER_DIR = os.path.join(BASE_DIR, "data", "silver")
GOLD_DIR   = os.path.join(BASE_DIR, "data", "gold")

# Raw file
RAW_FLIGHTS = os.path.join(RAW_DIR, "Airline_Delay_Cause.csv")

# Bronze paths
BRONZE_FLIGHTS = os.path.join(BRONZE_DIR, "flights")

# Silver paths
SILVER_FLIGHTS = os.path.join(SILVER_DIR, "flights_enriched")

# Gold paths
GOLD_AIRLINE_PERFORMANCE = os.path.join(GOLD_DIR, "airline_performance")
GOLD_DELAY_CAUSES        = os.path.join(GOLD_DIR, "delay_causes")
GOLD_AIRPORT_RANKINGS    = os.path.join(GOLD_DIR, "airport_rankings")
GOLD_YEARLY_TRENDS       = os.path.join(GOLD_DIR, "yearly_trends")


# ── Logger ─────────────────────────────────────────────────────────────────

def get_logger(name: str) -> logging.Logger:
    """
    Returns a configured logger.
    Usage: log = get_logger(__name__)
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    return logging.getLogger(name)


# ── SparkSession factory ────────────────────────────────────────────────────

def get_spark(app_name: str = "FlightDelayPlatform") -> SparkSession:
    """
    Creates and returns a SparkSession configured for local development.

    On Windows: if you see a HADOOP_HOME error, uncomment the os.environ line.
    On a cloud cluster: change master("local[*]") to master("yarn").
    """
    # Uncomment this line if you see HADOOP_HOME / winutils errors on Windows:
    os.environ["HADOOP_HOME"] = "C:\\hadoop"

    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.adaptive.enabled", "true")        # AQE — auto-optimizes joins
        .config("spark.sql.shuffle.partitions", "8")          # tuned for local dev (default 200 is too high)
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")  # safe incremental writes
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")  # silence noisy INFO/WARN logs
    return spark