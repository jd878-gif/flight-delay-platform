"""
pipeline.py — Main Entry Point
================================
Runs the complete Flight Delay Intelligence Platform pipeline:
  Bronze → Silver → Gold

Usage:
  python pipeline.py              # runs all layers
  python pipeline.py --bronze     # runs Bronze only
  python pipeline.py --silver     # runs Silver only
  python pipeline.py --gold       # runs Gold only

This is what you run from the VS Code terminal to execute the full pipeline.
"""

import sys
import time
import argparse
import os

# Add src/ to path so we can import our modules
sys.path.append(os.path.join(os.path.dirname(__file__), "src"))

from utils import get_spark, get_logger, BRONZE_FLIGHTS, SILVER_FLIGHTS
from bronz_layer import run_bronze, RAW_FLIGHTS
from silver_layer import run_silver
from gold_layer import run_gold


def main(run_all=True, bronze=False, silver=False, gold=False):
    log   = get_logger("pipeline")
    spark = get_spark("FlightDelay_Pipeline")

    start_time = time.time()

    log.info("╔══════════════════════════════════════════════════════╗")
    log.info("║   US FLIGHT DELAY INTELLIGENCE PLATFORM             ║")
    log.info("║   Bronze → Silver → Gold                            ║")
    log.info("╚══════════════════════════════════════════════════════╝")

    try:
        # ── Bronze ──────────────────────────────────────────
        if run_all or bronze:
            t0 = time.time()
            bronze_count = run_bronze(spark, RAW_FLIGHTS, BRONZE_FLIGHTS)
            log.info(f"Bronze complete in {time.time()-t0:.1f}s "
                     f"| {bronze_count:,} rows")

        # ── Silver ──────────────────────────────────────────
        if run_all or silver:
            t0 = time.time()
            silver_count = run_silver(spark, BRONZE_FLIGHTS, SILVER_FLIGHTS)
            log.info(f"Silver complete in {time.time()-t0:.1f}s "
                     f"| {silver_count:,} rows")

        # ── Gold ────────────────────────────────────────────
        if run_all or gold:
            t0 = time.time()
            run_gold(spark, SILVER_FLIGHTS)
            log.info(f"Gold complete in {time.time()-t0:.1f}s | 4 tables")

        # ── Summary ─────────────────────────────────────────
        elapsed = time.time() - start_time
        log.info("╔══════════════════════════════════════════════════════╗")
        log.info(f"║  PIPELINE COMPLETE in {elapsed:.1f}s                       ║")
        log.info("║  Bronze ✓  Silver ✓  Gold ✓                        ║")
        log.info("╚══════════════════════════════════════════════════════╝")

    except Exception as e:
        log.error(f"Pipeline failed: {e}")
        raise

    finally:
        spark.stop()
        log.info("SparkSession stopped.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Flight Delay Intelligence Platform Pipeline"
    )
    parser.add_argument("--bronze", action="store_true", help="Run Bronze only")
    parser.add_argument("--silver", action="store_true", help="Run Silver only")
    parser.add_argument("--gold",   action="store_true", help="Run Gold only")
    args = parser.parse_args()

    run_all = not (args.bronze or args.silver or args.gold)
    main(run_all=run_all, bronze=args.bronze,
         silver=args.silver, gold=args.gold)