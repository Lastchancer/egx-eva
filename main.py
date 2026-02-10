#!/usr/bin/env python3
"""
EGX EVA Analyzer — Main Entry Point
Orchestrates the full pipeline: Discover → Collect → Compute → Report

Usage:
    python main.py                         # Full pipeline
    python main.py --collect-only          # Data collection only
    python main.py --eva-only              # EVA calc on existing data
    python main.py --tickers COMI TMGH     # Specific stocks only
    python main.py --sources yahoo mubasher # Specific sources only
    python main.py --export xlsx           # Export format
    python main.py --quick                 # Quick mode: Yahoo only, known tickers
"""

import argparse
import logging
import sys
import time
from datetime import datetime

from config import KNOWN_EGX_TICKERS, CollectionConfig
import database as db
from collectors import (
    YahooFinanceCollector,
    MubasherCollector,
    EGXOfficialCollector,
    StockAnalysisCollector,
)
from eva_engine import EVAEngine
from report_generator import ReportGenerator

# ═══════════════════════════════════════════════════════════════
# LOGGING SETUP
# ═══════════════════════════════════════════════════════════════

def setup_logging(verbose: bool = True):
    """Configure logging with colored console output."""
    level = logging.INFO if verbose else logging.WARNING
    fmt = "%(asctime)s │ %(levelname)-7s │ %(message)s"
    datefmt = "%H:%M:%S"

    logging.basicConfig(level=level, format=fmt, datefmt=datefmt,
                       handlers=[logging.StreamHandler(sys.stdout)])

    # Also log to file
    file_handler = logging.FileHandler("egx_agent.log")
    file_handler.setFormatter(logging.Formatter(fmt, datefmt=datefmt))
    logging.getLogger().addHandler(file_handler)


# ═══════════════════════════════════════════════════════════════
# PIPELINE STEPS
# ═══════════════════════════════════════════════════════════════

def step_discover(conn, config: CollectionConfig) -> list:
    """Step 1: Discover all EGX tickers from multiple sources."""
    logger = logging.getLogger(__name__)
    logger.info("\n" + "═" * 60)
    logger.info("STEP 1: TICKER DISCOVERY")
    logger.info("═" * 60)

    all_tickers = set(KNOWN_EGX_TICKERS)

    if config.tickers:
        # User specified tickers — use those
        all_tickers = set(config.tickers)
        logger.info(f"Using {len(all_tickers)} user-specified tickers")
    else:
        # Discover from EGX official site
        if "egx" in config.sources:
            try:
                egx = EGXOfficialCollector(conn)
                discovered = egx.discover_all_listed_stocks()
                new_tickers = {d["ticker"] for d in discovered}
                all_tickers |= new_tickers
                logger.info(f"EGX Official: +{len(new_tickers)} tickers")
            except Exception as e:
                logger.warning(f"EGX discovery failed: {e}")

        # Discover from Mubasher
        if "mubasher" in config.sources:
            try:
                mub = MubasherCollector(conn)
                discovered = mub.discover_tickers()
                new_tickers = {d["ticker"] for d in discovered}
                all_tickers |= new_tickers
                logger.info(f"Mubasher: +{len(new_tickers)} tickers")
            except Exception as e:
                logger.warning(f"Mubasher discovery failed: {e}")

    # Seed all tickers into the database
    for ticker in all_tickers:
        db.upsert_company(conn, ticker=ticker, source="seed")

    tickers = sorted(all_tickers)
    logger.info(f"Total unique tickers: {len(tickers)}")
    return tickers


def step_collect(conn, tickers: list, config: CollectionConfig):
    """Step 2: Collect financial data from all sources."""
    logger = logging.getLogger(__name__)
    logger.info("\n" + "═" * 60)
    logger.info("STEP 2: DATA COLLECTION")
    logger.info(f"Sources: {', '.join(config.sources)}")
    logger.info(f"Tickers: {len(tickers)}")
    logger.info("═" * 60)

    total_records = 0
    start = time.time()

    # ── Yahoo Finance (primary source — most structured data) ──
    if "yahoo" in config.sources:
        logger.info("\n── Yahoo Finance ──")
        try:
            yahoo = YahooFinanceCollector(conn)
            results = yahoo.collect_all(tickers)
            records = sum(r["records"] for r in results)
            total_records += records
            success = sum(1 for r in results if r["records"] > 0)
            logger.info(f"Yahoo Finance: {records} records from {success}/{len(tickers)} stocks")
        except Exception as e:
            logger.error(f"Yahoo Finance collection failed: {e}")

    # ── Mubasher Info ──
    if "mubasher" in config.sources:
        logger.info("\n── Mubasher Info ──")
        try:
            mubasher = MubasherCollector(conn)
            results = mubasher.collect_all(tickers)
            records = sum(r["records"] for r in results)
            total_records += records
            logger.info(f"Mubasher: {records} records collected")
        except Exception as e:
            logger.error(f"Mubasher collection failed: {e}")

    # ── StockAnalysis.com ──
    if "stockanalysis" in config.sources:
        logger.info("\n── StockAnalysis.com ──")
        try:
            sa = StockAnalysisCollector(conn)
            results = sa.collect_all(tickers)
            records = sum(r["records"] for r in results)
            total_records += records
            logger.info(f"StockAnalysis: {records} records collected")
        except Exception as e:
            logger.error(f"StockAnalysis collection failed: {e}")

    duration = time.time() - start
    logger.info(f"\nCollection complete: {total_records} total records in {duration:.1f}s")


def step_eva(conn, tickers: list) -> list:
    """Step 3: Run EVA calculations."""
    logger = logging.getLogger(__name__)
    logger.info("\n" + "═" * 60)
    logger.info("STEP 3: EVA CALCULATION")
    logger.info("═" * 60)

    engine = EVAEngine(conn)
    results = engine.run_all(tickers)
    return results


def step_report(conn, config: CollectionConfig):
    """Step 4: Generate reports."""
    logger = logging.getLogger(__name__)
    logger.info("\n" + "═" * 60)
    logger.info("STEP 4: REPORT GENERATION")
    logger.info("═" * 60)

    reporter = ReportGenerator(conn)
    reporter.generate_all(export_format=config.export_format)


# ═══════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════

def parse_args():
    parser = argparse.ArgumentParser(
        description="EGX EVA Analyzer — Automated Egyptian Stock Market Analysis",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py                              # Full pipeline
  python main.py --quick                      # Quick mode (Yahoo only)
  python main.py --tickers COMI TMGH SWDY     # Specific stocks
  python main.py --collect-only               # Just collect data
  python main.py --eva-only                   # Just compute EVA
  python main.py --export xlsx                # Export as Excel
  python main.py --sources yahoo stockanalysis # Specific data sources
        """
    )

    parser.add_argument("--tickers", nargs="+", help="Specific tickers to analyze")
    parser.add_argument("--sources", nargs="+",
                       choices=["yahoo", "mubasher", "egx", "stockanalysis"],
                       default=["yahoo", "mubasher", "egx", "stockanalysis"],
                       help="Data sources to use")
    parser.add_argument("--collect-only", action="store_true", help="Only collect data")
    parser.add_argument("--eva-only", action="store_true", help="Only run EVA (use existing data)")
    parser.add_argument("--report-only", action="store_true", help="Only generate reports")
    parser.add_argument("--export", choices=["csv", "json", "xlsx"], default="csv",
                       help="Export format for reports")
    parser.add_argument("--quick", action="store_true",
                       help="Quick mode: Yahoo only, known tickers")
    parser.add_argument("--quiet", action="store_true", help="Suppress verbose output")

    return parser.parse_args()


def main():
    args = parse_args()
    setup_logging(verbose=not args.quiet)
    logger = logging.getLogger(__name__)

    # Build config
    config = CollectionConfig(
        tickers=args.tickers,
        sources=["yahoo"] if args.quick else args.sources,
        export_format=args.export,
        verbose=not args.quiet,
    )

    if args.quick:
        config.tickers = config.tickers or KNOWN_EGX_TICKERS

    logger.info("╔══════════════════════════════════════════════════════════╗")
    logger.info("║        EGX EVA ANALYZER — Data Collection Agent         ║")
    logger.info("║   Automated Egyptian Stock Market Value Analysis         ║")
    logger.info("╚══════════════════════════════════════════════════════════╝")
    logger.info(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Sources: {', '.join(config.sources)}")
    logger.info(f"Export:  {config.export_format}")

    pipeline_start = time.time()

    # Connect to database
    conn = db.get_connection()

    try:
        if args.report_only:
            step_report(conn, config)
        elif args.eva_only:
            tickers = config.tickers or db.get_all_tickers(conn)
            step_eva(conn, tickers)
            step_report(conn, config)
        elif args.collect_only:
            tickers = step_discover(conn, config)
            step_collect(conn, tickers, config)
        else:
            # Full pipeline
            tickers = step_discover(conn, config)
            step_collect(conn, tickers, config)
            step_eva(conn, tickers)
            step_report(conn, config)

    except KeyboardInterrupt:
        logger.info("\n⚠ Interrupted by user. Partial results saved.")
    except Exception as e:
        logger.error(f"\n✗ Pipeline error: {e}", exc_info=True)
    finally:
        conn.close()

    total_time = time.time() - pipeline_start
    logger.info(f"\nTotal pipeline time: {total_time:.1f}s")
    logger.info("Done! ✓")


if __name__ == "__main__":
    main()
