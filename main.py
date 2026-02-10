#!/usr/bin/env python3
"""
EGX EVA Analyzer — Main Entry Point
Orchestrates: Discover → Collect → Compute → Push to Supabase

Usage:
    python main.py                         # Full pipeline
    python main.py --quick                 # Quick mode: Yahoo only, known tickers
    python main.py --tickers COMI TMGH     # Specific stocks only
    python main.py --sources yahoo mubasher # Specific sources
    python main.py --export xlsx           # Also export locally
"""

import argparse
import logging
import os
import sys
import time
from datetime import datetime
from typing import List, Dict

# ═══════════════════════════════════════════════════════════════
# FIX: Ensure project root is on sys.path so all imports work
# regardless of the working directory (GitHub Actions, cron, etc.)
# ═══════════════════════════════════════════════════════════════
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from config import KNOWN_EGX_TICKERS, CollectionConfig
import database as db

# ═══════════════════════════════════════════════════════════════
# LOGGING SETUP
# ═══════════════════════════════════════════════════════════════

def setup_logging(verbose: bool = True):
    """Configure logging."""
    level = logging.INFO if verbose else logging.WARNING
    fmt = "%(asctime)s | %(levelname)-7s | %(message)s"
    datefmt = "%H:%M:%S"
    logging.basicConfig(level=level, format=fmt, datefmt=datefmt,
                        handlers=[logging.StreamHandler(sys.stdout)])
    try:
        fh = logging.FileHandler(os.path.join(PROJECT_ROOT, "egx_agent.log"))
        fh.setFormatter(logging.Formatter(fmt, datefmt=datefmt))
        logging.getLogger().addHandler(fh)
    except Exception:
        pass

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════
# DYNAMIC IMPORTS — gracefully handle missing optional collectors
# ═══════════════════════════════════════════════════════════════

def _import_collector(name: str):
    """Try to import a collector class. Returns None if unavailable."""
    try:
        if name == "yahoo":
            from collectors.yahoo_collector import YahooFinanceCollector
            return YahooFinanceCollector
        elif name == "mubasher":
            from collectors.mubasher_collector import MubasherCollector
            return MubasherCollector
        elif name == "egx":
            from collectors.egx_collector import EGXOfficialCollector
            return EGXOfficialCollector
        elif name == "stockanalysis":
            from collectors.stockanalysis_collector import StockAnalysisCollector
            return StockAnalysisCollector
    except ImportError as e:
        logger.warning(f"Collector '{name}' not available: {e}")
    return None


# ═══════════════════════════════════════════════════════════════
# PIPELINE STEPS
# ═══════════════════════════════════════════════════════════════

def step_discover(conn, config: CollectionConfig) -> List[str]:
    """Step 1: Determine which tickers to analyze."""
    logger.info("=" * 60)
    logger.info("STEP 1: TICKER DISCOVERY")
    logger.info("=" * 60)

    if config.tickers:
        tickers = sorted(set(config.tickers))
        logger.info(f"Using {len(tickers)} user-specified tickers")
    else:
        tickers = sorted(set(KNOWN_EGX_TICKERS))
        logger.info(f"Using {len(tickers)} known EGX tickers")

    for ticker in tickers:
        db.upsert_company(conn, ticker=ticker, source="seed")

    return tickers


def step_collect(conn, tickers: List[str], config: CollectionConfig) -> int:
    """Step 2: Collect financial data from available sources."""
    logger.info("=" * 60)
    logger.info("STEP 2: DATA COLLECTION")
    logger.info(f"Sources: {', '.join(config.sources)}")
    logger.info(f"Tickers: {len(tickers)}")
    logger.info("=" * 60)

    total_records = 0

    for source_name in config.sources:
        CollectorClass = _import_collector(source_name)
        if CollectorClass is None:
            logger.warning(f"Skipping source '{source_name}' (not available)")
            continue

        logger.info(f"\n--- {source_name.upper()} ---")
        try:
            collector = CollectorClass(conn)
            results = collector.collect_all(tickers)
            records = sum(r.get("records", 0) for r in results)
            success = sum(1 for r in results if r.get("records", 0) > 0)
            total_records += records
            logger.info(f"{source_name}: {records} records from {success}/{len(tickers)} stocks")
        except Exception as e:
            logger.error(f"{source_name} collection failed: {e}")

    logger.info(f"\nTotal records collected: {total_records}")
    return total_records


def step_eva(conn, tickers: List[str]) -> List[Dict]:
    """Step 3: Run EVA calculations."""
    logger.info("=" * 60)
    logger.info("STEP 3: EVA CALCULATION")
    logger.info("=" * 60)

    from eva_engine import EVAEngine
    engine = EVAEngine(conn)
    results = engine.run_all(tickers)
    return results


def step_report(conn, config: CollectionConfig):
    """Step 4: Generate local reports."""
    logger.info("=" * 60)
    logger.info("STEP 4: LOCAL REPORTS")
    logger.info("=" * 60)

    from report_generator import ReportGenerator
    reporter = ReportGenerator(conn)
    reporter.generate_all(export_format=config.export_format)


def step_push_supabase(conn, tickers: List[str], eva_results: List[Dict],
                        total_records: int, duration: float, sources: List[str]):
    """Step 5: Push everything to Supabase cloud database."""
    logger.info("=" * 60)
    logger.info("STEP 5: PUSH TO SUPABASE")
    logger.info("=" * 60)

    supabase_url = os.environ.get("SUPABASE_URL", "")
    supabase_key = os.environ.get("SUPABASE_KEY", "")

    if not supabase_url or not supabase_key:
        logger.warning("SUPABASE_URL / SUPABASE_KEY not set. Skipping cloud push.")
        return

    try:
        import supabase_client as supa
        client = supa.get_client()
    except ImportError as e:
        logger.warning(f"Supabase client not available: {e}")
        return
    except Exception as e:
        logger.error(f"Supabase connection failed: {e}")
        return

    # Push companies
    companies = []
    for ticker in tickers:
        row = conn.execute("SELECT * FROM companies WHERE ticker = ?", (ticker,)).fetchone()
        if row:
            companies.append(dict(row))
    if companies:
        supa.upsert_companies(client, companies)

    # Push latest market data per ticker
    market_rows = []
    for ticker in tickers:
        row = conn.execute(
            "SELECT * FROM market_data WHERE ticker = ? ORDER BY date DESC LIMIT 1",
            (ticker,)
        ).fetchone()
        if row:
            market_rows.append(dict(row))
    if market_rows:
        supa.upsert_market_data(client, market_rows)

    # Push latest income statements
    income_rows = []
    for ticker in tickers:
        row = conn.execute(
            "SELECT * FROM income_statements WHERE ticker = ? ORDER BY period_end DESC LIMIT 1",
            (ticker,)
        ).fetchone()
        if row:
            income_rows.append(dict(row))
    if income_rows:
        supa.upsert_income_statements(client, income_rows)

    # Push latest balance sheets
    balance_rows = []
    for ticker in tickers:
        row = conn.execute(
            "SELECT * FROM balance_sheets WHERE ticker = ? ORDER BY period_end DESC LIMIT 1",
            (ticker,)
        ).fetchone()
        if row:
            balance_rows.append(dict(row))
    if balance_rows:
        supa.upsert_balance_sheets(client, balance_rows)

    # Push EVA results (main table the Lovable frontend reads)
    if eva_results:
        supa.upsert_eva_results(client, eva_results)

    # Log the pipeline run
    undervalued = [r for r in eva_results if r.get("signal") == "UNDERVALUED"]
    positive_eva = [r for r in eva_results if (r.get("eva") or 0) > 0]
    supa.upsert_run_log(client, {
        "stocks_analyzed": len(eva_results),
        "undervalued_count": len(undervalued),
        "positive_eva_count": len(positive_eva),
        "total_records": total_records,
        "sources": sources,
        "duration": duration,
        "status": "success",
    })

    logger.info(f"Supabase push complete: {len(eva_results)} EVA results, "
                f"{len(undervalued)} undervalued")


# ═══════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════

def parse_args():
    parser = argparse.ArgumentParser(
        description="EGX EVA Analyzer — Egyptian Stock Market Value Analysis")
    parser.add_argument("--tickers", nargs="+", help="Specific tickers")
    parser.add_argument("--sources", nargs="+",
                        choices=["yahoo", "mubasher", "egx", "stockanalysis"],
                        default=["yahoo"], help="Data sources (default: yahoo)")
    parser.add_argument("--export", choices=["csv", "json", "xlsx"], default="csv")
    parser.add_argument("--quick", action="store_true", help="Yahoo only, known tickers")
    parser.add_argument("--quiet", action="store_true")
    parser.add_argument("--no-supabase", action="store_true", help="Skip Supabase push")
    return parser.parse_args()


def main():
    args = parse_args()
    setup_logging(verbose=not args.quiet)

    config = CollectionConfig(
        tickers=args.tickers,
        sources=["yahoo"] if args.quick else args.sources,
        export_format=args.export,
        verbose=not args.quiet,
    )
    if args.quick and not config.tickers:
        config.tickers = KNOWN_EGX_TICKERS

    logger.info("+" + "=" * 58 + "+")
    logger.info("|        EGX EVA ANALYZER — Data Collection Agent         |")
    logger.info("+" + "=" * 58 + "+")
    logger.info(f"Started:  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Sources:  {', '.join(config.sources)}")
    logger.info(f"Supabase: {'ENABLED' if os.environ.get('SUPABASE_URL') else 'DISABLED'}")

    pipeline_start = time.time()
    conn = db.get_connection()
    eva_results = []
    total_records = 0

    try:
        tickers = step_discover(conn, config)
        total_records = step_collect(conn, tickers, config)
        eva_results = step_eva(conn, tickers)
        step_report(conn, config)

        duration = time.time() - pipeline_start
        if not args.no_supabase:
            step_push_supabase(conn, tickers, eva_results, total_records,
                               duration, config.sources)

    except KeyboardInterrupt:
        logger.info("\nInterrupted. Partial results saved.")
    except Exception as e:
        logger.error(f"Pipeline error: {e}", exc_info=True)
        # Log failure to Supabase
        if not args.no_supabase and os.environ.get("SUPABASE_URL"):
            try:
                import supabase_client as supa
                client = supa.get_client()
                supa.upsert_run_log(client, {
                    "stocks_analyzed": 0, "status": "failed",
                    "error": str(e), "duration": time.time() - pipeline_start,
                    "sources": config.sources,
                })
            except Exception:
                pass
    finally:
        conn.close()

    total_time = time.time() - pipeline_start
    logger.info(f"\nPipeline time: {total_time:.1f}s | Results: {len(eva_results)} stocks")
    logger.info("Done!")


if __name__ == "__main__":
    main()
