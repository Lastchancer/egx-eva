"""
EGX EVA Analyzer — Report Generator
Exports EVA analysis results to CSV, JSON, and formatted console reports.
"""

import os
import json
import logging
from datetime import datetime
from typing import List, Dict

import pandas as pd

from config import REPORTS_DIR
import database as db

logger = logging.getLogger(__name__)


class ReportGenerator:
    """Generates analysis reports from EVA results."""

    def __init__(self, conn):
        self.conn = conn
        os.makedirs(REPORTS_DIR, exist_ok=True)

    def generate_all(self, export_format: str = "csv"):
        """Generate all reports."""
        results = db.get_all_eva_results(self.conn)
        if not results:
            logger.warning("No EVA results found. Run the analysis first.")
            return

        date_str = datetime.now().strftime("%Y%m%d")
        self._export_full_report(results, date_str, export_format)
        self._export_undervalued(results, date_str, export_format)
        self._export_summary_json(results, date_str)
        self._export_collection_stats(date_str)
        self._print_console_summary(results)

    def _export_full_report(self, results: List[Dict], date_str: str, fmt: str):
        """Export full EVA report."""
        df = pd.DataFrame(results)
        cols_order = [
            "ticker", "name", "sector", "signal", "eva", "eva_spread", "roic", "wacc",
            "nopat", "invested_capital", "capital_charge", "eva_per_share",
            "intrinsic_value", "market_cap", "intrinsic_premium",
            "cost_of_equity", "cost_of_debt_after_tax", "equity_weight", "debt_weight",
            "data_quality_score", "calculation_date",
        ]
        existing_cols = [c for c in cols_order if c in df.columns]
        df = df[existing_cols].sort_values("eva", ascending=False)

        path = os.path.join(REPORTS_DIR, f"eva_full_report_{date_str}")
        if fmt == "csv":
            df.to_csv(f"{path}.csv", index=False)
            logger.info(f"Full report saved: {path}.csv")
        elif fmt == "json":
            df.to_json(f"{path}.json", orient="records", indent=2)
            logger.info(f"Full report saved: {path}.json")
        elif fmt == "xlsx":
            df.to_excel(f"{path}.xlsx", index=False, sheet_name="EVA Analysis")
            logger.info(f"Full report saved: {path}.xlsx")

    def _export_undervalued(self, results: List[Dict], date_str: str, fmt: str):
        """Export filtered undervalued picks."""
        undervalued = [r for r in results if r.get("signal") == "UNDERVALUED"]
        if not undervalued:
            logger.info("No undervalued stocks found.")
            return

        df = pd.DataFrame(undervalued)
        df = df.sort_values("intrinsic_premium", ascending=False)

        path = os.path.join(REPORTS_DIR, f"undervalued_picks_{date_str}")
        if fmt == "csv":
            df.to_csv(f"{path}.csv", index=False)
        elif fmt == "json":
            df.to_json(f"{path}.json", orient="records", indent=2)
        elif fmt == "xlsx":
            df.to_excel(f"{path}.xlsx", index=False, sheet_name="Undervalued")

        logger.info(f"Undervalued picks saved: {path}.{fmt} ({len(undervalued)} stocks)")

    def _export_summary_json(self, results: List[Dict], date_str: str):
        """Export machine-readable summary."""
        positive_eva = [r for r in results if r["eva"] > 0]
        undervalued = [r for r in results if r.get("signal") == "UNDERVALUED"]
        overvalued = [r for r in results if r.get("signal") == "OVERVALUED"]

        summary = {
            "generated_at": datetime.now().isoformat(),
            "total_stocks_analyzed": len(results),
            "positive_eva_count": len(positive_eva),
            "undervalued_count": len(undervalued),
            "overvalued_count": len(overvalued),
            "avg_wacc": sum(r["wacc"] for r in results) / len(results) if results else 0,
            "avg_roic": sum(r["roic"] for r in results) / len(results) if results else 0,
            "total_market_eva": sum(r["eva"] for r in results),
            "avg_data_quality": sum(r.get("data_quality_score", 0) for r in results) / len(results) if results else 0,
            "top_undervalued": [
                {"ticker": r["ticker"], "eva": r["eva"], "upside": r.get("intrinsic_premium"),
                 "roic": r["roic"], "signal": r["signal"]}
                for r in sorted(undervalued, key=lambda x: x.get("intrinsic_premium") or 0, reverse=True)[:10]
            ],
            "top_value_creators": [
                {"ticker": r["ticker"], "eva": r["eva"], "roic": r["roic"]}
                for r in sorted(results, key=lambda x: x["eva"], reverse=True)[:10]
            ],
            "top_value_destroyers": [
                {"ticker": r["ticker"], "eva": r["eva"], "roic": r["roic"]}
                for r in sorted(results, key=lambda x: x["eva"])[:5]
            ],
        }

        path = os.path.join(REPORTS_DIR, f"summary_{date_str}.json")
        with open(path, "w") as f:
            json.dump(summary, f, indent=2, default=str)
        logger.info(f"Summary saved: {path}")

    def _export_collection_stats(self, date_str: str):
        """Export data collection statistics."""
        rows = self.conn.execute("""
            SELECT source, status, COUNT(*) as count, AVG(duration_seconds) as avg_duration,
                   SUM(records_collected) as total_records
            FROM collection_log
            GROUP BY source, status
            ORDER BY source, status
        """).fetchall()

        if rows:
            df = pd.DataFrame([dict(r) for r in rows])
            path = os.path.join(REPORTS_DIR, f"collection_stats_{date_str}.csv")
            df.to_csv(path, index=False)
            logger.info(f"Collection stats saved: {path}")

    def _print_console_summary(self, results: List[Dict]):
        """Print a formatted summary to console."""
        undervalued = [r for r in results if r.get("signal") == "UNDERVALUED"]
        fair = [r for r in results if r.get("signal") == "FAIR VALUE"]
        overvalued = [r for r in results if r.get("signal") == "OVERVALUED"]
        positive_eva = [r for r in results if r["eva"] > 0]

        print("\n" + "=" * 70)
        print("  EGX EVA ANALYSIS REPORT")
        print(f"  Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
        print("=" * 70)
        print(f"\n  Stocks Analyzed:     {len(results)}")
        print(f"  Value Creators (EVA+): {len(positive_eva)}")
        print(f"  UNDERVALUED:          {len(undervalued)}")
        print(f"  FAIR VALUE:           {len(fair)}")
        print(f"  OVERVALUED:           {len(overvalued)}")

        if undervalued:
            print(f"\n  {'─' * 66}")
            print(f"  🎯 TOP UNDERVALUED OPPORTUNITIES")
            print(f"  {'─' * 66}")
            print(f"  {'Ticker':<8} {'Name':<30} {'EVA':>12} {'ROIC':>8} {'Upside':>8} {'Quality':>8}")
            print(f"  {'─' * 66}")

            for r in sorted(undervalued, key=lambda x: x.get("intrinsic_premium") or 0, reverse=True)[:15]:
                name = (r.get("name") or "")[:28]
                eva_str = f"{r['eva']:>11,.0f}"
                roic_str = f"{r['roic']:>7.1%}"
                upside_str = f"+{r.get('intrinsic_premium', 0):>6.1%}" if r.get("intrinsic_premium") else "  N/A"
                quality_str = f"{r.get('data_quality_score', 0):>7.0%}"
                print(f"  {r['ticker']:<8} {name:<30} {eva_str} {roic_str} {upside_str} {quality_str}")

        if positive_eva:
            print(f"\n  {'─' * 66}")
            print(f"  💰 TOP VALUE CREATORS (by EVA)")
            print(f"  {'─' * 66}")
            print(f"  {'Ticker':<8} {'Name':<30} {'EVA':>12} {'Spread':>8} {'Signal':>12}")
            print(f"  {'─' * 66}")

            for r in sorted(results, key=lambda x: x["eva"], reverse=True)[:10]:
                name = (r.get("name") or "")[:28]
                eva_str = f"{r['eva']:>11,.0f}"
                spread_str = f"{r['eva_spread']:>7.1%}"
                print(f"  {r['ticker']:<8} {name:<30} {eva_str} {spread_str} {r['signal']:>12}")

        print(f"\n  Reports saved to: {REPORTS_DIR}/")
        print("=" * 70 + "\n")
