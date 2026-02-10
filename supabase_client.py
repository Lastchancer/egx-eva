"""
EGX EVA Analyzer — Supabase Integration Layer
Replaces local SQLite with Supabase (PostgreSQL cloud database).
All EVA results are pushed to Supabase where the Lovable frontend reads them.
"""

import os
import json
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any

try:
    from supabase import create_client, Client
except ImportError:
    raise ImportError("Install supabase-py: pip install supabase")

logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════
# CONFIGURATION — Set these as environment variables
# ═══════════════════════════════════════════════════════════════
# You get these from: Supabase Dashboard → Settings → API
#
#   SUPABASE_URL=https://xxxxx.supabase.co
#   SUPABASE_KEY=eyJhbGciOiJIUzI1NiIs...  (use the "service_role" key for backend writes)
#

SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")


def get_client() -> Client:
    """Create and return a Supabase client."""
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise ValueError(
            "Missing Supabase credentials.\n"
            "Set environment variables:\n"
            "  export SUPABASE_URL='https://your-project.supabase.co'\n"
            "  export SUPABASE_KEY='your-service-role-key'"
        )
    return create_client(SUPABASE_URL, SUPABASE_KEY)


# ═══════════════════════════════════════════════════════════════
# DATA PUSH FUNCTIONS
# ═══════════════════════════════════════════════════════════════

def upsert_companies(client: Client, companies: List[Dict]):
    """Push company master data to Supabase."""
    if not companies:
        return
    # Clean data for Supabase
    rows = []
    for c in companies:
        rows.append({
            "ticker": c.get("ticker", ""),
            "name": c.get("name", ""),
            "sector": c.get("sector", ""),
            "industry": c.get("industry", ""),
            "source": c.get("source", ""),
            "updated_at": datetime.now().isoformat(),
        })
    try:
        result = client.table("companies").upsert(rows, on_conflict="ticker").execute()
        logger.info(f"  [Supabase] Upserted {len(rows)} companies")
    except Exception as e:
        logger.error(f"  [Supabase] Companies upsert failed: {e}")


def upsert_market_data(client: Client, data_list: List[Dict]):
    """Push market/price data to Supabase."""
    if not data_list:
        return
    rows = []
    for d in data_list:
        rows.append({
            "ticker": d.get("ticker"),
            "date": d.get("date", datetime.now().strftime("%Y-%m-%d")),
            "close_price": _safe_float(d.get("close_price")),
            "open_price": _safe_float(d.get("open_price")),
            "high_price": _safe_float(d.get("high_price")),
            "low_price": _safe_float(d.get("low_price")),
            "volume": d.get("volume"),
            "market_cap": _safe_float(d.get("market_cap")),
            "shares_outstanding": _safe_float(d.get("shares_outstanding")),
            "pe_ratio": _safe_float(d.get("pe_ratio")),
            "pb_ratio": _safe_float(d.get("pb_ratio")),
            "dividend_yield": _safe_float(d.get("dividend_yield")),
            "beta": _safe_float(d.get("beta")),
            "fifty_two_week_high": _safe_float(d.get("fifty_two_week_high")),
            "fifty_two_week_low": _safe_float(d.get("fifty_two_week_low")),
            "source": d.get("source", ""),
            "updated_at": datetime.now().isoformat(),
        })
    try:
        result = client.table("market_data").upsert(
            rows, on_conflict="ticker,date,source"
        ).execute()
        logger.info(f"  [Supabase] Upserted {len(rows)} market data rows")
    except Exception as e:
        logger.error(f"  [Supabase] Market data upsert failed: {e}")


def upsert_income_statements(client: Client, data_list: List[Dict]):
    """Push income statement data to Supabase."""
    if not data_list:
        return
    rows = []
    for d in data_list:
        rows.append({
            "ticker": d.get("ticker"),
            "period": d.get("period", "annual"),
            "period_end": d.get("period_end"),
            "revenue": _safe_float(d.get("revenue")),
            "cost_of_revenue": _safe_float(d.get("cost_of_revenue")),
            "gross_profit": _safe_float(d.get("gross_profit")),
            "ebit": _safe_float(d.get("ebit")),
            "ebitda": _safe_float(d.get("ebitda")),
            "interest_expense": _safe_float(d.get("interest_expense")),
            "tax_expense": _safe_float(d.get("tax_expense")),
            "net_income": _safe_float(d.get("net_income")),
            "eps": _safe_float(d.get("eps")),
            "source": d.get("source", ""),
            "updated_at": datetime.now().isoformat(),
        })
    try:
        result = client.table("income_statements").upsert(
            rows, on_conflict="ticker,period,period_end,source"
        ).execute()
        logger.info(f"  [Supabase] Upserted {len(rows)} income statement rows")
    except Exception as e:
        logger.error(f"  [Supabase] Income statements upsert failed: {e}")


def upsert_balance_sheets(client: Client, data_list: List[Dict]):
    """Push balance sheet data to Supabase."""
    if not data_list:
        return
    rows = []
    for d in data_list:
        rows.append({
            "ticker": d.get("ticker"),
            "period": d.get("period", "annual"),
            "period_end": d.get("period_end"),
            "total_assets": _safe_float(d.get("total_assets")),
            "current_assets": _safe_float(d.get("current_assets")),
            "current_liabilities": _safe_float(d.get("current_liabilities")),
            "total_liabilities": _safe_float(d.get("total_liabilities")),
            "total_debt": _safe_float(d.get("total_debt")),
            "long_term_debt": _safe_float(d.get("long_term_debt")),
            "total_equity": _safe_float(d.get("total_equity")),
            "cash_and_equivalents": _safe_float(d.get("cash_and_equivalents")),
            "source": d.get("source", ""),
            "updated_at": datetime.now().isoformat(),
        })
    try:
        result = client.table("balance_sheets").upsert(
            rows, on_conflict="ticker,period,period_end,source"
        ).execute()
        logger.info(f"  [Supabase] Upserted {len(rows)} balance sheet rows")
    except Exception as e:
        logger.error(f"  [Supabase] Balance sheets upsert failed: {e}")


def upsert_eva_results(client: Client, results: List[Dict]):
    """Push EVA calculation results to Supabase. This is the main table the frontend reads."""
    if not results:
        return
    rows = []
    for r in results:
        rows.append({
            "ticker": r.get("ticker"),
            "calculation_date": r.get("calculation_date", datetime.now().strftime("%Y-%m-%d")),
            "nopat": _safe_float(r.get("nopat")),
            "invested_capital": _safe_float(r.get("invested_capital")),
            "wacc": _safe_float(r.get("wacc")),
            "cost_of_equity": _safe_float(r.get("cost_of_equity")),
            "cost_of_debt_after_tax": _safe_float(r.get("cost_of_debt_after_tax")),
            "equity_weight": _safe_float(r.get("equity_weight")),
            "debt_weight": _safe_float(r.get("debt_weight")),
            "eva": _safe_float(r.get("eva")),
            "eva_spread": _safe_float(r.get("eva_spread")),
            "roic": _safe_float(r.get("roic")),
            "capital_charge": _safe_float(r.get("capital_charge")),
            "eva_per_share": _safe_float(r.get("eva_per_share")),
            "intrinsic_value": _safe_float(r.get("intrinsic_value")),
            "market_cap": _safe_float(r.get("market_cap")),
            "intrinsic_premium": _safe_float(r.get("intrinsic_premium")),
            "signal": r.get("signal", ""),
            "data_quality_score": _safe_float(r.get("data_quality_score")),
        })
    try:
        result = client.table("eva_results").upsert(
            rows, on_conflict="ticker,calculation_date"
        ).execute()
        logger.info(f"  [Supabase] Upserted {len(rows)} EVA results ✓")
    except Exception as e:
        logger.error(f"  [Supabase] EVA results upsert failed: {e}")


def upsert_run_log(client: Client, run_data: Dict):
    """Log each pipeline run for monitoring on the frontend."""
    try:
        client.table("pipeline_runs").insert({
            "run_date": datetime.now().isoformat(),
            "stocks_analyzed": run_data.get("stocks_analyzed", 0),
            "undervalued_count": run_data.get("undervalued_count", 0),
            "positive_eva_count": run_data.get("positive_eva_count", 0),
            "total_records_collected": run_data.get("total_records", 0),
            "sources_used": json.dumps(run_data.get("sources", [])),
            "duration_seconds": run_data.get("duration", 0),
            "status": run_data.get("status", "success"),
            "error_message": run_data.get("error"),
        }).execute()
        logger.info(f"  [Supabase] Pipeline run logged ✓")
    except Exception as e:
        logger.error(f"  [Supabase] Run log failed: {e}")


# ═══════════════════════════════════════════════════════════════
# READ FUNCTIONS (for the Python backend if needed)
# ═══════════════════════════════════════════════════════════════

def get_latest_eva_results(client: Client) -> List[Dict]:
    """Get the most recent EVA results."""
    try:
        result = client.table("eva_results") \
            .select("*, companies(name, sector)") \
            .order("eva", desc=True) \
            .execute()
        return result.data
    except Exception as e:
        logger.error(f"  [Supabase] Read EVA results failed: {e}")
        return []


def get_undervalued_stocks(client: Client) -> List[Dict]:
    """Get only undervalued stocks."""
    try:
        result = client.table("eva_results") \
            .select("*, companies(name, sector)") \
            .eq("signal", "UNDERVALUED") \
            .order("intrinsic_premium", desc=True) \
            .execute()
        return result.data
    except Exception as e:
        logger.error(f"  [Supabase] Read undervalued failed: {e}")
        return []


# ═══════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════

def _safe_float(val) -> Optional[float]:
    """Safely convert to float, returning None for invalid values."""
    if val is None:
        return None
    try:
        f = float(val)
        if f != f:  # NaN check
            return None
        return round(f, 6)
    except (ValueError, TypeError):
        return None
