"""
EGX EVA Analyzer — Database Layer
SQLite storage for collected financial data and EVA results.
"""

import sqlite3
import json
import os
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Any
from config import DB_PATH


def get_connection() -> sqlite3.Connection:
    """Get a database connection, creating tables if needed."""
    os.makedirs(os.path.dirname(DB_PATH) if os.path.dirname(DB_PATH) else ".", exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    _create_tables(conn)
    return conn


def _create_tables(conn: sqlite3.Connection):
    """Create all database tables."""
    conn.executescript("""
    -- Master list of all EGX tickers
    CREATE TABLE IF NOT EXISTS companies (
        ticker TEXT PRIMARY KEY,
        name TEXT,
        name_ar TEXT,
        sector TEXT,
        industry TEXT,
        isin TEXT,
        listed_date TEXT,
        source TEXT,
        updated_at TEXT DEFAULT (datetime('now'))
    );

    -- Market / price data
    CREATE TABLE IF NOT EXISTS market_data (
        ticker TEXT,
        date TEXT,
        open_price REAL,
        high_price REAL,
        low_price REAL,
        close_price REAL,
        volume INTEGER,
        market_cap REAL,
        shares_outstanding REAL,
        pe_ratio REAL,
        pb_ratio REAL,
        dividend_yield REAL,
        beta REAL,
        fifty_two_week_high REAL,
        fifty_two_week_low REAL,
        source TEXT,
        updated_at TEXT DEFAULT (datetime('now')),
        PRIMARY KEY (ticker, date, source)
    );

    -- Income statement data
    CREATE TABLE IF NOT EXISTS income_statements (
        ticker TEXT,
        period TEXT,           -- 'annual' or 'quarterly'
        period_end TEXT,       -- e.g., '2024-12-31'
        revenue REAL,
        cost_of_revenue REAL,
        gross_profit REAL,
        operating_expenses REAL,
        ebit REAL,             -- Earnings Before Interest & Tax
        ebitda REAL,
        interest_expense REAL,
        pretax_income REAL,
        tax_expense REAL,
        net_income REAL,
        eps REAL,
        source TEXT,
        updated_at TEXT DEFAULT (datetime('now')),
        PRIMARY KEY (ticker, period, period_end, source)
    );

    -- Balance sheet data
    CREATE TABLE IF NOT EXISTS balance_sheets (
        ticker TEXT,
        period TEXT,
        period_end TEXT,
        total_assets REAL,
        current_assets REAL,
        non_current_assets REAL,
        total_liabilities REAL,
        current_liabilities REAL,
        non_current_liabilities REAL,
        total_debt REAL,
        long_term_debt REAL,
        short_term_debt REAL,
        total_equity REAL,
        retained_earnings REAL,
        cash_and_equivalents REAL,
        source TEXT,
        updated_at TEXT DEFAULT (datetime('now')),
        PRIMARY KEY (ticker, period, period_end, source)
    );

    -- Cash flow data
    CREATE TABLE IF NOT EXISTS cash_flows (
        ticker TEXT,
        period TEXT,
        period_end TEXT,
        operating_cash_flow REAL,
        investing_cash_flow REAL,
        financing_cash_flow REAL,
        capex REAL,
        free_cash_flow REAL,
        dividends_paid REAL,
        source TEXT,
        updated_at TEXT DEFAULT (datetime('now')),
        PRIMARY KEY (ticker, period, period_end, source)
    );

    -- Key ratios (pre-calculated from sources)
    CREATE TABLE IF NOT EXISTS financial_ratios (
        ticker TEXT,
        period_end TEXT,
        roe REAL,
        roa REAL,
        roic REAL,
        current_ratio REAL,
        debt_to_equity REAL,
        net_margin REAL,
        operating_margin REAL,
        asset_turnover REAL,
        source TEXT,
        updated_at TEXT DEFAULT (datetime('now')),
        PRIMARY KEY (ticker, period_end, source)
    );

    -- EVA calculation results
    CREATE TABLE IF NOT EXISTS eva_results (
        ticker TEXT,
        calculation_date TEXT,
        nopat REAL,
        invested_capital REAL,
        wacc REAL,
        cost_of_equity REAL,
        cost_of_debt_after_tax REAL,
        equity_weight REAL,
        debt_weight REAL,
        eva REAL,
        eva_spread REAL,           -- ROIC - WACC
        roic REAL,
        capital_charge REAL,
        eva_per_share REAL,
        intrinsic_value REAL,
        market_cap REAL,
        intrinsic_premium REAL,    -- (intrinsic - market) / market
        signal TEXT,               -- UNDERVALUED / FAIR VALUE / OVERVALUED
        data_quality_score REAL,   -- 0-1, how complete the data is
        PRIMARY KEY (ticker, calculation_date)
    );

    -- Data collection log
    CREATE TABLE IF NOT EXISTS collection_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ticker TEXT,
        source TEXT,
        status TEXT,           -- 'success', 'partial', 'failed'
        records_collected INTEGER,
        error_message TEXT,
        duration_seconds REAL,
        timestamp TEXT DEFAULT (datetime('now'))
    );

    -- Cache for raw API/scrape responses
    CREATE TABLE IF NOT EXISTS raw_cache (
        cache_key TEXT PRIMARY KEY,
        data TEXT,             -- JSON serialized
        source TEXT,
        created_at TEXT DEFAULT (datetime('now')),
        expires_at TEXT
    );
    """)
    conn.commit()


def upsert_company(conn: sqlite3.Connection, ticker: str, **kwargs):
    """Insert or update a company record."""
    kwargs["ticker"] = ticker
    kwargs["updated_at"] = datetime.now().isoformat()
    cols = ", ".join(kwargs.keys())
    placeholders = ", ".join(["?"] * len(kwargs))
    updates = ", ".join([f"{k}=excluded.{k}" for k in kwargs if k != "ticker"])
    conn.execute(
        f"INSERT INTO companies ({cols}) VALUES ({placeholders}) ON CONFLICT(ticker) DO UPDATE SET {updates}",
        list(kwargs.values())
    )
    conn.commit()


def upsert_market_data(conn: sqlite3.Connection, data: Dict):
    """Insert or update market data."""
    data["updated_at"] = datetime.now().isoformat()
    cols = ", ".join(data.keys())
    placeholders = ", ".join(["?"] * len(data))
    conn.execute(
        f"INSERT OR REPLACE INTO market_data ({cols}) VALUES ({placeholders})",
        list(data.values())
    )
    conn.commit()


def upsert_income_statement(conn: sqlite3.Connection, data: Dict):
    """Insert or update income statement data."""
    data["updated_at"] = datetime.now().isoformat()
    cols = ", ".join(data.keys())
    placeholders = ", ".join(["?"] * len(data))
    conn.execute(
        f"INSERT OR REPLACE INTO income_statements ({cols}) VALUES ({placeholders})",
        list(data.values())
    )
    conn.commit()


def upsert_balance_sheet(conn: sqlite3.Connection, data: Dict):
    """Insert or update balance sheet data."""
    data["updated_at"] = datetime.now().isoformat()
    cols = ", ".join(data.keys())
    placeholders = ", ".join(["?"] * len(data))
    conn.execute(
        f"INSERT OR REPLACE INTO balance_sheets ({cols}) VALUES ({placeholders})",
        list(data.values())
    )
    conn.commit()


def upsert_cash_flow(conn: sqlite3.Connection, data: Dict):
    """Insert or update cash flow data."""
    data["updated_at"] = datetime.now().isoformat()
    cols = ", ".join(data.keys())
    placeholders = ", ".join(["?"] * len(data))
    conn.execute(
        f"INSERT OR REPLACE INTO cash_flows ({cols}) VALUES ({placeholders})",
        list(data.values())
    )
    conn.commit()


def upsert_eva_result(conn: sqlite3.Connection, data: Dict):
    """Insert or update EVA calculation result."""
    cols = ", ".join(data.keys())
    placeholders = ", ".join(["?"] * len(data))
    conn.execute(
        f"INSERT OR REPLACE INTO eva_results ({cols}) VALUES ({placeholders})",
        list(data.values())
    )
    conn.commit()


def log_collection(conn: sqlite3.Connection, ticker: str, source: str, status: str,
                   records: int = 0, error: str = None, duration: float = 0):
    """Log a data collection attempt."""
    conn.execute(
        "INSERT INTO collection_log (ticker, source, status, records_collected, error_message, duration_seconds) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (ticker, source, status, records, error, duration)
    )
    conn.commit()


def get_cache(conn: sqlite3.Connection, key: str) -> Optional[Dict]:
    """Get cached data if not expired."""
    row = conn.execute(
        "SELECT data FROM raw_cache WHERE cache_key = ? AND expires_at > datetime('now')",
        (key,)
    ).fetchone()
    if row:
        return json.loads(row["data"])
    return None


def set_cache(conn: sqlite3.Connection, key: str, data: Any, source: str, ttl_hours: int = 24):
    """Cache data with TTL."""
    expires = (datetime.now() + timedelta(hours=ttl_hours)).isoformat()
    conn.execute(
        "INSERT OR REPLACE INTO raw_cache (cache_key, data, source, expires_at) VALUES (?, ?, ?, ?)",
        (key, json.dumps(data, default=str), source, expires)
    )
    conn.commit()


def get_all_tickers(conn: sqlite3.Connection) -> List[str]:
    """Get all known tickers."""
    rows = conn.execute("SELECT ticker FROM companies ORDER BY ticker").fetchall()
    return [r["ticker"] for r in rows]


def get_latest_financials(conn: sqlite3.Connection, ticker: str) -> Dict:
    """Get the most recent financial data for a ticker, merged from all sources."""
    result = {}

    # Latest income statement
    row = conn.execute(
        "SELECT * FROM income_statements WHERE ticker = ? AND period = 'annual' ORDER BY period_end DESC LIMIT 1",
        (ticker,)
    ).fetchone()
    if row:
        result["income"] = dict(row)

    # Latest balance sheet
    row = conn.execute(
        "SELECT * FROM balance_sheets WHERE ticker = ? AND period = 'annual' ORDER BY period_end DESC LIMIT 1",
        (ticker,)
    ).fetchone()
    if row:
        result["balance"] = dict(row)

    # Latest market data
    row = conn.execute(
        "SELECT * FROM market_data WHERE ticker = ? ORDER BY date DESC LIMIT 1",
        (ticker,)
    ).fetchone()
    if row:
        result["market"] = dict(row)

    # Company info
    row = conn.execute("SELECT * FROM companies WHERE ticker = ?", (ticker,)).fetchone()
    if row:
        result["company"] = dict(row)

    return result


def get_all_eva_results(conn: sqlite3.Connection) -> List[Dict]:
    """Get all EVA results for the latest calculation date."""
    rows = conn.execute("""
        SELECT e.*, c.name, c.sector
        FROM eva_results e
        LEFT JOIN companies c ON e.ticker = c.ticker
        WHERE e.calculation_date = (SELECT MAX(calculation_date) FROM eva_results)
        ORDER BY e.eva DESC
    """).fetchall()
    return [dict(r) for r in rows]
