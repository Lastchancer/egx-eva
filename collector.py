#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════╗
║  EGX DATA COLLECTION AGENT                                      ║
║  Automated financial data gathering for Egyptian Stock Market    ║
║  Sources: Yahoo Finance, StockAnalysis, Investing.com,           ║
║           Mubasher, EGX Official                                 ║
╚══════════════════════════════════════════════════════════════════╝
"""

import json
import csv
import time
import os
import sqlite3
import logging
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict, field
from typing import Optional
from pathlib import Path

# ─── Conditional imports with install guidance ───
try:
    import yfinance as yf
except ImportError:
    yf = None
    print("⚠ yfinance not installed. Run: pip install yfinance")

try:
    import requests
except ImportError:
    requests = None
    print("⚠ requests not installed. Run: pip install requests")

try:
    from bs4 import BeautifulSoup
except ImportError:
    BeautifulSoup = None
    print("⚠ beautifulsoup4 not installed. Run: pip install beautifulsoup4")

try:
    import pandas as pd
except ImportError:
    pd = None
    print("⚠ pandas not installed. Run: pip install pandas")


# ═══════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("EGX_Agent")

DB_PATH = os.environ.get("EGX_DB_PATH", "egx_eva_data.db")
OUTPUT_DIR = os.environ.get("EGX_OUTPUT_DIR", "output")
REQUEST_DELAY = float(os.environ.get("EGX_REQUEST_DELAY", "2.0"))  # seconds between requests

# Egypt macro assumptions (update periodically)
EGYPT_RISK_FREE_RATE = 0.26    # Egyptian T-bill rate (~26%)
EGYPT_MARKET_PREMIUM = 0.08    # Equity risk premium (~8%)
EGYPT_COST_OF_DEBT = 0.22      # Avg corporate borrowing rate (~22%)
EGYPT_CORPORATE_TAX = 0.225    # Corporate tax rate (22.5%)


# ═══════════════════════════════════════════════════════════════
# MASTER TICKER LIST — ALL EGX LISTED STOCKS
# ═══════════════════════════════════════════════════════════════
# Yahoo Finance uses .CA suffix for Cairo Exchange
# This list covers the most actively traded EGX stocks

EGX_TICKERS = {
    # ── EGX 30 Components (Blue Chips) ──
    "COMI": {"name": "Commercial International Bank (CIB)", "sector": "Banking", "yahoo": "COMI.CA"},
    "SWDY": {"name": "El Sewedy Electric", "sector": "Industrials", "yahoo": "SWDY.CA"},
    "TMGH": {"name": "Talaat Moustafa Group Holding", "sector": "Real Estate", "yahoo": "TMGH.CA"},
    "EAST": {"name": "Eastern Company", "sector": "Consumer Staples", "yahoo": "EAST.CA"},
    "ETEL": {"name": "Telecom Egypt", "sector": "Telecom", "yahoo": "ETEL.CA"},
    "HRHO": {"name": "EFG Holding (Hermes)", "sector": "Financial Services", "yahoo": "HRHO.CA"},
    "ABUK": {"name": "Abu Qir Fertilizers", "sector": "Materials", "yahoo": "ABUK.CA"},
    "FWRY": {"name": "Fawry for Banking Technology", "sector": "Technology", "yahoo": "FWRY.CA"},
    "PHDC": {"name": "Palm Hills Developments", "sector": "Real Estate", "yahoo": "PHDC.CA"},
    "ORAS": {"name": "Orascom Construction", "sector": "Industrials", "yahoo": "ORAS.CA"},
    "EFIH": {"name": "e-finance for Digital Investments", "sector": "Technology", "yahoo": "EFIH.CA"},
    "EMFD": {"name": "Emaar Misr for Development", "sector": "Real Estate", "yahoo": "EMFD.CA"},
    "HDBK": {"name": "Housing and Development Bank", "sector": "Banking", "yahoo": "HDBK.CA"},
    "QNBE": {"name": "Qatar National Bank Egypt", "sector": "Banking", "yahoo": "QNBE.CA"},
    "EGAL": {"name": "Egypt Aluminum", "sector": "Materials", "yahoo": "EGAL.CA"},
    "MFPC": {"name": "Misr Fertilizer Production", "sector": "Materials", "yahoo": "MFPC.CA"},
    "ALCN": {"name": "Alexandria Container & Cargo", "sector": "Industrials", "yahoo": "ALCN.CA"},
    "JUFO": {"name": "Juhayna Food Industries", "sector": "Consumer Staples", "yahoo": "JUFO.CA"},
    "EFID": {"name": "Edita Food Industries", "sector": "Consumer Staples", "yahoo": "EFID.CA"},
    "ORHD": {"name": "Orascom Development Egypt", "sector": "Real Estate", "yahoo": "ORHD.CA"},
    "CLHO": {"name": "Cleopatra Hospitals Group", "sector": "Healthcare", "yahoo": "CLHO.CA"},
    "ORWE": {"name": "Oriental Weavers", "sector": "Consumer Discretionary", "yahoo": "ORWE.CA"},
    "SKPC": {"name": "Sidi Kerir Petrochemicals", "sector": "Energy", "yahoo": "SKPC.CA"},
    "CIEB": {"name": "Credit Agricole Egypt", "sector": "Banking", "yahoo": "CIEB.CA"},
    "OCDI": {"name": "SODIC (6th October Dev.)", "sector": "Real Estate", "yahoo": "OCDI.CA"},
    "ADIB": {"name": "Abu Dhabi Islamic Bank Egypt", "sector": "Banking", "yahoo": "ADIB.CA"},
    "ARCC": {"name": "Arabian Cement", "sector": "Materials", "yahoo": "ARCC.CA"},
    "GBCO": {"name": "GB Corp (GB Auto)", "sector": "Consumer Discretionary", "yahoo": "GBCO.CA"},
    "TAQA": {"name": "TAQA Arabia", "sector": "Energy", "yahoo": "TAQA.CA"},
    "CCAP": {"name": "QALA Financial Investments", "sector": "Financial Services", "yahoo": "CCAP.CA"},

    # ── EGX 70 / Mid-Caps ──
    "ESRS": {"name": "Ezz Steel", "sector": "Materials", "yahoo": "ESRS.CA"},
    "AMOC": {"name": "Alexandria Mineral Oils", "sector": "Energy", "yahoo": "AMOC.CA"},
    "IRON": {"name": "Egyptian Iron and Steel", "sector": "Materials", "yahoo": "IRON.CA"},
    "MNHD": {"name": "Madinet Nasr Housing", "sector": "Real Estate", "yahoo": "MNHD.CA"},
    "EGCH": {"name": "Egyptian Chemical Industries", "sector": "Materials", "yahoo": "EGCH.CA"},
    "EFIC": {"name": "Egyptian Financial & Industrial", "sector": "Materials", "yahoo": "EFIC.CA"},
    "SCEM": {"name": "Sinai Cement", "sector": "Materials", "yahoo": "SCEM.CA"},
    "MCQE": {"name": "Misr Cement (Qena)", "sector": "Materials", "yahoo": "MCQE.CA"},
    "MBSC": {"name": "Misr Beni Suef Cement", "sector": "Materials", "yahoo": "MBSC.CA"},
    "PHAR": {"name": "Egyptian Intl Pharmaceuticals", "sector": "Healthcare", "yahoo": "PHAR.CA"},
    "RAYA": {"name": "Raya Holding", "sector": "Technology", "yahoo": "RAYA.CA"},
    "HELI": {"name": "Heliopolis Housing", "sector": "Real Estate", "yahoo": "HELI.CA"},
    "EGSA": {"name": "Nilesat", "sector": "Telecom", "yahoo": "EGSA.CA"},
    "MTIE": {"name": "MM Group for Industry", "sector": "Industrials", "yahoo": "MTIE.CA"},
    "BTFH": {"name": "Beltone Holding", "sector": "Financial Services", "yahoo": "BTFH.CA"},
    "VALU": {"name": "U Consumer Finance", "sector": "Financial Services", "yahoo": "VALU.CA"},
    "FAIT": {"name": "Faisal Islamic Bank", "sector": "Banking", "yahoo": "FAIT.CA"},
    "CANA": {"name": "Suez Canal Bank", "sector": "Banking", "yahoo": "CANA.CA"},
    "EXPA": {"name": "Export Development Bank", "sector": "Banking", "yahoo": "EXPA.CA"},
    "UBEE": {"name": "The United Bank", "sector": "Banking", "yahoo": "UBEE.CA"},
    "FERC": {"name": "Ferchem Misr Fertilizers", "sector": "Materials", "yahoo": "FERC.CA"},
    "SCTS": {"name": "Suez Canal Tech Settling", "sector": "Industrials", "yahoo": "SCTS.CA"},
    "GPPL": {"name": "Golden Pyramids Plaza", "sector": "Real Estate", "yahoo": "GPPL.CA"},
    "TALM": {"name": "Taaleem Management", "sector": "Education", "yahoo": "TALM.CA"},

    # ── Additional Liquid Stocks ──
    "DCRC": {"name": "Delta Construction", "sector": "Industrials", "yahoo": "DCRC.CA"},
    "EKHOA": {"name": "El Khair Holding", "sector": "Consumer Staples", "yahoo": "EKHOA.CA"},
    "ACGC": {"name": "Arabian Cotton Ginning", "sector": "Industrials", "yahoo": "ACGC.CA"},
    "CIRA": {"name": "Cairo for Investment & Real Estate", "sector": "Education", "yahoo": "CIRA.CA"},
    "ISPH": {"name": "Ibnsina Pharma", "sector": "Healthcare", "yahoo": "ISPH.CA"},
    "EGAS": {"name": "Egypt Gas Company", "sector": "Energy", "yahoo": "EGAS.CA"},
    "AMER": {"name": "Amer Group Holding", "sector": "Real Estate", "yahoo": "AMER.CA"},
    "SPMD": {"name": "Speed Medical", "sector": "Healthcare", "yahoo": "SPMD.CA"},
    "MNHD": {"name": "Madinet Nasr Housing", "sector": "Real Estate", "yahoo": "MNHD.CA"},
    "ELMS": {"name": "Elsaba Automotive", "sector": "Consumer Discretionary", "yahoo": "ELMS.CA"},
    "EKHO": {"name": "Ekho Holding", "sector": "Consumer Staples", "yahoo": "EKHO.CA"},
    "PIOH": {"name": "Pioneers Holding", "sector": "Financial Services", "yahoo": "PIOH.CA"},
    "EDBM": {"name": "Egyptian Drilling Company", "sector": "Energy", "yahoo": "EDBM.CA"},
    "EIOD": {"name": "Egypt for Information Dissemination", "sector": "Technology", "yahoo": "EIOD.CA"},
    "ELCAB": {"name": "Electro Cable Egypt", "sector": "Industrials", "yahoo": "ELCAB.CA"},
    "NASR": {"name": "Nasr City Housing", "sector": "Real Estate", "yahoo": "NASR.CA"},
    "MPRC": {"name": "Misr for Production Projects", "sector": "Consumer Staples", "yahoo": "MPRC.CA"},
    "AUTO": {"name": "GB Corp (Auto segment)", "sector": "Consumer Discretionary", "yahoo": "AUTO.CA"},
}


# ═══════════════════════════════════════════════════════════════
# DATA MODELS
# ═══════════════════════════════════════════════════════════════

@dataclass
class StockFinancials:
    """Complete financial data for EVA calculation"""
    ticker: str
    name: str
    sector: str
    collection_date: str = ""
    data_source: str = ""

    # Price & Market Data
    current_price: float = 0.0
    market_cap: float = 0.0
    shares_outstanding: float = 0.0
    volume_avg: float = 0.0
    fifty_two_week_high: float = 0.0
    fifty_two_week_low: float = 0.0
    beta: float = 1.0
    dividend_yield: float = 0.0

    # Income Statement
    revenue: float = 0.0
    cost_of_revenue: float = 0.0
    gross_profit: float = 0.0
    operating_income: float = 0.0  # EBIT
    ebit: float = 0.0
    ebitda: float = 0.0
    net_income: float = 0.0
    interest_expense: float = 0.0
    tax_expense: float = 0.0
    tax_rate: float = EGYPT_CORPORATE_TAX

    # Balance Sheet
    total_assets: float = 0.0
    current_assets: float = 0.0
    total_liabilities: float = 0.0
    current_liabilities: float = 0.0
    long_term_debt: float = 0.0
    short_term_debt: float = 0.0
    total_debt: float = 0.0
    total_equity: float = 0.0
    book_value_per_share: float = 0.0
    cash_and_equivalents: float = 0.0

    # Cash Flow
    operating_cash_flow: float = 0.0
    capital_expenditure: float = 0.0
    free_cash_flow: float = 0.0

    # Key Ratios
    roe: float = 0.0
    roa: float = 0.0
    debt_to_equity: float = 0.0
    current_ratio: float = 0.0
    pe_ratio: float = 0.0
    pb_ratio: float = 0.0
    eps: float = 0.0

    # EVA Calculations (computed after data collection)
    nopat: float = 0.0
    invested_capital: float = 0.0
    wacc: float = 0.0
    cost_of_equity: float = 0.0
    after_tax_cost_of_debt: float = 0.0
    equity_weight: float = 0.0
    debt_weight: float = 0.0
    eva: float = 0.0
    eva_spread: float = 0.0
    roic: float = 0.0
    eva_per_share: float = 0.0
    capital_charge: float = 0.0
    intrinsic_premium: float = 0.0
    signal: str = ""

    # Data quality
    data_completeness: float = 0.0  # 0-1 score
    warnings: str = ""


# ═══════════════════════════════════════════════════════════════
# DATABASE MANAGER
# ═══════════════════════════════════════════════════════════════

class DatabaseManager:
    """SQLite database for persistent storage of financial data"""

    def __init__(self, db_path: str = DB_PATH):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self._create_tables()
        logger.info(f"Database initialized at {db_path}")

    def _create_tables(self):
        cursor = self.conn.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS stock_financials (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT NOT NULL,
                name TEXT,
                sector TEXT,
                collection_date TEXT NOT NULL,
                data_source TEXT,
                data_json TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS eva_results (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT NOT NULL,
                name TEXT,
                sector TEXT,
                calculation_date TEXT NOT NULL,
                current_price REAL,
                market_cap REAL,
                revenue REAL,
                ebit REAL,
                nopat REAL,
                invested_capital REAL,
                wacc REAL,
                roic REAL,
                eva REAL,
                eva_spread REAL,
                eva_per_share REAL,
                intrinsic_premium REAL,
                signal TEXT,
                data_completeness REAL,
                full_data_json TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS collection_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_date TEXT NOT NULL,
                source TEXT,
                tickers_attempted INTEGER,
                tickers_success INTEGER,
                tickers_failed INTEGER,
                errors TEXT,
                duration_seconds REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_eva_ticker_date
            ON eva_results(ticker, calculation_date)
        """)

        self.conn.commit()

    def save_financials(self, stock: StockFinancials):
        cursor = self.conn.cursor()
        cursor.execute("""
            INSERT INTO stock_financials (ticker, name, sector, collection_date, data_source, data_json)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (stock.ticker, stock.name, stock.sector, stock.collection_date,
              stock.data_source, json.dumps(asdict(stock))))
        self.conn.commit()

    def save_eva_result(self, stock: StockFinancials):
        cursor = self.conn.cursor()
        cursor.execute("""
            INSERT INTO eva_results (
                ticker, name, sector, calculation_date, current_price, market_cap,
                revenue, ebit, nopat, invested_capital, wacc, roic, eva, eva_spread,
                eva_per_share, intrinsic_premium, signal, data_completeness, full_data_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            stock.ticker, stock.name, stock.sector, stock.collection_date,
            stock.current_price, stock.market_cap, stock.revenue, stock.ebit,
            stock.nopat, stock.invested_capital, stock.wacc, stock.roic,
            stock.eva, stock.eva_spread, stock.eva_per_share,
            stock.intrinsic_premium, stock.signal, stock.data_completeness,
            json.dumps(asdict(stock))
        ))
        self.conn.commit()

    def log_collection_run(self, source, attempted, success, failed, errors, duration):
        cursor = self.conn.cursor()
        cursor.execute("""
            INSERT INTO collection_log (run_date, source, tickers_attempted, tickers_success,
                                        tickers_failed, errors, duration_seconds)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (datetime.now().isoformat(), source, attempted, success, failed,
              json.dumps(errors), duration))
        self.conn.commit()

    def get_latest_results(self):
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT full_data_json FROM eva_results
            WHERE calculation_date = (SELECT MAX(calculation_date) FROM eva_results)
            ORDER BY eva DESC
        """)
        rows = cursor.fetchall()
        return [json.loads(row[0]) for row in rows]

    def close(self):
        self.conn.close()


# ═══════════════════════════════════════════════════════════════
# SOURCE 1: YAHOO FINANCE (via yfinance)
# Best for: price data, market cap, beta, some fundamentals
# ═══════════════════════════════════════════════════════════════

class YahooFinanceCollector:
    """Collect data from Yahoo Finance using yfinance library"""

    SOURCE_NAME = "yahoo_finance"

    def __init__(self):
        if yf is None:
            raise ImportError("yfinance is required. Install: pip install yfinance")

    def collect_stock(self, ticker: str, meta: dict) -> Optional[StockFinancials]:
        """Collect financial data for a single EGX stock"""
        yahoo_ticker = meta.get("yahoo", f"{ticker}.CA")
        logger.info(f"  [Yahoo] Fetching {ticker} ({yahoo_ticker})...")

        try:
            stock = yf.Ticker(yahoo_ticker)
            info = stock.info or {}

            if not info or info.get("regularMarketPrice") is None:
                logger.warning(f"  [Yahoo] No data for {ticker}")
                return None

            sf = StockFinancials(
                ticker=ticker,
                name=meta.get("name", info.get("shortName", "")),
                sector=meta.get("sector", info.get("sector", "")),
                collection_date=datetime.now().strftime("%Y-%m-%d"),
                data_source=self.SOURCE_NAME,
            )

            # ── Price & Market Data ──
            sf.current_price = info.get("regularMarketPrice", 0) or info.get("currentPrice", 0) or 0
            sf.market_cap = info.get("marketCap", 0) or 0
            sf.shares_outstanding = info.get("sharesOutstanding", 0) or 0
            sf.volume_avg = info.get("averageVolume", 0) or 0
            sf.fifty_two_week_high = info.get("fiftyTwoWeekHigh", 0) or 0
            sf.fifty_two_week_low = info.get("fiftyTwoWeekLow", 0) or 0
            sf.beta = info.get("beta", 1.0) or 1.0
            sf.dividend_yield = info.get("dividendYield", 0) or 0

            # ── Income Statement from info dict ──
            sf.revenue = info.get("totalRevenue", 0) or 0
            sf.gross_profit = info.get("grossProfits", 0) or 0
            sf.operating_income = info.get("operatingIncome", 0) or info.get("ebitda", 0) or 0
            sf.ebit = info.get("ebit", 0) or sf.operating_income
            sf.ebitda = info.get("ebitda", 0) or 0
            sf.net_income = info.get("netIncomeToCommon", 0) or 0

            # ── Balance Sheet from info dict ──
            sf.total_assets = info.get("totalAssets", 0) or 0
            sf.current_assets = info.get("totalCurrentAssets", 0) or 0
            sf.total_liabilities = info.get("totalLiab", 0) or info.get("totalDebt", 0) or 0
            sf.current_liabilities = info.get("totalCurrentLiabilities", 0) or 0
            sf.total_debt = info.get("totalDebt", 0) or 0
            sf.total_equity = info.get("totalStockholderEquity", 0) or 0
            sf.book_value_per_share = info.get("bookValue", 0) or 0
            sf.cash_and_equivalents = info.get("totalCash", 0) or 0

            # ── Cash Flow ──
            sf.operating_cash_flow = info.get("operatingCashflow", 0) or 0
            sf.free_cash_flow = info.get("freeCashflow", 0) or 0

            # ── Key Ratios ──
            sf.roe = info.get("returnOnEquity", 0) or 0
            sf.roa = info.get("returnOnAssets", 0) or 0
            sf.debt_to_equity = info.get("debtToEquity", 0) or 0
            sf.current_ratio = info.get("currentRatio", 0) or 0
            sf.pe_ratio = info.get("trailingPE", 0) or info.get("forwardPE", 0) or 0
            sf.pb_ratio = info.get("priceToBook", 0) or 0
            sf.eps = info.get("trailingEps", 0) or 0

            # ── Try to get detailed financials from statements ──
            self._enrich_from_statements(stock, sf)

            return sf

        except Exception as e:
            logger.error(f"  [Yahoo] Error fetching {ticker}: {e}")
            return None

    def _enrich_from_statements(self, yf_ticker, sf: StockFinancials):
        """Try to fill gaps from yfinance financial statements"""
        try:
            # Income Statement
            income = yf_ticker.financials
            if income is not None and not income.empty:
                latest = income.iloc[:, 0]  # Most recent period
                if sf.revenue == 0:
                    sf.revenue = self._safe_get(latest, "Total Revenue")
                if sf.ebit == 0:
                    sf.ebit = self._safe_get(latest, "EBIT")
                if sf.operating_income == 0:
                    sf.operating_income = self._safe_get(latest, "Operating Income")
                if sf.net_income == 0:
                    sf.net_income = self._safe_get(latest, "Net Income")
                sf.interest_expense = abs(self._safe_get(latest, "Interest Expense"))
                sf.tax_expense = self._safe_get(latest, "Tax Provision")
                sf.cost_of_revenue = self._safe_get(latest, "Cost Of Revenue")

                # Calculate effective tax rate
                pretax = self._safe_get(latest, "Pretax Income")
                if pretax and pretax > 0 and sf.tax_expense:
                    sf.tax_rate = sf.tax_expense / pretax
        except Exception as e:
            logger.debug(f"  [Yahoo] Could not get income statement: {e}")

        try:
            # Balance Sheet
            balance = yf_ticker.balance_sheet
            if balance is not None and not balance.empty:
                latest = balance.iloc[:, 0]
                if sf.total_assets == 0:
                    sf.total_assets = self._safe_get(latest, "Total Assets")
                if sf.current_assets == 0:
                    sf.current_assets = self._safe_get(latest, "Current Assets")
                if sf.current_liabilities == 0:
                    sf.current_liabilities = self._safe_get(latest, "Current Liabilities")
                if sf.total_equity == 0:
                    sf.total_equity = self._safe_get(latest, "Stockholders Equity")
                if sf.total_debt == 0:
                    sf.total_debt = self._safe_get(latest, "Total Debt")
                if sf.long_term_debt == 0:
                    sf.long_term_debt = self._safe_get(latest, "Long Term Debt")
                if sf.short_term_debt == 0:
                    sf.short_term_debt = self._safe_get(latest, "Current Debt")
        except Exception as e:
            logger.debug(f"  [Yahoo] Could not get balance sheet: {e}")

        try:
            # Cash Flow
            cashflow = yf_ticker.cashflow
            if cashflow is not None and not cashflow.empty:
                latest = cashflow.iloc[:, 0]
                if sf.operating_cash_flow == 0:
                    sf.operating_cash_flow = self._safe_get(latest, "Operating Cash Flow")
                if sf.capital_expenditure == 0:
                    sf.capital_expenditure = abs(self._safe_get(latest, "Capital Expenditure"))
                if sf.free_cash_flow == 0:
                    sf.free_cash_flow = self._safe_get(latest, "Free Cash Flow")
        except Exception as e:
            logger.debug(f"  [Yahoo] Could not get cash flow: {e}")

    @staticmethod
    def _safe_get(series, key):
        """Safely get a value from a pandas Series"""
        try:
            val = series.get(key, 0)
            if pd and pd.isna(val):
                return 0.0
            return float(val) if val else 0.0
        except:
            return 0.0

    def collect_all(self, tickers: dict = None) -> list:
        """Collect data for all EGX tickers"""
        tickers = tickers or EGX_TICKERS
        results = []
        for ticker, meta in tickers.items():
            stock = self.collect_stock(ticker, meta)
            if stock:
                results.append(stock)
            time.sleep(REQUEST_DELAY)
        return results


# ═══════════════════════════════════════════════════════════════
# SOURCE 2: WEB SCRAPER (StockAnalysis.com / Investing.com)
# Best for: comprehensive list of tickers, market data
# ═══════════════════════════════════════════════════════════════

class WebScraperCollector:
    """Scrape financial data from publicly available websites"""

    SOURCE_NAME = "web_scraper"
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9,ar;q=0.8",
    }

    def __init__(self):
        if requests is None:
            raise ImportError("requests is required. Install: pip install requests")
        if BeautifulSoup is None:
            raise ImportError("beautifulsoup4 is required. Install: pip install beautifulsoup4")

    def scrape_stockanalysis_list(self) -> list:
        """
        Scrape the full list of EGX stocks from stockanalysis.com
        Returns list of dicts with basic info
        """
        logger.info("[WebScraper] Fetching EGX stock list from StockAnalysis.com...")
        url = "https://stockanalysis.com/list/egyptian-stock-exchange/"
        try:
            resp = requests.get(url, headers=self.HEADERS, timeout=30)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "html.parser")

            stocks = []
            table = soup.find("table")
            if table:
                rows = table.find_all("tr")[1:]  # Skip header
                for row in rows:
                    cols = row.find_all("td")
                    if len(cols) >= 5:
                        ticker = cols[1].get_text(strip=True)
                        name = cols[2].get_text(strip=True)
                        market_cap_text = cols[3].get_text(strip=True)
                        price_text = cols[4].get_text(strip=True)
                        stocks.append({
                            "ticker": ticker,
                            "name": name,
                            "market_cap_text": market_cap_text,
                            "price_text": price_text,
                        })
            logger.info(f"[WebScraper] Found {len(stocks)} stocks on StockAnalysis")
            return stocks

        except Exception as e:
            logger.error(f"[WebScraper] Error scraping StockAnalysis: {e}")
            return []

    def scrape_investing_com_fundamentals(self, ticker: str, investing_slug: str) -> dict:
        """
        Scrape financial summary from Investing.com
        investing_slug is the URL slug like 'commercial-intl-bank'
        """
        url = f"https://www.investing.com/equities/{investing_slug}-financial-summary"
        logger.info(f"  [WebScraper] Fetching {ticker} from Investing.com...")

        try:
            resp = requests.get(url, headers=self.HEADERS, timeout=30)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "html.parser")

            data = {}
            # Parse financial summary tables
            tables = soup.find_all("table")
            for table in tables:
                rows = table.find_all("tr")
                for row in rows:
                    cells = row.find_all(["td", "th"])
                    if len(cells) >= 2:
                        key = cells[0].get_text(strip=True)
                        val = cells[1].get_text(strip=True)
                        data[key] = val

            return data

        except Exception as e:
            logger.error(f"  [WebScraper] Error: {e}")
            return {}

    def scrape_mubasher_financials(self, ticker: str) -> dict:
        """
        Scrape financial statements from Mubasher Info
        URL pattern: english.mubasher.info/markets/EGX/stocks/{TICKER}/financial-statements
        """
        url = f"https://english.mubasher.info/markets/EGX/stocks/{ticker}/financial-statements"
        logger.info(f"  [WebScraper] Fetching {ticker} from Mubasher...")

        try:
            resp = requests.get(url, headers=self.HEADERS, timeout=30)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "html.parser")

            data = {"income_statement": {}, "balance_sheet": {}, "cash_flow": {}}

            # Parse financial tables
            tables = soup.find_all("table")
            for table in tables:
                rows = table.find_all("tr")
                for row in rows:
                    cells = row.find_all(["td", "th"])
                    if len(cells) >= 2:
                        key = cells[0].get_text(strip=True)
                        val = cells[-1].get_text(strip=True)  # Latest period
                        # Categorize based on common field names
                        key_lower = key.lower()
                        if any(k in key_lower for k in ["revenue", "sales", "income", "expense", "profit", "ebit", "tax"]):
                            data["income_statement"][key] = val
                        elif any(k in key_lower for k in ["asset", "liabilit", "equity", "debt", "cash", "capital"]):
                            data["balance_sheet"][key] = val
                        elif any(k in key_lower for k in ["operating", "investing", "financing", "cash flow"]):
                            data["cash_flow"][key] = val

            return data

        except Exception as e:
            logger.error(f"  [WebScraper] Error: {e}")
            return {}

    def scrape_egx_official(self, ticker: str) -> dict:
        """
        Scrape data from EGX official website
        Note: EGX.com.eg uses ASP.NET with ViewState, may need Selenium for full scraping
        """
        logger.info(f"  [WebScraper] Fetching {ticker} from EGX.com.eg...")
        # EGX official site typically requires session handling
        # This is a best-effort scraper; consider using Selenium for production
        url = f"https://www.egx.com.eg/en/stocksdata.aspx"
        try:
            resp = requests.get(url, headers=self.HEADERS, timeout=30)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "html.parser")

            data = {}
            table = soup.find("table", {"id": lambda x: x and "stocks" in x.lower()} if x else None)
            if table:
                rows = table.find_all("tr")
                for row in rows:
                    cells = row.find_all("td")
                    if len(cells) >= 4:
                        stock_ticker = cells[0].get_text(strip=True)
                        if stock_ticker == ticker:
                            data["close"] = cells[1].get_text(strip=True)
                            data["change"] = cells[2].get_text(strip=True)
                            data["volume"] = cells[3].get_text(strip=True)
                            break

            return data

        except Exception as e:
            logger.error(f"  [WebScraper] Error: {e}")
            return {}


# ═══════════════════════════════════════════════════════════════
# SOURCE 3: MANUAL / CSV DATA IMPORT
# For importing data from downloaded EGX reports, Excel files, etc.
# ═══════════════════════════════════════════════════════════════

class ManualDataImporter:
    """Import financial data from CSV/Excel files"""

    SOURCE_NAME = "manual_import"

    @staticmethod
    def import_from_csv(filepath: str) -> list:
        """
        Import stock data from CSV file.
        Expected columns: ticker, name, sector, revenue, ebit, total_assets,
                          current_liabilities, total_debt, total_equity,
                          market_cap, shares_outstanding, current_price, beta
        """
        logger.info(f"[ManualImport] Reading {filepath}...")
        results = []

        with open(filepath, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                sf = StockFinancials(
                    ticker=row.get("ticker", ""),
                    name=row.get("name", ""),
                    sector=row.get("sector", ""),
                    collection_date=datetime.now().strftime("%Y-%m-%d"),
                    data_source="csv_import",
                )
                # Map CSV columns to fields
                for field_name in [
                    "revenue", "ebit", "ebitda", "net_income", "operating_income",
                    "total_assets", "current_assets", "current_liabilities",
                    "total_debt", "total_equity", "long_term_debt",
                    "market_cap", "shares_outstanding", "current_price", "beta",
                    "interest_expense", "tax_expense", "operating_cash_flow",
                    "capital_expenditure", "free_cash_flow", "dividend_yield",
                    "roe", "roa", "pe_ratio", "pb_ratio", "eps",
                    "cash_and_equivalents", "cost_of_revenue", "gross_profit",
                ]:
                    if field_name in row and row[field_name]:
                        try:
                            setattr(sf, field_name, float(row[field_name].replace(",", "")))
                        except (ValueError, AttributeError):
                            pass

                if sf.ticker:
                    results.append(sf)

        logger.info(f"[ManualImport] Imported {len(results)} stocks from CSV")
        return results

    @staticmethod
    def generate_template_csv(filepath: str = "egx_data_template.csv"):
        """Generate a blank CSV template for manual data entry"""
        columns = [
            "ticker", "name", "sector",
            "current_price", "market_cap", "shares_outstanding", "beta", "dividend_yield",
            "revenue", "cost_of_revenue", "gross_profit", "operating_income", "ebit",
            "ebitda", "net_income", "interest_expense", "tax_expense",
            "total_assets", "current_assets", "current_liabilities",
            "total_debt", "long_term_debt", "total_equity", "cash_and_equivalents",
            "operating_cash_flow", "capital_expenditure", "free_cash_flow",
            "roe", "roa", "pe_ratio", "pb_ratio", "eps"
        ]

        with open(filepath, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(columns)
            # Write a sample row
            writer.writerow([
                "COMI", "Commercial International Bank", "Banking",
                "116.80", "358680000000", "1420000000", "0.95", "0.028",
                "124000000000", "80000000000", "44000000000", "22500000000", "22500000000",
                "28000000000", "18000000000", "5000000000", "4500000000",
                "680000000000", "120000000000", "42000000000",
                "125000000000", "85000000000", "78000000000", "45000000000",
                "25000000000", "8000000000", "17000000000",
                "0.285", "0.03", "11.2", "2.5", "12.68"
            ])

        logger.info(f"Template CSV generated: {filepath}")
        return filepath


# ═══════════════════════════════════════════════════════════════
# EVA CALCULATION ENGINE
# ═══════════════════════════════════════════════════════════════

class EVACalculator:
    """Compute EVA and related metrics for each stock"""

    def __init__(
        self,
        risk_free_rate: float = EGYPT_RISK_FREE_RATE,
        market_premium: float = EGYPT_MARKET_PREMIUM,
        cost_of_debt: float = EGYPT_COST_OF_DEBT,
        corporate_tax: float = EGYPT_CORPORATE_TAX,
    ):
        self.risk_free_rate = risk_free_rate
        self.market_premium = market_premium
        self.cost_of_debt = cost_of_debt
        self.corporate_tax = corporate_tax

    def calculate(self, stock: StockFinancials) -> StockFinancials:
        """Calculate EVA and all derived metrics"""
        warnings = []

        # ── Determine EBIT ──
        if stock.ebit == 0 and stock.operating_income > 0:
            stock.ebit = stock.operating_income
        elif stock.ebit == 0 and stock.ebitda > 0:
            stock.ebit = stock.ebitda * 0.8  # Rough approximation
            warnings.append("EBIT estimated from EBITDA")
        elif stock.ebit == 0 and stock.net_income > 0:
            stock.ebit = stock.net_income * 1.3  # Very rough
            warnings.append("EBIT estimated from Net Income")

        # ── Tax Rate ──
        if stock.tax_rate == 0:
            stock.tax_rate = self.corporate_tax

        # ── NOPAT ──
        stock.nopat = stock.ebit * (1 - stock.tax_rate)

        # ── Invested Capital ──
        if stock.total_assets > 0 and stock.current_liabilities > 0:
            stock.invested_capital = stock.total_assets - stock.current_liabilities
        elif stock.total_equity > 0 and stock.total_debt > 0:
            stock.invested_capital = stock.total_equity + stock.total_debt
            warnings.append("Invested capital from equity + debt")
        elif stock.total_equity > 0:
            stock.invested_capital = stock.total_equity
            warnings.append("Invested capital = equity only (no debt data)")

        if stock.invested_capital <= 0:
            stock.warnings = "; ".join(warnings + ["Cannot compute EVA: no invested capital data"])
            stock.signal = "INSUFFICIENT DATA"
            return stock

        # ── Capital Structure Weights ──
        total_capital = stock.total_equity + stock.total_debt
        if total_capital > 0:
            stock.equity_weight = stock.total_equity / total_capital
            stock.debt_weight = stock.total_debt / total_capital
        else:
            stock.equity_weight = 0.7  # Default assumption
            stock.debt_weight = 0.3
            warnings.append("Capital weights assumed (70/30)")

        # ── Cost of Equity (CAPM) ──
        beta = stock.beta if stock.beta > 0 else 1.0
        stock.cost_of_equity = self.risk_free_rate + beta * self.market_premium

        # ── After-tax Cost of Debt ──
        if stock.interest_expense > 0 and stock.total_debt > 0:
            implied_rate = stock.interest_expense / stock.total_debt
            stock.after_tax_cost_of_debt = implied_rate * (1 - stock.tax_rate)
        else:
            stock.after_tax_cost_of_debt = self.cost_of_debt * (1 - stock.tax_rate)

        # ── WACC ──
        stock.wacc = (
            stock.equity_weight * stock.cost_of_equity +
            stock.debt_weight * stock.after_tax_cost_of_debt
        )

        # ── EVA ──
        stock.capital_charge = stock.wacc * stock.invested_capital
        stock.eva = stock.nopat - stock.capital_charge

        # ── ROIC ──
        stock.roic = stock.nopat / stock.invested_capital if stock.invested_capital > 0 else 0

        # ── EVA Spread ──
        stock.eva_spread = stock.roic - stock.wacc

        # ── EVA per Share ──
        if stock.shares_outstanding > 0:
            stock.eva_per_share = stock.eva / stock.shares_outstanding
        elif stock.current_price > 0 and stock.market_cap > 0:
            shares = stock.market_cap / stock.current_price
            stock.eva_per_share = stock.eva / shares if shares > 0 else 0

        # ── Intrinsic Value Premium ──
        if stock.market_cap > 0 and stock.wacc > 0:
            intrinsic_value = (stock.eva / stock.wacc) + stock.invested_capital
            stock.intrinsic_premium = (intrinsic_value - stock.market_cap) / stock.market_cap
        else:
            stock.intrinsic_premium = 0
            warnings.append("Cannot compute intrinsic premium")

        # ── Signal ──
        if stock.intrinsic_premium > 0.15:
            stock.signal = "UNDERVALUED"
        elif stock.intrinsic_premium < -0.15:
            stock.signal = "OVERVALUED"
        else:
            stock.signal = "FAIR VALUE"

        # ── Data Completeness Score ──
        key_fields = [
            stock.current_price, stock.market_cap, stock.revenue, stock.ebit,
            stock.total_assets, stock.current_liabilities, stock.total_debt,
            stock.total_equity, stock.beta
        ]
        stock.data_completeness = sum(1 for f in key_fields if f > 0) / len(key_fields)

        stock.warnings = "; ".join(warnings)
        return stock


# ═══════════════════════════════════════════════════════════════
# MULTI-SOURCE DATA MERGER
# ═══════════════════════════════════════════════════════════════

class DataMerger:
    """Merge data from multiple sources, preferring the most complete"""

    @staticmethod
    def merge(primary: StockFinancials, secondary: StockFinancials) -> StockFinancials:
        """Merge two StockFinancials objects, filling gaps in primary with secondary"""
        if primary is None:
            return secondary
        if secondary is None:
            return primary

        # For each numeric field, use primary if available, else secondary
        numeric_fields = [
            "current_price", "market_cap", "shares_outstanding", "volume_avg",
            "fifty_two_week_high", "fifty_two_week_low", "beta", "dividend_yield",
            "revenue", "cost_of_revenue", "gross_profit", "operating_income",
            "ebit", "ebitda", "net_income", "interest_expense", "tax_expense",
            "total_assets", "current_assets", "total_liabilities", "current_liabilities",
            "long_term_debt", "short_term_debt", "total_debt", "total_equity",
            "book_value_per_share", "cash_and_equivalents",
            "operating_cash_flow", "capital_expenditure", "free_cash_flow",
            "roe", "roa", "debt_to_equity", "current_ratio", "pe_ratio", "pb_ratio", "eps",
        ]

        for f in numeric_fields:
            primary_val = getattr(primary, f, 0)
            secondary_val = getattr(secondary, f, 0)
            if (primary_val == 0 or primary_val is None) and secondary_val:
                setattr(primary, f, secondary_val)

        primary.data_source = f"{primary.data_source}+{secondary.data_source}"
        return primary


# ═══════════════════════════════════════════════════════════════
# ORCHESTRATOR — MAIN AGENT
# ═══════════════════════════════════════════════════════════════

class EGXDataAgent:
    """
    Main orchestrator that coordinates data collection from all sources,
    merges data, computes EVA, and stores results.
    """

    def __init__(self, db_path: str = DB_PATH, output_dir: str = OUTPUT_DIR):
        self.db = DatabaseManager(db_path)
        self.eva_calc = EVACalculator()
        self.merger = DataMerger()
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)

    def run_full_collection(
        self,
        tickers: dict = None,
        use_yahoo: bool = True,
        use_web_scraper: bool = False,
        csv_import_path: str = None,
    ) -> list:
        """
        Run the full data collection pipeline:
        1. Collect from all enabled sources
        2. Merge data
        3. Calculate EVA
        4. Store results
        5. Export reports
        """
        tickers = tickers or EGX_TICKERS
        start_time = time.time()
        all_results = {}
        errors = []

        logger.info("=" * 60)
        logger.info("EGX DATA COLLECTION AGENT — STARTING RUN")
        logger.info(f"Tickers to process: {len(tickers)}")
        logger.info("=" * 60)

        # ── Source 1: Yahoo Finance ──
        if use_yahoo and yf:
            logger.info("\n📡 Source 1: Yahoo Finance")
            logger.info("-" * 40)
            yahoo = YahooFinanceCollector()
            for ticker, meta in tickers.items():
                try:
                    result = yahoo.collect_stock(ticker, meta)
                    if result:
                        all_results[ticker] = result
                        logger.info(f"  ✅ {ticker}: price={result.current_price}, "
                                    f"revenue={result.revenue/1e6:.0f}M, "
                                    f"assets={result.total_assets/1e6:.0f}M")
                    else:
                        logger.warning(f"  ❌ {ticker}: No data")
                        errors.append(f"{ticker}: no Yahoo data")
                except Exception as e:
                    logger.error(f"  ❌ {ticker}: {e}")
                    errors.append(f"{ticker}: {str(e)}")
                time.sleep(REQUEST_DELAY)

        # ── Source 2: Web Scraper ──
        if use_web_scraper and requests and BeautifulSoup:
            logger.info("\n🌐 Source 2: Web Scraper")
            logger.info("-" * 40)
            scraper = WebScraperCollector()
            for ticker, meta in tickers.items():
                try:
                    mubasher_data = scraper.scrape_mubasher_financials(ticker)
                    if mubasher_data:
                        # Convert scraped data to StockFinancials
                        scraped_sf = self._parse_mubasher_data(ticker, meta, mubasher_data)
                        if scraped_sf:
                            if ticker in all_results:
                                all_results[ticker] = self.merger.merge(all_results[ticker], scraped_sf)
                            else:
                                all_results[ticker] = scraped_sf
                            logger.info(f"  ✅ {ticker}: Mubasher data merged")
                except Exception as e:
                    logger.error(f"  ❌ {ticker}: {e}")
                    errors.append(f"{ticker}: web scrape error - {str(e)}")
                time.sleep(REQUEST_DELAY)

        # ── Source 3: CSV Import ──
        if csv_import_path:
            logger.info(f"\n📄 Source 3: CSV Import ({csv_import_path})")
            logger.info("-" * 40)
            importer = ManualDataImporter()
            csv_stocks = importer.import_from_csv(csv_import_path)
            for cs in csv_stocks:
                if cs.ticker in all_results:
                    all_results[cs.ticker] = self.merger.merge(all_results[cs.ticker], cs)
                else:
                    all_results[cs.ticker] = cs
                logger.info(f"  ✅ {cs.ticker}: CSV data merged")

        # ── Calculate EVA for all collected stocks ──
        logger.info("\n📊 Computing EVA Analysis...")
        logger.info("-" * 40)
        final_results = []
        for ticker, stock in all_results.items():
            stock = self.eva_calc.calculate(stock)
            self.db.save_financials(stock)
            self.db.save_eva_result(stock)
            final_results.append(stock)
            logger.info(
                f"  {ticker}: EVA={'EGP {:,.0f}'.format(stock.eva)} | "
                f"ROIC={stock.roic*100:.1f}% | WACC={stock.wacc*100:.1f}% | "
                f"Signal={stock.signal} | Completeness={stock.data_completeness*100:.0f}%"
            )

        # Sort by EVA descending
        final_results.sort(key=lambda x: x.eva, reverse=True)

        # ── Log the run ──
        duration = time.time() - start_time
        success_count = len(final_results)
        fail_count = len(tickers) - success_count
        self.db.log_collection_run(
            source="multi_source",
            attempted=len(tickers),
            success=success_count,
            failed=fail_count,
            errors=errors,
            duration=duration
        )

        # ── Export results ──
        self._export_json(final_results)
        self._export_csv(final_results)
        self._print_summary(final_results, duration, errors)

        return final_results

    def _parse_mubasher_data(self, ticker: str, meta: dict, data: dict) -> Optional[StockFinancials]:
        """Parse Mubasher scraped data into StockFinancials"""
        sf = StockFinancials(
            ticker=ticker,
            name=meta.get("name", ""),
            sector=meta.get("sector", ""),
            collection_date=datetime.now().strftime("%Y-%m-%d"),
            data_source="mubasher",
        )

        def parse_number(text):
            if not text:
                return 0.0
            text = text.replace(",", "").replace("(", "-").replace(")", "").strip()
            try:
                return float(text) * 1_000_000  # Mubasher shows in millions
            except ValueError:
                return 0.0

        is_data = data.get("income_statement", {})
        bs_data = data.get("balance_sheet", {})

        for key, val in is_data.items():
            kl = key.lower()
            if "revenue" in kl or "sales" in kl:
                sf.revenue = parse_number(val)
            elif "ebit" in kl and "ebitda" not in kl:
                sf.ebit = parse_number(val)
            elif "net income" in kl or "net profit" in kl:
                sf.net_income = parse_number(val)

        for key, val in bs_data.items():
            kl = key.lower()
            if "total assets" in kl:
                sf.total_assets = parse_number(val)
            elif "current liabilities" in kl:
                sf.current_liabilities = parse_number(val)
            elif "total equity" in kl or "shareholders" in kl:
                sf.total_equity = parse_number(val)
            elif "total debt" in kl or "borrowings" in kl:
                sf.total_debt = parse_number(val)

        return sf if sf.revenue > 0 or sf.total_assets > 0 else None

    def _export_json(self, results: list):
        """Export results to JSON for the dashboard"""
        filepath = os.path.join(self.output_dir, "egx_eva_results.json")
        data = {
            "metadata": {
                "generated_at": datetime.now().isoformat(),
                "total_stocks": len(results),
                "sources": list(set(r.data_source for r in results)),
                "assumptions": {
                    "risk_free_rate": EGYPT_RISK_FREE_RATE,
                    "market_premium": EGYPT_MARKET_PREMIUM,
                    "cost_of_debt": EGYPT_COST_OF_DEBT,
                    "corporate_tax": EGYPT_CORPORATE_TAX,
                }
            },
            "stocks": [asdict(r) for r in results]
        }
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        logger.info(f"📁 JSON exported: {filepath}")

    def _export_csv(self, results: list):
        """Export results to CSV"""
        filepath = os.path.join(self.output_dir, "egx_eva_results.csv")
        if not results:
            return

        fieldnames = [
            "ticker", "name", "sector", "signal", "current_price", "market_cap",
            "revenue", "ebit", "nopat", "invested_capital", "wacc", "roic",
            "eva", "eva_spread", "eva_per_share", "intrinsic_premium",
            "beta", "data_completeness", "data_source", "warnings"
        ]

        with open(filepath, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            for r in results:
                row = asdict(r)
                # Format percentages
                for pct_field in ["wacc", "roic", "eva_spread", "intrinsic_premium", "data_completeness"]:
                    if pct_field in row:
                        row[pct_field] = f"{row[pct_field] * 100:.2f}%"
                writer.writerow(row)

        logger.info(f"📁 CSV exported: {filepath}")

    def _print_summary(self, results: list, duration: float, errors: list):
        """Print a beautiful summary to console"""
        print("\n" + "═" * 70)
        print("  EGX EVA ANALYSIS — SUMMARY REPORT")
        print(f"  Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"  Duration: {duration:.1f}s | Stocks analyzed: {len(results)}")
        print("═" * 70)

        undervalued = [r for r in results if r.signal == "UNDERVALUED"]
        overvalued = [r for r in results if r.signal == "OVERVALUED"]
        fair = [r for r in results if r.signal == "FAIR VALUE"]
        positive_eva = [r for r in results if r.eva > 0]

        print(f"\n  📊 SIGNALS: {len(undervalued)} Undervalued | {len(fair)} Fair | {len(overvalued)} Overvalued")
        print(f"  📈 EVA+ Companies: {len(positive_eva)}/{len(results)}")

        if undervalued:
            print("\n  🎯 TOP UNDERVALUED PICKS:")
            print("  " + "-" * 66)
            print(f"  {'Ticker':<8} {'Name':<30} {'EVA (M)':<12} {'Upside':<10} {'ROIC'}")
            print("  " + "-" * 66)
            for s in sorted(undervalued, key=lambda x: x.intrinsic_premium, reverse=True)[:10]:
                print(f"  {s.ticker:<8} {s.name[:28]:<30} "
                      f"{'EGP {:,.0f}'.format(s.eva/1e6):<12} "
                      f"+{s.intrinsic_premium*100:.1f}%{'':>4} "
                      f"{s.roic*100:.1f}%")

        if errors:
            print(f"\n  ⚠ {len(errors)} errors encountered")
            for e in errors[:5]:
                print(f"    • {e}")
            if len(errors) > 5:
                print(f"    ... and {len(errors)-5} more")

        print("\n" + "═" * 70)
        print(f"  Output files: {self.output_dir}/egx_eva_results.json")
        print(f"                {self.output_dir}/egx_eva_results.csv")
        print("═" * 70 + "\n")


# ═══════════════════════════════════════════════════════════════
# CLI ENTRY POINT
# ═══════════════════════════════════════════════════════════════

def main():
    """CLI entry point for the EGX Data Agent"""
    import argparse

    parser = argparse.ArgumentParser(
        description="EGX EVA Data Collection Agent",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run with Yahoo Finance data (default)
  python collector.py

  # Run with web scraping enabled
  python collector.py --web-scrape

  # Import from CSV file
  python collector.py --csv data/my_egx_data.csv

  # Generate CSV template for manual entry
  python collector.py --template

  # Analyze specific tickers only
  python collector.py --tickers COMI SWDY TMGH EAST

  # Custom output directory
  python collector.py --output-dir ./my_results

  # Custom delay between API requests
  python collector.py --delay 3.0
        """
    )
    parser.add_argument("--tickers", nargs="+", help="Specific tickers to analyze")
    parser.add_argument("--csv", type=str, help="Path to CSV file for import")
    parser.add_argument("--web-scrape", action="store_true", help="Enable web scraping")
    parser.add_argument("--no-yahoo", action="store_true", help="Disable Yahoo Finance")
    parser.add_argument("--template", action="store_true", help="Generate CSV template")
    parser.add_argument("--output-dir", type=str, default=OUTPUT_DIR, help="Output directory")
    parser.add_argument("--db", type=str, default=DB_PATH, help="Database file path")
    parser.add_argument("--delay", type=float, default=REQUEST_DELAY, help="Delay between requests (seconds)")
    parser.add_argument("--list-tickers", action="store_true", help="List all known EGX tickers")

    args = parser.parse_args()

    # Generate template
    if args.template:
        ManualDataImporter.generate_template_csv("egx_data_template.csv")
        return

    # List tickers
    if args.list_tickers:
        print(f"\n{'Ticker':<10} {'Sector':<25} {'Name'}")
        print("-" * 80)
        for t, m in sorted(EGX_TICKERS.items()):
            print(f"{t:<10} {m['sector']:<25} {m['name']}")
        print(f"\nTotal: {len(EGX_TICKERS)} tickers")
        return

    # Set delay
    global REQUEST_DELAY
    REQUEST_DELAY = args.delay

    # Filter tickers if specified
    tickers = EGX_TICKERS
    if args.tickers:
        tickers = {t: EGX_TICKERS[t] for t in args.tickers if t in EGX_TICKERS}
        unknown = [t for t in args.tickers if t not in EGX_TICKERS]
        if unknown:
            print(f"⚠ Unknown tickers (skipped): {', '.join(unknown)}")

    # Run the agent
    agent = EGXDataAgent(db_path=args.db, output_dir=args.output_dir)
    agent.run_full_collection(
        tickers=tickers,
        use_yahoo=not args.no_yahoo,
        use_web_scraper=args.web_scrape,
        csv_import_path=args.csv,
    )


if __name__ == "__main__":
    main()
