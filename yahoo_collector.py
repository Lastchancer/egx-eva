"""
EGX EVA Analyzer — Yahoo Finance Data Collector
Primary source for financial statements via yfinance library.
EGX stocks use the .CA suffix on Yahoo Finance.
"""

import time
import logging
from datetime import datetime
from typing import Dict, List, Optional

import yfinance as yf
import pandas as pd

from config import (
    YAHOO_FINANCE_SUFFIX, REQUEST_DELAY_MIN, REQUEST_DELAY_MAX,
    KNOWN_EGX_TICKERS
)
import database as db

logger = logging.getLogger(__name__)


class YahooFinanceCollector:
    """Collects financial data from Yahoo Finance for EGX stocks."""

    SOURCE = "yahoo_finance"

    def __init__(self, conn):
        self.conn = conn

    def _yahoo_ticker(self, ticker: str) -> str:
        """Convert EGX ticker to Yahoo Finance format."""
        return f"{ticker}{YAHOO_FINANCE_SUFFIX}"

    def collect_stock(self, ticker: str) -> Dict:
        """
        Collect all available data for a single stock.
        Returns a summary dict of what was collected.
        """
        yahoo_sym = self._yahoo_ticker(ticker)
        start_time = time.time()
        result = {"ticker": ticker, "source": self.SOURCE, "records": 0, "errors": []}

        try:
            stock = yf.Ticker(yahoo_sym)

            # ── Company Info ──
            try:
                info = stock.info or {}
                if info.get("shortName") or info.get("longName"):
                    db.upsert_company(
                        self.conn,
                        ticker=ticker,
                        name=info.get("longName") or info.get("shortName", ""),
                        sector=info.get("sector", ""),
                        industry=info.get("industry", ""),
                        source=self.SOURCE,
                    )
                    result["records"] += 1

                    # ── Market Data ──
                    market_data = {
                        "ticker": ticker,
                        "date": datetime.now().strftime("%Y-%m-%d"),
                        "close_price": info.get("currentPrice") or info.get("previousClose"),
                        "open_price": info.get("open"),
                        "high_price": info.get("dayHigh"),
                        "low_price": info.get("dayLow"),
                        "volume": info.get("volume"),
                        "market_cap": info.get("marketCap"),
                        "shares_outstanding": info.get("sharesOutstanding"),
                        "pe_ratio": info.get("trailingPE"),
                        "pb_ratio": info.get("priceToBook"),
                        "dividend_yield": info.get("dividendYield"),
                        "beta": info.get("beta"),
                        "fifty_two_week_high": info.get("fiftyTwoWeekHigh"),
                        "fifty_two_week_low": info.get("fiftyTwoWeekLow"),
                        "source": self.SOURCE,
                    }
                    db.upsert_market_data(self.conn, market_data)
                    result["records"] += 1
                    logger.info(f"  [Yahoo] {ticker}: Market data ✓")
                else:
                    logger.warning(f"  [Yahoo] {ticker}: No info returned")
                    result["errors"].append("No company info available")
            except Exception as e:
                result["errors"].append(f"Info: {str(e)}")
                logger.warning(f"  [Yahoo] {ticker}: Info failed - {e}")

            # ── Income Statements ──
            try:
                for period_type, stmt_attr in [("annual", "income_stmt"), ("quarterly", "quarterly_income_stmt")]:
                    stmt = getattr(stock, stmt_attr, None)
                    if stmt is not None and not stmt.empty:
                        for col in stmt.columns:
                            period_end = col.strftime("%Y-%m-%d") if hasattr(col, 'strftime') else str(col)
                            income_data = {
                                "ticker": ticker,
                                "period": period_type,
                                "period_end": period_end,
                                "revenue": _safe_get(stmt, col, ["Total Revenue", "TotalRevenue"]),
                                "cost_of_revenue": _safe_get(stmt, col, ["Cost Of Revenue", "CostOfRevenue"]),
                                "gross_profit": _safe_get(stmt, col, ["Gross Profit", "GrossProfit"]),
                                "operating_expenses": _safe_get(stmt, col, ["Operating Expense", "OperatingExpense", "Total Operating Expenses"]),
                                "ebit": _safe_get(stmt, col, ["EBIT", "Operating Income"]),
                                "ebitda": _safe_get(stmt, col, ["EBITDA", "Normalized EBITDA"]),
                                "interest_expense": _safe_get(stmt, col, ["Interest Expense", "InterestExpense", "Interest Expense Non Operating"]),
                                "pretax_income": _safe_get(stmt, col, ["Pretax Income", "PretaxIncome"]),
                                "tax_expense": _safe_get(stmt, col, ["Tax Provision", "TaxProvision", "Income Tax Expense"]),
                                "net_income": _safe_get(stmt, col, ["Net Income", "NetIncome", "Net Income Common Stockholders"]),
                                "eps": _safe_get(stmt, col, ["Basic EPS", "Diluted EPS"]),
                                "source": self.SOURCE,
                            }
                            db.upsert_income_statement(self.conn, income_data)
                            result["records"] += 1
                        logger.info(f"  [Yahoo] {ticker}: Income statement ({period_type}) ✓ ({len(stmt.columns)} periods)")
            except Exception as e:
                result["errors"].append(f"Income: {str(e)}")
                logger.warning(f"  [Yahoo] {ticker}: Income stmt failed - {e}")

            # ── Balance Sheets ──
            try:
                for period_type, stmt_attr in [("annual", "balance_sheet"), ("quarterly", "quarterly_balance_sheet")]:
                    stmt = getattr(stock, stmt_attr, None)
                    if stmt is not None and not stmt.empty:
                        for col in stmt.columns:
                            period_end = col.strftime("%Y-%m-%d") if hasattr(col, 'strftime') else str(col)
                            bs_data = {
                                "ticker": ticker,
                                "period": period_type,
                                "period_end": period_end,
                                "total_assets": _safe_get(stmt, col, ["Total Assets", "TotalAssets"]),
                                "current_assets": _safe_get(stmt, col, ["Current Assets", "CurrentAssets"]),
                                "non_current_assets": _safe_get(stmt, col, ["Total Non Current Assets"]),
                                "total_liabilities": _safe_get(stmt, col, ["Total Liabilities Net Minority Interest", "Total Liab"]),
                                "current_liabilities": _safe_get(stmt, col, ["Current Liabilities", "CurrentLiabilities"]),
                                "non_current_liabilities": _safe_get(stmt, col, ["Total Non Current Liabilities Net Minority Interest"]),
                                "total_debt": _safe_get(stmt, col, ["Total Debt", "TotalDebt"]),
                                "long_term_debt": _safe_get(stmt, col, ["Long Term Debt", "LongTermDebt"]),
                                "short_term_debt": _safe_get(stmt, col, ["Current Debt", "Current Debt And Capital Lease Obligation"]),
                                "total_equity": _safe_get(stmt, col, ["Total Equity Gross Minority Interest", "Stockholders Equity", "StockholdersEquity"]),
                                "retained_earnings": _safe_get(stmt, col, ["Retained Earnings", "RetainedEarnings"]),
                                "cash_and_equivalents": _safe_get(stmt, col, ["Cash And Cash Equivalents", "Cash Cash Equivalents And Short Term Investments"]),
                                "source": self.SOURCE,
                            }
                            db.upsert_balance_sheet(self.conn, bs_data)
                            result["records"] += 1
                        logger.info(f"  [Yahoo] {ticker}: Balance sheet ({period_type}) ✓ ({len(stmt.columns)} periods)")
            except Exception as e:
                result["errors"].append(f"Balance: {str(e)}")
                logger.warning(f"  [Yahoo] {ticker}: Balance sheet failed - {e}")

            # ── Cash Flow ──
            try:
                for period_type, stmt_attr in [("annual", "cashflow"), ("quarterly", "quarterly_cashflow")]:
                    stmt = getattr(stock, stmt_attr, None)
                    if stmt is not None and not stmt.empty:
                        for col in stmt.columns:
                            period_end = col.strftime("%Y-%m-%d") if hasattr(col, 'strftime') else str(col)
                            cf_data = {
                                "ticker": ticker,
                                "period": period_type,
                                "period_end": period_end,
                                "operating_cash_flow": _safe_get(stmt, col, ["Operating Cash Flow", "Cash Flow From Continuing Operating Activities"]),
                                "investing_cash_flow": _safe_get(stmt, col, ["Investing Cash Flow", "Cash Flow From Continuing Investing Activities"]),
                                "financing_cash_flow": _safe_get(stmt, col, ["Financing Cash Flow", "Cash Flow From Continuing Financing Activities"]),
                                "capex": _safe_get(stmt, col, ["Capital Expenditure", "CapitalExpenditure"]),
                                "free_cash_flow": _safe_get(stmt, col, ["Free Cash Flow", "FreeCashFlow"]),
                                "dividends_paid": _safe_get(stmt, col, ["Common Stock Dividend Paid", "Cash Dividends Paid"]),
                                "source": self.SOURCE,
                            }
                            db.upsert_cash_flow(self.conn, cf_data)
                            result["records"] += 1
                        logger.info(f"  [Yahoo] {ticker}: Cash flow ({period_type}) ✓")
            except Exception as e:
                result["errors"].append(f"CashFlow: {str(e)}")

        except Exception as e:
            result["errors"].append(f"General: {str(e)}")
            logger.error(f"  [Yahoo] {ticker}: Failed completely - {e}")

        duration = time.time() - start_time
        status = "success" if not result["errors"] else ("partial" if result["records"] > 0 else "failed")
        db.log_collection(self.conn, ticker, self.SOURCE, status,
                         result["records"], "; ".join(result["errors"]) if result["errors"] else None, duration)

        return result

    def collect_all(self, tickers: List[str] = None) -> List[Dict]:
        """Collect data for all tickers."""
        tickers = tickers or KNOWN_EGX_TICKERS
        results = []
        total = len(tickers)

        logger.info(f"[Yahoo Finance] Starting collection for {total} tickers...")

        for i, ticker in enumerate(tickers, 1):
            logger.info(f"[Yahoo] ({i}/{total}) Processing {ticker}...")
            result = self.collect_stock(ticker)
            results.append(result)

            # Rate limiting — be respectful
            if i < total:
                delay = REQUEST_DELAY_MIN + (REQUEST_DELAY_MAX - REQUEST_DELAY_MIN) * (i % 3) / 3
                time.sleep(delay)

        success = sum(1 for r in results if not r["errors"])
        partial = sum(1 for r in results if r["errors"] and r["records"] > 0)
        failed = sum(1 for r in results if r["records"] == 0 and r["errors"])
        logger.info(f"[Yahoo Finance] Complete: {success} success, {partial} partial, {failed} failed")

        return results


def _safe_get(df: pd.DataFrame, col, possible_keys: List[str]) -> Optional[float]:
    """Safely extract a value from a DataFrame, trying multiple possible key names."""
    for key in possible_keys:
        if key in df.index:
            val = df.loc[key, col]
            if pd.notna(val):
                return float(val)
    return None
