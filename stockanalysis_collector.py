"""
EGX EVA Analyzer — StockAnalysis.com Data Collector
Supplementary data source for EGX stocks.
URL pattern: https://stockanalysis.com/quote/egx/{TICKER}/
Provides: financials, market data, ratios for many EGX companies.
"""

import time
import random
import logging
from datetime import datetime
from typing import Dict, List, Optional

import requests
from bs4 import BeautifulSoup

from config import (
    STOCKANALYSIS_BASE, USER_AGENTS,
    REQUEST_TIMEOUT, REQUEST_DELAY_MIN, REQUEST_DELAY_MAX
)
import database as db

logger = logging.getLogger(__name__)


class StockAnalysisCollector:
    """Collects EGX stock data from stockanalysis.com."""

    SOURCE = "stockanalysis"

    def __init__(self, conn):
        self.conn = conn
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": random.choice(USER_AGENTS),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        })

    def _get(self, url: str) -> Optional[BeautifulSoup]:
        """Make a GET request and return parsed HTML."""
        try:
            resp = self.session.get(url, timeout=REQUEST_TIMEOUT)
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            return BeautifulSoup(resp.text, "lxml")
        except requests.RequestException as e:
            logger.debug(f"  [StockAnalysis] Request failed: {url} — {e}")
            return None

    def collect_stock(self, ticker: str) -> Dict:
        """Collect data for a single stock from StockAnalysis."""
        result = {"ticker": ticker, "source": self.SOURCE, "records": 0, "errors": []}
        start = time.time()

        # ── Overview page ──
        url = f"{STOCKANALYSIS_BASE}/{ticker}/"
        soup = self._get(url)

        if not soup:
            result["errors"].append("Stock page not found")
            duration = time.time() - start
            db.log_collection(self.conn, ticker, self.SOURCE, "failed", 0,
                            "Page not found", duration)
            return result

        try:
            market_data = {
                "ticker": ticker,
                "date": datetime.now().strftime("%Y-%m-%d"),
                "source": self.SOURCE,
            }

            # Extract price from overview
            price_el = soup.select_one("[data-test='overview-quote']")
            if not price_el:
                # Try alternative selectors
                price_el = soup.select_one(".price-value, [class*='price']")

            if price_el:
                try:
                    price_text = price_el.get_text(strip=True).replace(",", "").replace("EGP", "").strip()
                    market_data["close_price"] = float(price_text)
                except ValueError:
                    pass

            # Extract key stats from overview table/grid
            # StockAnalysis shows stats in a grid format
            stat_items = soup.select("[data-test='overview-info'] tr, .overview-item, [class*='stat-row']")
            for item in stat_items:
                cells = item.find_all(["td", "span", "div"])
                if len(cells) >= 2:
                    label = cells[0].get_text(strip=True).lower()
                    value_text = cells[-1].get_text(strip=True).replace(",", "").replace("%", "").replace("EGP", "").strip()

                    try:
                        value = float(value_text.replace("B", "e9").replace("M", "e6").replace("K", "e3")
                                     if any(c in value_text for c in ["B", "M", "K"]) else value_text)
                    except (ValueError, OverflowError):
                        continue

                    if "market cap" in label:
                        market_data["market_cap"] = value
                    elif "p/e" in label or "pe ratio" in label:
                        market_data["pe_ratio"] = value
                    elif "p/b" in label or "price to book" in label:
                        market_data["pb_ratio"] = value
                    elif "dividend yield" in label:
                        market_data["dividend_yield"] = value / 100 if value > 1 else value
                    elif "beta" in label:
                        market_data["beta"] = value
                    elif "shares" in label and "outstanding" in label:
                        market_data["shares_outstanding"] = value
                    elif "52" in label and "high" in label:
                        market_data["fifty_two_week_high"] = value
                    elif "52" in label and "low" in label:
                        market_data["fifty_two_week_low"] = value
                    elif "volume" in label and "avg" not in label:
                        market_data["volume"] = int(value)

            if market_data.get("close_price") or market_data.get("market_cap"):
                db.upsert_market_data(self.conn, market_data)
                result["records"] += 1

        except Exception as e:
            result["errors"].append(f"Overview: {str(e)}")

        # ── Financials page ──
        time.sleep(random.uniform(0.5, 1.5))

        for fin_type in ["financials", "financials/?p=balance-sheet"]:
            fin_url = f"{STOCKANALYSIS_BASE}/{ticker}/{fin_type}"
            fin_soup = self._get(fin_url)
            if not fin_soup:
                continue

            try:
                tables = fin_soup.select("table")
                for table in tables:
                    headers = []
                    thead = table.find("thead")
                    if thead:
                        headers = [th.get_text(strip=True) for th in thead.find_all(["th", "td"])]

                    tbody = table.find("tbody")
                    if not tbody:
                        continue

                    for row in tbody.find_all("tr"):
                        cells = row.find_all("td")
                        if len(cells) < 2:
                            continue

                        label = cells[0].get_text(strip=True).lower()
                        value_text = cells[1].get_text(strip=True).replace(",", "").replace("(", "-").replace(")", "")

                        try:
                            value = float(value_text)
                        except ValueError:
                            continue

                        period_end = datetime.now().strftime("%Y-12-31")

                        # Income statement items
                        if "revenue" in label or "total revenue" in label:
                            db.upsert_income_statement(self.conn, {
                                "ticker": ticker, "period": "annual", "period_end": period_end,
                                "revenue": value, "source": self.SOURCE
                            })
                            result["records"] += 1
                        elif label.startswith("ebit") and "da" not in label:
                            db.upsert_income_statement(self.conn, {
                                "ticker": ticker, "period": "annual", "period_end": period_end,
                                "ebit": value, "source": self.SOURCE
                            })
                            result["records"] += 1
                        elif "net income" in label:
                            db.upsert_income_statement(self.conn, {
                                "ticker": ticker, "period": "annual", "period_end": period_end,
                                "net_income": value, "source": self.SOURCE
                            })
                            result["records"] += 1

                        # Balance sheet items
                        elif "total assets" in label:
                            db.upsert_balance_sheet(self.conn, {
                                "ticker": ticker, "period": "annual", "period_end": period_end,
                                "total_assets": value, "source": self.SOURCE
                            })
                            result["records"] += 1
                        elif "current liabilities" in label:
                            db.upsert_balance_sheet(self.conn, {
                                "ticker": ticker, "period": "annual", "period_end": period_end,
                                "current_liabilities": value, "source": self.SOURCE
                            })
                            result["records"] += 1
                        elif "total debt" in label:
                            db.upsert_balance_sheet(self.conn, {
                                "ticker": ticker, "period": "annual", "period_end": period_end,
                                "total_debt": value, "source": self.SOURCE
                            })
                            result["records"] += 1
                        elif "total equity" in label or "stockholders" in label:
                            db.upsert_balance_sheet(self.conn, {
                                "ticker": ticker, "period": "annual", "period_end": period_end,
                                "total_equity": value, "source": self.SOURCE
                            })
                            result["records"] += 1

            except Exception as e:
                result["errors"].append(f"Financials: {str(e)}")

            time.sleep(random.uniform(0.5, 1.0))

        duration = time.time() - start
        status = "success" if not result["errors"] else ("partial" if result["records"] > 0 else "failed")
        db.log_collection(self.conn, ticker, self.SOURCE, status,
                         result["records"], "; ".join(result["errors"]) if result["errors"] else None, duration)

        if result["records"] > 0:
            logger.info(f"  [StockAnalysis] {ticker}: ✓ ({result['records']} records)")

        return result

    def collect_all(self, tickers: List[str] = None) -> List[Dict]:
        """Collect data for all tickers."""
        if not tickers:
            tickers = db.get_all_tickers(self.conn)

        results = []
        total = len(tickers)
        logger.info(f"[StockAnalysis] Starting collection for {total} tickers...")

        for i, ticker in enumerate(tickers, 1):
            logger.info(f"[StockAnalysis] ({i}/{total}) Processing {ticker}...")
            result = self.collect_stock(ticker)
            results.append(result)

            if i < total:
                time.sleep(random.uniform(REQUEST_DELAY_MIN, REQUEST_DELAY_MAX))

        success = sum(1 for r in results if r["records"] > 0)
        logger.info(f"[StockAnalysis] Complete: {success}/{total} with data")
        return results
