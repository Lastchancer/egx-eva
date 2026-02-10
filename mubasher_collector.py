"""
EGX EVA Analyzer — Mubasher Info Data Collector
Scrapes stock listings, prices, and financial data from english.mubasher.info.
This is one of the most comprehensive free sources for EGX financial data.
"""

import time
import random
import logging
from datetime import datetime
from typing import Dict, List, Optional

import requests
from bs4 import BeautifulSoup

from config import (
    MUBASHER_BASE, MUBASHER_EGX_STOCKS,
    MUBASHER_STOCK_URL, MUBASHER_FINANCIALS_URL,
    USER_AGENTS, REQUEST_TIMEOUT, REQUEST_DELAY_MIN, REQUEST_DELAY_MAX
)
import database as db

logger = logging.getLogger(__name__)


class MubasherCollector:
    """Collects EGX data from Mubasher Info website."""

    SOURCE = "mubasher"

    def __init__(self, conn):
        self.conn = conn
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": random.choice(USER_AGENTS),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9,ar;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Referer": MUBASHER_BASE,
        })

    def _get(self, url: str) -> Optional[BeautifulSoup]:
        """Make a GET request and return parsed HTML."""
        try:
            resp = self.session.get(url, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            return BeautifulSoup(resp.text, "lxml")
        except requests.RequestException as e:
            logger.warning(f"  [Mubasher] Request failed for {url}: {e}")
            return None

    def discover_tickers(self) -> List[Dict]:
        """
        Discover all EGX listed stock tickers from Mubasher index pages.
        Scrapes EGX30, EGX70, EGX100 index pages for comprehensive coverage.
        """
        discovered = []
        index_urls = [
            f"{MUBASHER_EGX_STOCKS}/indices/EGX30",
            f"{MUBASHER_EGX_STOCKS}/indices/EGX70%20EWI",
            f"{MUBASHER_EGX_STOCKS}/indices/EGX100%20EWI",
        ]

        for url in index_urls:
            logger.info(f"  [Mubasher] Scanning {url}...")
            soup = self._get(url)
            if not soup:
                continue

            # Look for stock table rows
            # Mubasher typically renders stock data in table rows with class patterns
            rows = soup.select("table tbody tr")
            for row in rows:
                try:
                    cells = row.find_all("td")
                    if len(cells) >= 3:
                        # First cell often has the ticker link
                        link = row.find("a", href=True)
                        if link and "/stocks/" in link.get("href", ""):
                            href = link["href"]
                            ticker = href.rstrip("/").split("/")[-1]
                            name = link.get_text(strip=True)

                            if ticker and len(ticker) >= 2 and ticker.isalpha():
                                discovered.append({
                                    "ticker": ticker.upper(),
                                    "name": name,
                                })
                except Exception:
                    continue

            # Also try JSON-style embedded data (some pages embed stock lists)
            scripts = soup.find_all("script")
            for script in scripts:
                text = script.get_text()
                if "stockCode" in text or "ticker" in text:
                    # Try to extract tickers from embedded JSON
                    import re
                    ticker_matches = re.findall(r'"(?:stockCode|ticker)"\s*:\s*"([A-Z]{2,6})"', text)
                    for t in ticker_matches:
                        discovered.append({"ticker": t, "name": ""})

            time.sleep(random.uniform(REQUEST_DELAY_MIN, REQUEST_DELAY_MAX))

        # Deduplicate
        seen = set()
        unique = []
        for d in discovered:
            if d["ticker"] not in seen:
                seen.add(d["ticker"])
                unique.append(d)

        logger.info(f"  [Mubasher] Discovered {len(unique)} unique tickers")
        return unique

    def collect_stock_page(self, ticker: str) -> Dict:
        """
        Scrape main stock page for price, market data, and key metrics.
        URL: english.mubasher.info/markets/EGX/stocks/{ticker}
        """
        url = MUBASHER_STOCK_URL.format(ticker=ticker)
        result = {"ticker": ticker, "records": 0, "errors": []}

        soup = self._get(url)
        if not soup:
            result["errors"].append("Failed to fetch stock page")
            return result

        try:
            # Extract price data from the page
            data = {"ticker": ticker, "date": datetime.now().strftime("%Y-%m-%d"), "source": self.SOURCE}

            # Try to find price elements
            # Mubasher uses various CSS classes for stock data
            price_selectors = [
                ".stockPageContent .price",
                ".stock-price",
                "[class*='price']",
                ".last-price",
            ]
            for sel in price_selectors:
                el = soup.select_one(sel)
                if el:
                    try:
                        price_text = el.get_text(strip=True).replace(",", "")
                        data["close_price"] = float(price_text)
                        break
                    except ValueError:
                        continue

            # Extract key stats table
            # Mubasher shows metrics like Market Cap, P/E, Volume, etc. in info tables
            stat_tables = soup.select("table")
            for table in stat_tables:
                rows = table.find_all("tr")
                for row in rows:
                    cells = row.find_all(["td", "th"])
                    if len(cells) >= 2:
                        label = cells[0].get_text(strip=True).lower()
                        value_text = cells[-1].get_text(strip=True).replace(",", "").replace("%", "")

                        try:
                            value = float(value_text)
                        except ValueError:
                            continue

                        if "market cap" in label or "mkt cap" in label:
                            data["market_cap"] = value
                        elif "p/e" in label or "pe ratio" in label:
                            data["pe_ratio"] = value
                        elif "p/b" in label:
                            data["pb_ratio"] = value
                        elif "volume" in label and "avg" not in label:
                            data["volume"] = int(value)
                        elif "52" in label and "high" in label:
                            data["fifty_two_week_high"] = value
                        elif "52" in label and "low" in label:
                            data["fifty_two_week_low"] = value
                        elif "dividend" in label and "yield" in label:
                            data["dividend_yield"] = value / 100 if value > 1 else value
                        elif "beta" in label:
                            data["beta"] = value
                        elif "shares" in label and ("outstanding" in label or "issued" in label):
                            data["shares_outstanding"] = value

            # Also look for info in div-based key stats
            key_stat_divs = soup.select("[class*='key-stat'], [class*='info-item'], [class*='stock-info']")
            for div in key_stat_divs:
                label_el = div.find(["span", "label", "dt"])
                value_el = div.find(["strong", "b", "dd"])
                if label_el and value_el:
                    label = label_el.get_text(strip=True).lower()
                    try:
                        value = float(value_el.get_text(strip=True).replace(",", "").replace("%", ""))
                        if "sector" in label:
                            pass  # text field
                        elif "open" in label:
                            data["open_price"] = value
                        elif "high" in label and "52" not in label:
                            data["high_price"] = value
                        elif "low" in label and "52" not in label:
                            data["low_price"] = value
                    except ValueError:
                        continue

            if data.get("close_price") or data.get("market_cap"):
                db.upsert_market_data(self.conn, data)
                result["records"] += 1
                logger.info(f"  [Mubasher] {ticker}: Market data ✓")

            # Try to extract company name and sector
            name_el = soup.select_one("h1, .company-name, [class*='stock-name']")
            sector_el = soup.find(string=lambda s: s and "sector" in s.lower() if s else False)

            company_name = name_el.get_text(strip=True) if name_el else ""
            if company_name:
                db.upsert_company(self.conn, ticker=ticker, name=company_name, source=self.SOURCE)
                result["records"] += 1

        except Exception as e:
            result["errors"].append(str(e))
            logger.warning(f"  [Mubasher] {ticker}: Parse error - {e}")

        return result

    def collect_financials(self, ticker: str) -> Dict:
        """
        Scrape financial statements page for income, balance sheet, cash flow.
        URL: english.mubasher.info/markets/EGX/stocks/{ticker}/financial-statements
        """
        url = MUBASHER_FINANCIALS_URL.format(ticker=ticker)
        result = {"ticker": ticker, "records": 0, "errors": []}

        soup = self._get(url)
        if not soup:
            result["errors"].append("Failed to fetch financials page")
            return result

        try:
            # Mubasher financial statements are in tabular format
            # Look for financial data tables
            tables = soup.select("table")

            for table in tables:
                headers = []
                header_row = table.find("thead")
                if header_row:
                    headers = [th.get_text(strip=True) for th in header_row.find_all(["th", "td"])]

                rows = table.find("tbody")
                if not rows:
                    continue

                for row in rows.find_all("tr"):
                    cells = row.find_all("td")
                    if len(cells) < 2:
                        continue

                    label = cells[0].get_text(strip=True).lower()
                    values = []
                    for cell in cells[1:]:
                        text = cell.get_text(strip=True).replace(",", "").replace("(", "-").replace(")", "")
                        try:
                            values.append(float(text))
                        except ValueError:
                            values.append(None)

                    # Map common financial line items
                    if not values:
                        continue

                    latest_value = values[0]  # Most recent period
                    if latest_value is None:
                        continue

                    # Determine which statement this belongs to and store accordingly
                    # This is a simplified mapping — real implementation would be more thorough
                    period_end = datetime.now().strftime("%Y-12-31")  # Approximate

                    if any(k in label for k in ["revenue", "sales", "turnover"]):
                        db.upsert_income_statement(self.conn, {
                            "ticker": ticker, "period": "annual", "period_end": period_end,
                            "revenue": latest_value, "source": self.SOURCE
                        })
                        result["records"] += 1
                    elif "net income" in label or "net profit" in label:
                        db.upsert_income_statement(self.conn, {
                            "ticker": ticker, "period": "annual", "period_end": period_end,
                            "net_income": latest_value, "source": self.SOURCE
                        })
                        result["records"] += 1
                    elif "total assets" in label:
                        db.upsert_balance_sheet(self.conn, {
                            "ticker": ticker, "period": "annual", "period_end": period_end,
                            "total_assets": latest_value, "source": self.SOURCE
                        })
                        result["records"] += 1
                    elif "total equity" in label or "shareholders" in label:
                        db.upsert_balance_sheet(self.conn, {
                            "ticker": ticker, "period": "annual", "period_end": period_end,
                            "total_equity": latest_value, "source": self.SOURCE
                        })
                        result["records"] += 1

            if result["records"] > 0:
                logger.info(f"  [Mubasher] {ticker}: Financials ✓ ({result['records']} items)")

        except Exception as e:
            result["errors"].append(str(e))
            logger.warning(f"  [Mubasher] {ticker}: Financial parse error - {e}")

        return result

    def collect_stock(self, ticker: str) -> Dict:
        """Collect all data for a single stock from Mubasher."""
        combined = {"ticker": ticker, "source": self.SOURCE, "records": 0, "errors": []}
        start = time.time()

        # Stock page (price + market data)
        r1 = self.collect_stock_page(ticker)
        combined["records"] += r1["records"]
        combined["errors"].extend(r1["errors"])

        time.sleep(random.uniform(REQUEST_DELAY_MIN, REQUEST_DELAY_MAX))

        # Financial statements
        r2 = self.collect_financials(ticker)
        combined["records"] += r2["records"]
        combined["errors"].extend(r2["errors"])

        duration = time.time() - start
        status = "success" if not combined["errors"] else ("partial" if combined["records"] > 0 else "failed")
        db.log_collection(self.conn, ticker, self.SOURCE, status,
                         combined["records"], "; ".join(combined["errors"]) if combined["errors"] else None, duration)

        return combined

    def collect_all(self, tickers: List[str] = None) -> List[Dict]:
        """Collect data for all tickers from Mubasher."""
        # First discover tickers if we don't have a list
        if not tickers:
            discovered = self.discover_tickers()
            for d in discovered:
                db.upsert_company(self.conn, ticker=d["ticker"], name=d["name"], source=self.SOURCE)
            tickers = [d["ticker"] for d in discovered] if discovered else []

        if not tickers:
            logger.warning("[Mubasher] No tickers to collect")
            return []

        results = []
        total = len(tickers)
        logger.info(f"[Mubasher] Starting collection for {total} tickers...")

        for i, ticker in enumerate(tickers, 1):
            logger.info(f"[Mubasher] ({i}/{total}) Processing {ticker}...")
            result = self.collect_stock(ticker)
            results.append(result)

            if i < total:
                time.sleep(random.uniform(REQUEST_DELAY_MIN, REQUEST_DELAY_MAX))

        success = sum(1 for r in results if not r["errors"])
        logger.info(f"[Mubasher] Complete: {success}/{total} successful")
        return results
