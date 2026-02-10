"""
EGX EVA Analyzer — EGX Official Website Data Collector
Scrapes the official Egyptian Exchange website (egx.com.eg) for:
  - Complete list of all listed stocks
  - Company codes, names, sectors
  - Market indicators
"""

import time
import random
import logging
from datetime import datetime
from typing import Dict, List, Optional

import requests
from bs4 import BeautifulSoup

from config import (
    EGX_LISTED_STOCKS_URL, USER_AGENTS,
    REQUEST_TIMEOUT, REQUEST_DELAY_MIN, REQUEST_DELAY_MAX
)
import database as db

logger = logging.getLogger(__name__)


class EGXOfficialCollector:
    """Scrapes the official EGX website for comprehensive stock listings."""

    SOURCE = "egx_official"

    def __init__(self, conn):
        self.conn = conn
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": random.choice(USER_AGENTS),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://www.egx.com.eg/en/homepage.aspx",
        })

    def discover_all_listed_stocks(self) -> List[Dict]:
        """
        Scrape the EGX Listed Stocks page to get ALL listed tickers.
        URL: https://www.egx.com.eg/en/ListedStocks.aspx

        The EGX website uses ASP.NET ViewState for pagination.
        This method handles the initial page load and attempts to get
        as many stocks as possible.
        """
        discovered = []
        logger.info(f"  [EGX] Fetching listed stocks from {EGX_LISTED_STOCKS_URL}")

        try:
            resp = self.session.get(EGX_LISTED_STOCKS_URL, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "lxml")

            # The EGX listed stocks page has a table with stock information
            # Look for the main data table
            tables = soup.find_all("table")

            for table in tables:
                rows = table.find_all("tr")
                for row in rows:
                    cells = row.find_all("td")
                    if len(cells) >= 3:
                        # Try to extract ticker and company name
                        # EGX table format: Reuters Code | ISIN | Company Name | ...
                        for i, cell in enumerate(cells):
                            text = cell.get_text(strip=True)
                            link = cell.find("a", href=True)

                            # Check if this looks like a ticker (2-6 uppercase letters)
                            if text and len(text) >= 2 and len(text) <= 8 and text.replace(".", "").isalpha():
                                # Get the next cells for name
                                name = ""
                                isin = ""
                                if i + 1 < len(cells):
                                    next_text = cells[i + 1].get_text(strip=True)
                                    if next_text.startswith("EGS") or next_text.startswith("EG"):
                                        isin = next_text
                                        if i + 2 < len(cells):
                                            name = cells[i + 2].get_text(strip=True)
                                    else:
                                        name = next_text

                                ticker = text.upper().replace(".", "")
                                if len(ticker) >= 2:
                                    discovered.append({
                                        "ticker": ticker,
                                        "name": name,
                                        "isin": isin,
                                    })
                                break

            # Also look for links that contain stock ticker patterns
            all_links = soup.find_all("a", href=True)
            for link in all_links:
                href = link.get("href", "")
                text = link.get_text(strip=True)

                # Look for patterns like /en/stocksdata/companylookup.aspx?ticker=COMI
                if "ticker=" in href:
                    ticker = href.split("ticker=")[-1].split("&")[0].upper()
                    if len(ticker) >= 2 and ticker.isalpha():
                        discovered.append({
                            "ticker": ticker,
                            "name": text,
                            "isin": "",
                        })

                # Look for patterns like /stocks/COMI
                if "/stocks/" in href:
                    parts = href.rstrip("/").split("/")
                    ticker = parts[-1].upper()
                    if len(ticker) >= 2 and len(ticker) <= 6 and ticker.isalpha():
                        discovered.append({
                            "ticker": ticker,
                            "name": text,
                            "isin": "",
                        })

        except requests.RequestException as e:
            logger.error(f"  [EGX] Failed to fetch listed stocks: {e}")

        # Deduplicate
        seen = set()
        unique = []
        for d in discovered:
            if d["ticker"] not in seen and len(d["ticker"]) >= 2:
                seen.add(d["ticker"])
                unique.append(d)

        logger.info(f"  [EGX] Discovered {len(unique)} stocks from official website")

        # Store all in database
        for stock in unique:
            db.upsert_company(
                self.conn,
                ticker=stock["ticker"],
                name=stock["name"],
                isin=stock.get("isin", ""),
                source=self.SOURCE,
            )

        return unique

    def scrape_market_indicators(self) -> Dict:
        """
        Scrape market-level indicators from EGX.
        These can be used for market-wide analysis.
        """
        indicators = {}
        try:
            url = "https://www.egx.com.eg/en/MarketIndicator.aspx"
            resp = self.session.get(url, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "lxml")

            # Extract market indicators from tables
            tables = soup.find_all("table")
            for table in tables:
                rows = table.find_all("tr")
                for row in rows:
                    cells = row.find_all("td")
                    if len(cells) >= 2:
                        label = cells[0].get_text(strip=True)
                        value_text = cells[-1].get_text(strip=True).replace(",", "")
                        try:
                            indicators[label] = float(value_text)
                        except ValueError:
                            indicators[label] = value_text

            logger.info(f"  [EGX] Market indicators: {len(indicators)} items")
        except Exception as e:
            logger.warning(f"  [EGX] Failed to scrape market indicators: {e}")

        return indicators

    def collect_all(self, tickers=None) -> List[Dict]:
        """Discover all stocks and collect basic data."""
        start = time.time()
        discovered = self.discover_all_listed_stocks()

        time.sleep(random.uniform(REQUEST_DELAY_MIN, REQUEST_DELAY_MAX))

        indicators = self.scrape_market_indicators()

        duration = time.time() - start
        db.log_collection(self.conn, "ALL", self.SOURCE, "success",
                         len(discovered), None, duration)

        return [{"ticker": d["ticker"], "source": self.SOURCE, "records": 1, "errors": []}
                for d in discovered]
