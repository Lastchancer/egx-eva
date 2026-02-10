"""
EGX EVA Analyzer — Configuration
Egypt-specific financial constants and application settings.
"""

import os
from dataclasses import dataclass, field
from typing import List, Optional

# ═══════════════════════════════════════════════════════════════
# EGYPT MACRO FINANCIAL CONSTANTS
# ═══════════════════════════════════════════════════════════════

RISK_FREE_RATE = 0.26          # Egyptian Treasury Bill rate (~26%)
EQUITY_RISK_PREMIUM = 0.08     # Egypt equity risk premium (~8%)
COST_OF_DEBT_PRETAX = 0.22     # Average corporate borrowing rate (~22%)
CORPORATE_TAX_RATE = 0.225     # Egyptian corporate tax rate (22.5%)
DEFAULT_BETA = 1.0             # Default beta if unavailable

# EVA Signal Thresholds
UNDERVALUED_THRESHOLD = 0.15   # >15% intrinsic premium = UNDERVALUED
OVERVALUED_THRESHOLD = -0.15   # <-15% intrinsic premium = OVERVALUED

# ═══════════════════════════════════════════════════════════════
# DATA SOURCE CONFIGURATION
# ═══════════════════════════════════════════════════════════════

YAHOO_FINANCE_SUFFIX = ".CA"   # EGX stocks use .CA suffix on Yahoo Finance

# Request settings
REQUEST_TIMEOUT = 30
REQUEST_DELAY_MIN = 1.0        # Min seconds between requests (be respectful)
REQUEST_DELAY_MAX = 3.0        # Max seconds between requests
MAX_RETRIES = 3

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
]

# URLs
MUBASHER_BASE = "https://english.mubasher.info"
MUBASHER_EGX_STOCKS = f"{MUBASHER_BASE}/markets/EGX"
MUBASHER_STOCK_URL = f"{MUBASHER_BASE}/markets/EGX/stocks/{{ticker}}"
MUBASHER_FINANCIALS_URL = f"{MUBASHER_BASE}/markets/EGX/stocks/{{ticker}}/financial-statements"

EGX_LISTED_STOCKS_URL = "https://www.egx.com.eg/en/ListedStocks.aspx"
EGX_STOCK_DATA_URL = "https://www.egx.com.eg/en/stocksdata/companylookup.aspx?Ession=&Ession2=&ticker={ticker}"

INVESTING_BASE = "https://www.investing.com"

STOCKANALYSIS_BASE = "https://stockanalysis.com/quote/egx"

# ═══════════════════════════════════════════════════════════════
# DATABASE
# ═══════════════════════════════════════════════════════════════

DB_PATH = os.path.join(os.path.dirname(__file__), "egx_data.db")
REPORTS_DIR = os.path.join(os.path.dirname(__file__), "reports")

# ═══════════════════════════════════════════════════════════════
# KNOWN EGX TICKERS (Major stocks — agent will also discover more)
# ═══════════════════════════════════════════════════════════════

# EGX30 + major EGX70/EGX100 components
KNOWN_EGX_TICKERS = [
    # ── EGX30 Core ──
    "COMI",   # Commercial International Bank (CIB)
    "HRHO",   # EFG Hermes Holding
    "TMGH",   # Talaat Moustafa Group
    "EAST",   # Eastern Company
    "SWDY",   # Elsewedy Electric
    "ETEL",   # Telecom Egypt
    "ABUK",   # Abu Qir Fertilizers
    "ESRS",   # Ezz Steel
    "PHDC",   # Palm Hills Developments
    "FWRY",   # Fawry
    "AMOC",   # Alexandria Mineral Oils
    "ORWE",   # Oriental Weavers
    "MNHD",   # Madinet Nasr Housing
    "EKHO",   # El Khair Holding
    "CLHO",   # Cleopatra Hospital Group
    "SKPC",   # Sidi Kerir Petrochemicals
    "OCDI",   # Orascom Development
    "ORAS",   # Orascom Construction
    "CCAP",   # Citadel Capital (Qalaa Holdings)
    "HELI",   # Heliopolis Housing
    "EFIH",   # EFG Finance Holding
    "ADIB",   # Abu Dhabi Islamic Bank Egypt
    "CIEB",   # Credit Agricole Egypt
    "ISPH",   # Egyptian International Pharmaceutical (EIPICO)
    "JUFO",   # Juhayna Food Industries
    "ACGC",   # Arabian Cement Group
    "AUTO",   # GB Auto
    "EGTS",   # Egyptian Tourism Resorts
    "AMER",   # Amer Group Holding
    "EMFD",   # Emaar Misr for Development

    # ── EGX70 / Broader Market ──
    "EDBM",   # Edita Food Industries
    "ALCN",   # Alcanza Pharma
    "DCRC",   # Delta Construction
    "CSAG",   # Canal Shipping Agencies
    "PHAR",   # Egyptian Pharma
    "ELEC",   # Electro Cable
    "IRON",   # Egyptian Iron & Steel
    "BINV",   # Beltone Financial
    "MCQE",   # Misr Cement Qena
    "MNHD",   # Madinet Nasr Housing
    "EGCH",   # Egypt Chem Industries
    "SUCE",   # Suez Cement
    "SUGR",   # Delta Sugar
    "UEGP",   # Upper Egypt General Contracting
    "RAYA",   # Raya Holding
    "MPRC",   # Misr for Production
    "POUL",   # Cairo Poultry Group
    "CIRA",   # Cairo for Investment & Real Estate
    "EFID",   # E-Finance for Digital & Financial Investments
    "SPMD",   # Speed Medical
    "NASR",   # Nasr City Housing & Development
]

# Remove duplicates and sort
KNOWN_EGX_TICKERS = sorted(list(set(KNOWN_EGX_TICKERS)))


@dataclass
class CollectionConfig:
    """Runtime configuration for a data collection run."""
    tickers: Optional[List[str]] = None          # None = all discovered tickers
    sources: List[str] = field(default_factory=lambda: ["yahoo", "mubasher", "egx", "stockanalysis"])
    max_workers: int = 4
    use_cache: bool = True
    cache_ttl_hours: int = 24
    export_format: str = "csv"                    # csv, json, xlsx
    verbose: bool = True
