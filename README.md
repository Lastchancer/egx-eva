# 🏦 EGX EVA Analyzer

## Automated Economic Value Added (EVA) Analysis for the Egyptian Stock Market

A complete tool that **automatically collects financial data** from multiple sources for all listed companies on the Egyptian Exchange (EGX), computes **EVA (Economic Value Added)** analysis, and highlights **undervalued stock opportunities**.

---

## 🏗 Architecture

```
┌──────────────────────────────────────────────────────────┐
│                    EGX EVA ANALYZER                       │
├──────────────┬───────────────┬───────────────────────────┤
│  DATA AGENT  │  EVA ENGINE   │      DASHBOARD            │
│              │               │                           │
│ ┌──────────┐ │ ┌───────────┐ │  ┌─────────────────────┐  │
│ │  Yahoo   │ │ │  NOPAT    │ │  │  React Dashboard    │  │
│ │ Finance  │─┤ │  Calc     │─┤  │  - KPI Cards        │  │
│ ├──────────┤ │ ├───────────┤ │  │  - EVA Charts       │  │
│ │ Mubasher │ │ │  WACC     │ │  │  - Stock Screener   │  │
│ │  Info    │─┤ │  (CAPM)   │─┤  │  - Detail View      │  │
│ ├──────────┤ │ ├───────────┤ │  │  - Sector Analysis  │  │
│ │ Investing│ │ │  EVA =    │ │  │  - WACC Sensitivity │  │
│ │  .com    │─┤ │NOPAT-WACC │ │  └─────────────────────┘  │
│ ├──────────┤ │ │ ×Inv.Cap  │ │                           │
│ │ EGX.com  │ │ ├───────────┤ │  ┌─────────────────────┐  │
│ │ .eg      │─┤ │ Valuation │─┤  │  Alerts             │  │
│ ├──────────┤ │ │  Signal   │ │  │  - Telegram Bot     │  │
│ │  CSV     │ │ │  Engine   │ │  │  - Email            │  │
│ │ Import   │ │ └───────────┘ │  └─────────────────────┘  │
│ └──────────┘ │               │                           │
├──────────────┴───────────────┴───────────────────────────┤
│  SQLite Database  │  JSON/CSV Export  │  Scheduler       │
└───────────────────┴───────────────────┴──────────────────┘
```

---

## 📦 Quick Start

### 1. Install Dependencies
```bash
cd egx_data_agent
pip install -r requirements.txt
```

### 2. Run Data Collection
```bash
# Collect data for all EGX stocks (using Yahoo Finance)
python collector.py

# Collect specific tickers only
python collector.py --tickers COMI SWDY TMGH EAST ETEL

# Enable web scraping from Mubasher
python collector.py --web-scrape

# Import data from CSV
python collector.py --csv my_data.csv

# See all available tickers
python collector.py --list-tickers
```

### 3. View Results
Results are exported to `output/`:
- `egx_eva_results.json` — Full data for the React dashboard
- `egx_eva_results.csv` — Spreadsheet-ready EVA analysis

---

## 📡 Data Sources

| Source | Data Available | Access Method | Cost |
|--------|---------------|---------------|------|
| **Yahoo Finance** | Price, market cap, beta, financials, ratios | yfinance library | Free |
| **Mubasher Info** | Income statement, balance sheet, cash flow | Web scraping | Free |
| **Investing.com** | Financial summary, ratios, fundamentals | Web scraping | Free |
| **EGX Official** | Listed stocks, market data, disclosures | Web scraping | Free |
| **StockAnalysis** | Ticker list, market caps, prices | Web scraping | Free |
| **CSV/Excel** | Any manual data | File import | Free |
| **EODHD API** | Full fundamentals + historical | REST API | Paid |

---

## 📊 EVA Methodology

### Formula
```
EVA = NOPAT − (WACC × Invested Capital)

NOPAT = EBIT × (1 − Tax Rate)
Invested Capital = Total Assets − Current Liabilities
WACC = (E/V × Ke) + (D/V × Kd × (1−T))
Ke = Rf + β × (Rm − Rf)
```

### Egypt Assumptions
| Parameter | Value |
|-----------|-------|
| Risk-Free Rate | 26% (T-bill) |
| Equity Risk Premium | 8% |
| Cost of Debt | 22% |
| Corporate Tax | 22.5% |

---

## ⏰ Scheduling & Alerts

```bash
python scheduler.py --schedule daily    # Run daily at 4 PM
python scheduler.py --schedule weekly   # Run every Sunday
```

Set `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID` env vars for alerts.

---

## ⚠ Disclaimer

Not financial advice. For research and educational purposes only.
