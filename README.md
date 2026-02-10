# EGX EVA Data Collection Agent

## Overview
An automated data collection agent that gathers financial data for all Egyptian Stock Exchange (EGX) listed companies from multiple public sources, then computes Economic Value Added (EVA) analysis to identify undervalued stocks.

## Architecture
```
┌─────────────────────────────────────────────────────┐
│                  EGX EVA ANALYZER                    │
├──────────┬──────────┬──────────┬───────────────────┤
│  Source1 │  Source2 │  Source3 │     Source4        │
│  Yahoo   │  Mubasher│  EGX.com │  Investing.com    │
│  Finance │  Info    │  .eg     │                   │
├──────────┴──────────┴──────────┴───────────────────┤
│              Data Merger & Validator                 │
├────────────────────────────────────────────────────┤
│              EVA Calculation Engine                  │
├────────────────────────────────────────────────────┤
│            SQLite Database (egx_data.db)            │
├────────────────────────────────────────────────────┤
│       Reports: CSV + JSON + Console Summary         │
└────────────────────────────────────────────────────┘
```

## Data Sources
1. **Yahoo Finance** (via `yfinance`) — Ticker suffix `.CA` for EGX stocks. Provides: income statements, balance sheets, cash flow, market data.
2. **Mubasher Info** — Scrapes stock listings, prices, and key financial ratios from english.mubasher.info
3. **EGX Official Website** — Scrapes listed stock tickers and company info from egx.com.eg
4. **Investing.com** — Supplementary financial data and ratios
5. **StockAnalysis.com** — Additional coverage for EGX stocks

## Setup
```bash
pip install -r requirements.txt
```

## Usage
```bash
# Full pipeline: collect data → compute EVA → generate report
python main.py

# Collect data only
python main.py --collect-only

# Run EVA on existing data
python main.py --eva-only

# Target specific stocks
python main.py --tickers COMI TMGH SWDY EAST

# Export results
python main.py --export csv    # or json, xlsx
```

## Output
- `egx_data.db` — SQLite database with all collected data
- `reports/eva_report_YYYYMMDD.csv` — Full EVA analysis
- `reports/undervalued_picks.csv` — Filtered undervalued opportunities
- `reports/summary.json` — Machine-readable summary

## EVA Formula
```
EVA = NOPAT - (WACC × Invested Capital)

Where:
  NOPAT = EBIT × (1 - Tax Rate)
  Invested Capital = Total Assets - Current Liabilities
  WACC = (E/V × Ke) + (D/V × Kd × (1-T))
  Ke = Rf + β × (Rm - Rf)   ← CAPM

Egypt Defaults:
  Risk-Free Rate = 26% (T-bill rate)
  Market Premium = 8%
  Cost of Debt = 22%
  Tax Rate = 22.5%
```
