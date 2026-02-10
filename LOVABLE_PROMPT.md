# Lovable Prompt — EGX EVA Analyzer Dashboard

Paste this prompt into Lovable (lovable.dev) AFTER you've connected your Supabase project.

---

## PROMPT TO PASTE INTO LOVABLE:

```
Build an EGX EVA Analyzer dashboard — a financial analysis tool for the Egyptian Stock Exchange that displays Economic Value Added (EVA) analysis results from a Supabase database.

## Supabase Tables to Connect:
The data comes from these Supabase views and tables:
- `eva_dashboard` view — main data (ticker, name, sector, signal, eva, roic, wacc, eva_spread, nopat, invested_capital, intrinsic_premium, market_cap, close_price, data_quality_score, calculation_date)
- `undervalued_stocks` view — filtered undervalued picks
- `sector_summary` view — sector-level aggregated EVA data
- `latest_run` view — when the pipeline last ran
- `companies` table — company master list
- `market_data` table — price history

## Pages & Features:

### 1. Dashboard (Home)
- Top KPI cards showing: Total stocks analyzed, Undervalued count, Average WACC, Total Market EVA
- Last updated timestamp from `latest_run`
- Bar chart: EVA by company (top 15, sorted descending, green for positive, red for negative)
- Scatter plot: ROIC vs EVA Spread (colored by signal: green=undervalued, yellow=fair, red=overvalued)
- Sector breakdown horizontal bar chart from `sector_summary`
- "Top Undervalued Picks" card grid at bottom showing top opportunities

### 2. Screener Page
- Full sortable/filterable data table with ALL stocks from `eva_dashboard`
- Search by ticker or company name
- Filter dropdown by sector
- Filter dropdown by signal (UNDERVALUED / FAIR VALUE / OVERVALUED)
- Columns: Ticker, Name, Sector, Price, NOPAT, Invested Capital, WACC, ROIC, EVA Spread, EVA, Intrinsic Premium, Signal, Data Quality
- Click any row to expand into detail view

### 3. Stock Detail Page (when clicking a stock)
- Company header with ticker, name, sector, current price, signal badge
- WACC sensitivity slider (+/- 5%) that recalculates EVA in real-time on the client
- 12 metric cards: NOPAT, Invested Capital, WACC, Capital Charge, EVA, EVA Spread, ROIC, EVA/Share, Intrinsic Premium, Beta, Market Cap, P/E Ratio
- EVA Decomposition waterfall bar chart (Revenue → EBIT → NOPAT → Capital Charge → EVA)

### 4. Methodology Page
- Explain EVA formula: EVA = NOPAT - (WACC × Invested Capital)
- Show Egypt-specific assumptions (Risk-free rate 26%, Equity premium 8%, Debt cost 22%, Tax 22.5%)
- Signal logic explanation
- Data sources description

## Design Requirements:
- Dark theme (background #050505, cards #0f0f0f, borders #1f1f1f)
- Accent color: emerald green (#34d399) for positive/undervalued
- Red (#f87171) for negative/overvalued
- Yellow (#fbbf24) for fair value
- Purple (#c084fc) for WACC-related metrics
- Use monospace font for numbers and data
- Professional financial terminal aesthetic
- Mobile responsive
- Use Recharts for all charts
- Use shadcn/ui components

## Signal Badge Component:
- UNDERVALUED: green background (#0a3622), green text (#34d399), green border
- OVERVALUED: red background (#3b0f0f), red text (#f87171)
- FAIR VALUE: dark background, yellow text (#fbbf24)

## Data Refresh:
- Show "Last updated" from the latest_run view
- Add a manual refresh button that re-fetches from Supabase

## Important:
- All data is READ-ONLY from Supabase (the Python backend writes)
- Use Supabase anon key for frontend reads
- No authentication needed — this is a public dashboard
- Format large numbers as K/M/B (e.g., EGP 2.5B)
- Format percentages to 1 decimal (e.g., 28.3%)
- Show EGP currency prefix for monetary values
```

---

## FOLLOW-UP PROMPTS (paste these one at a time after the initial build):

### Add real-time updates:
```
Add Supabase realtime subscription to the eva_results table so the dashboard updates live when new data is pushed by the Python backend. Show a subtle "New data available" toast notification.
```

### Add historical tracking:
```
Add a "History" tab that shows EVA trends over time for a selected stock. Query eva_results filtered by ticker, ordered by calculation_date. Show a line chart with EVA, ROIC, and WACC over time.
```

### Add export functionality:
```
Add an "Export CSV" button on the screener page that downloads all visible rows as a CSV file. Also add "Export JSON" option.
```

### Add comparison mode:
```
Add a "Compare" feature where users can select 2-4 stocks and see their EVA metrics side by side in a comparison table and overlaid charts.
```
