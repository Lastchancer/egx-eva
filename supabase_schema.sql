-- ═══════════════════════════════════════════════════════════════
-- EGX EVA ANALYZER — Supabase Database Schema
-- ═══════════════════════════════════════════════════════════════
-- Run this SQL in: Supabase Dashboard → SQL Editor → New Query
-- This creates all tables needed for the EVA analysis pipeline
-- and enables Row Level Security for public read access.
-- ═══════════════════════════════════════════════════════════════


-- ────────────────────────────────────────────────────
-- 1. COMPANIES — Master list of EGX-listed stocks
-- ────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS companies (
    ticker TEXT PRIMARY KEY,
    name TEXT,
    sector TEXT,
    industry TEXT,
    isin TEXT,
    source TEXT,
    updated_at TIMESTAMPTZ DEFAULT now()
);

-- ────────────────────────────────────────────────────
-- 2. MARKET DATA — Prices, market cap, ratios
-- ────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS market_data (
    id BIGINT GENERATED ALWAYS AS IDENTITY,
    ticker TEXT REFERENCES companies(ticker) ON DELETE CASCADE,
    date DATE NOT NULL DEFAULT CURRENT_DATE,
    close_price NUMERIC,
    open_price NUMERIC,
    high_price NUMERIC,
    low_price NUMERIC,
    volume BIGINT,
    market_cap NUMERIC,
    shares_outstanding NUMERIC,
    pe_ratio NUMERIC,
    pb_ratio NUMERIC,
    dividend_yield NUMERIC,
    beta NUMERIC,
    fifty_two_week_high NUMERIC,
    fifty_two_week_low NUMERIC,
    source TEXT,
    updated_at TIMESTAMPTZ DEFAULT now(),
    UNIQUE(ticker, date, source)
);

-- ────────────────────────────────────────────────────
-- 3. INCOME STATEMENTS
-- ────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS income_statements (
    id BIGINT GENERATED ALWAYS AS IDENTITY,
    ticker TEXT REFERENCES companies(ticker) ON DELETE CASCADE,
    period TEXT DEFAULT 'annual',      -- 'annual' or 'quarterly'
    period_end DATE,
    revenue NUMERIC,
    cost_of_revenue NUMERIC,
    gross_profit NUMERIC,
    ebit NUMERIC,
    ebitda NUMERIC,
    interest_expense NUMERIC,
    tax_expense NUMERIC,
    net_income NUMERIC,
    eps NUMERIC,
    source TEXT,
    updated_at TIMESTAMPTZ DEFAULT now(),
    UNIQUE(ticker, period, period_end, source)
);

-- ────────────────────────────────────────────────────
-- 4. BALANCE SHEETS
-- ────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS balance_sheets (
    id BIGINT GENERATED ALWAYS AS IDENTITY,
    ticker TEXT REFERENCES companies(ticker) ON DELETE CASCADE,
    period TEXT DEFAULT 'annual',
    period_end DATE,
    total_assets NUMERIC,
    current_assets NUMERIC,
    current_liabilities NUMERIC,
    total_liabilities NUMERIC,
    total_debt NUMERIC,
    long_term_debt NUMERIC,
    total_equity NUMERIC,
    cash_and_equivalents NUMERIC,
    source TEXT,
    updated_at TIMESTAMPTZ DEFAULT now(),
    UNIQUE(ticker, period, period_end, source)
);

-- ────────────────────────────────────────────────────
-- 5. EVA RESULTS — ⭐ Main table the frontend reads
-- ────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS eva_results (
    id BIGINT GENERATED ALWAYS AS IDENTITY,
    ticker TEXT REFERENCES companies(ticker) ON DELETE CASCADE,
    calculation_date DATE NOT NULL DEFAULT CURRENT_DATE,
    nopat NUMERIC,
    invested_capital NUMERIC,
    wacc NUMERIC,
    cost_of_equity NUMERIC,
    cost_of_debt_after_tax NUMERIC,
    equity_weight NUMERIC,
    debt_weight NUMERIC,
    eva NUMERIC,
    eva_spread NUMERIC,
    roic NUMERIC,
    capital_charge NUMERIC,
    eva_per_share NUMERIC,
    intrinsic_value NUMERIC,
    market_cap NUMERIC,
    intrinsic_premium NUMERIC,
    signal TEXT,                        -- 'UNDERVALUED', 'FAIR VALUE', 'OVERVALUED'
    data_quality_score NUMERIC,
    UNIQUE(ticker, calculation_date)
);

-- ────────────────────────────────────────────────────
-- 6. PIPELINE RUNS — Logging for monitoring
-- ────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS pipeline_runs (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_date TIMESTAMPTZ DEFAULT now(),
    stocks_analyzed INT,
    undervalued_count INT,
    positive_eva_count INT,
    total_records_collected INT,
    sources_used JSONB,
    duration_seconds NUMERIC,
    status TEXT DEFAULT 'success',
    error_message TEXT
);


-- ═══════════════════════════════════════════════════════════════
-- INDEXES for fast frontend queries
-- ═══════════════════════════════════════════════════════════════

CREATE INDEX IF NOT EXISTS idx_eva_signal ON eva_results(signal);
CREATE INDEX IF NOT EXISTS idx_eva_date ON eva_results(calculation_date DESC);
CREATE INDEX IF NOT EXISTS idx_eva_ticker ON eva_results(ticker);
CREATE INDEX IF NOT EXISTS idx_eva_eva ON eva_results(eva DESC);
CREATE INDEX IF NOT EXISTS idx_market_ticker_date ON market_data(ticker, date DESC);
CREATE INDEX IF NOT EXISTS idx_companies_sector ON companies(sector);


-- ═══════════════════════════════════════════════════════════════
-- ROW LEVEL SECURITY (RLS)
-- Enable public READ access for the frontend (anon key)
-- Only service_role key can WRITE (Python backend)
-- ═══════════════════════════════════════════════════════════════

-- Enable RLS on all tables
ALTER TABLE companies ENABLE ROW LEVEL SECURITY;
ALTER TABLE market_data ENABLE ROW LEVEL SECURITY;
ALTER TABLE income_statements ENABLE ROW LEVEL SECURITY;
ALTER TABLE balance_sheets ENABLE ROW LEVEL SECURITY;
ALTER TABLE eva_results ENABLE ROW LEVEL SECURITY;
ALTER TABLE pipeline_runs ENABLE ROW LEVEL SECURITY;

-- Public READ policies (anyone with the anon key can read)
CREATE POLICY "Public read companies" ON companies FOR SELECT USING (true);
CREATE POLICY "Public read market_data" ON market_data FOR SELECT USING (true);
CREATE POLICY "Public read income_statements" ON income_statements FOR SELECT USING (true);
CREATE POLICY "Public read balance_sheets" ON balance_sheets FOR SELECT USING (true);
CREATE POLICY "Public read eva_results" ON eva_results FOR SELECT USING (true);
CREATE POLICY "Public read pipeline_runs" ON pipeline_runs FOR SELECT USING (true);

-- Service role can do everything (Python backend uses service_role key)
-- Note: service_role bypasses RLS by default, so no explicit policy needed.


-- ═══════════════════════════════════════════════════════════════
-- VIEWS for convenient frontend queries
-- ═══════════════════════════════════════════════════════════════

-- Latest EVA results with company info (the main view for the dashboard)
CREATE OR REPLACE VIEW eva_dashboard AS
SELECT
    e.ticker,
    c.name,
    c.sector,
    e.signal,
    e.eva,
    e.eva_spread,
    e.roic,
    e.wacc,
    e.nopat,
    e.invested_capital,
    e.capital_charge,
    e.eva_per_share,
    e.intrinsic_value,
    e.market_cap,
    e.intrinsic_premium,
    e.cost_of_equity,
    e.cost_of_debt_after_tax,
    e.equity_weight,
    e.debt_weight,
    e.data_quality_score,
    e.calculation_date,
    m.close_price,
    m.pe_ratio,
    m.pb_ratio,
    m.dividend_yield,
    m.beta,
    m.volume,
    m.fifty_two_week_high,
    m.fifty_two_week_low
FROM eva_results e
LEFT JOIN companies c ON e.ticker = c.ticker
LEFT JOIN LATERAL (
    SELECT * FROM market_data md
    WHERE md.ticker = e.ticker
    ORDER BY md.date DESC
    LIMIT 1
) m ON true
WHERE e.calculation_date = (
    SELECT MAX(calculation_date) FROM eva_results
)
ORDER BY e.eva DESC;

-- Undervalued stocks only
CREATE OR REPLACE VIEW undervalued_stocks AS
SELECT * FROM eva_dashboard
WHERE signal = 'UNDERVALUED'
ORDER BY intrinsic_premium DESC;

-- Sector summary
CREATE OR REPLACE VIEW sector_summary AS
SELECT
    c.sector,
    COUNT(*) as stock_count,
    SUM(e.eva) as total_eva,
    AVG(e.roic) as avg_roic,
    AVG(e.wacc) as avg_wacc,
    AVG(e.eva_spread) as avg_spread,
    COUNT(*) FILTER (WHERE e.signal = 'UNDERVALUED') as undervalued_count,
    COUNT(*) FILTER (WHERE e.eva > 0) as positive_eva_count
FROM eva_results e
JOIN companies c ON e.ticker = c.ticker
WHERE e.calculation_date = (SELECT MAX(calculation_date) FROM eva_results)
GROUP BY c.sector
ORDER BY total_eva DESC;

-- Latest pipeline run info
CREATE OR REPLACE VIEW latest_run AS
SELECT * FROM pipeline_runs
ORDER BY run_date DESC
LIMIT 1;
