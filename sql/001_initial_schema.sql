-- EnergyOracle PPA Settlement Database Schema
-- Run this in Supabase SQL Editor: https://supabase.com/dashboard/project/YOUR_PROJECT/sql

-- ============================================
-- SYSTEM PRICES (Elexon BMRS)
-- Primary PPA settlement index
-- ============================================
CREATE TABLE IF NOT EXISTS system_prices (
    id BIGSERIAL PRIMARY KEY,
    settlement_date DATE NOT NULL,
    settlement_period INT NOT NULL CHECK (settlement_period BETWEEN 1 AND 50),
    system_sell_price DECIMAL(12,4) NOT NULL,  -- SSP in £/MWh
    system_buy_price DECIMAL(12,4) NOT NULL,   -- SBP in £/MWh
    price DECIMAL(12,4) NOT NULL,              -- Net/average price in £/MWh
    data_source TEXT DEFAULT 'elexon_bmrs',
    fetched_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(settlement_date, settlement_period)
);

COMMENT ON TABLE system_prices IS 'UK System Prices from Elexon BMRS - primary PPA settlement index';
COMMENT ON COLUMN system_prices.settlement_period IS 'Half-hourly period (1-48, or 46/50 on clock change days)';
COMMENT ON COLUMN system_prices.system_sell_price IS 'System Sell Price - price paid to generators';
COMMENT ON COLUMN system_prices.system_buy_price IS 'System Buy Price - price paid by suppliers';
COMMENT ON COLUMN system_prices.price IS 'Net price - typically average of SSP and SBP';

-- ============================================
-- DAY AHEAD PRICES (Market Index)
-- Forward pricing reference
-- ============================================
CREATE TABLE IF NOT EXISTS day_ahead_prices (
    id BIGSERIAL PRIMARY KEY,
    settlement_date DATE NOT NULL,
    settlement_period INT NOT NULL CHECK (settlement_period BETWEEN 1 AND 50),
    price DECIMAL(12,4) NOT NULL,              -- Day-ahead price in £/MWh
    data_provider TEXT DEFAULT 'APXMIDP',      -- APXMIDP or N2EXMIDP
    data_source TEXT DEFAULT 'elexon_bmrs',
    fetched_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(settlement_date, settlement_period, data_provider)
);

COMMENT ON TABLE day_ahead_prices IS 'Day-ahead market prices from power exchanges';
COMMENT ON COLUMN day_ahead_prices.data_provider IS 'Price source: APXMIDP (APX) or N2EXMIDP (N2EX)';

-- ============================================
-- CARBON INTENSITY (National Grid)
-- Green premium calculations
-- ============================================
CREATE TABLE IF NOT EXISTS carbon_intensity (
    id BIGSERIAL PRIMARY KEY,
    datetime TIMESTAMPTZ NOT NULL,
    intensity INT NOT NULL,                    -- gCO2/kWh
    intensity_index TEXT,                      -- very low, low, moderate, high, very high
    data_source TEXT DEFAULT 'national_grid',
    fetched_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(datetime)
);

COMMENT ON TABLE carbon_intensity IS 'UK grid carbon intensity from National Grid';
COMMENT ON COLUMN carbon_intensity.intensity IS 'Carbon intensity in gCO2/kWh';
COMMENT ON COLUMN carbon_intensity.intensity_index IS 'Qualitative index: very low, low, moderate, high, very high';

-- ============================================
-- FUEL MIX (National Grid)
-- Generation breakdown by fuel type
-- ============================================
CREATE TABLE IF NOT EXISTS fuel_mix (
    id BIGSERIAL PRIMARY KEY,
    datetime TIMESTAMPTZ NOT NULL,
    biomass DECIMAL(5,2),      -- percentage
    coal DECIMAL(5,2),
    gas DECIMAL(5,2),
    hydro DECIMAL(5,2),
    imports DECIMAL(5,2),
    nuclear DECIMAL(5,2),
    other DECIMAL(5,2),
    solar DECIMAL(5,2),
    wind DECIMAL(5,2),
    data_source TEXT DEFAULT 'national_grid',
    fetched_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(datetime)
);

COMMENT ON TABLE fuel_mix IS 'UK generation mix by fuel type';

-- ============================================
-- FETCH LOGS (Audit Trail)
-- Track all data fetches for debugging
-- ============================================
CREATE TABLE IF NOT EXISTS fetch_logs (
    id BIGSERIAL PRIMARY KEY,
    fetch_type TEXT NOT NULL,                  -- system_prices, day_ahead, carbon
    started_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ,
    records_fetched INT DEFAULT 0,
    records_inserted INT DEFAULT 0,
    records_updated INT DEFAULT 0,
    status TEXT DEFAULT 'running',             -- running, success, error
    error_message TEXT,
    metadata JSONB                             -- Additional context (date range, etc.)
);

COMMENT ON TABLE fetch_logs IS 'Audit trail for all data fetch operations';

-- ============================================
-- INDEXES
-- Optimize common PPA settlement queries
-- ============================================

-- System prices: query by date range (monthly settlement)
CREATE INDEX IF NOT EXISTS idx_system_prices_date ON system_prices(settlement_date);
CREATE INDEX IF NOT EXISTS idx_system_prices_date_period ON system_prices(settlement_date, settlement_period);

-- Day ahead prices: query by date range
CREATE INDEX IF NOT EXISTS idx_day_ahead_prices_date ON day_ahead_prices(settlement_date);
CREATE INDEX IF NOT EXISTS idx_day_ahead_prices_provider_date ON day_ahead_prices(data_provider, settlement_date);

-- Carbon intensity: query by datetime range
CREATE INDEX IF NOT EXISTS idx_carbon_intensity_datetime ON carbon_intensity(datetime);

-- Fuel mix: query by datetime
CREATE INDEX IF NOT EXISTS idx_fuel_mix_datetime ON fuel_mix(datetime);

-- Fetch logs: query recent fetches
CREATE INDEX IF NOT EXISTS idx_fetch_logs_type_started ON fetch_logs(fetch_type, started_at DESC);

-- ============================================
-- VIEWS
-- Convenient aggregations for PPA settlement
-- security_invoker = true ensures RLS is respected
-- ============================================

-- Daily average system price
CREATE OR REPLACE VIEW daily_system_price_avg
WITH (security_invoker = true) AS
SELECT
    settlement_date,
    COUNT(*) as num_periods,
    ROUND(AVG(price)::numeric, 2) as avg_price,
    MIN(price) as min_price,
    MAX(price) as max_price,
    ROUND(AVG(system_sell_price)::numeric, 2) as avg_ssp,
    ROUND(AVG(system_buy_price)::numeric, 2) as avg_sbp
FROM system_prices
GROUP BY settlement_date
ORDER BY settlement_date DESC;

-- Monthly average system price (primary PPA settlement)
CREATE OR REPLACE VIEW monthly_system_price_avg
WITH (security_invoker = true) AS
SELECT
    DATE_TRUNC('month', settlement_date)::date as month,
    COUNT(*) as num_periods,
    ROUND(AVG(price)::numeric, 2) as avg_price,
    MIN(price) as min_price,
    MAX(price) as max_price
FROM system_prices
GROUP BY DATE_TRUNC('month', settlement_date)
ORDER BY month DESC;

-- Daily average day-ahead price
CREATE OR REPLACE VIEW daily_day_ahead_avg
WITH (security_invoker = true) AS
SELECT
    settlement_date,
    data_provider,
    COUNT(*) as num_periods,
    ROUND(AVG(price)::numeric, 2) as avg_price,
    MIN(price) as min_price,
    MAX(price) as max_price
FROM day_ahead_prices
WHERE price > 0
GROUP BY settlement_date, data_provider
ORDER BY settlement_date DESC;

-- ============================================
-- ROW LEVEL SECURITY
-- Secures data access via Supabase REST API
-- ============================================

-- Enable RLS on all tables
ALTER TABLE system_prices ENABLE ROW LEVEL SECURITY;
ALTER TABLE day_ahead_prices ENABLE ROW LEVEL SECURITY;
ALTER TABLE carbon_intensity ENABLE ROW LEVEL SECURITY;
ALTER TABLE fuel_mix ENABLE ROW LEVEL SECURITY;
ALTER TABLE fetch_logs ENABLE ROW LEVEL SECURITY;

-- Public read access for price data (anon and authenticated can read)
CREATE POLICY "anon_read" ON system_prices FOR SELECT TO anon USING (true);
CREATE POLICY "anon_read" ON day_ahead_prices FOR SELECT TO anon USING (true);
CREATE POLICY "anon_read" ON carbon_intensity FOR SELECT TO anon USING (true);
CREATE POLICY "anon_read" ON fuel_mix FOR SELECT TO anon USING (true);

CREATE POLICY "authenticated_read" ON system_prices FOR SELECT TO authenticated USING (true);
CREATE POLICY "authenticated_read" ON day_ahead_prices FOR SELECT TO authenticated USING (true);
CREATE POLICY "authenticated_read" ON carbon_intensity FOR SELECT TO authenticated USING (true);
CREATE POLICY "authenticated_read" ON fuel_mix FOR SELECT TO authenticated USING (true);

-- Note: service_role bypasses RLS automatically, so no write policies needed
-- fetch_logs has no policies = only service_role can access (audit table)
