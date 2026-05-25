-- Connect to your database: psql market_data -f schema.sql

-- Main table: every price we fetch
CREATE TABLE IF NOT EXISTS crypto_prices (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    price DECIMAL(18, 8) NOT NULL,  -- 18 digits total, 8 after decimal (crypto precision)
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    -- Prevent duplicate prices for same symbol at same second
    CONSTRAINT unique_symbol_time UNIQUE (symbol, timestamp)
);

-- Index for fast lookups: "Show me BTC prices, newest first"
CREATE INDEX IF NOT EXISTS idx_symbol_timestamp 
ON crypto_prices(symbol, timestamp DESC);

-- Index for cleanup jobs
CREATE INDEX IF NOT EXISTS idx_created_at 
ON crypto_prices(created_at);

-- ETL audit log: track every pipeline run
CREATE TABLE IF NOT EXISTS etl_logs (
    id SERIAL PRIMARY KEY,
    run_start TIMESTAMPTZ NOT NULL,
    run_end TIMESTAMPTZ,
    records_processed INT DEFAULT 0,
    records_inserted INT DEFAULT 0,
    status VARCHAR(20) NOT NULL DEFAULT 'RUNNING',
    error_message TEXT,
    api_latency_ms INT
);

-- Index for checking recent runs
CREATE INDEX IF NOT EXISTS idx_run_start ON etl_logs(run_start DESC);
