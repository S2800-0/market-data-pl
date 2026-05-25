-- Table to track rejected/bad data
CREATE TABLE IF NOT EXISTS data_quality_rejects (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    price DECIMAL(18, 8) NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    reject_reason VARCHAR(100) NOT NULL,
    expected_range_min DECIMAL(18, 8),
    expected_range_max DECIMAL(18, 8),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_rejects_symbol ON data_quality_rejects(symbol, created_at DESC);
