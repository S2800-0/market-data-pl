-- Track which API each price came from
ALTER TABLE crypto_prices ADD COLUMN IF NOT EXISTS source VARCHAR(20) DEFAULT 'binance';

-- New table for CoinGecko prices (different schema, different IDs)
CREATE TABLE IF NOT EXISTS crypto_prices_coingecko (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    price DECIMAL(18, 8) NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    source VARCHAR(20) DEFAULT 'coingecko',
    
    CONSTRAINT unique_cg_symbol_time UNIQUE (symbol, timestamp)
);

CREATE INDEX IF NOT EXISTS idx_cg_symbol_timestamp 
ON crypto_prices_coingecko(symbol, timestamp DESC);

-- Unified view for analytics (joins both sources)
CREATE OR REPLACE VIEW unified_prices AS
SELECT symbol, price, timestamp, created_at, source 
FROM crypto_prices
UNION ALL
SELECT symbol, price, timestamp, created_at, source 
FROM crypto_prices_coingecko;
