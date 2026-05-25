CREATE TABLE IF NOT EXISTS daily_summary (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    date DATE NOT NULL,
    open_price DECIMAL(18,8) NOT NULL,
    high_price DECIMAL(18,8) NOT NULL,
    low_price DECIMAL(18,8) NOT NULL,
    close_price DECIMAL(18,8) NOT NULL,
    avg_price DECIMAL(18,8) NOT NULL,
    record_count INT NOT NULL,
    sources TEXT[] NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    
    CONSTRAINT unique_symbol_date UNIQUE (symbol, date)
);

CREATE INDEX IF NOT EXISTS idx_daily_summary_date ON daily_summary(date DESC);
CREATE INDEX IF NOT EXISTS idx_daily_summary_symbol ON daily_summary(symbol);
