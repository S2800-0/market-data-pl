# Real-Time Crypto Market Data ETL Pipeline

A production-ready ETL pipeline that fetches live cryptocurrency prices from multiple sources (Binance + CoinGecko), applies data quality checks with anomaly detection, and loads them into PostgreSQL for analytics. Built as a portfolio project for Data Engineering roles in fintech.

**Live Demo:** [Add your GitHub URL here]

---

## Architecture

```
┌─────────────┐     ┌──────────────┐     ┌─────────────┐     ┌─────────────┐
│  Binance    │────▶│   Python     │────▶│  PostgreSQL │────▶│    Cron     │
│   API       │     │  ETL Script  │     │  Database   │     │  Scheduler  │
└─────────────┘     └──────────────┘     └─────────────┘     └─────────────┘
       │                     │
       │              ┌──────┴──────┐
       │              │ Data Quality │
       │              │   Checks     │
       │              │  (Anomaly    │
       │              │  Detection)  │
       │              └─────────────┘
       │
┌─────────────┐
│ CoinGecko   │
│   API       │
└─────────────┘
```

**Pipeline Flow:**
1. **Extract**: Fetches 5 crypto symbols (BTC, ETH, SOL, XRP, DOGE) from Binance and CoinGecko public APIs
2. **Transform**: Converts string prices to `Decimal` for exact precision; validates against volatility-based anomaly thresholds
3. **Load**: Inserts into PostgreSQL with conflict handling; logs every run to `etl_logs`
4. **Aggregate**: Daily summary table with OHLC (Open/High/Low/Close) for warehouse analytics
5. **Export**: CSV generation for Power BI / Excel dashboards
6. **Schedule**: Cron runs every minute automatically

---

## Tech Stack

| Layer | Technology | Why |
|-------|-----------|-----|
| Data Sources | Binance REST API + CoinGecko API | Free, no API key, cross-validation |
| ETL | Python 3.13 | `requests`, `psycopg2`, `Decimal` for precision |
| Database | PostgreSQL 15 | ACID compliance, time-series indexing, CTEs |
| Scheduler | Cron | Native, reliable, zero dependencies |
| Data Quality | Custom validators | Per-asset volatility thresholds |
| Analytics | SQL + Python | Complex joins, window functions, aggregations |
| Dashboard | Power BI | CSV export for executive reporting |

---

## Schema Design

### `crypto_prices` (Primary Source: Binance)
| Column | Type | Notes |
|--------|------|-------|
| `id` | `SERIAL PRIMARY KEY` | Surrogate key |
| `symbol` | `VARCHAR(20)` | e.g., `BTCUSDT` |
| `price` | `DECIMAL(18,8)` | Exact precision — no float rounding |
| `timestamp` | `TIMESTAMPTZ` | Price observation time |
| `created_at` | `TIMESTAMPTZ` | Insertion time |
| `source` | `VARCHAR(20)` | `'binance'` |

**Indexes:** `(symbol, timestamp DESC)` for time-series queries; `created_at` for cleanup jobs.

### `crypto_prices_coingecko` (Secondary Source)
Same schema as above, `source = 'coingecko'`. Enables multi-source validation.

### `etl_logs`
Tracks every pipeline run: start/end time, records processed, status, API latency, errors.

### `data_quality_rejects`
Audit trail for rejected records: symbol, price, reason, expected range.

### `daily_summary` (Data Warehouse Table)
| Column | Type | Notes |
|--------|------|-------|
| `symbol` | `VARCHAR(20)` | Asset |
| `date` | `DATE` | Trading day |
| `open_price` | `DECIMAL(18,8)` | First price of day |
| `high_price` | `DECIMAL(18,8)` | Maximum price |
| `low_price` | `DECIMAL(18,8)` | Minimum price |
| `close_price` | `DECIMAL(18,8)` | Last price of day |
| `avg_price` | `DECIMAL(18,8)` | Volume-weighted average |
| `record_count` | `INT` | Number of observations |
| `sources` | `TEXT[]` | Which APIs contributed |

**Indexes:** `(symbol, date)` unique constraint; `(date DESC)` for range queries.

### `unified_prices` (View)
```sql
CREATE OR REPLACE VIEW unified_prices AS
SELECT symbol, price, timestamp, created_at, source FROM crypto_prices
UNION ALL
SELECT symbol, price, timestamp, created_at, source FROM crypto_prices_coingecko;
```
Enables cross-source analytics with a single query.

---

## Data Quality Checks

| Check | Implementation | Threshold |
|-------|---------------|-----------|
| Positive price | `price > 0` | Hard reject |
| Price spike | vs. last known price | BTC/ETH: 15%, SOL/XRP: 20%, DOGE: 25% |
| Duplicate detection | `ON CONFLICT` + `UNIQUE` constraint | Same symbol + timestamp |
| Cross-source validation | Compare Binance vs CoinGecko | Flag >1% divergence |
| API timeout | `requests.get(timeout=15)` | 15 seconds |

---

## Step-by-Step Build Process

### Phase 1: API Exploration & Understanding

**Test the Binance API:**
```bash
mkdir ~/market-data-pipeline
cd ~/market-data-pipeline
```

Create `test_one_price.py`:
```python
import requests

url = "https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT"
response = requests.get(url)
data = response.json()

print("What the API gave us:")
print(data)
print("
The symbol is:", data['symbol'])
print("The price is:", data['price'])
print("Price as a number:", float(data['price']))
```

**Output:**
```
{'symbol': 'BTCUSDT', 'price': '77089.98000000'}
The symbol is: BTCUSDT
The price is: 77089.98000000
Price as a number: 77089.98
```

**Understanding symbols:** `BTCUSDT` means "Bitcoin priced in USDT" (Tether, a stablecoin pegged to $1). So 1 BTC = ~$77,089.98.

---

### Phase 2: PostgreSQL Setup & First Insert

**Install PostgreSQL on macOS:**
```bash
# Install Homebrew (if not already)
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

# Install PostgreSQL
brew install postgresql@15
brew services start postgresql@15

# Create user and database
createuser -s $(whoami)
createdb market_data

# Verify
psql market_data
# You should see: market_data=#
# Type \q to exit
```

**Create the schema file** using nano:
```bash
cd ~/market-data-pipeline
nano schema.sql
```

In nano:
- Paste the SQL content
- Press `Ctrl + O` (letter O), then `Enter` to save
- Press `Ctrl + X` to exit

**Schema content:**
```sql
CREATE TABLE IF NOT EXISTS crypto_prices (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    price DECIMAL(18, 8) NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT unique_symbol_time UNIQUE (symbol, timestamp)
);

CREATE INDEX IF NOT EXISTS idx_symbol_timestamp 
ON crypto_prices(symbol, timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_created_at 
ON crypto_prices(created_at);

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

CREATE INDEX IF NOT EXISTS idx_run_start ON etl_logs(run_start DESC);
```

**Run the schema:**
```bash
psql market_data -f schema.sql
```

**Verify in psql:**
```bash
psql market_data
```
```sql
\dt
-- Output:
--              List of tables
--  Schema |     Name      | Type  | Owner 
-- --------+---------------+-------+-------
--  public | crypto_prices | table | shahy
--  public | etl_logs      | table | shahy

SELECT * FROM crypto_prices;
-- Shows your first inserted row

\q
```

---

### Phase 3: Multi-Coin Pipeline with Logging

**Install Python dependencies:**
```bash
pip install psycopg2-binary requests
```

**Key improvements from v1:**
- Multiple symbols (BTC, ETH, SOL, XRP, DOGE)
- `Decimal` instead of `float` for financial precision
- ETL logging with `etl_logs` table
- API failure handling with graceful degradation
- Batch insert with duplicate detection

---

### Phase 4: Data Quality Checks & Cron Scheduling

**Add rejects table:**
```bash
nano schema_v2.sql
# Paste content, Ctrl+O, Enter, Ctrl+X
psql market_data -f schema_v2.sql
```

**Set up Cron (using vim):**
```bash
crontab -e
```

Vim opens. Press `i` to enter insert mode. Add:
```
* * * * * cd ~/market-data-pipeline && /Library/Frameworks/Python.framework/Versions/3.13/bin/python3 pipeline_v2.py >> ~/market-data-pipeline/pipeline.log 2>&1
```

Press `Esc`, then type `:wq` and `Enter` to save.

**Verify cron:**
```bash
crontab -l
```

**Check logs:**
```bash
tail -f ~/market-data-pipeline/pipeline.log
```

---

### Phase 5: Multi-Source & Analytics

**Add CoinGecko as secondary source:**
```bash
psql market_data -f schema_v3.sql
```

**Add daily summary warehouse table:**
```bash
psql market_data -f schema_v4.sql
```

**Seed CoinGecko data:**
```bash
python3 scripts/seed_coingecko.py
```

**Run analytics:**
```bash
python3 analytics.py
```

**Sample output:**
```
=== MULTI-SOURCE PRICE COMPARISON ===
BTCUSDT: Binance=$77,102.68 | CoinGecko=$77,089.98 | Diff=0.0165%
ETHUSDT: Binance=$2,104.39 | CoinGecko=$2,103.50 | Diff=0.0423%
...

=== 24-HOUR VOLATILITY BY HOUR ===
BTCUSDT @ 2026-05-24 23:00:00+03:00: Avg=$77,101.84 | Vol=0.0806%
DOGEUSDT @ 2026-05-25 01:00:00+03:00: Avg=$0.10 | Vol=0.1904%

✅ Daily summary updated for 5 symbols
📁 Exported 5 rows to market_data_export_20260525.csv
```

---

## How to Run

```bash
# 1. Install PostgreSQL and create database
brew install postgresql@15
brew services start postgresql@15
createdb market_data

# 2. Run all schemas
psql market_data -f schema.sql
psql market_data -f schema_v2.sql
psql market_data -f schema_v3.sql
psql market_data -f schema_v4.sql

# 3. Install Python dependencies
pip install psycopg2-binary requests

# 4. Seed secondary source
python3 scripts/seed_coingecko.py

# 5. Run pipeline manually
python3 pipeline_v2.py

# 6. Run analytics
python3 analytics.py

# 7. Schedule with cron
crontab -e
# Add: * * * * * cd ~/market-data-pipeline && /Library/Frameworks/Python.framework/Versions/3.13/bin/python3 pipeline_v2.py >> ~/market-data-pipeline/pipeline.log 2>&1
```

---

## Sample Queries

```sql
-- Latest price for each symbol (multi-source)
SELECT DISTINCT ON (symbol) symbol, price, timestamp, source
FROM unified_prices
ORDER BY symbol, timestamp DESC;

-- Pipeline health: success rate last 24 hours
SELECT status, COUNT(*) 
FROM etl_logs 
WHERE run_start > NOW() - INTERVAL '24 hours'
GROUP BY status;

-- Daily OHLC summary
SELECT symbol, date, open_price, high_price, low_price, close_price, avg_price
FROM daily_summary
WHERE date >= CURRENT_DATE - 7
ORDER BY date DESC, symbol;

-- Cross-source price divergence
SELECT 
    b.symbol,
    b.price as binance_price,
    c.price as coingecko_price,
    ABS(b.price - c.price) as diff,
    ROUND((ABS(b.price - c.price) / b.price * 100), 4) as diff_pct
FROM (SELECT DISTINCT ON (symbol) * FROM crypto_prices ORDER BY symbol, timestamp DESC) b
LEFT JOIN (SELECT DISTINCT ON (symbol) * FROM crypto_prices_coingecko ORDER BY symbol, timestamp DESC) c
ON b.symbol = c.symbol;

-- Anomalies detected today
SELECT symbol, price, reject_reason, created_at
FROM data_quality_rejects
WHERE created_at > CURRENT_DATE;
```

---

## Project Structure

```
market-data-pipeline/
├── README.md                    # This file
├── schema.sql                   # Initial tables (prices, logs)
├── schema_v2.sql               # Data quality rejects table
├── schema_v3.sql               # Multi-source support + unified view
├── schema_v4.sql               # Daily summary warehouse table
├── pipeline_v2.py              # Main ETL script
├── fetchers.py                 # API abstraction (Binance + CoinGecko)
├── analytics.py                # Analytics engine + CSV export
├── scripts/
│   └── seed_coingecko.py       # One-time data seeder
├── requirements.txt            # Python dependencies
├── .gitignore                  # Excludes logs, CSV, env files
└── pipeline.log                # Runtime logs (gitignored)
```

---

## What I Learned

- **Financial precision**: Why `Decimal` beats `float` for money — no rounding errors on $0.00003412
- **Idempotent loads**: `ON CONFLICT DO NOTHING` makes reruns safe and duplicates impossible
- **Observability**: Logging every run is non-negotiable in production — you can't debug what you can't see
- **Defensive coding**: APIs fail, networks timeout, data is dirty — your pipeline shouldn't crash
- **Schema evolution**: Real projects need `schema_v1.sql`, `schema_v2.sql`... versioning matters
- **Multi-source validation**: One API can lie — two APIs tell the truth

---

## Future Improvements

- [ ] **Docker**: Containerize PostgreSQL + Python for one-command setup
- [ ] **WebSocket streaming**: Binance WebSocket for sub-second latency instead of polling
- [ ] **Apache Kafka**: Stream processing for 1000+ symbols at scale
- [ ] **TimescaleDB**: PostgreSQL extension for true time-series performance
- [ ] **Grafana dashboard**: Real-time price monitoring with alerts
- [ ] **Data retention**: Partition prices table by month, auto-archive old data
- [ ] **Unit tests**: `pytest` with mocked API responses
- [ ] **CI/CD**: GitHub Actions to run data quality checks on every commit

---

## Screen Recording Guide for Portfolio

Record these clips and upload to your portfolio (LinkedIn, personal site, or GitHub README):

### Clip 1: "The Pipeline in Action" (30-45 seconds)
- Open terminal
- Run `python3 pipeline_v2.py`
- Show the output: prices fetched, inserted, summary
- Cut to: `tail -f pipeline.log` showing cron running every minute
- Show: `psql market_data -c "SELECT * FROM etl_logs ORDER BY run_start DESC LIMIT 3;"`

### Clip 2: "Data Quality & Anomaly Detection" (30 seconds)
- Show `data_quality_rejects` table with a rejected record
- Explain: "If BTC drops 50% in one minute, we reject it — likely bad API data"
- Show the threshold config in code

### Clip 3: "Multi-Source Validation" (30 seconds)
- Run `python3 analytics.py`
- Show the Binance vs CoinGecko comparison
- Point out the 0.01% difference — "cross-validation catches bad data"

### Clip 4: "Analytics & Power BI Export" (30 seconds)
- Run `python3 analytics.py`
- Show the CSV file generated
- Open it in Excel / Power BI
- Show a simple line chart of daily prices

### Clip 5: "Schema Design" (20 seconds)
- Open `schema.sql` in VS Code
- Scroll through: tables, indexes, constraints, views
- Narrate: "Designed for time-series queries and auditability"

**Pro tip:** Use [Screen Studio](https://www.screen.studio/) (free for basic) or QuickTime Player (built into Mac) for clean recordings with cursor highlighting.

---

Built by [Shahesta Salama] · [LinkedIn]([https://linkedin.com/in/yourname](https://www.linkedin.com/in/shahesta-salama-396566320/)) · [GitHub]([https://github.com/yourusername](https://s2800-0.github.io/portfolio/#projects))
