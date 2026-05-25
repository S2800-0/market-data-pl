# Real-Time Crypto Market Data ETL Pipeline

A production-ready ETL pipeline that fetches live cryptocurrency prices from the Binance API, applies data quality checks, and loads them into PostgreSQL for analysis. Built as a portfolio project for Data Engineering roles in fintech.

## Architecture

```
┌─────────────┐     ┌──────────────┐     ┌─────────────┐     ┌─────────────┐
│  Binance    │────▶│   Python     │────▶│  PostgreSQL │────▶│    Cron     │
│   API       │     │  ETL Script  │     │  Database   │     │  Scheduler  │
└─────────────┘     └──────────────┘     └─────────────┘     └─────────────┘
                           │
                    ┌──────┴──────┐
                    │ Data Quality │
                    │   Checks     │
                    │  (Anomaly    │
                    │  Detection)  │
                    └─────────────┘
```

**Pipeline Flow:**
1. **Extract**: Fetches 5 crypto symbols (BTC, ETH, SOL, XRP, DOGE) from Binance public API
2. **Transform**: Converts string prices to `Decimal` for exact precision; validates against anomaly thresholds
3. **Load**: Inserts into PostgreSQL with conflict handling; logs every run
4. **Schedule**: Cron runs every minute automatically

## Tech Stack

| Layer | Technology | Why |
|-------|-----------|-----|
| Data Source | Binance REST API | Free, no API key, high rate limits |
| ETL | Python 3.13 | `requests`, `psycopg2`, `Decimal` for precision |
| Database | PostgreSQL 15 | ACID compliance, time-series indexing |
| Scheduler | Cron | Native, reliable, zero dependencies |
| Data Quality | Custom validators | Anomaly detection, duplicate handling |

## Schema Design

### `crypto_prices`
| Column | Type | Notes |
|--------|------|-------|
| `id` | `SERIAL PRIMARY KEY` | Surrogate key |
| `symbol` | `VARCHAR(20)` | e.g., `BTCUSDT` |
| `price` | `DECIMAL(18,8)` | Exact precision — no float rounding |
| `timestamp` | `TIMESTAMPTZ` | Price observation time |
| `created_at` | `TIMESTAMPTZ` | Insertion time |

**Indexes:** `(symbol, timestamp DESC)` for time-series queries; `created_at` for cleanup jobs.

### `etl_logs`
Tracks every pipeline run: start/end time, records processed, status, API latency, errors.

### `data_quality_rejects`
Audit trail for rejected records: symbol, price, reason, expected range.

## Data Quality Checks

| Check | Implementation | Threshold |
|-------|---------------|-----------|
| Positive price | `price > 0` | Hard reject |
| Price spike | vs. last known price | BTC/ETH: 15%, SOL/XRP: 20%, DOGE: 25% |
| Duplicate detection | `ON CONFLICT` + `UNIQUE` constraint | Same symbol + timestamp |
| API timeout | `requests.get(timeout=15)` | 15 seconds |

## How to Run

```bash
# 1. Install PostgreSQL and create database
brew install postgresql@15
brew services start postgresql@15
createdb market_data
psql market_data -f schema.sql
psql market_data -f schema_v2.sql

# 2. Install Python dependencies
pip install psycopg2-binary requests

# 3. Run manually
python3 pipeline_v2.py

# 4. Schedule with cron
crontab -e
# Add: * * * * * cd ~/market-data-pipeline && /Library/Frameworks/Python.framework/Versions/3.13/bin/python3 pipeline_v2.py >> ~/market-data-pipeline/pipeline.log 2>&1
```

## Sample Queries

```sql
-- Latest price for each symbol
SELECT DISTINCT ON (symbol) symbol, price, timestamp
FROM crypto_prices
ORDER BY symbol, timestamp DESC;

-- Pipeline health: success rate last 24 hours
SELECT status, COUNT(*) 
FROM etl_logs 
WHERE run_start > NOW() - INTERVAL '24 hours'
GROUP BY status;

-- Anomalies detected today
SELECT symbol, price, reject_reason, created_at
FROM data_quality_rejects
WHERE created_at > CURRENT_DATE;
```

## Project Structure

```
market-data-pipeline/
├── README.md              # This file
├── schema.sql             # Initial tables
├── schema_v2.sql          # Quality rejects table
├── pipeline_v2.py         # Main ETL script
├── pipeline.log           # Runtime logs (cron output)
└── .gitignore             # Excludes logs, env files
```

## What I Learned

- **Financial precision**: Why `Decimal` beats `float` for money
- **Idempotent loads**: `ON CONFLICT DO NOTHING` makes reruns safe
- **Observability**: Logging every run is non-negotiable in production
- **Defensive coding**: APIs fail — your pipeline shouldn't

## Future Improvements

- [ ] Add Docker for reproducible environments
- [ ] Switch to Binance WebSocket for true real-time (sub-second latency)
- [ ] Add Grafana dashboard for price monitoring
- [ ] Implement data retention policy (partitioning by month)
- [ ] Add unit tests with `pytest` and mocked API responses

---
Built by [Your Name] · [LinkedIn] · [GitHub]
