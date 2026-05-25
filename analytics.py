import psycopg2
from decimal import Decimal
import csv
from datetime import datetime, timedelta
import os

DB_CONFIG = {
    "dbname": "market_data",
    "user": os.getenv("USER"),
    "password": "",
    "host": "localhost",
    "port": "5432"
}

def run_analytics():
    conn = psycopg2.connect(**DB_CONFIG)
    
    # Query 1: Multi-source price comparison
    with conn.cursor() as cur:
        cur.execute("""
            WITH binance_latest AS (
                SELECT DISTINCT ON (symbol) symbol, price, timestamp
                FROM crypto_prices WHERE source = 'binance'
                ORDER BY symbol, timestamp DESC
            ),
            coingecko_latest AS (
                SELECT DISTINCT ON (symbol) symbol, price, timestamp
                FROM crypto_prices_coingecko
                ORDER BY symbol, timestamp DESC
            )
            SELECT 
                b.symbol,
                b.price as binance_price,
                c.price as coingecko_price,
                ABS(b.price - c.price) as price_diff,
                ROUND((ABS(b.price - c.price) / b.price * 100), 4) as diff_pct
            FROM binance_latest b
            LEFT JOIN coingecko_latest c ON b.symbol = c.symbol;
        """)
        comparison = cur.fetchall()
        print("=== MULTI-SOURCE PRICE COMPARISON ===")
        for row in comparison:
            symbol = row[0]
            binance_price = float(row[1])
            cg_price = float(row[2]) if row[2] else 0.0
            diff_pct = float(row[4]) if row[4] else 0.0
            print(f"{symbol}: Binance=${binance_price:,.2f} | CoinGecko=${cg_price:,.2f} | Diff={diff_pct:.4f}%")
    
    # Query 2: Hourly volatility
    with conn.cursor() as cur:
        cur.execute("""
            SELECT 
                symbol,
                DATE_TRUNC('hour', timestamp) as hour,
                MIN(price) as min_price,
                MAX(price) as max_price,
                AVG(price)::DECIMAL(18,8) as avg_price,
                MAX(price) - MIN(price) as range,
                ROUND(((MAX(price) - MIN(price)) / MIN(price) * 100), 4) as volatility_pct
            FROM unified_prices
            WHERE timestamp > NOW() - INTERVAL '24 hours'
            GROUP BY symbol, DATE_TRUNC('hour', timestamp)
            ORDER BY symbol, hour DESC;
        """)
        volatility = cur.fetchall()
        print("\n=== 24-HOUR VOLATILITY BY HOUR ===")
        for row in volatility[:10]:
            print(f"{row[0]} @ {row[1]}: Avg=${float(row[4]):,.2f} | Vol={float(row[6]):.4f}%")
    
    # Query 3: Daily summary for warehouse
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO daily_summary (symbol, date, open_price, high_price, low_price, close_price, avg_price, record_count, sources)
            SELECT 
                symbol,
                DATE(timestamp) as date,
                (ARRAY_AGG(price ORDER BY timestamp ASC))[1] as open_price,
                MAX(price) as high_price,
                MIN(price) as low_price,
                (ARRAY_AGG(price ORDER BY timestamp DESC))[1] as close_price,
                AVG(price)::DECIMAL(18,8) as avg_price,
                COUNT(*) as record_count,
                ARRAY_AGG(DISTINCT source) as sources
            FROM unified_prices
            WHERE DATE(timestamp) = CURRENT_DATE - 1
            GROUP BY symbol, DATE(timestamp)
            ON CONFLICT (symbol, date) DO UPDATE SET
                open_price = EXCLUDED.open_price,
                high_price = EXCLUDED.high_price,
                low_price = EXCLUDED.low_price,
                close_price = EXCLUDED.close_price,
                avg_price = EXCLUDED.avg_price,
                record_count = EXCLUDED.record_count,
                sources = EXCLUDED.sources,
                updated_at = NOW();
        """)
        conn.commit()
        print(f"\n✅ Daily summary updated for {cur.rowcount} symbols")
    
    conn.close()

def export_to_csv():
    conn = psycopg2.connect(**DB_CONFIG)
    with conn.cursor() as cur:
        cur.execute("""
            SELECT symbol, date, open_price, high_price, low_price, close_price, avg_price, record_count, sources
            FROM daily_summary
            WHERE date >= CURRENT_DATE - 7
            ORDER BY date DESC, symbol;
        """)
        rows = cur.fetchall()
        
        filename = f"market_data_export_{datetime.now().strftime('%Y%m%d')}.csv"
        with open(filename, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['Symbol', 'Date', 'Open', 'High', 'Low', 'Close', 'Avg', 'Records', 'Sources'])
            writer.writerows(rows)
        
        print(f"\n📁 Exported {len(rows)} rows to {filename}")
    
    conn.close()

if __name__ == "__main__":
    run_analytics()
    export_to_csv()