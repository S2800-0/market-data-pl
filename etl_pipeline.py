import requests
import psycopg2
from datetime import datetime
from decimal import Decimal
import os
import time
import sys

# ============ CONFIGURATION ============

# Coins we care about (USDT pairs = priced in dollars)
SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "DOGEUSDT"]

DB_CONFIG = {
    "dbname": "market_data",
    "user": os.getenv("USER"),
    "password": "",
    "host": "localhost",
    "port": "5432"
}

# ============ EXTRACT ============

def fetch_prices(symbols):
    """
    Fetch prices from Binance for multiple symbols.
    Uses batch endpoint (ticker/24hr) to get all in ONE request.
    More efficient than calling API 5 times.
    """
    url = "https://api.binance.com/api/v3/ticker/price"
    
    try:
        start_time = time.time()
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        latency_ms = int((time.time() - start_time) * 1000)
        
        all_prices = response.json()
        
        # Filter only the symbols we want
        # Binance returns ALL ~2000 symbols, we pick our 5
        symbol_set = set(symbols)
        filtered = [
            {
                "symbol": item["symbol"],
                "price": Decimal(item["price"]),  # Exact precision, no float rounding
                "timestamp": datetime.utcnow()
            }
            for item in all_prices
            if item["symbol"] in symbol_set
        ]
        
        # Check if we got all symbols
        found_symbols = {p["symbol"] for p in filtered}
        missing = symbol_set - found_symbols
        if missing:
            print(f"⚠️  Missing symbols from API: {missing}")
        
        return filtered, latency_ms, None
        
    except requests.exceptions.Timeout:
        return None, 0, "API timeout after 15 seconds"
    except requests.exceptions.HTTPError as e:
        return None, 0, f"HTTP error: {e.response.status_code}"
    except requests.exceptions.RequestException as e:
        return None, 0, f"Network error: {str(e)}"
    except Exception as e:
        return None, 0, f"Unexpected error: {str(e)}"

# ============ LOAD ============

def insert_prices(prices_data, conn):
    """
    Insert multiple prices. Returns (inserted_count, skipped_count).
    """
    if not prices_data:
        return 0, 0
    
    inserted = 0
    skipped = 0
    
    with conn.cursor() as cur:
        for price in prices_data:
            cur.execute("""
                INSERT INTO crypto_prices (symbol, price, timestamp)
                VALUES (%s, %s, %s)
                ON CONFLICT (symbol, timestamp) DO NOTHING
                RETURNING id;
            """, (price["symbol"], price["price"], price["timestamp"]))
            
            if cur.fetchone():
                inserted += 1
            else:
                skipped += 1
    
    return inserted, skipped

# ============ LOGGING ============

def start_etl_log(conn):
    """Create a new log entry, return its ID."""
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO etl_logs (run_start, status)
            VALUES (NOW(), 'RUNNING')
            RETURNING id;
        """)
        log_id = cur.fetchone()[0]
        conn.commit()
        return log_id

def finish_etl_log(conn, log_id, records_processed, records_inserted, status, error_msg, latency):
    """Update log entry with final results."""
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE etl_logs
            SET run_end = NOW(),
                records_processed = %s,
                records_inserted = %s,
                status = %s,
                error_message = %s,
                api_latency_ms = %s
            WHERE id = %s;
        """, (records_processed, records_inserted, status, error_msg, latency, log_id))
        conn.commit()

# ============ MAIN PIPELINE ============

def run_pipeline():
    print(f"\n{'='*50}")
    print(f"🚀 ETL Pipeline Started at {datetime.utcnow().isoformat()}Z")
    print(f"{'='*50}")
    
    conn = None
    log_id = None
    
    try:
        # Connect to database
        conn = psycopg2.connect(**DB_CONFIG)
        print("✅ Database connected")
        
        # Start logging
        log_id = start_etl_log(conn)
        print(f"📝 Log entry created: ID {log_id}")
        
        # EXTRACT
        print(f"\n📡 Fetching {len(SYMBOLS)} symbols from Binance...")
        prices_data, latency, error = fetch_prices(SYMBOLS)
        
        if error:
            print(f"❌ API Error: {error}")
            finish_etl_log(conn, log_id, 0, 0, "FAILED", error, latency)
            return False
        
        print(f"✅ Fetched {len(prices_data)} prices in {latency}ms")
        
        # TRANSFORM (already done in fetch_prices — Decimal conversion)
        
        # LOAD
        print(f"\n💾 Inserting into database...")
        inserted, skipped = insert_prices(prices_data, conn)
        
        # Finish logging
        finish_etl_log(
            conn, log_id, 
            records_processed=len(prices_data),
            records_inserted=inserted,
            status="SUCCESS",
            error_msg=None,
            latency=latency
        )
        
        # Summary
        print(f"\n{'='*50}")
        print("📊 PIPELINE SUMMARY")
        print(f"{'='*50}")
        print(f"Symbols processed: {len(prices_data)}")
        print(f"Rows inserted:     {inserted}")
        print(f"Duplicates skipped: {skipped}")
        print(f"API latency:       {latency}ms")
        print(f"Status:            SUCCESS")
        print(f"{'='*50}")
        
        # Show current prices
        print(f"\n💰 CURRENT PRICES:")
        for p in sorted(prices_data, key=lambda x: x["symbol"]):
            print(f"   {p['symbol']}: ${float(p['price']):,.2f}")
        
        return True
        
    except psycopg2.Error as e:
        print(f"❌ Database error: {e}")
        if conn and log_id:
            finish_etl_log(conn, log_id, 0, 0, "FAILED", str(e), 0)
        return False
        
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        if conn and log_id:
            finish_etl_log(conn, log_id, 0, 0, "FAILED", str(e), 0)
        return False
        
    finally:
        if conn:
            conn.close()
            print("\n🔌 Database connection closed")

if __name__ == "__main__":
    success = run_pipeline()
    sys.exit(0 if success else 1)