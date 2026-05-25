import requests
import psycopg2
from datetime import datetime
from decimal import Decimal
import os
import time
import sys
from fetchers import fetch_binance, fetch_coingecko


SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "DOGEUSDT"]

# Anomaly thresholds: max % change from last price
# Crypto is volatile, so 15% is reasonable for 1-minute windows
ANOMALY_THRESHOLDS = {
    "BTCUSDT": Decimal("0.15"),   # 15%
    "ETHUSDT": Decimal("0.15"),
    "SOLUSDT": Decimal("0.20"),   # Altcoins more volatile
    "XRPUSDT": Decimal("0.20"),
    "DOGEUSDT": Decimal("0.25"),  # Meme coins most volatile
}

DB_CONFIG = {
    "dbname": "market_data",
    "user": os.getenv("USER"),
    "password": "",
    "host": "localhost",
    "port": "5432"
}

# ============ DATABASE HELPERS ============

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

def get_last_prices(conn, symbols):
    """Fetch the most recent valid price for each symbol."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT ON (symbol) 
                symbol, price, timestamp
            FROM crypto_prices
            WHERE symbol = ANY(%s)
            ORDER BY symbol, timestamp DESC;
        """, (list(symbols),))
        return {row[0]: {"price": row[1], "time": row[2]} for row in cur.fetchall()}

# ============ DATA QUALITY ============

def validate_price(symbol, new_price, last_record, threshold):
    """
    Check if price is anomalous compared to last known price.
    Returns (is_valid, reason, expected_min, expected_max).
    """
    # Check 1: Price must be positive
    if new_price <= 0:
        return False, "PRICE_ZERO_OR_NEGATIVE", None, None
    
    # Check 2: If no history, accept it (first run for this symbol)
    if not last_record:
        return True, None, None, None
    
    last_price = Decimal(str(last_record["price"]))
    
    # Calculate allowed range
    max_change = last_price * threshold
    expected_min = last_price - max_change
    expected_max = last_price + max_change
    
    if new_price < expected_min:
        return False, "PRICE_DROP_ANOMALY", expected_min, expected_max
    
    if new_price > expected_max:
        return False, "PRICE_SPIKE_ANOMALY", expected_min, expected_max
    
    return True, None, expected_min, expected_max

def log_reject(conn, symbol, price, reason, expected_min, expected_max):
    """Log rejected price for audit trail."""
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO data_quality_rejects 
                (symbol, price, reject_reason, expected_range_min, expected_range_max)
            VALUES (%s, %s, %s, %s, %s);
        """, (symbol, price, reason, expected_min, expected_max))
        conn.commit()

# ============ EXTRACT ============

def fetch_prices(symbols):
    url = "https://api.binance.com/api/v3/ticker/price"
    
    try:
        start_time = time.time()
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        latency_ms = int((time.time() - start_time) * 1000)
        
        all_prices = response.json()
        symbol_set = set(symbols)
        
        filtered = []
        for item in all_prices:
            if item["symbol"] in symbol_set:
                filtered.append({
                    "symbol": item["symbol"],
                    "price": Decimal(item["price"]),
                    "timestamp": datetime.utcnow()
                })
        
        return filtered, latency_ms, None
        
    except requests.exceptions.Timeout:
        return None, 0, "API_TIMEOUT"
    except requests.exceptions.HTTPError as e:
        return None, 0, f"HTTP_{e.response.status_code}"
    except Exception as e:
        return None, 0, f"ERROR_{str(e)}"

# ============ TRANSFORM + LOAD ============

def process_and_load(conn, prices_data, last_prices):
    """
    Apply quality checks and load valid data.
    Returns (inserted, skipped, rejected, reject_details).
    """
    inserted = 0
    skipped = 0
    rejected = 0
    reject_details = []
    
    for price in prices_data:
        symbol = price["symbol"]
        new_price = price["price"]
        threshold = ANOMALY_THRESHOLDS.get(symbol, Decimal("0.15"))
        last_record = last_prices.get(symbol)
        
        # Validate
        is_valid, reason, exp_min, exp_max = validate_price(
            symbol, new_price, last_record, threshold
        )
        
        if not is_valid:
            rejected += 1
            reject_details.append({
                "symbol": symbol,
                "price": float(new_price),
                "reason": reason,
                "last_price": float(last_record["price"]) if last_record else None
            })
            log_reject(conn, symbol, new_price, reason, exp_min, exp_max)
            continue
        
        # Insert
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO crypto_prices (symbol, price, timestamp)
                VALUES (%s, %s, %s)
                ON CONFLICT (symbol, timestamp) DO NOTHING
                RETURNING id;
            """, (symbol, new_price, price["timestamp"]))
            
            if cur.fetchone():
                inserted += 1
            else:
                skipped += 1
        
        conn.commit()
    
    return inserted, skipped, rejected, reject_details

# ============ ETL LOGGING ============

def start_log(conn):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO etl_logs (run_start, status)
            VALUES (NOW(), 'RUNNING')
            RETURNING id;
        """)
        conn.commit()
        return cur.fetchone()[0]

def finish_log(conn, log_id, processed, inserted, rejected, status, error, latency):
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
        """, (processed, inserted, status, error, latency, log_id))
        conn.commit()

# ============ MAIN ============

def run_pipeline():
    print(f"\n{'='*60}")
    print(f"Pipeline v2 | {datetime.utcnow().isoformat()}Z")
    print(f"{'='*60}")
    
    conn = None
    log_id = None
    
    try:
        conn = get_db_connection()
        log_id = start_log(conn)
        
        # Get history for anomaly detection
        print("Loading price history for anomaly detection...")
        last_prices = get_last_prices(conn, SYMBOLS)
        print(f"   Found history for {len(last_prices)} symbols")
        
        # EXTRACT
        print(f"\n Fetching {len(SYMBOLS)} symbols...")
        prices_data, latency, error = fetch_prices(SYMBOLS)
        
        if error:
            print(f"API Error: {error}")
            finish_log(conn, log_id, 0, 0, 0, "FAILED", error, latency)
            return False
        
        print(f"Fetched {len(prices_data)} prices in {latency}ms")
        
        # TRANSFORM + LOAD with quality checks
        print(f"\nRunning data quality checks...")
        inserted, skipped, rejected, rejects = process_and_load(
            conn, prices_data, last_prices
        )
        
        # Finish logging
        finish_log(conn, log_id, len(prices_data), inserted, rejected, 
                   "SUCCESS", None, latency)
        
        # Summary
        print(f"\n{'='*60}")
        print("PIPELINE SUMMARY")
        print(f"{'='*60}")
        print(f"Processed:  {len(prices_data)}")
        print(f"Inserted:   {inserted}")
        print(f"Skipped:    {skipped} (duplicates)")
        print(f"Rejected:   {rejected} (anomalies)")
        print(f"Latency:    {latency}ms")
        
        if rejected > 0:
            print(f"\nREJECTED RECORDS:")
            for r in rejects:
                print(f"   {r['symbol']}: ${r['price']:,.2f} | Reason: {r['reason']}")
                if r['last_price']:
                    print(f"      Last known: ${r['last_price']:,.2f}")
        
        print(f"\nCURRENT PRICES:")
        for p in sorted(prices_data, key=lambda x: x["symbol"]):
            print(f"   {p['symbol']}: ${float(p['price']):,.2f}")
        
        print(f"{'='*60}")
        return True
        
    except Exception as e:
        print(f"Pipeline crashed: {e}")
        if conn and log_id:
            finish_log(conn, log_id, 0, 0, 0, "FAILED", str(e), 0)
        return False
        
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    success = run_pipeline()
    sys.exit(0 if success else 1)