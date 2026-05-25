import requests
import psycopg2
from datetime import datetime
import os

# Database config — we'll improve this later
DB_CONFIG = {
    "dbname": "market_data",
    "user": os.getenv("USER"),  # Your Mac username
    "password": "",  # No password if you used createuser -s
    "host": "localhost",
    "port": "5432"
}

def fetch_price(symbol="BTCUSDT"):
    """Extract: Get single price from Binance"""
    url = f"https://api.binance.com/api/v3/ticker/price?symbol={symbol}"
    
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        # Transform: Convert string price to Decimal-friendly format
        return {
            "symbol": data["symbol"],
            "price": float(data["price"]),  # We'll switch to Decimal later
            "timestamp": datetime.utcnow()
        }
    except Exception as e:
        print(f"Fetch failed: {e}")
        return None

def insert_price(price_data):
    """Load: Insert into PostgreSQL"""
    if not price_data:
        return False
    
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        # Insert with conflict handling (ignore duplicates)
        cur.execute("""
            INSERT INTO crypto_prices (symbol, price, timestamp)
            VALUES (%s, %s, %s)
            ON CONFLICT (symbol, timestamp) DO NOTHING
            RETURNING id;
        """, (price_data["symbol"], price_data["price"], price_data["timestamp"]))
        
        result = cur.fetchone()
        conn.commit()
        cur.close()
        
        if result:
            print(f"✅ Inserted: {price_data['symbol']} @ ${price_data['price']:,.2f}")
            return True
        else:
            print(f"⏭️  Duplicate skipped: {price_data['symbol']}")
            return True
            
    except psycopg2.Error as e:
        print(f"❌ Database error: {e}")
        return False
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    # Run the pipeline for one symbol
    data = fetch_price("BTCUSDT")
    insert_price(data)
    
    # Verify: count rows
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM crypto_prices;")
    count = cur.fetchone()[0]
    print(f"\n📊 Total rows in database: {count}")
    cur.close()
    conn.close()