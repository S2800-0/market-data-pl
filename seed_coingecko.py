import requests
from decimal import Decimal
import psycopg2
from datetime import datetime
import os

SYMBOLS_MAP = {
    "BTCUSDT": "bitcoin", "ETHUSDT": "ethereum", "SOLUSDT": "solana",
    "XRPUSDT": "ripple", "DOGEUSDT": "dogecoin"
}

conn = psycopg2.connect(dbname="market_data", user=os.getenv("USER"), host="localhost")
ids = ",".join(SYMBOLS_MAP.values())
url = f"https://api.coingecko.com/api/v3/simple/price?ids={ids}&vs_currencies=usd"
data = requests.get(url, timeout=15).json()

reverse_map = {v: k for k, v in SYMBOLS_MAP.items()}
cur = conn.cursor()

for cg_id, price_data in data.items():
    symbol = reverse_map.get(cg_id)
    if symbol:
        cur.execute("""
            INSERT INTO crypto_prices_coingecko (symbol, price, timestamp)
            VALUES (%s, %s, %s)
        """, (symbol, Decimal(str(price_data["usd"])), datetime.utcnow()))

conn.commit()
cur.close()
conn.close()
print("CoinGecko data inserted!")