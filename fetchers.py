import requests
from decimal import Decimal
from datetime import datetime

SYMBOLS_MAP = {
    "BTCUSDT": "bitcoin",
    "ETHUSDT": "ethereum",
    "SOLUSDT": "solana",
    "XRPUSDT": "ripple",
    "DOGEUSDT": "dogecoin"
}

def fetch_binance():
    """Fetch from Binance (existing logic, extracted)"""
    url = "https://api.binance.com/api/v3/ticker/price"
    response = requests.get(url, timeout=15)
    response.raise_for_status()
    
    symbol_set = set(SYMBOLS_MAP.keys())
    results = []
    for item in response.json():
        if item["symbol"] in symbol_set:
            results.append({
                "symbol": item["symbol"],
                "price": Decimal(item["price"]),
                "timestamp": datetime.utcnow(),
                "source": "binance"
            })
    return results

def fetch_coingecko():
    """Fetch from CoinGecko as secondary source"""
    ids = ",".join(SYMBOLS_MAP.values())
    url = f"https://api.coingecko.com/api/v3/simple/price?ids={ids}&vs_currencies=usd"
    
    response = requests.get(url, timeout=15)
    response.raise_for_status()
    data = response.json()
    
    # Reverse map: coingecko_id -> our_symbol
    reverse_map = {v: k for k, v in SYMBOLS_MAP.items()}
    
    results = []
    for cg_id, price_data in data.items():
        symbol = reverse_map.get(cg_id)
        if symbol:
            results.append({
                "symbol": symbol,
                "price": Decimal(str(price_data["usd"])),
                "timestamp": datetime.utcnow(),
                "source": "coingecko"
            })
    return results