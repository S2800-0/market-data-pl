import requests
import json

# Binance public endpoint — no API key needed
url = "https://api.binance.com/api/v3/ticker/price"

def fetch_raw_data():
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        # Pretty print first 3 records so you see the structure
        print("=== RAW API RESPONSE (first 3 symbols) ===")
        print(json.dumps(data[:3], indent=2))
        
        print(f"\n=== TOTAL SYMBOLS FETCHED: {len(data)} ===")
        print(f"=== SAMPLE SYMBOLS: {[d['symbol'] for d in data[:5]]} ===")
        
        return data
        
    except requests.exceptions.RequestException as e:
        print(f"API request failed: {e}")
        return None

if __name__ == "__main__":
    raw_data = fetch_raw_data()