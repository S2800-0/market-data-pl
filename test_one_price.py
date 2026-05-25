import requests

url = "https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT"

response = requests.get(url)
data = response.json()

print("What the API gave us:")
print(data)

print("\nThe symbol is:", data['symbol'])
print("The price is:", data['price'])
print("Price as a number:", float(data['price']))