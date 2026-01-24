import requests
import time
import os
from datetime import datetime, timezone
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

fmp_api_key = os.getenv("FMP_API_KEY")
if not fmp_api_key:
    raise RuntimeError("FMP_API_KEY is not set")

client = MongoClient("localhost", 27017)
db = client["value-search-py"]
stock_symbols_collection = db["stock-symbols"] 
stock_quotes_collection = db["stock-quotes"]

response = stock_symbols_collection.find({})
symbols = []

for symbol in response:
    symbols.append(symbol["symbol"])

session = requests.Session()

for symbol in symbols:
    url = f"https://financialmodelingprep.com/stable/quote?symbol={symbol}&apikey={fmp_api_key}"
    try:
        fmp_response = session.get(url, timeout=10)
        fmp_response.raise_for_status()
        quote_payload = fmp_response.json()
    except requests.RequestException as exc:
        print(f"Failed to fetch quote for {symbol}: {exc}")
        time.sleep(0.5)
        continue

    if not quote_payload:
        print(f"No quote returned for {symbol}")
        time.sleep(0.5)
        continue
    else:   
        print(f"✅ Quote returned for {symbol}...")
    
    stock_quotes_collection.update_one(
        {"symbol": symbol},  # match condition
        {
            "$set": {"quote": quote_payload[0], "lastUpdatedQuote": datetime.now(timezone.utc)}
        },
        upsert=True,
    )
    print(f"💽 MongoDB update complete for {symbol}.")
    time.sleep(0.5)

client.close()