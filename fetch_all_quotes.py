import requests
import time
import os
from datetime import datetime, timezone
from dotenv import load_dotenv
from mongo_client import get_mongo_client

load_dotenv()

fmp_api_key = os.getenv("FMP_API_KEY")
if not fmp_api_key:
    raise RuntimeError("FMP_API_KEY is not set")

client = get_mongo_client()
db = client["value-search-py"]
stock_symbols_collection = db["stock-symbols"] 
stock_quotes_collection = db["stock-quotes"]

response = stock_symbols_collection.find({})
symbols = []

for symbol in response:
    symbols.append(symbol["symbol"])

session = requests.Session()

INT64_MAX = 2**63 - 1
INT64_MIN = -2**63


def coerce_mongo_ints(value, path=""):
    if isinstance(value, int):
        if value > INT64_MAX or value < INT64_MIN:
            if path:
                print(f"⚠️  Coercing oversized int at {path} to string.")
            return str(value)
        return value
    if isinstance(value, list):
        return [coerce_mongo_ints(item, f"{path}[{idx}]") for idx, item in enumerate(value)]
    if isinstance(value, dict):
        return {
            key: coerce_mongo_ints(item, f"{path}.{key}" if path else key)
            for key, item in value.items()
        }
    return value

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
    
    safe_quote = coerce_mongo_ints(quote_payload[0])
    stock_quotes_collection.update_one(
        {"symbol": symbol},  # match condition
        {
            "$set": {"quote": safe_quote, "lastUpdatedQuote": datetime.now(timezone.utc)}
        },
        upsert=True,
    )
    print(f"💽 MongoDB update complete for {symbol}.")
    time.sleep(0.5)

client.close()