from fetch_fundamentals import fetch_fundamentals
import time
import os
from datetime import datetime, timezone
from dotenv import load_dotenv
from mongo_client import get_mongo_client

load_dotenv()


client = get_mongo_client()
db = client["value-search-py"]
stock_quotes_collection = db["stock-quotes"]

response = stock_quotes_collection.find({})
symbols = []

for symbol in response:
    symbols.append(symbol["symbol"])

for symbol in symbols:
    fetch_response = fetch_fundamentals(symbol)
    if type(fetch_response) is not str and isinstance(fetch_response["fundamentals"], dict):
        stock_quotes_collection.update_one(
            {"symbol": symbol},  # match condition
            {
                "$set": {
                    "fundamentals_original": fetch_response["fundamentals"],
                    "news": fetch_response["news"],
                    "industry": fetch_response["industry"],
                    "sector": fetch_response["sector"],
                    "country": fetch_response["country"],
                    "investmentDescription": fetch_response["investmentDescription"],
                    "lastUpdatedFundamentals": datetime.now(timezone.utc),
                }
            }
        )
        print(f"✅ Fundamentals fetched for {symbol}")
        time.sleep(2)
    else:
        print(f"⛔️ Failed to fetch fundamentals for {symbol}: {fetch_response}")
        time.sleep(2)

client.close()
