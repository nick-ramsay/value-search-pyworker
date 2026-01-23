import os
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv
from pymongo import MongoClient, UpdateOne
from pymongo.errors import PyMongoError

load_dotenv()

fmp_api_key = os.getenv("FMP_API_KEY")
if not fmp_api_key:
    raise SystemExit("Missing FMP_API_KEY environment variable.")

url = f"https://financialmodelingprep.com/stable/stock-list?apikey={fmp_api_key}"

client = MongoClient("localhost", 27017)
db = client["value-search-py"]
stock_symbols_collection = db["stock-symbols"]

try:
    try:
        print("⏳ Awaiting API response...")
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        payload = response.json()
    except requests.RequestException as exc:
        raise SystemExit(f"API request failed: {exc}") from exc
    except ValueError as exc:
        raise SystemExit(f"API response was not valid JSON: {exc}") from exc

    if not isinstance(payload, list):
        raise SystemExit("API response was not a list of symbols.")

    operations = []

    for item in payload:
        symbol = item.get("symbol")
        company_name = item.get("companyName")
        if not symbol or not company_name:
            continue
        filter_query = {"symbol": symbol}
        update_operation = {
            "$set": {
                "symbol": symbol,
                "companyName": company_name,
                "lastUpdated": datetime.now(timezone.utc),
            }
        }

        # Create the UpdateOne operation with upsert=True
        operations.append(UpdateOne(filter_query, update_operation, upsert=True))

    # Execute the bulk write operation
    if operations:
        try:
            print("⏳ Awaiting MongoDB bulk update. This may take a few minutes...")
            result = stock_symbols_collection.bulk_write(operations)
        except PyMongoError as exc:
            raise SystemExit(f"MongoDB bulk write failed: {exc}") from exc
        print("💽 Bulk write successful.")
        print(f"Inserted documents: {result.inserted_count}")
        print(f"Matched documents: {result.matched_count}")
        print(f"Modified documents: {result.modified_count}")
    else:
        print("No valid symbols found to update.")
finally:
    # Close the connection
    client.close()
