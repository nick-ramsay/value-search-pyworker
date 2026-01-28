import os
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv
from pymongo import UpdateOne, WriteConcern
from pymongo.errors import PyMongoError
from mongo_client import get_mongo_client

load_dotenv()

fmp_api_key = os.getenv("FMP_API_KEY")
if not fmp_api_key:
    raise SystemExit("Missing FMP_API_KEY environment variable.")

url = f"https://financialmodelingprep.com/stable/stock-list?apikey={fmp_api_key}"

client = get_mongo_client()
db = client["value-search-py"]
stock_symbols_collection = db.get_collection(
    "stock-symbols", write_concern=WriteConcern(w=1)
)

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

    
 
    # Execute the bulk write operation in batches to avoid timeouts
    if operations:
        total_operations = len(operations)
        batch_size = 500
        total_batches = (total_operations // batch_size) + 1
        total = len(operations)
        inserted = matched = modified = 0
        try:
            print("⏳ Awaiting MongoDB bulk update. This may take a few minutes...")
            for start in range(0, total, batch_size):
                batch = operations[start : start + batch_size]
                result = stock_symbols_collection.bulk_write(batch, ordered=False)
                inserted += result.inserted_count
                matched += result.matched_count
                modified += result.modified_count
                print(f"✅ Batch {start // batch_size + 1} of {total_batches} processed.")
        except PyMongoError as exc:
            raise SystemExit(f"MongoDB bulk write failed: {exc}") from exc
        print("💽 Bulk write successful.")
        print(f"Inserted documents: {inserted}")
        print(f"Matched documents: {matched}")
        print(f"Modified documents: {modified}")
    else:
        print("No valid symbols found to update.")
finally:
    # Close the connection
    client.close()
