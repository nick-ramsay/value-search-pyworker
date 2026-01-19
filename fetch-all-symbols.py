import requests
import os
from datetime import datetime, timezone
from pymongo import MongoClient, UpdateOne
from dotenv import load_dotenv

load_dotenv()

fmp_api_key = os.getenv("FMP_API_KEY")

url = f"https://financialmodelingprep.com/stable/stock-list?apikey={fmp_api_key}"

client = MongoClient('localhost', 27017) # Replace with your connection string
db = client['value-search-py'] # Replace with your database name
collection = db['stock-symbols'] # Replace with your collection name

response = requests.get(url).json()

# Prepare the list of UpdateOne operations
operations = []

for item in response:
    filter_query = {'symbol': item['symbol']}
    # Use $set to update specific fields, or insert if not found
    update_operation = {'$set': {'symbol': item['symbol'], 'companyName': item['companyName'], 'lastUpdated': datetime.now(timezone.utc)}} 
    
    # Create the UpdateOne operation with upsert=True
    operations.append(UpdateOne(filter_query, update_operation, upsert=True))

# Execute the bulk write operation
if operations:
    result = collection.bulk_write(operations)
    print(f"Bulk write successful.")
    print(f"Inserted documents: {result.inserted_count}")
    print(f"Matched documents: {result.matched_count}")
    print(f"Modified documents: {result.modified_count}")

# Close the connection
client.close()