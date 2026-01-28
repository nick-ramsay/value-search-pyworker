from fetch_fundamentals import fetch_fundamentals
from llm_prompt import llm_analysis
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
quotes = []

for quote in response:
    ai_dataset = {}
    if quote.get("fundamentals_original"):
        ai_dataset["fundamentals"] = quote.get("fundamentals_original")
    ai_dataset["investment_ticker_symbol"] = quote.get("quote").get("symbol")
    ai_dataset["investment_name"] = quote.get("quote").get("name")
    ai_dataset["quote"] = quote.get("quote")
    print(llm_analysis(ai_dataset))

    # quotes.append({"quote": quote["quote"], "fundamentals": quote["fundamentals"]})

    # fetch_response = fetch_fundamentals(symbol)
    # if isinstance(fetch_response, dict):
    #     print(fetch_response)
    #     stock_quotes_collection.update_one(
    #         {"symbol": symbol},  # match condition
    #         {
    #             "$set": {
    #                 "fundamentals_original": fetch_response,
    #                 "lastUpdatedFundamentals": datetime.now(timezone.utc),
    #             }
    #         }
    #     )
    #     time.sleep(2)
    # else:
    #     print(f"Failed to fetch fundamentals for {symbol}: {fetch_response}")
    #     time.sleep(2)

client.close()
