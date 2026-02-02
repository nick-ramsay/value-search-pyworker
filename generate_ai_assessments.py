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
stock_ai_assessments_collection = db["stock-ai-assessments"]

response = stock_quotes_collection.find({})

for quote in response:
    ai_dataset = {}
    if quote.get("fundamentals_original"):
        ai_dataset["fundamentals"] = quote.get("fundamentals_original")
    ai_dataset["investment_ticker_symbol"] = quote.get("quote").get("symbol")
    ai_dataset["investment_name"] = quote.get("quote").get("name")
    ai_dataset["quote"] = quote.get("quote")

    assessment, ai_rating = llm_analysis(ai_dataset)

    stock_ai_assessments_collection.update_one(
            {"symbol": ai_dataset['investment_ticker_symbol']},  # match condition
            {
                "$set": {
                    "assessment": assessment,
                    "aiRating": ai_rating,
                    "lastUpdatedAssessment": datetime.now(timezone.utc),
                }
            },
            upsert=True,
        )
    print(f"💽 MongoDB update complete for {ai_dataset['investment_ticker_symbol']}.")

client.close()
