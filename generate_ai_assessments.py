from fetch_fundamentals import fetch_fundamentals
from llm_prompt import llm_analysis
import time
import os
from datetime import datetime, timezone, date
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
    current_time = datetime.now(timezone.utc)
    if quote.get("lastUpdatedAiAssessment") is not None:
        lastAssessmentTime = quote.get("lastUpdatedAiAssessment").replace(tzinfo=timezone.utc)
        hoursSinceLastAssessment = (current_time - lastAssessmentTime).seconds / 3600

    if quote.get("fundamentals_original"):
        ai_dataset["fundamentals"] = quote.get("fundamentals_original")
    ai_dataset["investment_ticker_symbol"] = quote.get("quote").get("symbol")
    ai_dataset["investment_name"] = quote.get("quote").get("name")
    ai_dataset["quote"] = quote.get("quote")

    if hoursSinceLastAssessment >= 24 or quote.get("lastUpdatedAiAssessment") is None:
        assessment, ai_rating = llm_analysis(ai_dataset)

        stock_ai_assessments_collection.update_one(
                {"symbol": ai_dataset['investment_ticker_symbol']},  # match condition
                {
                    "$set": {
                        "assessment": assessment,
                        "aiRating": ai_rating,
                        "lastUpdatedAssessment": current_time,
                    }
                },
                upsert=True,
            )
        stock_quotes_collection.update_one({"symbol": ai_dataset['investment_ticker_symbol']}, {"$set": {"lastUpdatedAiAssessment": current_time}})
        print(f"💽 MongoDB update complete for {ai_dataset['investment_ticker_symbol']}.")
    else:
        print(f"💤 {ai_dataset['investment_ticker_symbol']} has been assessed within the last 24 hours. Skipping assessment.")

client.close()
