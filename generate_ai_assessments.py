from llm_prompt import llm_analysis
from datetime import datetime, timezone
from dotenv import load_dotenv
from mongo_client import get_mongo_client

load_dotenv()


client = get_mongo_client()
db = client["value-search-py"]
stock_quotes_collection = db["stock-quotes"]
stock_ai_assessments_collection = db["stock-ai-assessments"]

response = list(stock_quotes_collection.find({}))

for index, quote in enumerate(response):
    ai_dataset = {}
    current_time = datetime.now(timezone.utc)
    hoursSinceLastAssessment = None
    if quote.get("lastUpdatedAiAssessment") is not None:
        lastAssessmentTime = quote.get("lastUpdatedAiAssessment").replace(tzinfo=timezone.utc)
        hoursSinceLastAssessment = (current_time - lastAssessmentTime).total_seconds() / 3600

    if quote.get("fundamentals_original"):
        ai_dataset["fundamentals"] = quote.get("fundamentals_original")
    ai_dataset["investment_ticker_symbol"] = quote.get("quote").get("symbol")
    ai_dataset["investment_name"] = quote.get("quote").get("name")
    ai_dataset["quote"] = quote.get("quote")
    ai_dataset["news"] = quote.get("news")
    ai_dataset["industry"] = quote.get("industry")
    ai_dataset["sector"] = quote.get("sector")
    ai_dataset["country"] = quote.get("country")
    ai_dataset["investmentDescription"] = quote.get("investmentDescription")

    hourLimit = 0
    
    if (hoursSinceLastAssessment is None or hoursSinceLastAssessment >= hourLimit):
        if quote.get("fundamentals_original"):
            print(f"🔍 Fetching AI assessment for {ai_dataset['investment_name']} ({ai_dataset['investment_ticker_symbol']}) [{index}/{len(response)}]...")
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
            print(f"💤 {ai_dataset['investment_ticker_symbol']} has no fundamentals data. Skipping assessment.")
    else:
        print(f"💤 {ai_dataset['investment_ticker_symbol']} has been assessed within the last {hourLimit} hours. Skipping assessment.")

client.close()
