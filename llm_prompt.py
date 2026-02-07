import requests
import json
import re


def llm_analysis(stock_data: dict) -> tuple[str, str | None]:

    url = "http://localhost:11434/api/generate"

    prompt = f"""
    
    Here is the investment quote data in a dictionary format:

    {json.dumps(stock_data["quote"], indent=2)}

    Here is the investment fundamentals data in a dictionary format:
    {json.dumps(stock_data["fundamentals"], indent=2)}

    If any data between the 'quote' and 'fundamentals' object conflict, then consider the 'quote' object the primary source of truth, use the 'fundamentals' object as a secondary source.

    Here is the investment industry data:
    {stock_data["industry"]}

    Here is the investment sector data:
    {stock_data["sector"]}

    Here is the investment country data:
    {stock_data["country"]}

    Here is the investment description:
    {stock_data["investmentDescription"]}

    Provide a short, one to two paragraph summary of your thoughts on this investment. 
    The opening sentence should introduce the company with it's name and the ticker symbol. 

    Don't be too technical.

    """

    payload = {
        "model": "gemma3:4b",
        "prompt": prompt,
        "stream": False,
        "temperature": 0,
        "system": """
    
        You are a realistic and experienced investment analyst who strives to avoid significant losses.
        You are given a dictionary of stock data named 'stock_data' and you need to provide a 
        brief analysis of the key metrics and any notable observations.
        Consider data in the 'quote' object as the primary source of truth, use the 'fundamentals' object as a secondary source.
        
        For individual stocks which are NOT ETFs or mutual funds, you see profitable companies with healthy debt-to-equity ratios as the best investments.
        For individual stocks which are NOT ETFs or mutual funds, unhealthy debt-to-equity ratios OR are unprofitable are not good investments.
        You are open to individual stocks which are growth companies so long as they are profitable and have a healthy debt-to-equity ratio.
        
        Finish your analysis with a final sentence containing a recommendation of whether to buy, neutral, or sell the stock.
        This sentence MUST ALWAYS be structured as follows: 'In my current opinion, this is a [STRONG BUY, BUY, NEUTRAL, SELL, or STRONG SELL].'
        Don't return the response in markdown format.
        """,
    }

    try:
        response = requests.post(url, json=payload)
        response.raise_for_status()
        result = response.json().get("response", "No response received")
        rating_match = re.search(
            r"a (STRONG BUY|BUY|NEUTRAL|SELL|STRONG SELL|strong buy|buy|neutral|sell|strong sell)\.\s*$",
            result,
        )
        ai_rating = rating_match.group(1) if rating_match else None

        return result, ai_rating
    except requests.exceptions.RequestException as e:
        print(f"Error calling Ollama: {e}")
        return f"Error calling Ollama: {e}"