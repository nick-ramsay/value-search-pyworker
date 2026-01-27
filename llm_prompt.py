import requests
import json
import sys


def llm_analysis(stock_data: dict) -> str:
    print(stock_data["investment_ticker_symbol"])
    url = "http://localhost:11434/api/generate"
    
    prompt = f"""
    Provide a short, one to two paragraph summary of your thoughts on this investment. 
    The opening sentence should introduce the company with it's name and the ticker symbol. 

    Don't be too technical.
    
    Here is the stock data in a dictionary format:

    {json.dumps(stock_data, indent=2)}

    """

    payload = {
        "model": "gemma3:12b",
        "prompt": prompt,
        "stream": False,
        "temperature": 0,
        "system": """
        You are an investment analyst.
        You are given a dictionary of stock fundamentals and you need to provide a 
        brief analysis of the key metrics and any notable observations.
        You're a realististic analyst, so don't be too optimistic or pessimistic.
        You see profitable companies with healthy debt-to-equity ratios as good investments.
        Companies with unhealthy debt-to-equity ratios or are unprofitable are not good investments.
        Consider the 200 day moving average (SMA200), the 50 day moving average (SMA50), 
        and the 20 day moving average (SMA20) compared to the current price when considering whether the stock price may have reached a bottom.
        Finish your analysis with a simple sentence containing a recommendation of whether to buy, hold, or sell the stock.
        The sentence should begin with 'In my current opinion, this is a [STRONG BUY, BUY, HOLD, SELL, or STRONG SELL].'
        Don't return the response in markdown format.
        """
    }
    
    try:
        response = requests.post(url, json=payload)
        response.raise_for_status()
        result = response.json()
        return result.get("response", "No response received")
    except requests.exceptions.RequestException as e:
        return f"Error calling Ollama: {e}"