import sys
import requests
import pprint
from bs4 import BeautifulSoup

def fetch_fundamentals(symbol):
    current_symbol = symbol

    URL = f"https://finviz.com/quote.ashx?t={current_symbol}"

    headers = {"User-Agent": "value-search-pyworker"}
    try:
        response = requests.get(URL, headers=headers)
        response.raise_for_status()
        html_content = response.content
    except requests.exceptions.RequestException as e:
        return (f"Error fetching the URL: {e}")
        

    soup = BeautifulSoup(html_content, "html.parser")
    fundamentals = soup.find("table", class_="snapshot-table2")

    company_symbol = soup.find(
        "h1", class_="quote-header_ticker-wrapper_ticker"
    ).text.strip()
    company_name = soup.find(
        "h2", class_="quote-header_ticker-wrapper_company"
    ).a.text.strip()

    stock_data = {
        "company_name": company_name,
        "company_symbol": company_symbol,
        "fundamentals": {},
    }

    if fundamentals:
        for row in fundamentals.find_all("tr"):
            cells = row.find_all("td")
            # Iterate in pairs: key at even index, value at odd index
            for i in range(0, len(cells), 2):
                if i + 1 < len(cells):
                    key = cells[i].text.strip()
                    value = cells[i + 1].text.strip()
                    stock_data["fundamentals"][key] = value

    return stock_data["fundamentals"]