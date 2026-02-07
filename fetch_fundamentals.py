import requests
from bs4 import BeautifulSoup


def fetch_fundamentals(symbol):
    current_symbol = symbol
    company_name = None

    URL = f"https://finviz.com/quote.ashx?t={current_symbol}"

    headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36"}
    try:
        response = requests.get(URL, headers=headers)
        response.raise_for_status()
        html_content = response.content
    except requests.exceptions.RequestException as e:
        return f"Error fetching the URL: {e}"

    soup = BeautifulSoup(html_content, "html.parser")

    sector_node = soup.select_one(
        "div.quote-links > div:nth-child(1) > a:nth-child(1)"
    )

    industry_node = soup.select_one(
        "div.quote-links > div:nth-child(1) > a:nth-child(3)"
    )
    country_node = soup.select_one(
        "div.quote-links > div:nth-child(1) > a:nth-child(5)"
    )

    investmentDescription_node = soup.find(class_="quote_profile-bio")

    industry = industry_node.text.strip() if industry_node else None
    sector = sector_node.text.strip() if sector_node else None
    country = country_node.text.strip() if country_node else None
    investmentDescription = investmentDescription_node.text.strip() if investmentDescription_node else None

    fundamentals = soup.find("table", class_="snapshot-table2")
    news = soup.find(class_="news-table")

    company_symbol_node = soup.find(
        "h1", class_="quote-header_ticker-wrapper_ticker"
    )
    company_symbol = (
        company_symbol_node.text.strip() if company_symbol_node else current_symbol
    )
    company_content = soup.find("h2", class_="quote-header_ticker-wrapper_company")
    if company_content and company_content.a:
        company_name = company_content.a.text.strip()
    
    stock_data = {
        "company_name": company_name,
        "company_symbol": company_symbol,
        "fundamentals": {},
        "industry": industry,
        "sector": sector,
        "country": country,
        "investmentDescription": investmentDescription,
        "news": {}
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

    if news:
        for row in news.find_all("tr"):
            cells = row.find_all("td")
            for i in range(0, len(cells), 2):
                if i + 1 < len(cells):
                    key = cells[i].text.strip()
                    value = cells[i + 1].text.strip()
                    stock_data["news"][key] = value

    return stock_data
