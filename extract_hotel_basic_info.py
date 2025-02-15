import asyncio
import aiohttp
from selectolax.parser import HTMLParser
import pandas as pd
from tqdm.asyncio import tqdm
import re


# Function to fetch HTML content for a URL
async def fetch_html(session, url):
    async with session.get(url) as response:
        response.raise_for_status()
        return await response.text()


# Function to scrape hotel data
async def scrape_hotel_data(session, url):
    html_content = await fetch_html(session, url)
    tree = HTMLParser(html_content)

    # Extract hotel name
    hotel_name_elem = tree.css_first('h2.d2fee87262.pp-header__title')
    hotel_name = hotel_name_elem.text().strip() if hotel_name_elem else None

    # Extract hotel location with multiple selectors
    hotel_location = None
    location_selectors = [
        'span[data-node_tt_id="location_score_tooltip"]',  # Original selector
        'span[data-source="top_link"]'
    ]
    for selector in location_selectors:
        location_elem = tree.css_first(selector)
        if location_elem:
            hotel_location = location_elem.text().strip()
            break

    # Extract hotel description
    description_elem = tree.css_first('p[data-testid="property-description"]')
    hotel_description = description_elem.text().strip() if description_elem else None

    # Extract hotel point using regex
    hotel_point = None
    point_div = tree.css_first('div.a3b8729ab1.d86cee9b25')
    if point_div:
        next_div = point_div.css_first('div.ac4a7896c7')
        if next_div:
            match = re.search(r"Đạt điểm (\d+([,.]\d+)?)", next_div.text().strip())
            hotel_point = match.group(1) if match else None

    # Fallback for extracting hotel point
    if not hotel_point:
        point_div_alt = tree.css_first('div.a3b8729ab1.e6208ee469.cb2cbb3ccb')
        if point_div_alt:
            match = re.search(r"\D(\d+[.,]?\d*)\s*$", point_div_alt.text().strip())
            hotel_point = match.group(1) if match else None

    # Extract hotel star rating (count of star elements)
    hotel_star_elems = tree.css('span.fcd9eec8fb.d31eda6efc.c25361c37f')
    hotel_star = len(hotel_star_elems) if hotel_star_elems else None

    # Prepare hotel data dictionary
    hotel_data = {
        "url": url,
        "name": hotel_name,
        "location": hotel_location,
        "description": hotel_description,
        "point": hotel_point,
        "star": hotel_star
    }

    return hotel_data


# Function to handle multiple URLs and gather results with progress bar
async def scrape_hotels_from_urls(urls):
    async with aiohttp.ClientSession() as session:
        tasks = [scrape_hotel_data(session, url) for url in urls]
        results = await tqdm.gather(*tasks, desc="Scraping Hotel Data", total=len(urls))
    return results


# Main function to orchestrate scraping and save data to CSV
async def scrape_hotels_to_csv(input_csv, output_csv):
    # Load URLs from the CSV file
    df_hotels = pd.read_csv(input_csv)
    urls = df_hotels['Url'].tolist()
    total_links = len(urls)

    print(f"Total links to scrape: {total_links}")
    hotel_data_list = await scrape_hotels_from_urls(urls)

    # Convert the list of dictionaries to a DataFrame
    df_hotel_info = pd.DataFrame(hotel_data_list)

    # Save to CSV
    df_hotel_info.to_csv(output_csv, index=False)
    print(f"Scraping completed. Data saved to {output_csv}")

asyncio.run(scrape_hotels_to_csv('/content/Tuyen Quang_urls.csv', '/content/Tuyen Quang_hotels.csv'))
