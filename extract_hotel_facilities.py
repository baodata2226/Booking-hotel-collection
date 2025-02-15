import asyncio
import aiohttp
from selectolax.parser import HTMLParser
import pandas as pd
from tqdm.asyncio import tqdm


async def fetch_html(session, url):
    async with session.get(url) as response:
        response.raise_for_status()
        return await response.text()


async def scrape_hotel_facilities(session, url, hotel_id):
    html_content = await fetch_html(session, url)
    tree = HTMLParser(html_content)

    hotel_facilities = {}

    # Scraping the best facilities
    best_facilities_section = tree.css_first('ul.c807d72881.d1a624a1cc.e10711a42e')
    if best_facilities_section:
        best_facilities_items = best_facilities_section.css('li')
        best_facilities_set = {item.text().strip() for item in best_facilities_items}  # Using set to avoid duplicates
        hotel_facilities['best_facilities'] = list(best_facilities_set)  # Convert set back to list
    else:
        hotel_facilities['best_facilities'] = None

    # Scraping all other facilities
    all_facilities_data = {}
    facility_sections = tree.css('div.f1e6195c8b')
    for section in facility_sections:
        facility_type_elem = section.css_first('div.e1eebb6a1e.e6208ee469.d0caee4251')
        facility_type = facility_type_elem.text().strip() if facility_type_elem else 'Unknown'

        facility_items = section.css('li')

        if facility_items:
            # If <li> elements are found, extract their text into a set
            facility_set = {item.text().strip() for item in facility_items}
        else:
            # If no <li> elements, extract text from the div
            single_facility_elem = section.css_first('div.a53cbfa6de.f45d8e4c32.df64fda51b')
            facility_set = {single_facility_elem.text().strip()} if single_facility_elem else set()

        # Check if any facility includes "Phụ phí" and add a dash
        facility_set = {
            facility.replace("Phụ phí", "- Phụ phí") if "Phụ phí" in facility else facility
            for facility in facility_set
        }

        all_facilities_data[facility_type] = list(facility_set)

    hotel_facilities["all_facilities"] = all_facilities_data

    return {url: hotel_facilities}


async def scrape_facilities_from_urls(urls):
    async with aiohttp.ClientSession() as session:
        tasks = [scrape_hotel_facilities(session, url, hotel_id) for hotel_id, url in enumerate(urls, start=1)]
        # Wrap async task results with tqdm to track progress
        results = await tqdm.gather(*tasks, desc="Scraping Hotel Facilities", total=len(urls))
    return results


async def scrape_facilities_to_csv(input_csv, output_csv):
    # Load URLs from the CSV file
    df_hotels = pd.read_csv(input_csv)
    urls = df_hotels['Url'].tolist()
    total_links = len(urls)

    print(f"Total links to scrape: {total_links}")
    hotel_facilities_list = await scrape_facilities_from_urls(urls)

    # Flatten the list of dictionaries
    results = []
    for hotel_facility in hotel_facilities_list:
        for url, facilities in hotel_facility.items():
            results.append([url, facilities])

    # Create DataFrame with URL as first column and Facilities as second column
    df_facilities_info = pd.DataFrame(results, columns=['Url', 'Facilities'])

    # Save to CSV
    df_facilities_info.to_csv(output_csv, index=False)
    print(f"Scraping completed. Data saved to {output_csv}")


async def main():
    input_csv = 'Yen Bai_urls.csv'
    output_csv = 'Yen Bai_facility.csv'
    await scrape_facilities_to_csv(input_csv, output_csv)

if __name__ == '__main__':
    # Run the asynchronous main function
    asyncio.run(main())
