import pandas as pd
import aiohttp
import asyncio
from lxml import html
from tqdm import tqdm
import logging
import random
import time

# Set up logging to capture errors
logging.basicConfig(level=logging.ERROR, filename="scraping_errors.log",
                    format="%(asctime)s - %(message)s")

CONCURRENCY_LIMIT = 40
MAX_RETRIES = 3


# Function to add a small random delay between retries
async def retry_delay():
    await asyncio.sleep(random.uniform(1, 3))


# Async function to fetch and parse hotel data with retry logic
async def fetch(session, url, semaphore):
    retry_count = 0
    while retry_count < MAX_RETRIES:
        try:
            async with semaphore:
                headers = {
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                                  "Chrome/91.0.4472.124 Safari/537.36"
                }
                async with session.get(url, headers=headers, timeout=10) as response:
                    if response.status == 200:
                        html_content = await response.text()
                        return scrape_policy_info(html_content, url)
                    else:
                        logging.error(f"Error {response.status} for URL: {url}")
                        retry_count += 1
                        await retry_delay()
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            logging.error(f"Request error for URL: {url} - {e}")
            retry_count += 1
            await retry_delay()
    logging.error(f"Failed to fetch URL after {MAX_RETRIES} retries: {url}")
    return None


# Function to extract policy info using lxml
def scrape_policy_info(html_content, url):
    tree = html.fromstring(html_content)
    policy_info = {}
    policy_container = tree.xpath('//div[contains(@class, "a26e4f0adb")]')

    for policy in policy_container:
        service = policy.xpath('.//div[contains(@class, "e1eebb6a1e")]/text()')
        service = service[0].strip() if service else None

        service_info = policy.xpath('.//div[contains(@class, "f565581f7e")]')
        service_info = service_info[0] if service_info else None

        if not service or service_info is None or len(service_info) == 0:
            continue

        service_text = service_info.text_content().strip()
        policy_info[service] = service_text

    return {"url": url, "policy_info": policy_info if policy_info else None}


# Async main function to handle the scraping
async def scrape_policies_from_urls(urls):
    conn = aiohttp.TCPConnector(limit_per_host=CONCURRENCY_LIMIT)
    async with aiohttp.ClientSession(connector=conn) as session:
        semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)
        tasks = [fetch(session, url, semaphore) for url in urls]

        results = []
        for task in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="Scraping policies"):
            policy_data = await task
            if policy_data:
                results.append(policy_data)

        return results


# Function to handle scraping in batches with a delay
async def scrape_with_pause(urls, delay_seconds=150):
    total_urls = len(urls)
    first_batch_size = total_urls // 3  # Calculate 1/3 of the total links
    second_batch_size = total_urls // 3  # Calculate 1/3 of the total links

    # Split the URLs into two batches
    first_batch = urls[:first_batch_size]
    second_batch = urls[first_batch_size:(second_batch_size + first_batch_size)]
    third_batch = urls[(second_batch_size + first_batch_size):]

    print(f"Scraping first batch of {len(first_batch)} links...")
    results = await scrape_policies_from_urls(first_batch)

    print(f"Sleeping for {delay_seconds} seconds...")
    time.sleep(delay_seconds)  # Sleep for the specified time

    print(f"Scraping second batch of {len(second_batch)} links...")
    results += await scrape_policies_from_urls(second_batch)

    print(f"Sleeping for {delay_seconds} seconds...")
    time.sleep(delay_seconds)  # Sleep for the specified time

    print(f"Scraping third batch of {len(second_batch)} links...")
    results += await scrape_policies_from_urls(third_batch)

    return results


# Function to load URLs, scrape data, and save to CSV
async def scrape_policies_to_csv(input_csv, output_csv):
    # Load URLs from the CSV file
    df_hotels = pd.read_csv(input_csv)
    urls = df_hotels['Url'].tolist()
    total_links = len(urls)

    print(f"Total links to scrape: {total_links}")

    # Check if the file has more than 600 links
    if total_links > 600:
        print("More than 600 links found, splitting into batches...")
        hotel_policy_list = await scrape_with_pause(urls)
    else:
        print("Less than or equal to 600 links, scraping all at once...")
        hotel_policy_list = await scrape_policies_from_urls(urls)

    # Create a DataFrame from the successfully scraped data
    df_policy_info = pd.DataFrame([data for data in hotel_policy_list if data])

    # Save to CSV
    df_policy_info.to_csv(output_csv, index=False)
    print(f"Scraping completed. Data saved to {output_csv}")


