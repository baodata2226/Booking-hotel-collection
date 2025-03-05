import re
import pandas as pd
import asyncio
from playwright.async_api import async_playwright
from tqdm.asyncio import tqdm
import random

# List of user agents for randomized requests
user_agents = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.3 Safari/605.1.15"
]


async def scrape_reviews_from_url(url, context):
    """Scrape reviews from a single URL using Playwright."""
    try:
        page = await context.new_page()
        new_url = f"{url}#tab-reviews"
        await page.goto(new_url, timeout=120000)
        await page.wait_for_timeout(timeout=5000)  # For headless mode
        await page.wait_for_selector('div[class="c0528ecc22"]', timeout=60000)

        if await page.locator('div[class="dd5dccd82f"]').count() == 0:
            print(f"No reviews found on this page: {url}")
            return {url: []}

        reviews = []

        while True:
            # Wait for review cards to load
            review_cards = await page.locator('div[data-testid="review-card"]').all()
            if not review_cards:
                print(f"No reviews found on this page: {url}")
                break

            for review in review_cards:
                username = await review.locator('div[class="a3332d346a e6208ee469"]').inner_text(timeout=10000) if await review.locator('div[class="a3332d346a e6208ee469"]').count() > 0 else None
                created_at = None
                if await review.locator('span[data-testid="review-date"]').count() > 0:
                    created_at_text = await review.locator('span[data-testid="review-date"]').inner_text(timeout=10000)
                    created_at = created_at_text.split(": ")[1] if ": " in created_at_text else created_at_text

                score = None
                score_element = review.locator('div[class="ac4a7896c7"]')
                if await score_element.count() > 0:
                    score_text = await score_element.inner_text(timeout=10000)
                    score_match = re.search(r"\d+[.,]?\d*", score_text.strip())
                    score = score_match.group() if score_match else None

                stay_at = await review.locator('span[data-testid="review-stay-date"]').inner_text(timeout=10000) if await review.locator('span[data-testid="review-stay-date"]').count() > 0 else None

                comment = {"title": None, "positive": None, "negative": None}
                if await review.locator('h3[data-testid="review-title"]').count() > 0:
                    comment["title"] = await review.locator('h3[data-testid="review-title"]').inner_text(timeout=10000)

                positive_text = review.locator('div[data-testid="review-positive-text"]')
                if await positive_text.count() > 0:
                    comment["positive"] = await positive_text.inner_text(timeout=10000)

                negative_text = review.locator('div[data-testid="review-negative-text"]')
                if await negative_text.count() > 0:
                    comment["negative"] = await negative_text.inner_text(timeout=10000)

                reviews.append({
                    "username": username,
                    "score": score,
                    "comment": comment,
                    "created_at": created_at,
                    "stay_at": stay_at
                })

            next_button = page.locator('button[aria-label="Trang sau"]')
            if await next_button.count() == 0 or not await next_button.is_enabled():
                break
            await next_button.click()
            await page.wait_for_selector('div[data-testid="review-card"]', timeout=10000)

        await page.close()
        return {url: {"reviews": reviews, "review_count": len(reviews)}}
    except Exception as e:
        print(f"Error scraping {url}: {e}")
        return {url: {"reviews": [], "review_count": 0}}


async def run_review_scraper_in_batches(urls, batch_size=6):
    """Run the review scraper in parallel batches."""
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=["--disable-blink-features=AutomationControlled", "--no-sandbox", "--disable-setuid-sandbox",
                  "--blink-settings=imagesEnabled=false",
                  "--disable-features=SitePerProcess",
                  "--disable-extensions"
                  ]
        )
        context = await browser.new_context(
            viewport={"width": 1920, "height": 1080},
            java_script_enabled=True,
            user_agent=random.choice(user_agents),
        )

        results = []
        for i in range(0, len(urls), batch_size):
            batch = urls[i:i + batch_size]
            print(f"Processing batch {i // batch_size + 1}/{(len(urls) + batch_size - 1) // batch_size}")

            tasks = [scrape_reviews_from_url(url, context) for url in batch]
            async for result in tqdm(
                    asyncio.as_completed(tasks),
                    total=len(batch),
                    desc=f"Batch {i // batch_size + 1} Progress"
            ):
                results.append(await result)

        await browser.close()
        return results


async def main(input_csv, output_csv):
    """Main function to orchestrate review scraping."""
    df_urls = pd.read_csv(input_csv)
    urls = df_urls["Url"].tolist()
    print(f"Total URLs to scrape: {len(urls)}")

    scraped_data = await run_review_scraper_in_batches(urls, batch_size=6)

    # Flatten results and save to CSV
    all_reviews = []
    for result in scraped_data:
        for url, data in result.items():
            review_count = data["review_count"]
            for review in data["reviews"]:
                review["hotel_url"] = url
                review["review_count"] = review_count
                all_reviews.append(review)

    df_reviews = pd.DataFrame(all_reviews)
    df_reviews.to_csv(output_csv, index=False)
    print(f"Scraping completed. Reviews saved to {output_csv}")


input_csv = "urls/Bac Lieu_urls.csv"
output_csv = "urls/Bac Lieu_reviews.csv"
asyncio.run(main(input_csv, output_csv))