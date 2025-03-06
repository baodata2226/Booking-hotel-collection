import asyncio
from playwright.async_api import async_playwright
import random


url_1 = "https://www.booking.com/hotel/vn/happy-family-guesthouse.vi.html"
url_2 = "https://www.booking.com/hotel/vn/mekong-ecolodge-resort.vi.html"

user_agents = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/89.0.4389.82 Safari/537.36",

    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.3 Safari/605.1.15"
]


async def scrape_images_with_playwright(context, url):
    """Function to scrape all images from a single URL using Playwright."""
    images_url = set()  # Use a set to avoid duplicates
    async with async_playwright() as pw:
        page = await context.new_page()

        try:
            new_url = f"{url}?activeTab=photosGallery"
            await page.goto(new_url, wait_until="domcontentloaded", timeout=60000)
            await page.set_viewport_size({"width": 1920, "height": 1080})

            # Scroll to load all images
            await page.evaluate('''() => {
                const gallery = document.querySelector("div.bh-photo-modal-thumbs-grid.js-bh-photo-modal-layout.js-no-close");
                gallery.scrollTo(0, gallery.scrollHeight);
            }''')
            await page.wait_for_selector('img.bh-photo-modal-grid-image', timeout=120000)

            # Extract image URLs
            images = await page.evaluate('''() => {
                return Array.from(document.querySelectorAll('img.bh-photo-modal-grid-image')).map(img => img.src);
            }''')

            return {url: images}

        except Exception as e:
            print(f"Failed to scrape {url}: {e}")

        await page.close()
        return {url: list(images_url)}


async def run_scraper_concurrently(urls):
    """Run the Playwright scraper concurrently and track progress using tqdm."""
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        context = await browser.new_context(viewport={"width": 1920, "height": 1080}, java_script_enabled=True,
                                            user_agent=random.choice(user_agents))

        # Run async scraping tasks concurrently
        tasks = [scrape_images_with_playwright(context, url) for url in urls]
        results = await asyncio.gather(*tasks)

        await browser.close()
    return results

img = asyncio.run(run_scraper_concurrently([url_1]))
print(len(img[0][url_1]))
print(img)
