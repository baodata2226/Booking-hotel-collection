import asyncio
from playwright.async_api import async_playwright
import re
from tabulate import tabulate
import pandas as pd


async def scrape_booking_data(url):
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36"
        )
        page = await context.new_page()

        # Navigate to base URL
        await page.goto(url, wait_until="domcontentloaded")

        # Modify URL parameters explicitly
        await page.evaluate('''() => {
            const params = new URLSearchParams(window.location.search);
            params.set("checkin", "2024-12-30");
            params.set("checkout", "2024-12-31");
            window.location.search = params.toString();
        }''')
        await page.wait_for_timeout(3000)

        # # Debugging: Check if the page has loaded correctly
        # await page.screenshot(path="debug.png", full_page=True)

        table = await page.query_selector('table#hprt-table')
        room_list = []

        if table:
            rows = await table.query_selector_all('tbody > tr')
            for row in rows:
                room_data = {"url": url}

                # Extract price discount
                price_element = await row.query_selector('span.prco-valign-middle-helper')
                if price_element:
                    price_text = re.sub(r'\s+', ' ', await price_element.inner_text()).strip()
                    price_clean = float(re.search(r'VND\s*([\d.,]+)', price_text).group(1).replace('.', ''))
                    room_data['price_discount'] = price_text

                    # Extract original price if available
                    original_price_element = await row.query_selector(
                        'div.bui-f-color-destructive.js-strikethrough-price'
                        '.prco-inline-block-maker-helper.bui-price-display__original'
                    )
                    if original_price_element:
                        original_price_text = await original_price_element.inner_text()
                        original_price_clean = float(
                            re.search(r'VND\s*([\d.,]+)', original_price_text).group(1).replace('.', ''))
                        room_data['original_price'] = original_price_text
                        room_data['discount'] = 100 - round((price_clean / original_price_clean) * 100)
                    else:
                        room_data['original_price'] = price_text
                        room_data['discount'] = 0

                    room_list.append(room_data)
        else:
            room_list.append({"url": url, "original_price": None, "price_discount": None, "discount": None})

        await browser.close()
        return room_list


async def main():
    url = "https://www.booking.com/hotel/vn/khach-san-phuoc-hung.vi.html"
    data = await scrape_booking_data(url)
    df_rooms = pd.DataFrame(data)
    print(tabulate(df_rooms, headers='keys', tablefmt='fancy_grid'))


asyncio.run(main())
