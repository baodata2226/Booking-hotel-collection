import asyncio
import aiohttp
from selectolax.parser import HTMLParser
import re
import pandas as pd
from tqdm.asyncio import tqdm


async def fetch_html(session, url):
    async with session.get(url) as response:
        response.raise_for_status()
        return await response.text()


async def get_faqs_selectolax(session, url):
    html_content = await fetch_html(session, url)
    tree = HTMLParser(html_content)

    # Extract questions and answers using CSS selectors
    questions = [q.text().strip() for q in tree.css('h3.bui-accordion__title')]
    raw_answers = [a.text().strip() for a in tree.css('div.bui-accordion__content')]

    # Process answers to handle special character patterns and spacing
    answers = []
    for answer in raw_answers:
        answer = answer.replace("\ufeff", " ")
        answer = re.sub(
            r'(?<=[a-zA-Zàáâãèéêìíòóôõùúăđĩũơưạảấầẫậắằẳẵặẹẻẽềểễệỉịọỏốồổỗộớờởỡợụủứừửữựỳỵỷ'
            r'ỹ])(?=[A-ZÀÁÂÃÈÉÊÌÍÒÓÔÕÙÚĂĐĨŨƠƯẠẢẤẦẪẬẮẰẲẴẶẸẺẼỀỂỄỆỈỊỌỐỒỔỖỘỚỜỞỠỢỤỦỨỪỬỮỰỲỴỶ'
            r'(?<=[a-zA-Zàáâãèéêìíòóôõùúăđĩũơưạảấầẫậắằẳẵặẹẻẽềểễệỉịọỏốồổỗộớờởỡợụủứừửữựỳỵỷỹ])'
            r'(?=[A-ZÀÁÂÃÈÉÊÌÍÒÓÔÕÙÚĂĐĨŨƠƯẠẢẤẦẪẬẮẰẲẴẶẸẺẼỀỂỄỆỈỊỌỐỒỔỖỘỚỜỞỠỢỤỦỨỪỬỮỰỲỴỶŸ]'
            r'[a-zàáâãèéêìíòóôõùúăđĩũơưạảấầẫậắằẳẵặẹẻẽềểễệỉịọỏốồổỗộớờởỡợụủứừửữựỳỵỷỹ])'
            r'|(?<=[a-zàáâãèéêìíòóôõùúăđĩũơưạảấầẫậắằẳẵặẹẻẽềểễệỉịọỏốồổỗộớờởỡợụủứừửữựỳỵỷỹ])'
            r'(?=[A-ZÀÁÂÃÈÉÊÌÍÒÓÔÕÙÚĂĐĨŨƠƯẠẢẤẦẪẬẮẰẲẴẶẸẺẼỀỂỄỆỈỊỌỐỒỔỖỘỚỜỞỠỢỤỦỨỪỬỮỰỲỴỶŸ])',
            ', ', answer
        )
        answer = re.sub(r'(?<=[a-zA-Z\u00C0-\u017F])(?=[A-Z])', ', ', answer)
        answers.append(answer)

    # Pair questions with their corresponding answers
    faqs = dict(zip(questions, answers))
    return {url: faqs}


async def scrape_hotels(urls):
    async with aiohttp.ClientSession() as session:
        tasks = [get_faqs_selectolax(session, url) for url in urls]
        # Wrap async task results with tqdm to track progress
        results = await tqdm.gather(*tasks, desc="Scraping FAQs", total=len(urls))
    return results


# Function to read URLs from CSV and save results to CSV
async def scrape_faqs_to_csv(input_csv, output_csv):
    # Load URLs from the CSV file
    df_hotels = pd.read_csv(input_csv)
    urls = df_hotels['Url'].tolist()
    total_links = len(urls)

    print(f"Total links to scrape: {total_links}")
    hotel_faqs_list = await scrape_hotels(urls)

    # Flatten the list of dictionaries
    results = []
    for hotel_faq in hotel_faqs_list:
        for url, faqs in hotel_faq.items():
            results.append([url, faqs])

    # Create DataFrame with URL as first column and FAQs as second column
    df_faq_info = pd.DataFrame(results, columns=['Url', 'FAQs'])

    # Save to CSV
    df_faq_info.to_csv(output_csv, index=False)
    print(f"Scraping completed. Data saved to {output_csv}")


async def main():
    input_csv = 'Long An_urls.csv'
    output_csv = 'Long An_faqs.csv'
    await scrape_faqs_to_csv(input_csv, output_csv)

# Run the asynchronous main function
asyncio.run(main())
