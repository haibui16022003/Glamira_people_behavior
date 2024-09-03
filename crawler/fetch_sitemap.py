import os
import logging
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from crawler import Crawler

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('crawler.log'),
        logging.StreamHandler()
    ]
)


def fetch_sitemap(url='https://www.glamira.com/sitemap.xml'):
    try:
        response = requests.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        urls = [loc.text for loc in soup.find_all('loc')]
        return urls
    except requests.RequestException as e:
        logging.error(f"Failed to fetch sitemap: {e}")
        return []


def get_product_provider(urls):
    return [url for url in urls if 'product_provider' in url]


def fetch_product_urls(provider_url):
    try:
        response = requests.get(provider_url)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'xml')
        urls = [loc.text for loc in soup.find_all('loc')]
        return urls
    except requests.RequestException as e:
        logging.error(f"Failed to fetch product URLs from {provider_url}: {e}")
        return []


def process_url(url, processed_urls_file):
    try:
        logging.info(f"Processing URL: {url}")
        crawler = Crawler(url)
        crawler.run()

        # Save the successfully processed URL
        with open(processed_urls_file, 'a') as f:
            f.write(url + '\n')
        logging.info(f"Successfully processed URL: {url}")
    except Exception as e:
        logging.error(f"An error occurred while processing {url}: {e}")


def main(start_index=0):
    product_urls_file = 'product_urls.txt'
    processed_urls_file = 'processed_urls.txt'

    # Fetch sitemap and get product provider URLs
    sitemap_urls = fetch_sitemap()
    product_provider_urls = get_product_provider(sitemap_urls)
    product_urls = []

    for provider_url in product_provider_urls:
        product_urls.extend(fetch_product_urls(provider_url))

    logging.info(f"Total product URLs fetched: {len(product_urls)}")

    # Load processed URLs to avoid reprocessing
    if os.path.exists(processed_urls_file):
        with open(processed_urls_file, 'r') as f:
            processed_urls = set(line.strip() for line in f)
    else:
        processed_urls = set()

    # Filter out already processed URLs and apply the start index
    urls_to_process = [url for url in product_urls if url not in processed_urls][start_index:]

    logging.info(f"Starting crawl from index: {start_index}. Total URLs to process: {len(urls_to_process)}")

    # Use multithreading to process URLs concurrently
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = [executor.submit(process_url, url, processed_urls_file) for url in urls_to_process]

        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logging.error(f"Error in processing: {e}")


if __name__ == '__main__':
    start_index = 32824
    main(start_index=start_index)
