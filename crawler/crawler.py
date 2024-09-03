import json
import logging
import os
import time
import requests
from bs4 import BeautifulSoup

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('crawler.log'),
        logging.StreamHandler()
    ]
)


def safe_request(url, retries=3, timeout=10):
    """Safely makes a request with retries and timeout."""
    for attempt in range(retries):
        try:
            response = requests.get(url, timeout=timeout)
            response.raise_for_status()
            return response
        except requests.RequestException as e:
            logging.error(f"Request to {url} failed on attempt {attempt + 1}: {e}")
            time.sleep(2)  # Wait before retrying
    logging.error(f"Failed to retrieve {url} after {retries} attempts.")
    return None


class Crawler:
    def __init__(self, url):
        self.url = url
        self.extracted_data = {}
        self.img_urls = []
        self.downloaded_img_paths = []

    def extract_data(self):
        response = safe_request(self.url)
        if not response:
            return

        soup = BeautifulSoup(response.content, 'html.parser')
        scripts = soup.find_all('script', type='text/x-magento-init')

        for script in scripts:
            try:
                json_str = script.string.strip()
                json_data = json.loads(json_str)
                for key, value in json_data.items():
                    if "enhancedDataLayer" in value:
                        data_layer = value["enhancedDataLayer"].get("data", [])
                        if data_layer:
                            ecommerce_data = data_layer[0].get("ecommerce", {})
                            self.extracted_data = ecommerce_data.get("items", [])
                        break
            except json.JSONDecodeError as e:
                logging.error(f"JSON decoding error: {e}")
            except Exception as e:
                logging.error(f"Unexpected error during data extraction: {e}", exc_info=True)

    def extract_image_urls(self):
        response = safe_request(self.url)
        if not response:
            return

        soup = BeautifulSoup(response.content, 'html.parser')
        ld_json_scripts = soup.find_all('script', type='application/ld+json')
        ld_json_string = ld_json_scripts[2].string.replace('\r', '').replace('\n', '')

        try:
            data = json.loads(ld_json_string)
            self.img_urls = data.get("image", [])
        except json.JSONDecodeError as e:
            logging.error(f"JSON decoding error: {e}")

    def download_img(self, retries=3, timeout=10):
        img_dir = 'downloaded_images'
        if not os.path.exists(img_dir):
            os.makedirs(img_dir)

        product_id = self.extracted_data[0]['item_id'] if self.extracted_data else 'unknown_product'

        for idx, img_url in enumerate(self.img_urls):
            success = False
            for attempt in range(retries):
                try:
                    response = safe_request(img_url, retries=1, timeout=timeout)
                    if not response:
                        continue

                    img_filename = os.path.join(img_dir, f'{product_id}_{idx + 1}.jpg')
                    with open(img_filename, 'wb') as handler:
                        handler.write(response.content)
                    self.downloaded_img_paths.append(img_filename)
                    success = True
                    break  # Exit the retry loop on success
                except Exception as e:
                    logging.error(f"Error downloading image from {img_url} on attempt {attempt + 1}: {e}")
                    time.sleep(2)  # Wait before retrying
            if not success:
                logging.error(f"Failed to download image after {retries} attempts: {img_url}")

    def run(self):
        self.extract_data()
        self.extract_image_urls()
        self.download_img()
        self.save_data()

    def save_data(self):
        product_id = self.extracted_data[0]['item_id'] if self.extracted_data else 'unknown_product'
        data_to_save = {
            'product_id': product_id,
            'extracted_data': self.extracted_data,
            'downloaded_img_paths': self.downloaded_img_paths
        }

        with open('products_data.json', 'a') as json_file:
            json_file.write(json.dumps(data_to_save) + '\n')
        logging.info(f"Data for {product_id} saved to products_data.json")
