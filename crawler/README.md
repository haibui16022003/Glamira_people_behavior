# Web Scraping

This project is a web scraping tool designed to extract product URLs from a sitemap and process them using a custom `Crawler` class. It fetches URLs, processes them concurrently, and saves the processed URLs to avoid reprocessing.

## Table of Contents
- [Setup](#setup)
- [Usage](#usage)
- [Configuration](#configuration)
- [Logging](#logging)
- [Notes](#notes)
- [Contributing](#contributing)
- [License](#license)

## Setup

1. **Clone the repository**:
    ```bash
    git clone <repository-url>
    cd <repository-name>
    ```

2. **Install the required dependencies**:
    Make sure you have `pip` installed, then run:
    ```bash
    pip install -r requirements.txt
    ```

3. **Verify project files and directories**:
    Ensure the following files are present:
    - `crawler.py` (contains the `Crawler` class)
    - `crawler.log` (log file created automatically)
    - `product_urls.txt` (optional, for saving product URLs)
    - `processed_urls.txt` (stores processed URLs)

## Usage

1. **Running the Script**:
    To start the scraper, run the script:
    ```bash
    python <script_name>.py
    ```

2. **Resuming from a Specific Index**:
    If you need to resume processing from a specific index, adjust the `start_index` parameter in the script:
    ```python
    if __name__ == '__main__':
        start_index = 32824
        main(start_index=start_index)
    ```

3. **Command-Line Arguments**:
    - `start_index`: Optional parameter to start processing URLs from a specific index.

## Configuration

- **Sitemap URL**:
  The default sitemap URL is `https://www.glamira.com/sitemap.xml`. Update it in the `fetch_sitemap` function if needed.

- **Concurrency**:
  The script uses a `ThreadPoolExecutor` with 3 workers by default. Adjust the `max_workers` parameter in the `main` function for more or fewer concurrent processes based on your system's performance.

## Logging

- Logs are saved to `crawler.log` and also printed to the console.
- Log entries include timestamps, levels, and messages to help track progress and identify errors during scraping.

## Notes

- Make sure the `downloaded_images` directory is ignored in Git by adding it to the `.gitignore` file.
- The script skips URLs listed in `processed_urls.txt` to avoid reprocessing.
- Adjust the logging level and format in the `logging.basicConfig` setup as needed.
