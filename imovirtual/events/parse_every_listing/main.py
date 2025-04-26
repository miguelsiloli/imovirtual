# flow.py
"""
Main Prefect flow for scraping Imovirtual listings and uploading to GCS.
"""

import requests
from bs4 import BeautifulSoup
import json
import time
import random
from datetime import datetime, timezone
from typing import List, Dict, Optional, Any

from prefect import task, flow, get_run_logger
from prefect.task_runners import ConcurrentTaskRunner
# Import constants from config.py
from config import (
    BASE_URL,
    REQUEST_TIMEOUT,
    MIN_SLEEP,
    MAX_SLEEP,
    GCS_INPUT_PATH_PREFIX
)
# Import GCS/utility tasks and functions from utils.py
from utils import (
    load_environment_variables,
    get_gcs_credentials_from_env,
    get_latest_gcs_parquet_uri,
    read_slugs_from_gcs_parquet,
    save_data_to_gcs
)

# --- Scraping Task ---

@task(retries=2, retry_delay_seconds=random.randint(3, 7))
def scrape_single_url(url: str) -> Optional[Dict[str, Any]]:
    """
    Fetches URL using its OWN session, finds __NEXT_DATA__, parses JSON,
    adds ingestionDate & sourceUrl.
    Returns the processed data as a dictionary or None on failure.
    """
    logger = get_run_logger()
    with requests.Session() as session:
        try:
            sleep_duration = random.uniform(MIN_SLEEP, MAX_SLEEP)
            logger.debug(f"Sleeping for {sleep_duration:.2f}s before fetching {url}")
            time.sleep(sleep_duration)

            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36',
                'Accept-Language': 'en-US,en;q=0.9,pt;q=0.8',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
                'Referer': 'https://www.google.com/',
                'DNT': '1',
                'Upgrade-Insecure-Requests': '1',
            }
            logger.debug(f"Requesting URL: {url}")
            response = session.get(url, timeout=REQUEST_TIMEOUT, headers=headers)
            response.raise_for_status()

            soup = BeautifulSoup(response.content, 'lxml')
            script_tag = soup.find('script', {'id': '__NEXT_DATA__'})

            if not script_tag or not script_tag.string:
                logger.warning(f"Could not find or read __NEXT_DATA__ script tag on {url}")
                return None

            try:
                data = json.loads(script_tag.string)
            except json.JSONDecodeError as e:
                logger.error(f"Failed to decode JSON from {url}: {e} - Content snippet: {script_tag.string[:200]}...")
                return None

            data['ingestionDate'] = datetime.now(timezone.utc).date().isoformat()
            data['sourceUrl'] = url
            logger.debug(f"Successfully scraped and processed {url}")
            return data

        except requests.exceptions.Timeout:
            logger.warning(f"Request timed out ({REQUEST_TIMEOUT}s) for {url}")
            return None
        except requests.exceptions.HTTPError as e:
             logger.warning(f"HTTP Error {e.response.status_code} for {url}: {e}")
             return None
        except requests.exceptions.RequestException as e:
            logger.warning(f"Request failed for {url}: {e}")
            return None
        except Exception as e:
            logger.error(f"An unexpected error occurred while processing {url}: {e}", exc_info=True)
            return None

# --- Prefect Flow ---

@flow(
    name="Imovirtual Scrape and Upload Flow (Split Files)",
    task_runner=ConcurrentTaskRunner(max_workers=2)
)
def scrape_and_upload_flow(batch_size: int = 500):
    """
    Main flow: Finds latest slug file (GCS), scrapes listings concurrently in batches,
    uploads results (GCS). Uses utilities from utils.py.
    
    Args:
        batch_size: Number of URLs to process in each batch (default: 500)
    """
    logger = get_run_logger()
    logger.info(f"Starting Imovirtual scrape and upload flow with batch size: {batch_size}...")

    # --- Load Environment and Credentials ---
    gcs_bucket_name = load_environment_variables()  # Task returns bucket name
    gcs_credentials = get_gcs_credentials_from_env()  # Helper function
    if not gcs_credentials:
        logger.error("Failed to obtain GCS credentials. Aborting flow.")
        raise ValueError("GCS Credentials could not be loaded.")

    logger.info(f"Input path prefix: gs://{gcs_bucket_name}/{GCS_INPUT_PATH_PREFIX}")
    # Output path prefix is used within the save_data_to_gcs task

    # --- Find Latest Input File in GCS ---
    latest_input_uri = get_latest_gcs_parquet_uri(
        gcs_bucket_name=gcs_bucket_name,
        gcs_prefix=GCS_INPUT_PATH_PREFIX,
        gcs_credentials=gcs_credentials,
        wait_for=[gcs_bucket_name]  # Ensure env vars loaded first
    )

    # --- Read Input Slugs from GCS ---
    slugs = read_slugs_from_gcs_parquet(
        gcs_uri=latest_input_uri,
        gcs_credentials=gcs_credentials
    )

    # --- Generate URLs ---
    urls_to_scrape = [BASE_URL + slug for slug in slugs]
    total_urls = len(urls_to_scrape)
    logger.info(f"Generated {total_urls} URLs to scrape in batches of {batch_size}.")

    # --- Process in batches ---
    all_successful_data = []
    total_success = 0
    total_failure = 0
    
    # Create batches
    batches = [urls_to_scrape[i:i + batch_size] for i in range(0, total_urls, batch_size)]
    
    for batch_index, batch in enumerate(batches, 1):
        logger.info(f"Processing batch {batch_index}/{len(batches)} with {len(batch)} URLs...")
        
        # --- Scrape Data Concurrently within this batch ---
        scrape_futures = scrape_single_url.map(batch)
        
        # Collect results, filtering out None (failures)
        results = [future.result() for future in scrape_futures if future is not None]
        successful_data = [res for res in results if res is not None]
        
        success_count = len(successful_data)
        failure_count = len(batch) - success_count
        total_success += success_count
        total_failure += failure_count
        
        logger.info(f"Batch {batch_index} scraping finished. Success: {success_count}, Failures: {failure_count}")
        all_successful_data.extend(successful_data)
        
        # Optional: Add a small delay between batches to avoid overwhelming the target server
        if batch_index < len(batches):
            time.sleep(random.uniform(1, 3))  # Small delay between batches

    # --- Final Stats ---
    logger.info(f"All batches processed. Total success: {total_success}, Total failures: {total_failure}")

    # --- Save to GCS ---
    if all_successful_data:
        try:
            # Pass the bucket name explicitly now
            gcs_file_path = save_data_to_gcs(
                data_list=all_successful_data,
                gcs_bucket_name=gcs_bucket_name,
                gcs_credentials=gcs_credentials
                # Wait for scraping results implicitly via all_successful_data
            )
            logger.info(f"Flow finished. Final status/output path: {gcs_file_path}")
            return gcs_file_path
        except Exception as e:
            logger.error(f"Critical Error saving data to GCS: {e}. Flow failed.", exc_info=True)
            raise
    else:
        logger.warning("No data was successfully scraped. Final GCS save skipped.")
        return "No data scraped, upload skipped."
    
if __name__ == "__main__":
    scrape_and_upload_flow()