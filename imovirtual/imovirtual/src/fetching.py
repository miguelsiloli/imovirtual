import logging
import time

import requests
from tqdm import tqdm

from .utils import retry_on_failure


@retry_on_failure(retries=3, delay=60)
def make_api_call(url: str, headers: dict) -> dict:
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
        time.sleep(2)
        return data
    except Exception as e:
        logging.error(f"Error in API call to {url}: {str(e)}")
        raise


def fetch_district_data(
    district: str, id_list: list, base_url_template: str, headers: dict, buildid: str
) -> dict:
    """Fetches data for a district from the API, handling pagination."""
    all_data = {}
    for id in tqdm(id_list, desc=f"Processing IDs for {district}", leave=False):
        url = base_url_template.format(buildid, id)
        try:
            data = make_api_call(url, headers)
            all_data[id] = data["pageProps"]["data"]["searchAds"]["items"]
            num_pages = data["pageProps"]["tracking"]["listing"]["page_count"]
            if num_pages > 1:
                for page in tqdm(
                    range(2, num_pages + 1), desc="Fetching pages", leave=False
                ):
                    paged_url = f"{url}?page={page}"
                    paged_data = make_api_call(paged_url, headers)
                    all_data[id].extend(
                        paged_data["pageProps"]["data"]["searchAds"]["items"]
                    )
            logging.info(f"Successfully processed ID {id} with {num_pages} pages")
        except Exception as e:
            logging.error(f"Error processing ID {id}: {str(e)}")
            continue
    return all_data
