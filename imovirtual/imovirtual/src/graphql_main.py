import json
import threading
import concurrent.futures
import logging
import os
import time
from functools import wraps

import pandas as pd
import requests
from tqdm import tqdm

import glob
from datetime import datetime

import pyarrow as pa
import pyarrow.parquet as pq
from .fetching import make_api_call
from .persistence import save_district_data


def retry_on_failure(retries=3, delay=60):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            attempts = retries
            while attempts > 0:
                try:
                    return func(*args, **kwargs)
                except requests.RequestException as e:
                    attempts -= 1
                    print(f"Request failed: {e}. Retrying in {delay} seconds...")
                    time.sleep(delay)
            raise Exception(
                f"Failed to complete {func.__name__} after {retries} retries."
            )

        return wrapper

    return decorator

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


def process_district(
    district: str,
    id_list: list,
    base_url_template: str,
    headers: dict,
    buildid: str,
    output_dir: str,
) -> None:
    all_data = fetch_district_data(
        district, id_list, base_url_template, headers, buildid
    )
    save_district_data(district, all_data, output_dir)


def read_district_data(csv_file_path: str) -> dict:
    """Reads district data from a CSV file and returns a dictionary."""
    df = pd.read_csv(csv_file_path) # [:10]
    districts = df.groupby("district")["id"].apply(list).to_dict()
    return districts


def fetch_imovirtual_data(
    csv_file_path: str,
    output_dir: str,
    headers: dict,
    base_url_template: str,
    get_buildid,
    filename,
    workers: int = 4,
) -> None:
    districts = read_district_data(csv_file_path)
    buildid = get_buildid()

    logging.info(f"Starting data collection for {len(districts)} districts")
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(
                process_district,
                district,
                id_list,
                base_url_template,
                headers,
                buildid,
                output_dir,
            ): district
            for district, id_list in districts.items()
        }
        for future in concurrent.futures.as_completed(futures):
            district = futures[future]
            try:
                future.result()
                logging.info(f"Finished processing district: {district}")
            except Exception as e:
                logging.error(f"Error processing district {district}: {e}")

    process_json_files(output_dir, filename)


def process_json_files(output_dir: str, output_file: str) -> None:
    """Processes all JSON files in the output directory into a storage-efficient Parquet file."""

    output_file = os.path.join(output_dir, output_file)

    if os.path.exists(output_file):
        logging.info(f"Parquet file {output_file} already exists. Skipping processing.")
        return

    json_files = glob.glob(os.path.join(output_dir, "*.json"))

    if not json_files:
        logging.info(f"No JSON files found in {output_dir}.")
        return

    all_data = []
    for json_file in json_files:
        try:
            with open(json_file, "r", encoding="utf-8") as f:
                data = json.load(f)
                for id, items in data.items():
                    for item in items:
                        item["id"] = id  # Add the 'id' from the filename
                        all_data.append(item)
        except Exception as e:
            logging.error(f"Error reading JSON file {json_file}: {str(e)}")
            return

    if not all_data:
        logging.info("No data found in JSON files.")
        return

    try:
        table = pa.Table.from_pandas(pd.DataFrame(all_data))
        pq.write_table(table, output_file, compression="snappy")
        logging.info(f"Successfully wrote data to {output_file}")

        # Optionally remove the JSON files
        for json_file in json_files:
            os.remove(json_file)
        logging.info("Successfully removed JSON files.")

    except Exception as e:
        logging.error(f"Error writing Parquet file {output_file}: {str(e)}")
        return
