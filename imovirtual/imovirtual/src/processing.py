import glob
import json
import logging
import os
from datetime import datetime

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from .fetching import fetch_district_data


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
    # from .persistence import save_district_data
    from .graphql_main import save_district_data  # keeping it here for now

    save_district_data(district, all_data, output_dir)


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
                        item["ingestionDate"] = datetime.now().strftime(
                            "%Y-%m-%d"
                        )  # Add the 'scrapped_datetime'
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
