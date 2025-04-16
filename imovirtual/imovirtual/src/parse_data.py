import io
import json
import logging
import os
from datetime import datetime
from io import BytesIO

import boto3
import pandas as pd
from dotenv import load_dotenv

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

handler = logging.StreamHandler()
handler.setFormatter(
    logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
)
logger.addHandler(handler)


# Function to flatten the JSON data and add a location column
def flatten_json(data, location):
    all_data = []
    for item in data:
        flat_item = pd.json_normalize(item, sep="_")
        flat_item["location"] = location
        all_data.append(flat_item)
    return all_data


def convert_timestamp_to_date(timestamp):
    # Convert timestamp to datetime
    date_object = datetime.fromtimestamp(timestamp)
    # Format datetime as 'dd-mm-yyyy'
    formatted_date = date_object.strftime("%d-%m-%Y")
    return formatted_date


# def upload_df_to_backblaze(df, file_name, folder_name):
#     """
#     Uploads a pandas DataFrame to Backblaze B2 storage within a specified folder.
#     Creates the folder if it does not exist.

#     Args:
#         df (pandas.DataFrame): The DataFrame to upload
#         file_name (str): Name to give the file in B2 (should end in .csv)
#         folder_name (str): Name of the folder in B2 where the file should be stored

#     Returns:
#         bool: True if upload was successful, False otherwise

#     Raises:
#         Exception: If there's an error during upload
#     """

#     # Convert DataFrame to CSV in memory
#     csv_buffer = io.StringIO()
#     df.to_csv(csv_buffer, index=False)
#     csv_data = csv_buffer.getvalue().encode('utf-8')

#     # Setup B2 client
#     b2_api = B2Api()
#     b2_api.authorize_account("production", os.getenv("B2_KEY_ID"), os.getenv("B2_APPLICATION_KEY"))

#     # Prepare upload path and metadata
#     remote_path = f"{folder_name}/{file_name}"
#     bucket = b2_api.get_bucket_by_name(os.getenv("B2_BUCKET"))
#     file_info = {'Content-Type': 'text/csv'}

#     try:
#         # Upload data directly from memory
#         bucket.upload_bytes(
#             data_bytes=csv_data,
#             file_name=remote_path,
#             file_info=file_info
#         )
#         return True
#     except Exception as e:
#         print(f"Error uploading to B2: {str(e)}")
#         return False


def convert_timestamp_to_date(timestamp: float) -> str:
    return datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d")


# def process_and_upload_data(
#     source_path: str,
#     aws_access_key_id: str,
#     aws_secret_access_key: str,
#     region_name: str,
#     s3_folder: str,
# ) -> None:
#     """
#     Process JSON files in a given directory, flatten the data, and upload the result as a Parquet file to S3.

#     Parameters:
#     - source_path (str): The local directory containing JSON files.
#     - aws_access_key_id (str): AWS access key ID.
#     - aws_secret_access_key (str): AWS secret access key.
#     - region_name (str): AWS region name.
#     - bucket (str): S3 bucket name.
#     - s3_folder (str): Folder path in the S3 bucket to upload the processed file.
#     """

#     # Initialize S3 client
#     s3 = boto3.client(
#         "s3",
#         aws_access_key_id=aws_access_key_id,
#         aws_secret_access_key=aws_secret_access_key,
#         region_name=region_name,
#     )

#     # List JSON files in the source directory
#     files = [
#         pos_json for pos_json in os.listdir(source_path) if pos_json.endswith(".json")
#     ]

#     all_data = []

#     for file in files:
#         file_path = os.path.join(source_path, file)
#         print(f"Processing file: {file_path}")
#         with open(file_path, "r", encoding="utf-8") as f:
#             data = json.load(f)

#         for key, items in data.items():
#             all_data.extend(flatten_json(items, key))

#     df_flat = pd.concat(all_data, ignore_index=True)
#     df_flat["date"] = convert_timestamp_to_date(os.stat(file_path).st_ctime)

#     # Dropping columns containing '__typename' and 'images'
#     df_flat = df_flat[
#         [
#             col
#             for col in df_flat.columns
#             if "__typename" not in col and "images" not in col
#         ]
#     ]

#     # Generate the filename for the Parquet file
#     current_date = df_flat["date"].iloc[0]
#     filename = f"{s3_folder}imovirtual_data_{current_date}.parquet"
