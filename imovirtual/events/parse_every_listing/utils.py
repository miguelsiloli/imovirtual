# utils.py
"""
Utility functions and Prefect tasks for GCS interaction and environment setup.
"""

import os
import io
import logging # Keep standard logging for potential use outside Prefect contexts
from datetime import datetime, timezone
from typing import List, Tuple, Optional, Dict, Any
from urllib.parse import urlparse

import pandas as pd
from dotenv import load_dotenv
from prefect import task, get_run_logger
from google.oauth2 import service_account
from google.cloud import storage
from google.cloud.exceptions import NotFound

# Import constants from config
from config import GCS_OUTPUT_PATH_PREFIX

# --- Helper Functions ---

def parse_gcs_uri(uri: str) -> Tuple[str, str]:
    """Parses a gs:// URI into bucket name and object path."""
    parsed = urlparse(uri)
    if parsed.scheme != 'gs':
        raise ValueError(f"Invalid GCS URI scheme: {uri}")
    bucket_name = parsed.netloc
    object_name = parsed.path.lstrip('/')
    if not bucket_name or not object_name:
        raise ValueError(f"Could not parse bucket or object name from URI: {uri}")
    return bucket_name, object_name

def get_gcs_credentials_from_env() -> Optional[service_account.Credentials]:
    """
    Loads GCS service account credentials from environment variables.
    Uses Prefect logger if available, otherwise standard logging.
    """
    logger = get_run_logger()

    try:
        required_vars = [
            "TYPE", "PROJECT_ID", "PRIVATE_KEY_ID", "PRIVATE_KEY",
            "CLIENT_EMAIL", "CLIENT_ID", "AUTH_URI", "TOKEN_URI",
            "AUTH_PROVIDER_X509_CERT_URL", "CLIENT_X509_CERT_URL"
        ]
        if not all(os.getenv(var) for var in required_vars):
            logger.info("GCS credential environment variables not found during credential creation.")
            return None
        private_key = os.getenv("PRIVATE_KEY", "").replace('\\n', '\n')
        if not private_key:
            logger.info("PRIVATE_KEY environment variable is missing or empty.")
            return None

        creds_info = {
            "type": os.getenv("TYPE"),
            "project_id": os.getenv("PROJECT_ID"),
            "private_key_id": os.getenv("PRIVATE_KEY_ID"),
            "private_key": private_key,
            "client_email": os.getenv("CLIENT_EMAIL"),
            "client_id": os.getenv("CLIENT_ID"),
            "auth_uri": os.getenv("AUTH_URI"),
            "token_uri": os.getenv("TOKEN_URI"),
            "auth_provider_x509_cert_url": os.getenv("AUTH_PROVIDER_X509_CERT_URL"),
            "client_x509_cert_url": os.getenv("CLIENT_X509_CERT_URL"),
        }
        missing_cred_fields = [k for k, v in creds_info.items() if not v]
        if missing_cred_fields:
            logger.info(f"Missing credential fields loaded from environment: {missing_cred_fields}")
            return None

        credentials = service_account.Credentials.from_service_account_info(creds_info)
        logger.info("Successfully created GCS credentials object from environment.")
        return credentials
    except Exception as e:
        logger.info(f"Failed to create GCS credentials object: {e}", exc_info=True)
        return None

# --- Prefect Tasks for Environment and GCS I/O ---

@task
def load_environment_variables() -> str:
    """Loads environment variables from .env file and checks GCS requirements."""
    logger = get_run_logger()
    loaded = load_dotenv()
    if loaded:
        logger.info("Successfully loaded environment variables from .env file.")
    else:
        logger.warning("No .env file found or it was empty. Ensure required variables are set.")

    required_gcs_vars = [
        "TYPE", "PROJECT_ID", "PRIVATE_KEY_ID", "PRIVATE_KEY",
        "CLIENT_EMAIL", "CLIENT_ID", "AUTH_URI", "TOKEN_URI",
        "AUTH_PROVIDER_X509_CERT_URL", "CLIENT_X509_CERT_URL",
        "GCS_BUCKET_NAME"
    ]
    missing_vars = [var for var in required_gcs_vars if not os.getenv(var)]
    if missing_vars:
         logger.error(f"Missing required GCS/Auth environment variables: {missing_vars}")
         raise ValueError(f"Missing required environment variables: {missing_vars}")

    gcs_bucket_name = os.getenv('GCS_BUCKET_NAME')
    logger.info(f"Using GCS Bucket: {gcs_bucket_name}")
    # Return the bucket name for potential use downstream, ensures task provides output
    return gcs_bucket_name

@task(retries=2, retry_delay_seconds=5)
def get_latest_gcs_parquet_uri(
    gcs_bucket_name: str,
    gcs_prefix: str,
    gcs_credentials: service_account.Credentials
) -> str:
    """Finds the most recently updated Parquet file in a GCS path."""
    logger = get_run_logger()
    logger.info(f"Searching for latest Parquet file in gs://{gcs_bucket_name}/{gcs_prefix}")
    try:
        storage_client = storage.Client(credentials=gcs_credentials)
        bucket = storage_client.bucket(gcs_bucket_name)
        blobs_iterator = bucket.list_blobs(prefix=gcs_prefix)

        latest_blob = None
        latest_update_time = datetime.min.replace(tzinfo=timezone.utc)

        for blob in blobs_iterator:
            if blob.name.endswith(".parquet") and not blob.name.endswith("/"):
                blob_update_time = blob.updated or datetime.min.replace(tzinfo=timezone.utc)
                if blob_update_time > latest_update_time:
                    latest_update_time = blob_update_time
                    latest_blob = blob

        if not latest_blob:
            logger.error(f"No Parquet files found in gs://{gcs_bucket_name}/{gcs_prefix}")
            raise FileNotFoundError(f"No Parquet files found in gs://{gcs_bucket_name}/{gcs_prefix}")

        latest_file_uri = f"gs://{gcs_bucket_name}/{latest_blob.name}"
        logger.info(f"Found latest Parquet file: {latest_file_uri} (Updated: {latest_blob.updated})")
        return latest_file_uri
    except NotFound:
         logger.error(f"Bucket 'gs://{gcs_bucket_name}/' not found or no access.")
         raise
    except Exception as e:
        logger.error(f"Error accessing GCS to find latest file: {e}", exc_info=True)
        raise

@task
def read_slugs_from_gcs_parquet(
    gcs_uri: str,
    gcs_credentials: service_account.Credentials
) -> List[str]:
    """Reads the 'slug' column from a Parquet file located in GCS."""
    logger = get_run_logger()
    logger.info(f"Reading slugs from GCS Parquet file: {gcs_uri}...")
    try:
        bucket_name, blob_name = parse_gcs_uri(gcs_uri)
        storage_client = storage.Client(credentials=gcs_credentials)
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)

        logger.info(f"Downloading blob: {blob_name} from bucket: {bucket_name}...")
        parquet_bytes = blob.download_as_bytes()
        logger.info(f"Downloaded {len(parquet_bytes)} bytes. Reading into DataFrame...")

        parquet_buffer = io.BytesIO(parquet_bytes)
        df_input = pd.read_parquet(parquet_buffer)
        logger.info("Successfully read Parquet data into DataFrame.")

        if 'slug' not in df_input.columns:
            logger.error(f"'slug' column not found in {gcs_uri}")
            raise ValueError(f"'slug' column not found in {gcs_uri}")

        slugs = df_input['slug'].dropna().astype(str).unique().tolist()
        valid_slugs = [slug for slug in slugs if slug]
        logger.info(f"Found {len(valid_slugs)} unique valid slugs from {gcs_uri}.")
        return valid_slugs

    except NotFound:
        logger.error(f"GCS Parquet file not found: {gcs_uri}")
        raise FileNotFoundError(f"GCS Parquet file not found: {gcs_uri}")
    except Exception as e:
        logger.error(f"Error reading Parquet file from GCS {gcs_uri}: {e}", exc_info=True)
        raise


def _patch_empty_nested_experiments_recursive(data_item: Any):
    """
    Recursively finds 'experiments' keys in nested dicts/lists.
    If their value is an empty dict, it adds a dummy child field.
    Modifies data_item in-place.
    """
    # logger = get_run_logger() # Get logger for potential debug messages
    if isinstance(data_item, dict):
        for key, value in list(data_item.items()): # Iterate over a copy of items if modifying dict
            if key == 'experiments':
                if isinstance(value, dict) and not value: # Empty dict {}
                    data_item[key] = {'_dummy_child_': None} # Add dummy child
                    # logger.debug(f"Patched empty nested 'experiments' dict by adding '_dummy_child_'.")
                elif isinstance(value, list):
                    # Handle if 'experiments' is a list that might contain empty dicts
                    # or if the list itself is empty and infers to list<struct<>>
                    new_list = []
                    modified_list = False
                    for elem in value:
                        if isinstance(elem, dict) and not elem:
                            new_list.append({'_dummy_child_': None})
                            modified_list = True
                            # logger.debug("Patched empty dict within 'experiments' list.")
                        else:
                            _patch_empty_nested_experiments_recursive(elem) # Recurse into list elements
                            new_list.append(elem)
                    if modified_list:
                        data_item[key] = new_list
                    elif not value and key == 'experiments': # Empty list for 'experiments' key
                        # This case is tricky. If Arrow infers list<struct<>>, an empty list
                        # might be problematic. Converting to None or list with dummy struct.
                        # For safety, let's make it a list with one dummy struct if it's empty.
                        # data_item[key] = [{'_dummy_child_': None}]
                        # logger.debug(f"Replaced empty list for 'experiments' with list containing a dummy struct.")
                        # Alternative: set to None if that's acceptable for your schema
                        data_item[key] = None
                        # logger.debug(f"Replaced empty list for 'experiments' with None.")
            elif isinstance(value, (dict, list)): # Recurse for other keys
                _patch_empty_nested_experiments_recursive(value)
    elif isinstance(data_item, list):
        for item in data_item:
            _patch_empty_nested_experiments_recursive(item)


@task
def save_data_to_gcs(
    data_list: List[Dict[str, Any]],
    gcs_bucket_name: str, # Pass bucket name explicitly
    gcs_credentials: service_account.Credentials
) -> str:
    """
    Saves list of dicts as Parquet to GCS.
    Handles nested empty 'experiments' dicts by adding a dummy child.
    Uses GCS_OUTPUT_PATH_PREFIX from config.
    """
    logger = get_run_logger()
    if not data_list:
        logger.warning("No data was successfully scraped. Skipping GCS upload.")
        return "No data scraped."

    try:
        if not gcs_bucket_name:
             logger.error("GCS_BUCKET_NAME was not provided.")
             raise ValueError("GCS_BUCKET_NAME is required.")

        # --- BEGIN PREPROCESSING STEP ---
        logger.info(f"Preprocessing {len(data_list)} records to handle empty 'experiments' structs/lists...")
        processed_count = 0
        for record_dict in data_list:
            # Make a shallow copy if you want to preserve original data_list items,
            # though in-place modification is usually fine here.
            _patch_empty_nested_experiments_recursive(record_dict)
            processed_count +=1
        logger.info(f"Finished preprocessing {processed_count} records.")
        # --- END PREPROCESSING STEP ---

        logger.info(f"Converting {len(data_list)} records to DataFrame...")
        df_output = pd.DataFrame(data_list)
        logger.info(f"DataFrame shape: {df_output.shape}")

        # This part for dropping a TOP-LEVEL 'experiments' column might still be useful
        # if 'experiments' can sometimes be a top-level column you want to remove entirely.
        # However, the primary error was likely due to a NESTED 'experiments' field.
        column_to_drop_top_level = 'experiments'
        if column_to_drop_top_level in df_output.columns:
            # If the 'experiments' column was top-level and an empty struct,
            # the _patch_empty_nested_experiments_recursive function would have already
            # added a dummy child to it (e.g., {'_dummy_child_': None}).
            # Dropping it here means you want to remove it regardless.
            df_output = df_output.drop(column_to_drop_top_level, axis=1)
            logger.info(f"Dropped top-level column '{column_to_drop_top_level}' from DataFrame.")
        else:
            logger.info(f"Top-level column '{column_to_drop_top_level}' not found in DataFrame, no action taken to drop it.")


        today_date_str = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        output_filename = f"staging_imovirtual_listings_{today_date_str}.parquet"
        clean_prefix = GCS_OUTPUT_PATH_PREFIX.strip('/')
        blob_name = f"{clean_prefix}/{output_filename}"
        gcs_output_uri = f"gs://{gcs_bucket_name}/{blob_name}"

        logger.info(f"Serializing DataFrame to in-memory Parquet buffer for {gcs_output_uri}...")
        parquet_buffer = io.BytesIO()
        df_output.to_parquet(parquet_buffer, index=False, engine='pyarrow', compression='snappy')
        parquet_buffer.seek(0)
        buffer_size = parquet_buffer.getbuffer().nbytes
        logger.info(f"Serialized data size: {buffer_size / 1024 / 1024:.2f} MB.")

        if buffer_size > 1 * 1024 * 1024 * 1024: # 1 GB threshold warning
             logger.warning(f"Parquet buffer size ({buffer_size / 1024 / 1024:.2f} MB) is large. Consider chunking.")

        logger.info(f"Uploading Parquet buffer to GCS: {gcs_output_uri}")
        storage_client = storage.Client(credentials=gcs_credentials)
        bucket = storage_client.bucket(gcs_bucket_name)
        blob = bucket.blob(blob_name)

        blob.upload_from_file(parquet_buffer, content_type='application/parquet', timeout=300)

        logger.info(f"Successfully uploaded Parquet file to {gcs_output_uri}")
        return gcs_output_uri

    except ImportError as e:
        logger.error(f"Import error during GCS save: {e}. Ensure pandas, pyarrow installed.", exc_info=True)
        raise
    except Exception as e:
        logger.error(f"Failed to process data or upload Parquet file to GCS: {e}", exc_info=True)
        raise