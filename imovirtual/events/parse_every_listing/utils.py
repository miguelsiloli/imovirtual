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
    try:
        logger = get_run_logger()
        log_info = logger.info
        log_error = logger.error
    except Exception: # Fallback if not in Prefect context
        logger = logging.getLogger(__name__)
        log_info = logger.info
        log_error = logger.error

    try:
        required_vars = [
            "TYPE", "PROJECT_ID", "PRIVATE_KEY_ID", "PRIVATE_KEY",
            "CLIENT_EMAIL", "CLIENT_ID", "AUTH_URI", "TOKEN_URI",
            "AUTH_PROVIDER_X509_CERT_URL", "CLIENT_X509_CERT_URL"
        ]
        if not all(os.getenv(var) for var in required_vars):
            log_error("GCS credential environment variables not found during credential creation.")
            return None
        private_key = os.getenv("PRIVATE_KEY", "").replace('\\n', '\n')
        if not private_key:
            log_error("PRIVATE_KEY environment variable is missing or empty.")
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
            log_error(f"Missing credential fields loaded from environment: {missing_cred_fields}")
            return None

        credentials = service_account.Credentials.from_service_account_info(creds_info)
        log_info("Successfully created GCS credentials object from environment.")
        return credentials
    except Exception as e:
        log_error(f"Failed to create GCS credentials object: {e}", exc_info=True)
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


@task
def save_data_to_gcs(
    data_list: List[Dict[str, Any]],
    gcs_bucket_name: str, # Pass bucket name explicitly
    gcs_credentials: service_account.Credentials
) -> str:
    """
    Saves list of dicts as Parquet to GCS.
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

        logger.info(f"Converting {len(data_list)} records to DataFrame...")
        df_output = pd.DataFrame(data_list)
        logger.info(f"DataFrame shape: {df_output.shape}")

        column_to_drop = 'experiments'
        if column_to_drop in df_output.columns:
            df_output = df_output.drop("experiments", axis = 1)
            logger.info(f"Dropping '{column_to_drop}' column before saving to Parquet.")

        today_date_str = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        output_filename = f"staging_imovirtual_listings_{today_date_str}.parquet"
        clean_prefix = GCS_OUTPUT_PATH_PREFIX.strip('/') # Use imported constant
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