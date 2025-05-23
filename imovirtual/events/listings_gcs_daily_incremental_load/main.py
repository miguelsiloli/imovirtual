import os
import logging
from dotenv import load_dotenv
from google.cloud import storage, bigquery
import pandas as pd
from transforms_core import transform_listings_data
import tempfile
from datetime import date
import time
from google.oauth2 import service_account
from typing import Optional, List, Tuple
from datetime import datetime
import re

# --- Configuration Setup ---
# Load environment variables from .env file
load_dotenv()

# Optional: Logging level
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

# File extension to search for
FILE_EXTENSION = ".parquet"

def configure_logging(logging_name="imovirtual_listings_gcs_daily_incremental_load.log"):
    """Configures logging for the script."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(funcName)s() - %(message)s",
        handlers=[
            logging.FileHandler(logging_name),
            logging.StreamHandler(),
        ],
    )

def get_credentials():
    """
    Create and return service account credentials from environment variables.
    """
    try:
        # Check if credentials file path is provided
        creds_file = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
        if creds_file and os.path.exists(creds_file):
            logging.info(f"Using credentials from file: {creds_file}")
            return service_account.Credentials.from_service_account_file(creds_file)
        
        # Extract service account details from environment variables
        credentials_dict = {
            "type": os.getenv("TYPE"),
            "project_id": os.getenv("PROJECT_ID"),
            "private_key_id": os.getenv("PRIVATE_KEY_ID"),
            "private_key": os.getenv("PRIVATE_KEY").replace("\\n", "\n"),  # Fix newlines
            "client_email": os.getenv("CLIENT_EMAIL"),
            "client_id": os.getenv("CLIENT_ID"),
            "auth_uri": os.getenv("AUTH_URI"),
            "token_uri": os.getenv("TOKEN_URI"),
            "auth_provider_x509_cert_url": os.getenv("AUTH_PROVIDER_X509_CERT_URL"),
            "client_x509_cert_url": os.getenv("CLIENT_X509_CERT_URL"),
            "universe_domain": os.getenv("UNIVERSE_DOMAIN")
        }
        
        # Validate required credential fields
        required_fields = ["project_id", "private_key", "client_email"]
        missing_fields = [field for field in required_fields if not credentials_dict.get(field)]
        
        if missing_fields:
            raise ValueError(f"Missing required credential fields: {', '.join(missing_fields)}")
        
        # Create credentials object from dictionary
        return service_account.Credentials.from_service_account_info(credentials_dict)
        
    except Exception as e:
        logging.error(f"Failed to create credentials: {e}", exc_info=True)
        raise

def get_storage_client():
    """
    Create and return an authenticated GCS storage client.
    """
    try:
        # Get credentials
        credentials = get_credentials()
        
        # Create and return the storage client with the credentials
        project_id = os.getenv("GCP_PROJECT_ID") or os.getenv("PROJECT_ID")
        logging.info(f"Creating storage client for project: {project_id}")
        
        return storage.Client(project=project_id, credentials=credentials)
        
    except Exception as e:
        logging.error(f"Failed to create storage client: {e}", exc_info=True)
        raise

def get_bigquery_client():
    """
    Create and return an authenticated BigQuery client.
    """
    try:
        # Get credentials
        credentials = get_credentials()
        
        # Create and return the BigQuery client with the credentials
        project_id = os.getenv("BQ_PROJECT_ID") or os.getenv("GCP_PROJECT_ID") or os.getenv("PROJECT_ID")
        logging.info(f"Creating BigQuery client for project: {project_id}")
        
        return bigquery.Client(project=project_id, credentials=credentials)
        
    except Exception as e:
        logging.error(f"Failed to create BigQuery client: {e}", exc_info=True)
        raise

def find_latest_file_in_gcs(bucket_name: str, prefix: str, extension: str = ".parquet") -> Optional[str]:
    """
    Finds the most recent file in a GCS prefix based on file creation/update timestamp.

    Args:
        bucket_name: The name of the GCS bucket.
        prefix: The prefix (subfolder path) within the bucket.
        extension: The file extension to filter by (e.g., ".parquet").

    Returns:
        The full GCS path (gs://...) of the latest file found, or None if no
        files with the specified extension are found or an error occurs.
    """
    if not bucket_name or not prefix:
        logging.error("GCS_BUCKET_NAME and GCS_SUBFOLDER_PATH environment variables must be set.")
        return None

    # Ensure prefix ends with '/' for directory-like listing within GCS client API
    if not prefix.endswith('/'):
        prefix_with_slash = prefix + '/'
    else:
        prefix_with_slash = prefix
    
    full_gcs_prefix = f"gs://{bucket_name}/{prefix_with_slash}"
    logging.info(f"Searching for latest '{extension}' file by timestamp in: {full_gcs_prefix}")

    try:
        # Get authenticated storage client
        storage_client = get_storage_client()
        
        # List blobs (objects) in the specified prefix
        blobs = storage_client.list_blobs(bucket_name, prefix=prefix_with_slash)

        # Filter for files with the correct extension and track their metadata
        matching_files = []
        for blob in blobs:
            # Ignore "folders" themselves if they appear as 0-byte objects
            if blob.name.endswith('/') and blob.size == 0:
                logging.debug(f"Ignoring directory object: {blob.name}")
                continue
                
            if blob.name.endswith(extension):
                # Get the blob's metadata (includes timestamp information)
                blob.reload()  # Ensure we have the latest metadata
                
                # Store the blob object itself, which contains name and timestamp
                matching_files.append(blob)
                logging.debug(f"Found potential file: {blob.name}, time_created: {blob.time_created}")

        if not matching_files:
            logging.warning(f"No files with extension '{extension}' found in {full_gcs_prefix}")
            return None

        # Sort the blobs by time_created (ascending)
        # The last one will be the most recently created/updated file
        matching_files.sort(key=lambda blob: blob.time_created)
        
        # Log the top 5 latest files with their timestamps (if we have that many)
        num_to_show = min(5, len(matching_files))
        if num_to_show > 0:
            logging.info(f"Top {num_to_show} latest files by timestamp:")
            for i in range(1, num_to_show + 1):
                blob = matching_files[-i]  # Get files from newest to oldest
                logging.info(f"  {i}. {blob.name} (created: {blob.time_created})")

        # The last file in the sorted list is the "latest" based on creation time
        latest_blob = matching_files[-1]
        latest_file_gcs_path = f"gs://{bucket_name}/{latest_blob.name}"

        logging.info(f"Latest file found (by timestamp): {latest_file_gcs_path}")
        logging.info(f"  Created: {latest_blob.time_created}")
        logging.info(f"  Updated: {latest_blob.updated}")
        logging.info(f"  Size: {latest_blob.size} bytes")
        
        return latest_file_gcs_path

    except Exception as e:
        logging.error(f"An unexpected error occurred while accessing GCS: {e}", exc_info=True)
        return None
    
def read_parquet_from_gcs(file_path: str) -> pd.DataFrame:
    """
    Read a Parquet file from GCS using the Google Cloud Storage client.
    
    Args:
        file_path: The GCS path to the Parquet file (gs://bucket/path/to/file.parquet)
        
    Returns:
        A pandas DataFrame containing the Parquet data
        
    Raises:
        ValueError: If the GCS path is invalid
        Exception: If reading the file fails
    """
    logging.info(f"Reading Parquet file from GCS: {file_path}")
    
    if not file_path.startswith('gs://'):
        raise ValueError(f"Invalid GCS path: {file_path}. Must start with 'gs://'")
    
    # Parse the GCS path to get bucket and blob name
    path_parts = file_path[5:].split('/', 1)  # Remove 'gs://' and split
    if len(path_parts) != 2:
        raise ValueError(f"Invalid GCS path format: {file_path}")
    
    bucket_name, blob_name = path_parts
    
    try:
        
        storage_client = get_storage_client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=True) as temp_file:
            blob.download_to_filename(temp_file.name)
            # Ensure file write is flushed to disk
            temp_file.flush()
            os.fsync(temp_file.fileno())
            
            # Read the temporary file into pandas
            df = pd.read_parquet(temp_file.name)
            
        logging.info(f"Successfully read Parquet file with GCS client: {len(df)} rows")
        return df
    
    except Exception as e:
        logging.error(f"Failed to read Parquet file from GCS: {e}", exc_info=True)
        raise Exception(f"Failed to read Parquet file from GCS: {str(e)}")

def load_to_bigquery(df: pd.DataFrame) -> None:
    """
    Load the transformed DataFrame to BigQuery.
    
    Args:
        df: The DataFrame to load into BigQuery
    """
    # Get BigQuery configuration from environment variables
    bq_project_id = os.getenv("BQ_PROJECT_ID")
    bq_dataset_id = os.getenv("BQ_PROJECT_ID_IMOVIRTUAL_LISTINGS")
    bq_table_id = os.getenv("BQ_TABLE_ID_IMOVIRTUAL_LISTINGS")
    bq_write_disposition = os.getenv("BQ_WRITE_DISPOSITION", "WRITE_APPEND")
    bq_create_disposition = os.getenv("BQ_CREATE_DISPOSITION", "CREATE_NEVER")
    
    # Validate required configuration
    if not all([bq_project_id, bq_dataset_id, bq_table_id]):
        raise ValueError("Missing required BigQuery configuration in environment variables")
    
    # Construct full table ID
    full_table_id = f"{bq_project_id}.{bq_dataset_id}.{bq_table_id}"
    logging.info(f"Loading data to BigQuery table: {full_table_id}")
    
    try:
        # Get BigQuery client
        bq_client = get_bigquery_client()
        
        # Define job configuration
        job_config = bigquery.LoadJobConfig(
            write_disposition=bq_write_disposition,
            create_disposition=bq_create_disposition
        )
        
        # Load DataFrame to BigQuery
        job = bq_client.load_table_from_dataframe(df, full_table_id, job_config=job_config)
        job.result()  # Wait for job to complete
        
        # Get the resulting table and print info
        table = bq_client.get_table(full_table_id)
        logging.info(f"Loaded {len(df)} rows to {full_table_id}")
        logging.info(f"Table now has {table.num_rows} rows total")
        
    except Exception as e:
        logging.error(f"Error loading data to BigQuery: {e}", exc_info=True)
        raise

def check_existing_keys_in_bigquery(df: pd.DataFrame) -> List[tuple]:
    """
    Check which (slug, ingestionDate) pairs already exist in the BigQuery staging table.
    
    Args:
        df: DataFrame containing the keys to check
        
    Returns:
        List of (slug, ingestionDate) tuples that already exist in BigQuery
    """
    # Get BigQuery configuration from environment variables
    bq_project_id = os.getenv("BQ_PROJECT_ID")
    bq_dataset_id = os.getenv("BQ_PROJECT_ID_IMOVIRTUAL_LISTINGS")
    bq_table_id = os.getenv("BQ_TABLE_ID_IMOVIRTUAL_LISTINGS")
    
    
    # Validate required configuration
    if not all([bq_project_id, bq_dataset_id, bq_table_id]):
        raise ValueError("Missing required BigQuery configuration in environment variables")
    
    # Construct full table ID
    full_table_id = f"{bq_project_id}.{bq_dataset_id}.{bq_table_id}"
    
    try:
        # Get unique (slug, ingestionDate) pairs from DataFrame
        key_pairs = df[['id', 'ingestionDate']].drop_duplicates()
        
        if len(key_pairs) == 0:
            logging.info("No keys to check in BigQuery")
            return []
        
        # Create temporary table ID for the query
        temp_dataset = f"{bq_dataset_id}_temp"
        temp_table_name = f"temp_lookup_{int(time.time())}"
        temp_table_id = f"{bq_project_id}.{temp_dataset}.{temp_table_name}"
        
        # Get BigQuery client
        bq_client = get_bigquery_client()
        
        # Ensure temp dataset exists
        try:
            bq_client.get_dataset(f"{bq_project_id}.{temp_dataset}")
        except Exception:
            dataset = bigquery.Dataset(f"{bq_project_id}.{temp_dataset}")
            dataset.location = "europe-southwest1"  # Set to your region
            bq_client.create_dataset(dataset, exists_ok=True)
        
        # Create a DataFrame with proper date type for ingestionDate
        lookup_df = pd.DataFrame({
            'id': key_pairs['id'],
            'ingestionDate': pd.to_datetime(key_pairs['ingestionDate']).dt.date  # Ensure proper DATE format
        })
        
        # Define explicit schema to match the target table
        schema = [
            bigquery.SchemaField("id", "INT64"),
            bigquery.SchemaField("ingestionDate", "DATE")  # Use DATE type to match BigQuery
        ]
        
        # Create the table with explicit schema
        table = bigquery.Table(temp_table_id, schema=schema)
        table = bq_client.create_table(table, exists_ok=True)
        
        # Upload the lookup DataFrame to BigQuery
        logging.info(f"Creating temporary lookup table with {len(lookup_df)} key pairs")
        job_config = bigquery.LoadJobConfig(
            schema=schema,
            autodetect=False,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
        )
        
        load_job = bq_client.load_table_from_dataframe(
            lookup_df, temp_table_id, job_config=job_config
        )
        load_job.result()  # Wait for the job to complete
        
        # Execute query to find matching keys
        query = f"""
        SELECT t.id, t.ingestionDate
        FROM `{full_table_id}` t
        INNER JOIN `{temp_table_id}` l
        ON t.id = l.id
        AND t.ingestionDate = l.ingestionDate
        """
        
        # Run the query
        logging.info("Checking for existing key pairs in BigQuery")
        query_job = bq_client.query(query)
        
        # Collect results 
        existing_keys = []
        for row in query_job:
            # Convert back to the format needed for comparison
            existing_keys.append((row.id, row.ingestionDate))
            
        logging.info(f"Found {len(existing_keys)} existing key pairs in BigQuery")
        
        # Attempt to clean up the temporary table
        try:
            bq_client.delete_table(temp_table_id)
            logging.info(f"Deleted temporary table {temp_table_id}")
        except Exception as e:
            logging.warning(f"Could not delete temporary table: {e}")
        
        return existing_keys
        
    except Exception as e:
        logging.error(f"Error checking existing keys in BigQuery: {e}", exc_info=True)
        raise

def filter_out_duplicates(df: pd.DataFrame, existing_keys: List[tuple]) -> pd.DataFrame:
    """
    Filter out rows from the DataFrame whose (slug, ingestionDate) pair 
    already exists in BigQuery.
    
    Args:
        df: DataFrame containing the data to filter
        existing_keys: List of (slug, ingestionDate) tuples that already exist in BigQuery
        
    Returns:
        DataFrame with duplicates removed
    """
    logging.info(f"Filtering out {len(existing_keys)} duplicate keys from {len(df)} rows")
    
    # Convert existing_keys to a set for faster lookup
    existing_keys_set = set(existing_keys)
    
    # Filter out rows where (slug, ingestionDate) is in existing_keys
    filtered_df = df[~df.apply(lambda row: (row['id'], row['ingestionDate']) in existing_keys_set, axis=1)]
    
    logging.info(f"Filtered DataFrame from {len(df)} to {len(filtered_df)} rows")
    return filtered_df

# def find_problematic_unicode_data(df: pd.DataFrame):
#     problematic_entries = []
#     for col in df.columns:
#         # Focus on object columns, which are likely strings, or lists/dicts containing strings
#         if df[col].dtype == 'object':
#             for index, value in df[col].items():
#                 if isinstance(value, str):
#                     try:
#                         value.encode('utf-8')
#                     except UnicodeEncodeError as e:
#                         problematic_entries.append({
#                             "index": index,
#                             "column": col,
#                             "value_preview": value[:100], # Show a preview
#                             "error": str(e)
#                         })
#                 elif isinstance(value, list): # For ARRAY<STRING>
#                     for i, item in enumerate(value):
#                         if isinstance(item, str):
#                             try:
#                                 item.encode('utf-8')
#                             except UnicodeEncodeError as e:
#                                 problematic_entries.append({
#                                     "index": index,
#                                     "column": col,
#                                     "list_index": i,
#                                     "value_preview": item[:100],
#                                     "error": str(e)
#                                 })
#                 elif isinstance(value, dict): # For STRUCT
#                     # This needs to be recursive or handle nested structures if they also contain strings
#                     for k, v_item in value.items():
#                         if isinstance(v_item, str):
#                             try:
#                                 v_item.encode('utf-8')
#                             except UnicodeEncodeError as e:
#                                 problematic_entries.append({
#                                     "index": index,
#                                     "column": col,
#                                     "dict_key": k,
#                                     "value_preview": v_item[:100],
#                                     "error": str(e)
#                                 })
#                         # Add more checks for nested lists/dicts within the struct if necessary
    
#     if problematic_entries:
#         print(f"Found {len(problematic_entries)} problematic Unicode entries:")
#         for entry in problematic_entries:
#             print(entry)
#     else:
#         print("No problematic Unicode entries found with basic check.")
#     return problematic_entries

def clean_surrogates(text: Optional[str]) -> Optional[str]:
    if text is None:
        return None
    if not isinstance(text, str):
        # In case some non-string data slipped into description_text
        return str(text) 

    try:
        # This sequence attempts to correctly interpret UTF-16 surrogate pairs
        # and then re-encode them into valid multi-byte UTF-8 sequences.
        return text.encode('utf-16', 'surrogatepass').decode('utf-16').encode('utf-8').decode('utf-8')
    except Exception:
        # Fallback: A more aggressive cleaning if the above fails for some reason.
        # Replace any character that cannot be represented in valid UTF-8 after attempting surrogate pass.
        # U+FFFD is the official Unicode replacement character.
        return "".join(c if ord(c) < 0xD800 or ord(c) > 0xDFFF else '\uFFFD' for c in text)

# --- New function to add ingestionDate from filepath ---
def add_ingestion_date_from_filepath(df: pd.DataFrame, file_path: str) -> pd.DataFrame:
    """
    Extracts the date from the filename and adds it as 'ingestionDate' column.
    Assumes filename format like 'prefix_YYYY-MM-DD.extension'.

    Args:
        df (pd.DataFrame): The input DataFrame.
        file_path (str): The full path to the file (e.g., GCS path or local path).

    Returns:
        pd.DataFrame: The DataFrame with 'ingestionDate' column added/updated.
    """
    try:
        filename = os.path.basename(file_path)
        # Regex to find a date pattern like _YYYY-MM-DD. (e.g., _2025-05-10.)
        # This captures the date part. It assumes the date is followed by an underscore
        # or is at the end of the relevant filename part before the extension.
        # Specifically targets '_YYYY-MM-DD' followed by a dot and the extension.
        match = re.search(r'_(\d{4}-\d{2}-\d{2})\.[^.]+$', filename)
        
        if match:
            date_str = match.group(1) # The captured YYYY-MM-DD string
            ingestion_date_obj = datetime.strptime(date_str, '%Y-%m-%d').date()
            df['ingestionDate'] = ingestion_date_obj # Assign Python date object
            logging.info(f"Successfully set 'ingestionDate' to {ingestion_date_obj} from filename: {filename}")
        else:
            logging.warning(f"Could not extract date from filename: {filename}. "
                            "'ingestionDate' column will not be set from filename.")
            # Fallback: if 'ingestionDate' doesn't exist, create it with NaT
            if 'ingestionDate' not in df.columns:
                 df['ingestionDate'] = pd.NaT
    except Exception as e:
        logging.error(f"Error extracting ingestion date from filepath '{file_path}': {e}")
        if 'ingestionDate' not in df.columns:
            df['ingestionDate'] = pd.NaT # Ensure column exists with NaT on error
    return df

if __name__ == "__main__":
    configure_logging()
    
    # --- Configuration from Environment Variables ---
    GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "your-default-bucket") # Provide default for easy testing
    GCS_SUBFOLDER_PATH = os.getenv("GCS_SUBFOLDER_PATH_IMOVIRTUAL_LISTINGS", "staging/imovirtual_listings")
    
    logging.info(f"Using bucket: {GCS_BUCKET_NAME}")
    logging.info(f"Using subfolder path: {GCS_SUBFOLDER_PATH}")
    
    if not GCS_BUCKET_NAME or not GCS_SUBFOLDER_PATH:
        logging.error("Exiting: Missing required environment variables: GCS_BUCKET_NAME, GCS_SUBFOLDER_PATH")
        exit(1)
    
    logging.info(f"Searching for latest file in gs://{GCS_BUCKET_NAME}/{GCS_SUBFOLDER_PATH}")
    latest_file = find_latest_file_in_gcs(
        bucket_name=GCS_BUCKET_NAME,
        prefix=GCS_SUBFOLDER_PATH,
        extension=FILE_EXTENSION
    )
    
    if not latest_file:
        logging.error("No Parquet files found. Exiting.")
        exit(1)
    logging.info(f"Processing file: {latest_file}")
    
    raw_data = read_parquet_from_gcs(latest_file)
    
    transformed_df = transform_listings_data(input_df=raw_data, props_column_name='props')

    if 'description_text' in transformed_df.columns:
        transformed_df['description_text'] = transformed_df['description_text'].apply(
            lambda x: clean_surrogates(x) if pd.notna(x) and isinstance(x, str) else x
        )
    else:
        logging.warning("'description_text' column not found for surrogate cleaning.")

    # --- Add ingestionDate using the new function ---
    transformed_df = add_ingestion_date_from_filepath(transformed_df, latest_file)
    # --- End of new function call ---

    # Process created_at and modified_at (original logic for modified_at retained)
    if 'created_at' in transformed_df.columns:
        transformed_df["created_at"] = pd.to_datetime(transformed_df["created_at"], errors='coerce', utc=True)
        transformed_df["modified_at"] = pd.to_datetime(transformed_df["created_at"], errors='coerce', utc=True) # Uses created_at
    else:
        logging.warning("'created_at' column not found. Cannot process created_at/modified_at timestamps.")
    
    # The line below is now replaced by the call to add_ingestion_date_from_filepath:
    # transformed_df["ingestionDate"] = pd.to_datetime(transformed_df["created_at"], errors='coerce', utc=True)

    existing_keys = check_existing_keys_in_bigquery(transformed_df)
    new_data = filter_out_duplicates(transformed_df, existing_keys)
    
    logging.info("--- New Data Head ---")
    logging.info("\n%s", new_data.head().to_string()) # .to_string() for better log formatting

    load_to_bigquery(new_data)
    
    logging.info("Processing completed successfully")