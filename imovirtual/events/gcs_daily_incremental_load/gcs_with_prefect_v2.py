import os
import logging # Keep standard logging for setup outside Prefect flows if needed
import json
import tempfile
from datetime import date, datetime # Import datetime for timestamp comparison
from typing import Optional, Dict, Any, List

import pandas as pd
from google.cloud import storage, bigquery
from google.cloud.exceptions import NotFound, GoogleCloudError # Import specific cloud error
from google.oauth2 import service_account
import time # Needed for temporary table name
from dotenv import load_dotenv

# Prefect imports
from prefect import task, flow, get_run_logger

# --- Configuration Setup ---
load_dotenv()
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s - %(levelname)s - %(message)s")
FILE_EXTENSION = ".parquet"

# --- Authentication Setup (Keeping original helpers) ---
def get_credentials(logger: Optional[logging.Logger] = None):
    log = logger or logging
    try:
        creds_file = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
        if creds_file and os.path.exists(creds_file):
            log.info(f"Using credentials from file: {creds_file}")
            return service_account.Credentials.from_service_account_file(creds_file)
        credentials_dict = {
            "type": os.getenv("TYPE"), "project_id": os.getenv("PROJECT_ID"),
            "private_key_id": os.getenv("PRIVATE_KEY_ID"),
            "private_key": os.getenv("PRIVATE_KEY", "").replace("\\n", "\n"),
            "client_email": os.getenv("CLIENT_EMAIL"), "client_id": os.getenv("CLIENT_ID"),
            "auth_uri": os.getenv("AUTH_URI"), "token_uri": os.getenv("TOKEN_URI"),
            "auth_provider_x509_cert_url": os.getenv("AUTH_PROVIDER_X509_CERT_URL"),
            "client_x509_cert_url": os.getenv("CLIENT_X509_CERT_URL"),
            "universe_domain": os.getenv("UNIVERSE_DOMAIN")
        }
        required_fields = ["project_id", "private_key", "client_email"]
        missing_fields = [f for f in required_fields if not credentials_dict.get(f)]
        if missing_fields:
            raise ValueError(f"Missing required credential fields: {', '.join(missing_fields)}")
        log.info("Creating credentials from environment variables.")
        return service_account.Credentials.from_service_account_info(credentials_dict)
    except Exception as e:
        log.error(f"Failed to create credentials: {e}", exc_info=True)
        raise

def get_storage_client(logger: Optional[logging.Logger] = None):
    log = logger or logging
    try:
        credentials = get_credentials(logger=log)
        project_id = os.getenv("GCP_PROJECT_ID") or os.getenv("PROJECT_ID")
        if not project_id: raise ValueError("Missing GCP_PROJECT_ID or PROJECT_ID for storage.")
        log.info(f"Creating storage client for project: {project_id}")
        return storage.Client(project=project_id, credentials=credentials)
    except Exception as e:
        log.error(f"Failed to create storage client: {e}", exc_info=True)
        raise

def get_bigquery_client(logger: Optional[logging.Logger] = None):
    log = logger or logging
    try:
        credentials = get_credentials(logger=log)
        project_id = os.getenv("BQ_PROJECT_ID") or os.getenv("GCP_PROJECT_ID") or os.getenv("PROJECT_ID")
        if not project_id: raise ValueError("Missing BQ_PROJECT_ID/GCP_PROJECT_ID/PROJECT_ID for BQ.")
        log.info(f"Creating BigQuery client for project: {project_id}")
        return bigquery.Client(project=project_id, credentials=credentials)
    except Exception as e:
        log.error(f"Failed to create BigQuery client: {e}", exc_info=True)
        raise

# --- Prefect Tasks (Reverting check_existing_keys_in_bigquery) ---

@task
def find_latest_file_in_gcs(bucket_name: str, prefix: str, extension: str = ".parquet") -> Optional[str]:
    """(Prefect Task) Finds the most recent file in GCS by modification timestamp."""
    logger = get_run_logger()
    if not bucket_name or not prefix:
        raise ValueError("GCS bucket name and prefix must be provided.")
    prefix_with_slash = prefix if prefix.endswith('/') else prefix + '/'
    full_gcs_prefix = f"gs://{bucket_name}/{prefix_with_slash}"
    logger.info(f"Searching for latest '{extension}' file by modification timestamp in: {full_gcs_prefix}")
    try:
        storage_client = get_storage_client(logger=logger)
        blobs = storage_client.list_blobs(bucket_name, prefix=prefix_with_slash)
        matching_files = []
        for blob in blobs:
            if blob.name.endswith('/') and blob.size == 0: continue
            if blob.name.endswith(extension):
                ts = blob.updated or blob.time_created
                if ts: matching_files.append((blob, ts))
                else: logger.warning(f"No timestamp for blob: {blob.name}. Skipping.")
        if not matching_files:
            logger.warning(f"No files with extension '{extension}' found in {full_gcs_prefix}")
            return None
        matching_files.sort(key=lambda item: item[1], reverse=True)
        latest_blob, latest_timestamp = matching_files[0]
        latest_file_gcs_path = f"gs://{bucket_name}/{latest_blob.name}"
        logger.info(f"Latest file found: {latest_file_gcs_path} (Timestamp: {latest_timestamp})")
        return latest_file_gcs_path
    except NotFound:
        logger.error(f"Bucket '{bucket_name}' or prefix '{prefix_with_slash}' not found.")
        raise
    except GoogleCloudError as e:
        logger.error(f"Google Cloud error accessing GCS: {e}", exc_info=True)
        raise
    except Exception as e:
        logger.error(f"Unexpected error accessing GCS: {e}", exc_info=True)
        raise

@task
def read_parquet_from_gcs(file_path: str) -> pd.DataFrame:
    """(Prefect Task) Reads a Parquet file from GCS."""
    logger = get_run_logger()
    logger.info(f"Reading Parquet file from GCS: {file_path}")
    if not file_path or not file_path.startswith('gs://'):
        raise ValueError(f"Invalid GCS path: {file_path}.")
    path_parts = file_path[5:].split('/', 1)
    if len(path_parts) != 2: raise ValueError(f"Invalid GCS path format: {file_path}")
    bucket_name, blob_name = path_parts
    temp_filename = None
    try:
        storage_client = get_storage_client(logger=logger)
        blob = storage_client.bucket(bucket_name).blob(blob_name)
        logger.info(f"Downloading {blob_name} from bucket {bucket_name}...")
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as temp_file:
            temp_filename = temp_file.name
            blob.download_to_filename(temp_filename)
            logger.info(f"Downloaded to temporary file: {temp_filename}")
        logger.info(f"Reading Parquet data from temporary file: {temp_filename}")
        df = pd.read_parquet(temp_filename)
        logger.info(f"Successfully read Parquet file: {len(df)} rows, {len(df.columns)} columns")
        return df
    except GoogleCloudError as e:
         logger.error(f"Google Cloud error during GCS read/download: {e}", exc_info=True)
         raise
    except FileNotFoundError:
         logger.error(f"Downloaded temporary file not found: {temp_filename}", exc_info=True)
         raise
    except Exception as e:
        logger.error(f"Failed to read Parquet file from GCS: {e}", exc_info=True)
        raise
    finally:
        if temp_filename and os.path.exists(temp_filename):
            try:
                os.remove(temp_filename)
                logger.info(f"Removed temporary file: {temp_filename}")
            except Exception as cleanup_e:
                logger.warning(f"Failed to remove temporary file {temp_filename}: {cleanup_e}")

@task
def process_data(df: pd.DataFrame) -> pd.DataFrame:
    """(Prefect Task) Transforms raw data to match BigQuery schema."""
    logger = get_run_logger()
    logger.info(f"Starting transformation for {len(df)} raw records.")
    final_columns = [ # Define final columns structure first
        'id', 'title', 'slug', 'estate', 'transaction', 'developmentId', 'developmentTitle',
        'developmentUrl', 'city', 'province', 'street', 'mapRadius', 'isExclusiveOffer',
        'isPrivateOwner', 'isPromoted', 'source', 'agencyId', 'agencyName', 'agencySlug',
        'agencyType', 'agencyBrandingVisible', 'agencyHighlightedAds', 'agencyImageUrl',
        'openDays', 'totalPriceCurrency', 'totalPriceValue', 'pricePerSquareMeterCurrency',
        'pricePerSquareMeterValue', 'areaInSquareMeters', 'terrainAreaInSquareMeters',
        'roomsNumber', 'hidePrice', 'floorNumber', 'dateCreated', 'dateCreatedFirst',
        'shortDescription', 'totalPossibleImages', 'additionalInfo', 'ingestionDate'
    ]
    if df.empty:
        logger.warning("Input DataFrame is empty. Returning empty DataFrame with expected columns.")
        return pd.DataFrame(columns=final_columns)
    try:
        transformed_df = pd.DataFrame()
        # --- Mappings (using .get for safety) ---
        transformed_df['id'] = df['id'].astype(str) # Assuming 'id' always exists
        transformed_df['title'] = df['title'].astype(str) # Assuming 'title' always exists
        transformed_df['slug'] = df['slug'].astype(str) # Assuming 'slug' always exists
        if transformed_df['slug'].isnull().any() or (transformed_df['slug'] == '').any():
             logger.warning("Found rows with null or empty 'slug'. Ensure this is allowed by BQ schema.")
        transformed_df['estate'] = df.get('estate', pd.NA).astype(str).fillna(pd.NA)
        transformed_df['transaction'] = df.get('transaction', pd.NA).astype(str).fillna(pd.NA)
        transformed_df['developmentId'] = pd.to_numeric(df.get('developmentId'), errors='coerce').astype('Int64')
        transformed_df['developmentTitle'] = df.get('developmentTitle', pd.NA).astype(str).fillna(pd.NA)
        transformed_df['developmentUrl'] = df.get('developmentUrl', pd.NA).astype(str).fillna(pd.NA)
        transformed_df['isExclusiveOffer'] = df.get('isExclusiveOffer', False).astype(bool)
        transformed_df['isPrivateOwner'] = df.get('isPrivateOwner', False).astype(bool)
        transformed_df['isPromoted'] = df.get('isPromoted', False).astype(bool)
        transformed_df['source'] = df.get('source', pd.NA).astype(str).fillna(pd.NA)
        transformed_df['openDays'] = df.get('openDays', pd.NA).astype(str).fillna(pd.NA)
        transformed_df['areaInSquareMeters'] = pd.to_numeric(df.get('areaInSquareMeters'), errors='coerce').astype(float)
        transformed_df['terrainAreaInSquareMeters'] = pd.to_numeric(df.get('terrainAreaInSquareMeters'), errors='coerce').astype(float)
        transformed_df['roomsNumber'] = df.get('roomsNumber', pd.NA).astype(str).fillna(pd.NA)
        transformed_df['hidePrice'] = df.get('hidePrice', False).astype(bool)
        transformed_df['floorNumber'] = df.get('floorNumber', pd.NA).astype(str).fillna(pd.NA)
        transformed_df['shortDescription'] = df.get('shortDescription', pd.NA).astype(str).fillna(pd.NA)
        transformed_df['totalPossibleImages'] = pd.to_numeric(df.get('totalPossibleImages'), errors='coerce').astype('Int64')
        # --- Nested ---
        def safe_get(data, keys, default=None):
            if not isinstance(data, dict): return default
            temp = data
            for key in keys:
                if isinstance(temp, dict): temp = temp.get(key)
                else: return default
            return temp if temp is not None else default
        location_col = df.get('location', pd.Series([None] * len(df)))
        transformed_df['city'] = location_col.apply(lambda loc: safe_get(loc, ['address', 'city', 'name'])).astype(str).fillna(pd.NA)
        transformed_df['province'] = location_col.apply(lambda loc: safe_get(loc, ['address', 'province', 'name'])).astype(str).fillna(pd.NA)
        transformed_df['street'] = location_col.apply(lambda loc: safe_get(loc, ['address', 'street'])).astype(str).fillna(pd.NA)
        transformed_df['mapRadius'] = pd.to_numeric(location_col.apply(lambda loc: safe_get(loc, ['mapDetails', 'radius'])), errors='coerce').astype(float)
        agency_col = df.get('agency', pd.Series([None] * len(df)))
        transformed_df['agencyId'] = pd.to_numeric(agency_col.apply(lambda ag: safe_get(ag, ['id'])), errors='coerce').astype('Int64')
        transformed_df['agencyName'] = agency_col.apply(lambda ag: safe_get(ag, ['name'])).astype(str).fillna(pd.NA)
        transformed_df['agencySlug'] = agency_col.apply(lambda ag: safe_get(ag, ['slug'])).astype(str).fillna(pd.NA)
        transformed_df['agencyType'] = agency_col.apply(lambda ag: safe_get(ag, ['type'])).astype(str).fillna(pd.NA)
        transformed_df['agencyBrandingVisible'] = agency_col.apply(lambda ag: safe_get(ag, ['brandingVisible'])).astype('boolean')
        transformed_df['agencyHighlightedAds'] = agency_col.apply(lambda ag: safe_get(ag, ['highlightedAds'])).astype('boolean')
        transformed_df['agencyImageUrl'] = agency_col.apply(lambda ag: safe_get(ag, ['imageUrl'])).astype(str).fillna(pd.NA)
        total_price_col = df.get('totalPrice', pd.Series([None] * len(df)))
        transformed_df['totalPriceCurrency'] = total_price_col.apply(lambda price: safe_get(price, ['currency'])).astype(str).fillna(pd.NA)
        transformed_df['totalPriceValue'] = pd.to_numeric(total_price_col.apply(lambda price: safe_get(price, ['value'])), errors='coerce').astype(float)
        price_m2_col = df.get('pricePerSquareMeter', pd.Series([None] * len(df)))
        transformed_df['pricePerSquareMeterCurrency'] = price_m2_col.apply(lambda price: safe_get(price, ['currency'])).astype(str).fillna(pd.NA)
        transformed_df['pricePerSquareMeterValue'] = pd.to_numeric(price_m2_col.apply(lambda price: safe_get(price, ['value'])), errors='coerce').astype(float)
        # --- Timestamps ---
        transformed_df['dateCreated'] = pd.to_datetime(df.get('dateCreated'), errors='coerce', utc=True).dt.tz_convert(None)
        transformed_df['dateCreatedFirst'] = pd.to_datetime(df.get('dateCreatedFirst'), errors='coerce', utc=True).dt.tz_convert(None)
        # --- Other ---
        transformed_df['additionalInfo'] = None # As original
        transformed_df['ingestionDate'] = pd.to_datetime(date.today()).date()
        # --- Finalize ---
        for col in final_columns: # Ensure all columns exist
            if col not in transformed_df.columns:
                logger.warning(f"Column '{col}' missing after transformation. Adding as NA.")
                transformed_df[col] = pd.NA
        transformed_df = transformed_df[final_columns] # Enforce order
        logger.info(f"Successfully transformed data. Resulting shape: {transformed_df.shape}")
        return transformed_df
    except KeyError as e:
        logger.error(f"Transformation failed due to missing key: {e}", exc_info=True)
        raise
    except Exception as e:
        logger.error(f"Unexpected error during data transformation: {e}", exc_info=True)
        raise


# --- Reverted check_existing_keys_in_bigquery Task ---
@task
def check_existing_keys_in_bigquery(df: pd.DataFrame) -> List[tuple]:
    """
    (Prefect Task) Check which (slug, ingestionDate) pairs already exist in the
    BigQuery staging table using a temporary table.

    Args:
        df: DataFrame containing the keys to check (must have 'slug' and 'ingestionDate').

    Returns:
        List of (slug, ingestionDate) tuples that already exist in BigQuery.
        The ingestionDate will be a datetime.date object.
    """
    logger = get_run_logger() # Use Prefect logger

    # Get BigQuery configuration from environment variables
    bq_project_id = os.getenv("BQ_PROJECT_ID")
    bq_dataset_id = os.getenv("BQ_DATASET_ID")
    bq_table_id = os.getenv("BQ_TABLE_ID")
    bq_temp_dataset_location = os.getenv("BQ_TEMP_DATASET_LOCATION", "europe-west1") # Allow configurable temp location

    # Validate required configuration
    if not all([bq_project_id, bq_dataset_id, bq_table_id]):
        raise ValueError("Missing required BigQuery config (BQ_PROJECT_ID, BQ_DATASET_ID, BQ_TABLE_ID)")

    # Construct full table ID
    full_table_id = f"{bq_project_id}.{bq_dataset_id}.{bq_table_id}"
    logger.info(f"Checking existing keys against BQ table: {full_table_id}")

    # Define temp table details
    temp_dataset = f"{bq_dataset_id}_temp"
    temp_table_name = f"temp_lookup_{int(time.time())}_{os.getpid()}" # Add PID for potential parallelism
    temp_table_id = f"{bq_project_id}.{temp_dataset}.{temp_table_name}"
    temp_table_ref = bigquery.TableReference.from_string(temp_table_id)

    # Prepare keys from DataFrame
    if 'slug' not in df.columns or 'ingestionDate' not in df.columns:
        raise ValueError("Input DataFrame for key checking is missing 'slug' or 'ingestionDate' column.")

    key_pairs = df[['slug', 'ingestionDate']].drop_duplicates().dropna()
    if key_pairs.empty:
        logger.info("No valid (slug, ingestionDate) key pairs found in DataFrame to check.")
        return []

    # Ensure 'ingestionDate' is date object
    lookup_df = pd.DataFrame({
        'slug': key_pairs['slug'].astype(str), # Ensure slug is string
        'ingestionDate': pd.to_datetime(key_pairs['ingestionDate']).dt.date
    })

    logger.info(f"Prepared {len(lookup_df)} unique key pairs for lookup.")

    bq_client = get_bigquery_client(logger=logger) # Get client

    # --- Temp Table Operations ---
    # No broad try-except here, let BQ errors propagate to Prefect unless specific handling (like cleanup) is needed
    try:
        # Ensure temp dataset exists (keep specific try-except for this setup step)
        try:
            bq_client.get_dataset(f"{bq_project_id}.{temp_dataset}")
        except NotFound:
            logger.info(f"Temporary dataset {temp_dataset} not found. Creating...")
            dataset = bigquery.Dataset(f"{bq_project_id}.{temp_dataset}")
            dataset.location = bq_temp_dataset_location # Use configured location
            bq_client.create_dataset(dataset, exists_ok=True)
            logger.info(f"Temporary dataset {temp_dataset} created or already exists.")

        # Define schema for temp table
        schema = [
            bigquery.SchemaField("slug", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("ingestionDate", "DATE", mode="REQUIRED")
        ]

        # Create temp table (exists_ok=True is safe)
        # table = bigquery.Table(temp_table_id, schema=schema)
        # bq_client.create_table(table, exists_ok=True) # Creating explicitly can sometimes race condition? Load can create.

        # Load data into temp table
        logger.info(f"Loading {len(lookup_df)} keys into temporary table: {temp_table_id}")
        job_config = bigquery.LoadJobConfig(
            schema=schema,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE, # Overwrite if somehow exists
            create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED # Let load create the table
        )
        load_job = bq_client.load_table_from_dataframe(
            lookup_df, temp_table_ref, job_config=job_config
        )
        load_job.result() # Wait for completion and raise errors on failure

        # Check load job for errors (redundant if result() raises, but explicit check)
        if load_job.errors:
             logger.error(f"Load job for temporary table {temp_table_id} failed: {load_job.errors}")
             raise GoogleCloudError(f"Failed to load temp table {temp_table_id}", errors=load_job.errors)
        logger.info(f"Temporary table {temp_table_id} loaded successfully.")

        # --- Query for Existing Keys ---
        query = f"""
        SELECT t.slug, t.ingestionDate
        FROM `{full_table_id}` AS t
        INNER JOIN `{temp_table_id}` AS l
        ON t.slug = l.slug AND t.ingestionDate = l.ingestionDate
        """
        logger.info("Querying BigQuery to find existing keys using temporary table join...")
        query_job = bq_client.query(query)

        # Collect results directly from iterator (handles result waiting)
        existing_keys = [(row.slug, row.ingestionDate) for row in query_job]
        logger.info(f"Found {len(existing_keys)} existing key pairs in BigQuery.")

        return existing_keys

    except GoogleCloudError as e:
        # Catch BQ specific errors during load or query
        logger.error(f"BigQuery operation failed: {e}", exc_info=True)
        if hasattr(e, 'errors'): logger.error(f"Detailed BQ Errors: {e.errors}")
        raise # Let Prefect handle task failure
    except Exception as e:
        # Catch other unexpected errors during the process
        logger.error(f"Unexpected error checking existing keys: {e}", exc_info=True)
        raise # Let Prefect handle task failure
    finally:
        # --- Cleanup Temporary Table (crucial) ---
        # Use a separate try-except for cleanup, failure here shouldn't fail the main task if keys were found
        try:
            logger.info(f"Attempting to delete temporary table: {temp_table_id}")
            bq_client.delete_table(temp_table_id, not_found_ok=True) # Use not_found_ok=True
            logger.info(f"Successfully deleted temporary table: {temp_table_id}")
        except GoogleCloudError as e:
            logger.warning(f"Could not delete temporary table {temp_table_id}: {e}")
        except Exception as e:
            logger.warning(f"Unexpected error deleting temporary table {temp_table_id}: {e}")


@task
def filter_out_duplicates(df: pd.DataFrame, existing_keys: List[tuple]) -> pd.DataFrame:
    """(Prefect Task) Filters out rows with existing (slug, ingestionDate) pairs."""
    logger = get_run_logger()
    if df.empty:
        logger.info("Input DataFrame empty, no filtering.")
        return df
    if not existing_keys:
        logger.info("No existing keys provided, keeping all rows.")
        return df
    if 'slug' not in df.columns or 'ingestionDate' not in df.columns:
        raise ValueError("DataFrame needs 'slug'/'ingestionDate' for filtering.")
    existing_keys_set = set(existing_keys)
    initial_count = len(df)
    logger.info(f"Filtering {initial_count} rows against {len(existing_keys_set)} existing keys.")
    df_copy = df.copy()
    df_copy['ingestionDate_date'] = pd.to_datetime(df_copy['ingestionDate']).dt.date
    keep_mask = df_copy.apply(
        lambda row: pd.notna(row['slug']) and pd.notna(row['ingestionDate_date']) and \
                    (row['slug'], row['ingestionDate_date']) not in existing_keys_set,
        axis=1
    )
    filtered_df = df_copy[keep_mask].drop(columns=['ingestionDate_date'])
    removed_count = initial_count - len(filtered_df)
    logger.info(f"Filtered out {removed_count} duplicate/invalid rows. Keeping {len(filtered_df)} new rows.")
    return filtered_df.reset_index(drop=True)

@task
def load_to_bigquery(df: pd.DataFrame) -> None:
    """(Prefect Task) Loads DataFrame to BigQuery."""
    logger = get_run_logger()
    if df.empty:
        logger.warning("Input DataFrame empty. Skipping BigQuery load.")
        return
    bq_project_id = os.getenv("BQ_PROJECT_ID")
    bq_dataset_id = os.getenv("BQ_DATASET_ID")
    bq_table_id = os.getenv("BQ_TABLE_ID")
    bq_write_disposition = os.getenv("BQ_WRITE_DISPOSITION", "WRITE_APPEND")
    bq_create_disposition = os.getenv("BQ_CREATE_DISPOSITION", "CREATE_NEVER")
    if not all([bq_project_id, bq_dataset_id, bq_table_id]):
        raise ValueError("Missing BQ config (PROJECT_ID, DATASET_ID, TABLE_ID)")
    full_table_id = f"{bq_project_id}.{bq_dataset_id}.{bq_table_id}"
    logger.info(f"Loading {len(df)} rows to BQ table: {full_table_id} ({bq_write_disposition})")
    bq_client = get_bigquery_client(logger=logger)
    job_config = bigquery.LoadJobConfig(
        write_disposition=bq_write_disposition,
        create_disposition=bq_create_disposition,
        autodetect=False,
    )
    try:
        job = bq_client.load_table_from_dataframe(df, full_table_id, job_config=job_config)
        logger.info(f"Submitted BQ load job: {job.job_id}")
        job.result()
        if job.errors:
             logger.error(f"BQ load job {job.job_id} finished with errors: {job.errors}")
             raise GoogleCloudError(f"BQ load job {job.job_id} failed.", errors=job.errors)
        else:
            table = bq_client.get_table(full_table_id)
            logger.info(f"Successfully loaded {job.output_rows} rows to {full_table_id} (Total: {table.num_rows})")
    except GoogleCloudError as e:
        logger.error(f"Error loading data to BigQuery: {e}", exc_info=True)
        if hasattr(e, 'errors'): logger.error(f"BQ detailed errors: {e.errors}")
        raise

# --- Cloud Function/Run Entry Point (Optional) ---
def process_gcs_parquet_file(event: Dict[str, Any], context=None):
    """Cloud Function Trigger - Recommended: Trigger Prefect flow."""
    try:
        bucket = event.get('bucket')
        name = event.get('name')
        if not bucket or not name:
            logging.error("Invalid event: missing bucket or name")
            return
        gcs_subfolder = os.getenv('GCS_SUBFOLDER_PATH', '')
        if gcs_subfolder and not name.startswith(gcs_subfolder.strip('/')):
            logging.info(f"Ignoring file '{name}' outside '{gcs_subfolder}'")
            return
        if not name.endswith('.parquet'):
            logging.info(f"Ignoring non-Parquet file: {name}")
            return
        gcs_path = f"gs://{bucket}/{name}"
        logging.info(f"CF Triggered for: {gcs_path}")
        logging.info(f"Triggering Prefect flow 'gcs_to_bigquery_etl_flow'")
        # --- Add Prefect Flow Trigger Logic Here ---
        logging.warning("Prefect flow trigger mechanism TBD in 'process_gcs_parquet_file'.")
        # Example placeholder call (needs flow modification/deployment setup):
        # gcs_to_bigquery_etl_flow(gcs_uri_param=gcs_path)
    except Exception as e:
        logging.error(f"Error in Cloud Function trigger: {str(e)}", exc_info=True)
        raise

# --- Prefect Flow ---
@flow(name="GCS Parquet to BigQuery ETL")
def gcs_to_bigquery_etl_flow(gcs_uri_param: Optional[str] = None):
    """Prefect Flow orchestrating the ETL process."""
    logger = get_run_logger()
    logger.info("Starting GCS Parquet to BigQuery ETL Flow...")
    try:
        # --- Determine Input File ---
        if gcs_uri_param:
            logger.info(f"Using provided GCS URI: {gcs_uri_param}")
            latest_file = gcs_uri_param
            if not latest_file.endswith(FILE_EXTENSION):
                 logger.error(f"URI '{latest_file}' lacks '{FILE_EXTENSION}' extension.")
                 raise ValueError(f"Invalid GCS URI extension: {latest_file}")
        else:
            logger.info("No GCS URI provided, finding latest file...")
            gcs_bucket_name = os.getenv("GCS_BUCKET_NAME")
            gcs_subfolder_path = os.getenv("GCS_SUBFOLDER_PATH")
            if not gcs_bucket_name or not gcs_subfolder_path:
                raise ValueError("Missing GCS_BUCKET_NAME/GCS_SUBFOLDER_PATH env vars.")
            logger.info(f"Searching gs://{gcs_bucket_name}/{gcs_subfolder_path}")
            latest_file = find_latest_file_in_gcs( # Task call
                bucket_name=gcs_bucket_name,
                prefix=gcs_subfolder_path,
                extension=FILE_EXTENSION
            )
        if not latest_file:
            logger.warning("No Parquet file found/specified. Exiting flow gracefully.")
            return # Graceful exit

        # --- Execute ETL Steps ---
        raw_data = read_parquet_from_gcs(latest_file)
        transformed_data = process_data(df=raw_data)

        if not transformed_data.empty:
             # *** Use the reverted check_existing_keys_in_bigquery task ***
             existing_keys = check_existing_keys_in_bigquery(transformed_data)
             new_data = filter_out_duplicates(transformed_data, existing_keys)
        else:
             logger.info("Transformed data empty, skipping duplicate checks/filtering.")
             new_data = transformed_data

        if not new_data.empty:
            load_to_bigquery(new_data)
        else:
            logger.info("No new data after filtering. Skipping BigQuery load.")

        logger.info("GCS Parquet to BigQuery ETL Flow completed successfully.")

    except Exception as e:
        logger.error(f"Flow 'GCS Parquet to BigQuery ETL' failed: {e}", exc_info=True)
        raise # Re-raise for Prefect failure state

# --- Main Execution Block ---
if __name__ == "__main__":
    logging.info("Script started directly. Running Prefect flow 'gcs_to_bigquery_etl_flow'...")
    gcs_to_bigquery_etl_flow()
    logging.info("Prefect flow execution attempt finished.")