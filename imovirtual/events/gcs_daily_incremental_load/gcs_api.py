import os
import logging
import json
import tempfile
from datetime import date
from typing import Optional, Dict, Any, List

import pandas as pd
from google.cloud import storage, bigquery
from google.cloud.exceptions import NotFound
from google.oauth2 import service_account
import time

# --- Authentication Setup ---
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

    except NotFound:
        logging.error(f"Bucket '{bucket_name}' not found or prefix '{prefix_with_slash}' does not exist.")
        return None
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
        # Use GCS client directly without trying the pandas/pyarrow integration
        from google.cloud import storage
        
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

def process_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform the raw Parquet data (assumed to have structure similar to the sample JSON) 
    to match the BigQuery schema `poised-space-456813-t0.staging_housing.staging`.
    
    Args:
        df: The DataFrame containing raw data from the Parquet file. 
            Assumes columns might contain nested dicts/lists similar to the JSON structure.
        
    Returns:
        A DataFrame with the transformed data, ready for BigQuery insertion.
    """
    logging.info(f"Starting transformation for {len(df)} raw records.")
    
    transformed_df = pd.DataFrame()
    
    try:
        # --- Direct Mappings (with type safety where appropriate) ---
        transformed_df['id'] = df['id'].astype(str)
        transformed_df['title'] = df['title'].astype(str)
        transformed_df['slug'] = df['slug'].astype(str) # Crucial: Ensure source slug is never null/empty if BQ is NOT NULL
        transformed_df['estate'] = df['estate'].astype(str).fillna(pd.NA)
        transformed_df['transaction'] = df['transaction'].astype(str).fillna(pd.NA)
        transformed_df['developmentId'] = pd.to_numeric(df['developmentId'], errors='coerce').astype('Int64')
        transformed_df['developmentTitle'] = df['developmentTitle'].astype(str).fillna(pd.NA)
        transformed_df['developmentUrl'] = df['developmentUrl'].astype(str).fillna(pd.NA)
        transformed_df['isExclusiveOffer'] = df['isExclusiveOffer'].astype(bool)
        transformed_df['isPrivateOwner'] = df['isPrivateOwner'].astype(bool)
        transformed_df['isPromoted'] = df['isPromoted'].astype(bool)
        transformed_df['source'] = df['source'].astype(str).fillna(pd.NA)
        transformed_df['openDays'] = df['openDays'].astype(str).fillna(pd.NA)
        transformed_df['areaInSquareMeters'] = pd.to_numeric(df['areaInSquareMeters'], errors='coerce').astype(float)
        transformed_df['terrainAreaInSquareMeters'] = pd.to_numeric(df['terrainAreaInSquareMeters'], errors='coerce').astype(float)
        transformed_df['roomsNumber'] = df['roomsNumber'].astype(str).fillna(pd.NA) # Keep as STRING per DDL
        transformed_df['hidePrice'] = df['hidePrice'].astype(bool)
        transformed_df['floorNumber'] = df['floorNumber'].astype(str).fillna(pd.NA) # Keep as STRING per DDL
        transformed_df['shortDescription'] = df['shortDescription'].astype(str).fillna(pd.NA)
        transformed_df['totalPossibleImages'] = pd.to_numeric(df['totalPossibleImages'], errors='coerce').astype('Int64')

        # --- Nested Field Extractions (using safe .get() within apply) ---

        # Location Details
        transformed_df['city'] = df['location'].apply(lambda loc: loc.get('address', {}).get('city', {}).get('name') if isinstance(loc, dict) else None).astype(str).fillna(pd.NA)
        transformed_df['province'] = df['location'].apply(lambda loc: loc.get('address', {}).get('province', {}).get('name') if isinstance(loc, dict) else None).astype(str).fillna(pd.NA)
        # Corrected street access: Access the value directly, handle if location or address is missing
        transformed_df['street'] = df['location'].apply(lambda loc: loc.get('address', {}).get('street') if isinstance(loc, dict) else None).astype(str).fillna(pd.NA)
        transformed_df['mapRadius'] = df['location'].apply(lambda loc: loc.get('mapDetails', {}).get('radius') if isinstance(loc, dict) else None)
        transformed_df['mapRadius'] = pd.to_numeric(transformed_df['mapRadius'], errors='coerce').astype(float)

        # Agency Details
        transformed_df['agencyId'] = df['agency'].apply(lambda ag: ag.get('id') if isinstance(ag, dict) else None)
        transformed_df['agencyId'] = pd.to_numeric(transformed_df['agencyId'], errors='coerce').astype('Int64')
        transformed_df['agencyName'] = df['agency'].apply(lambda ag: ag.get('name') if isinstance(ag, dict) else None).astype(str).fillna(pd.NA)
        transformed_df['agencySlug'] = df['agency'].apply(lambda ag: ag.get('slug') if isinstance(ag, dict) else None).astype(str).fillna(pd.NA)
        transformed_df['agencyType'] = df['agency'].apply(lambda ag: ag.get('type') if isinstance(ag, dict) else None).astype(str).fillna(pd.NA)
        transformed_df['agencyBrandingVisible'] = df['agency'].apply(lambda ag: ag.get('brandingVisible') if isinstance(ag, dict) else None).astype('boolean') # Use nullable boolean
        transformed_df['agencyHighlightedAds'] = df['agency'].apply(lambda ag: ag.get('highlightedAds') if isinstance(ag, dict) else None).astype('boolean') # Use nullable boolean
        transformed_df['agencyImageUrl'] = df['agency'].apply(lambda ag: ag.get('imageUrl') if isinstance(ag, dict) else None).astype(str).fillna(pd.NA)

        # Price Details
        transformed_df['totalPriceCurrency'] = df['totalPrice'].apply(lambda price: price.get('currency') if isinstance(price, dict) else None).astype(str).fillna(pd.NA)
        transformed_df['totalPriceValue'] = df['totalPrice'].apply(lambda price: price.get('value') if isinstance(price, dict) else None)
        transformed_df['totalPriceValue'] = pd.to_numeric(transformed_df['totalPriceValue'], errors='coerce').astype(float)
        
        transformed_df['pricePerSquareMeterCurrency'] = df['pricePerSquareMeter'].apply(lambda price: price.get('currency') if isinstance(price, dict) else None).astype(str).fillna(pd.NA)
        transformed_df['pricePerSquareMeterValue'] = df['pricePerSquareMeter'].apply(lambda price: price.get('value') if isinstance(price, dict) else None)
        transformed_df['pricePerSquareMeterValue'] = pd.to_numeric(transformed_df['pricePerSquareMeterValue'], errors='coerce').astype(float)

        # --- Timestamps ---
        transformed_df['dateCreated'] = pd.to_datetime(df['dateCreated'], errors='coerce')
        transformed_df['dateCreatedFirst'] = pd.to_datetime(df['dateCreatedFirst'], errors='coerce')

        # --- Additional Info (JSON) ---
        # Option 1: Set to None (as in original code) - Simpler, loses data.
        transformed_df['additionalInfo'] = None 
        
        # Option 2: Populate with selected fields (requires import json)
        # def create_additional_info(row):
        #     info = {}
        #     # Add fields selectively, checking for existence
        #     if pd.notna(row.get('investmentState')): info['investmentState'] = row['investmentState']
        #     # ... add other fields like pushedUpAt, href, specialOffer etc.
        #     if isinstance(row.get('images'), list):
        #         info['imageUrls'] = [img.get('large') for img in row['images'] if isinstance(img, dict)]
        #     # Add more fields as needed...
        #     return json.dumps(info) if info else None # Return JSON string or None
        # transformed_df['additionalInfo'] = df.apply(create_additional_info, axis=1)
        
        # --- Ingestion Date ---
        transformed_df['ingestionDate'] = date.today()
        
        # --- Final Check & Column Order (Match DDL Order for Clarity) ---
        final_columns = [
            'id', 'title', 'slug', 'estate', 'transaction', 'developmentId', 'developmentTitle', 
            'developmentUrl', 'city', 'province', 'street', 'mapRadius', 'isExclusiveOffer', 
            'isPrivateOwner', 'isPromoted', 'source', 'agencyId', 'agencyName', 'agencySlug', 
            'agencyType', 'agencyBrandingVisible', 'agencyHighlightedAds', 'agencyImageUrl', 
            'openDays', 'totalPriceCurrency', 'totalPriceValue', 'pricePerSquareMeterCurrency', 
            'pricePerSquareMeterValue', 'areaInSquareMeters', 'terrainAreaInSquareMeters', 
            'roomsNumber', 'hidePrice', 'floorNumber', 'dateCreated', 'dateCreatedFirst', 
            'shortDescription', 'totalPossibleImages', 'additionalInfo', 'ingestionDate'
        ]
        transformed_df = transformed_df[final_columns]

        logging.info(f"Successfully transformed data. Resulting shape: {transformed_df.shape}")
        
        # Optional: Log data types for verification
        # logging.debug(f"Transformed DataFrame dtypes:\n{transformed_df.dtypes}")

    except KeyError as e:
        logging.error(f"Missing expected column in source DataFrame: {e}", exc_info=True)
        raise  # Re-raise to halt processing or handle upstream
    except Exception as e:
        logging.error(f"Error during data transformation: {e}", exc_info=True)
        raise # Re-raise

    return transformed_df

def load_to_bigquery(df: pd.DataFrame) -> None:
    """
    Load the transformed DataFrame to BigQuery.
    
    Args:
        df: The DataFrame to load into BigQuery
    """
    # Get BigQuery configuration from environment variables
    bq_project_id = os.getenv("BQ_PROJECT_ID")
    bq_dataset_id = os.getenv("BQ_DATASET_ID")
    bq_table_id = os.getenv("BQ_TABLE_ID")
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
    bq_dataset_id = os.getenv("BQ_DATASET_ID")
    bq_table_id = os.getenv("BQ_TABLE_ID")
    
    # Validate required configuration
    if not all([bq_project_id, bq_dataset_id, bq_table_id]):
        raise ValueError("Missing required BigQuery configuration in environment variables")
    
    # Construct full table ID
    full_table_id = f"{bq_project_id}.{bq_dataset_id}.{bq_table_id}"
    
    try:
        # Get unique (slug, ingestionDate) pairs from DataFrame
        key_pairs = df[['slug', 'ingestionDate']].drop_duplicates()
        
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
            'slug': key_pairs['slug'],
            'ingestionDate': pd.to_datetime(key_pairs['ingestionDate']).dt.date  # Ensure proper DATE format
        })
        
        # Define explicit schema to match the target table
        schema = [
            bigquery.SchemaField("slug", "STRING"),
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
        SELECT t.slug, t.ingestionDate
        FROM `{full_table_id}` t
        INNER JOIN `{temp_table_id}` l
        ON t.slug = l.slug
        AND t.ingestionDate = l.ingestionDate
        """
        
        # Run the query
        logging.info("Checking for existing key pairs in BigQuery")
        query_job = bq_client.query(query)
        
        # Collect results 
        existing_keys = []
        for row in query_job:
            # Convert back to the format needed for comparison
            existing_keys.append((row.slug, row.ingestionDate))
            
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
        DataFrame with only the new rows
    """
    if not existing_keys:
        logging.info("No existing keys found - keeping all rows")
        return df
        
    try:
        # Convert existing keys to a set for faster lookup
        existing_keys_set = set(existing_keys)
        
        # Create a mask for rows to keep
        initial_count = len(df)
        
        # Match format of dates for comparison (convert to string if needed)
        keep_mask = ~df.apply(
            lambda row: (
                row['slug'],
                row['ingestionDate'].date() if hasattr(row['ingestionDate'], 'date') else row['ingestionDate']
            ) in existing_keys_set, 
            axis=1
        )
        
        # Apply the mask to get only new rows
        filtered_df = df[keep_mask].reset_index(drop=True)
        
        removed_count = initial_count - len(filtered_df)
        logging.info(f"Filtered out {removed_count} duplicate rows, keeping {len(filtered_df)} new rows")
        
        return filtered_df
        
    except Exception as e:
        logging.error(f"Error filtering out duplicates: {e}", exc_info=True)
        raise

def process_gcs_parquet_file(event: Dict[str, Any], context=None) -> None:
    """
    Cloud Function/Run entry point for processing a Parquet file from GCS
    and loading it to BigQuery.
    
    Args:
        event: Cloud Functions/Run event payload
        context: Event metadata
    """
    try:
        # Extract GCS file path from event
        bucket = event.get('bucket')
        name = event.get('name')
        
        if not bucket or not name:
            logging.error("Invalid event data: missing bucket or file name")
            return
        
        # Check if file is in the monitored subfolder
        gcs_subfolder = os.getenv('GCS_SUBFOLDER_PATH', '')
        if gcs_subfolder and not name.startswith(gcs_subfolder):
            logging.info(f"Ignoring file not in monitored subfolder: {name}")
            return
        
        # Only process Parquet files
        if not name.endswith('.parquet'):
            logging.info(f"Ignoring non-Parquet file: {name}")
            return
            
        # Construct full GCS path
        gcs_path = f"gs://{bucket}/{name}"
        logging.info(f"Processing Parquet file: {gcs_path}")
        
        # Read the Parquet file
        raw_df = read_parquet_from_gcs(gcs_path)
        
        # Process the data
        transformed_df = process_data(raw_df)
        
        # Load to BigQuery
        load_to_bigquery(transformed_df)
        
        logging.info(f"Successfully processed and loaded file: {gcs_path}")
        
    except Exception as e:
        logging.error(f"Error processing file: {str(e)}", exc_info=True)
        raise

