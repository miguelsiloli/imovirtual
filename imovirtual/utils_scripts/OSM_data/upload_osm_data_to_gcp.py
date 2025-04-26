# main_osm_nodes.py

import os
import logging
import time
from dotenv import load_dotenv
from google.oauth2 import service_account
from google.cloud import storage, bigquery
# Import the necessary functions from utils, including the corrected cleaner
from utils import (process_osm_pbf,
                   validate_and_clean_geojsonl,
                   load_gcs_to_bigquery,
                   upload_to_gcs)

# --- Basic Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
load_dotenv()

# --- Security Warning ---
logging.warning("Loading credentials directly from .env. Review security implications.")

# --- GCP Credentials (Loaded from .env) ---
try:
    credentials_info = {
        "type": os.getenv("TYPE"),
        "project_id": os.getenv("PROJECT_ID"),
        "private_key_id": os.getenv("PRIVATE_KEY_ID"),
        "private_key": os.getenv("PRIVATE_KEY").replace('\\n', '\n'),
        "client_email": os.getenv("CLIENT_EMAIL"),
        "client_id": os.getenv("CLIENT_ID"),
        "auth_uri": os.getenv("AUTH_URI"),
        "token_uri": os.getenv("TOKEN_URI"),
        "auth_provider_x509_cert_url": os.getenv("AUTH_PROVIDER_X509_CERT_URL"),
        "client_x509_cert_url": os.getenv("CLIENT_X509_CERT_URL"),
        "universe_domain": os.getenv("UNIVERSE_DOMAIN")
    }
    # Handle 'universe_domain' to prevent possible credential errors
    if credentials_info.get("universe_domain") is None:
        credentials_info.pop("universe_domain", None)
        
    if not all([credentials_info["project_id"], credentials_info["private_key"], credentials_info["client_email"]]):
        raise ValueError("Missing essential credential information in .env file")
    credentials = service_account.Credentials.from_service_account_info(credentials_info)
    project_id = credentials.project_id
    logging.info(f"Successfully loaded credentials via .env for project: {project_id}")
except Exception as e:
    logging.error(f"Failed to load or validate credentials from .env: {e}")
    exit(1)

# --- Shared GCS Configuration ---
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
if not GCS_BUCKET_NAME:
    logging.error("GCS_BUCKET_NAME not found in .env file. Please define it.")
    exit(1)

# --- OSM Node Processing Configuration ---
# Ensure these paths are correct for your environment
OSM_SOURCE_PBF_PATH = 'imovirtual/utils_scripts/OSM_data/portugal-latest.osm.pbf'
OSM_RAW_GEOJSONL_PATH = '/home/miguel/Projects/imovirtual/portugal_nodes.geojsonl' # Raw output from osmium
OSM_CLEANED_GEOJSONL_PATH = '/home/miguel/Projects/imovirtual/portugal_nodes_cleaned.geojsonl' # Cleaned file for upload
OSM_FILTER = 'n' # Process nodes ('n')

# GCS path based on the CLEANED file name
OSM_GCS_DESTINATION_BLOB_NAME = f'staging/osm/{os.path.basename(OSM_CLEANED_GEOJSONL_PATH)}'

# BigQuery Configuration
OSM_BQ_DATASET_ID = "staging_housing"
OSM_BQ_TABLE_ID = "osm_pt_nodes"
OSM_BQ_TABLE_FULL_ID = f"{project_id}.{OSM_BQ_DATASET_ID}.{OSM_BQ_TABLE_ID}"
OSM_BQ_WRITE_DISPOSITION = bigquery.WriteDisposition.WRITE_TRUNCATE
OSM_BQ_CREATE_DISPOSITION = bigquery.CreateDisposition.CREATE_IF_NEEDED
OSM_BQ_MAX_BAD_RECORDS = 0 # Fail on any error during load

# *** CORRECTED BIGQUERY SCHEMA ***
OSM_BQ_SCHEMA = [
    bigquery.SchemaField("geometry", "GEOGRAPHY"),
    # Changed from STRING to JSON to correctly load the nested properties object
    bigquery.SchemaField("properties", "JSON"),
    bigquery.SchemaField("id", "STRING"),
]

# ==============================================================================
# Main Execution Logic
# ==============================================================================

if __name__ == "__main__":

    # --- Check Source PBF File Existence ---
    if not os.path.exists(OSM_SOURCE_PBF_PATH):
        logging.error(f"Source OSM PBF file not found: {OSM_SOURCE_PBF_PATH}")
        exit(1)
    else:
        logging.info(f"Found source OSM PBF file: {OSM_SOURCE_PBF_PATH}")

    # --- Initialize GCP Clients ---
    storage_client = None
    bigquery_client = None
    try:
        storage_client = storage.Client(credentials=credentials, project=project_id)
        bigquery_client = bigquery.Client(credentials=credentials, project=project_id)
        logging.info("GCS and BigQuery clients initialized successfully.")
    except Exception as e:
        logging.error(f"Failed to initialize GCP clients: {e}")
        exit(1)

    # --- Process ALL OSM Nodes ---
    logging.info(f"--- Starting OSM Node Processing (Filter: '{OSM_FILTER}') ---")

    # 1. Process OSM PBF to RAW GeoJSON Lines using osmium-tool
    #    Output format should be geojsonseq (which uses \x1e)
    logging.info(f"Generating raw GeoJSONSeq file: {OSM_RAW_GEOJSONL_PATH}")
    osm_processing_success = process_osm_pbf(
        OSM_SOURCE_PBF_PATH,
        OSM_RAW_GEOJSONL_PATH, # Output to the raw file path
        OSM_FILTER
    )

    # Check if Osmium step succeeded
    if not osm_processing_success:
        logging.error("OSM node processing failed during PBF conversion (osmium step).")
        exit(1)
    # Even if osmium reported success, double check the raw file exists
    if not os.path.exists(OSM_RAW_GEOJSONL_PATH):
         logging.error(f"Osmium processing reported success, but raw output file not found: {OSM_RAW_GEOJSONL_PATH}")
         exit(1)
    logging.info(f"Successfully created raw GeoJSONSeq file: {OSM_RAW_GEOJSONL_PATH}")

    # 2. Validate and Clean the RAW GeoJSON Lines file
    #    This step now removes the leading \x1e and ensures valid JSON per line
    logging.info(f"Starting cleaning process: {OSM_RAW_GEOJSONL_PATH} -> {OSM_CLEANED_GEOJSONL_PATH}")
    cleaning_success = validate_and_clean_geojsonl(
        OSM_RAW_GEOJSONL_PATH,
        OSM_CLEANED_GEOJSONL_PATH # Output to the cleaned file path
    )

    if not cleaning_success:
        logging.error("OSM node processing failed during GeoJSON validation/cleaning step.")
        exit(1)
    # Check if the cleaned file actually exists and has content
    if not os.path.exists(OSM_CLEANED_GEOJSONL_PATH) or os.path.getsize(OSM_CLEANED_GEOJSONL_PATH) == 0:
         logging.error(f"GeoJSON cleaning reported success, but cleaned file is missing or empty: {OSM_CLEANED_GEOJSONL_PATH}")
         exit(1)
    logging.info(f"Successfully created cleaned GeoJSONL file (NDJSON): {OSM_CLEANED_GEOJSONL_PATH}")

    # 3. Upload the CLEANED GeoJSONL to GCS
    osm_gcs_uri = upload_to_gcs(
        GCS_BUCKET_NAME,
        OSM_CLEANED_GEOJSONL_PATH, # Upload the cleaned file
        OSM_GCS_DESTINATION_BLOB_NAME, # Use the GCS path based on cleaned file name
        storage_client
    )

    # 4. If GCS upload succeeded, proceed to BigQuery load
    if osm_gcs_uri:
        logging.info(f"Configuring BigQuery load job with schema: {OSM_BQ_SCHEMA}")
        
        # Create a temporary table first
        temp_table_id = f"{project_id}.{OSM_BQ_DATASET_ID}.temp_osm_nodes_{int(time.time())}"
        
        # Configure job for initial load to temp table - without geography conversion
        temp_job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField("geometry", "JSON"),  # Load as JSON initially
                bigquery.SchemaField("properties", "JSON"),
                bigquery.SchemaField("id", "STRING"),
            ],
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            create_disposition=OSM_BQ_CREATE_DISPOSITION,
            write_disposition=OSM_BQ_WRITE_DISPOSITION,
            max_bad_records=OSM_BQ_MAX_BAD_RECORDS,
            ignore_unknown_values=True
        )
        
        logging.info(f"Loading raw data to temp table: {temp_table_id}")
        temp_load_success = load_gcs_to_bigquery(
            osm_gcs_uri,
            temp_table_id,
            temp_job_config,
            bigquery_client
        )
        
        if temp_load_success:
            # Now use SQL to transform and create the final table with proper GEOGRAPHY type
            transform_sql = f"""
            CREATE OR REPLACE TABLE `{OSM_BQ_TABLE_FULL_ID}` AS
            SELECT 
              ST_GEOGFROMGEOJSON(TO_JSON_STRING(geometry)) AS geometry,
              properties,
              id
            FROM `{temp_table_id}`
            """
            
            try:
                logging.info(f"Running transformation SQL to convert geometry to GEOGRAPHY type")
                query_job = bigquery_client.query(transform_sql)
                query_job.result()  # Wait for query to complete
                
                # Clean up temp table
                bigquery_client.delete_table(temp_table_id)
                logging.info(f"Successfully transformed data and created final table: {OSM_BQ_TABLE_FULL_ID}")
                logging.info(f"OSM node processing completed successfully.")
            except Exception as e:
                logging.error(f"Failed during SQL transformation: {e}")
                exit(1)
        else:
            logging.error("Failed to load data to temporary table.")
            exit(1)
    else:
        logging.error("OSM node processing failed during GCS upload of cleaned file.")
        exit(1)

    logging.info("Main script finished.")