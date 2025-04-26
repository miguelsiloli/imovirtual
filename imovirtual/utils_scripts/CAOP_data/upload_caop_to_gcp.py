import os
import logging
from dotenv import load_dotenv
from google.oauth2 import service_account
from google.cloud import storage, bigquery
from utils import process_geopackage, load_gcs_to_bigquery, upload_to_gcs

# --- Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load environment variables from .env file
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
    if not all([credentials_info["project_id"], credentials_info["private_key"], credentials_info["client_email"]]):
        raise ValueError("Missing essential credential information in .env file")

    credentials = service_account.Credentials.from_service_account_info(credentials_info)
    project_id = credentials.project_id
    logging.info(f"Successfully loaded credentials via .env for project: {project_id}")

except Exception as e:
    logging.error(f"Failed to load or validate credentials from .env: {e}")
    exit(1)


# --- File Path Configuration ---
SOURCE_GPKG_PATH = 'imovirtual/utils_scripts/CAOP_data/Continente_CAOP2024.gpkg'       # Source GeoPackage file name
LOCAL_GEOJSONL_PATH = 'imovirtual/utils_scripts/CAOP_data/caop_freguesias_4326.geojsonl' # Intermediate GeoJSON Lines file

# --- GCS Configuration ---
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
if not GCS_BUCKET_NAME:
    logging.error("GCS_BUCKET_NAME not found in .env file. Please define it.")
    exit(1)
GCS_DESTINATION_BLOB_NAME = f'staging/caop/{os.path.basename(LOCAL_GEOJSONL_PATH)}'

# --- BigQuery Configuration ---
BQ_DATASET_ID = "staging_housing"
BQ_TABLE_ID = "freguesia_boundaries_osm" # Table to store the boundaries
BQ_TABLE_FULL_ID = f"{project_id}.{BQ_DATASET_ID}.{BQ_TABLE_ID}"
BQ_WRITE_DISPOSITION = bigquery.WriteDisposition.WRITE_TRUNCATE
BQ_CREATE_DISPOSITION = bigquery.CreateDisposition.CREATE_IF_NEEDED

# --- Define BigQuery Schema ---
# Based on the columns printed earlier: ['id', 'dtmnfr', 'freguesia', 'tipo_area_administrativa', 'municipio', 'distrito_ilha', 'nuts3', 'nuts2', 'nuts1', 'area_ha', 'perimetro_km', 'geometry']
schema = [
    bigquery.SchemaField("id", "STRING"),
    bigquery.SchemaField("dtmnfr", "STRING"),
    bigquery.SchemaField("freguesia", "STRING"),
    bigquery.SchemaField("tipo_area_administrativa", "STRING"),
    bigquery.SchemaField("municipio", "STRING"),
    bigquery.SchemaField("distrito_ilha", "STRING"),
    bigquery.SchemaField("nuts3", "STRING"),
    bigquery.SchemaField("nuts2", "STRING"),
    bigquery.SchemaField("nuts1", "STRING"),
    bigquery.SchemaField("area_ha", "FLOAT64"),
    bigquery.SchemaField("perimetro_km", "FLOAT64"),
    bigquery.SchemaField("geometry", "GEOGRAPHY"),
]
# ==============================================================================
# Main Execution Logic
# ==============================================================================

if __name__ == "__main__":
    # 1. Process GeoPackage to GeoJSON Lines
    processing_success = process_geopackage(SOURCE_GPKG_PATH, LOCAL_GEOJSONL_PATH)

    if not processing_success:
        logging.error("Aborting script due to GeoPackage processing failure.")
        exit(1)

    # 2. Initialize GCP Clients (only after successful processing)
    try:
        storage_client = storage.Client(credentials=credentials, project=project_id)
        bigquery_client = bigquery.Client(credentials=credentials, project=project_id)
        logging.info("GCS and BigQuery clients initialized successfully.")
    except Exception as e:
        logging.error(f"Failed to initialize GCP clients: {e}")
        exit(1)

    # 3. Upload the generated GeoJSONL to GCS
    gcs_file_uri = upload_to_gcs(
        GCS_BUCKET_NAME,
        LOCAL_GEOJSONL_PATH, # Use the generated file path
        GCS_DESTINATION_BLOB_NAME,
        storage_client
    )

    # 4. If GCS upload succeeded, proceed to BigQuery load
    if gcs_file_uri:
        # Configure the BigQuery load job
        job_config = bigquery.LoadJobConfig(
            schema=schema,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            json_extension="GEOJSON", # Tells BQ how to parse the geometry field
            create_disposition=BQ_CREATE_DISPOSITION,
            write_disposition=BQ_WRITE_DISPOSITION,
        )

        # Execute the load job
        bq_load_success = load_gcs_to_bigquery(
            gcs_file_uri,
            BQ_TABLE_FULL_ID,
            job_config,
            bigquery_client
        )

        if bq_load_success:
            logging.info("Process completed successfully: File processed, uploaded to GCS, and loaded into BigQuery.")
            # Optional: Clean up local GeoJSONL file
            # try:
            #     os.remove(LOCAL_GEOJSONL_PATH)
            #     logging.info(f"Removed local file: {LOCAL_GEOJSONL_PATH}")
            # except OSError as e:
            #     logging.warning(f"Could not remove local file {LOCAL_GEOJSONL_PATH}: {e}")
        else:
            logging.error("Process failed during BigQuery load.")
            exit(1) # Exit with error status
    else:
        logging.error("Process failed: GCS upload step failed.")
        exit(1) # Exit with error status