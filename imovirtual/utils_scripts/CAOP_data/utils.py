
import logging, os 
import geopandas as gpd
from google.cloud.exceptions import NotFound
import json

# --- Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
# ==============================================================================
# Helper Functions
# ==============================================================================

def process_geopackage(source_gpkg, output_geojsonl):
    """Reads a GeoPackage, reprojects to EPSG:4326, and saves as GeoJSON Lines."""
    logging.info(f"Processing GeoPackage: {source_gpkg}")
    try:
        # Check if source file exists
        if not os.path.exists(source_gpkg):
             logging.error(f"Source GeoPackage file not found: {source_gpkg}")
             return False

        # Read the first layer by default, adjust if needed
        # You might need to specify layer='layer_name' if gpkg has multiple layers
        gdf = gpd.read_file(source_gpkg) # Reads the first layer by default
        logging.info(f"Read {len(gdf)} features. Original CRS: {gdf.crs}")
        logging.info(f"Columns found: {gdf.columns.tolist()}")

        # Check if 'geometry' column exists
        if 'geometry' not in gdf.columns:
            logging.error("No 'geometry' column found in the GeoPackage layer.")
            return False

        # Reproject if necessary
        if gdf.crs and gdf.crs != 'EPSG:4326':
            logging.info("Reprojecting to EPSG:4326...")
            gdf = gdf.to_crs('EPSG:4326')
            logging.info(f"Reprojection complete. New CRS: {gdf.crs}")
        elif not gdf.crs:
             logging.warning("GeoDataFrame CRS is not set. Assuming it doesn't need reprojection, but BQ requires WGS84.")
        else:
             logging.info("Data is already in EPSG:4326. No reprojection needed.")

        # Ensure required columns match the schema (optional but good practice)
        # Add checks here if needed

        # Save to GeoJSON Lines
        logging.info(f"Saving processed data to: {output_geojsonl}")
        gdf.to_file(output_geojsonl, driver='GeoJSONSeq', encoding='utf-8')
        logging.info("GeoJSON Lines file created successfully.")
        return True

    except Exception as e:
        logging.error(f"Error processing GeoPackage file {source_gpkg}: {e}")
        return False

def upload_to_gcs(bucket_name, source_file_name, destination_blob_name, gcs_client):
    """Uploads a file to the bucket."""
    if not os.path.exists(source_file_name):
        logging.error(f"Local source file not found for upload: {source_file_name}")
        return None
    try:
        bucket = gcs_client.get_bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        logging.info(f"Uploading {source_file_name} to gs://{bucket_name}/{destination_blob_name}...")
        blob.upload_from_filename(source_file_name, timeout=300)
        logging.info("File uploaded successfully.")
        return f"gs://{bucket_name}/{destination_blob_name}"
    except NotFound:
        logging.error(f"GCS Bucket '{bucket_name}' not found or access denied.")
        return None
    except Exception as e:
        logging.error(f"Failed to upload file to GCS: {e}")
        return None

def load_gcs_to_bigquery(gcs_uri, table_id, job_config, bq_client):
    """Loads data from GCS URI to BigQuery table."""
    if not gcs_uri or not gcs_uri.startswith("gs://"):
        logging.error(f"Invalid GCS URI provided for BigQuery load: {gcs_uri}")
        return False

    logging.info(f"Starting BigQuery load job from {gcs_uri} to {table_id}...")
    load_job = bq_client.load_table_from_uri(
        gcs_uri, table_id, job_config=job_config
    )
    logging.info(f"Load job submitted. Job ID: {load_job.job_id}. Waiting for completion...")
    try:
        load_job.result(timeout=600) # Wait up to 10 minutes
        logging.info(f"BigQuery load job {load_job.job_id} completed successfully.")
        destination_table = bq_client.get_table(table_id)
        logging.info(f"Loaded {destination_table.num_rows} rows into {table_id}.")
        return True
    except Exception as e:
        logging.error(f"BigQuery load job failed for Job ID {load_job.job_id}: {e}")
        if hasattr(load_job, 'errors') and load_job.errors:
            for error in load_job.errors:
                logging.error(f"  Error detail: {error.get('reason', 'N/A')} - {error.get('message', 'No message')}")
        elif load_job.error_result:
             logging.error(f"  Error result: {load_job.error_result}")
        return False
