# utils.py

import logging
import os
import subprocess # Import subprocess
import shutil # To check if osmium exists

import geopandas as gpd # Keep existing imports
from google.cloud.exceptions import NotFound
from google.cloud import bigquery # Add bigquery for SchemaField access if needed later
import json

# --- Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# ==============================================================================
# Helper Functions
# ==============================================================================

# --- process_osm_pbf function remains the same as the last version ---
def process_osm_pbf(source_pbf, output_geojsonl, osm_filter):
    """
    Uses osmium-tool (must be installed) to filter an OSM PBF file
    and export the results as GeoJSON Lines, overwriting the output if it exists.
    """
    logging.info(f"Processing OSM PBF: {source_pbf} with filter '{osm_filter}'")
    if not shutil.which("osmium"):
        logging.error("osmium command not found. Please install osmium-tool.")
        return False
    if not os.path.exists(source_pbf):
        logging.error(f"Source OSM PBF file not found: {source_pbf}")
        return False
    command = [
        "osmium", "export", "--overwrite", source_pbf,
        "--output-format", "geojsonseq", "--output", output_geojsonl
    ]
    logging.info(f"Executing command: {' '.join(command)}")
    try:
        result = subprocess.run(command, capture_output=True, text=True, check=True, encoding='utf-8')
        if result.stderr: logging.info(f"Osmium command STDERR:\n{result.stderr}")
        if result.stdout: logging.info(f"Osmium command STDOUT:\n{result.stdout}")
        if os.path.exists(output_geojsonl) and os.path.getsize(output_geojsonl) > 0:
             logging.info(f"Successfully created/overwritten GeoJSON Lines file: {output_geojsonl}")
             return True
        elif os.path.exists(output_geojsonl):
             logging.warning(f"Osmium command succeeded but output file is empty: {output_geojsonl}")
             return False
        else:
             logging.error(f"Osmium command finished but output file does not exist: {output_geojsonl}")
             return False
    except FileNotFoundError:
         logging.error("'osmium' command failed to execute. Is it correctly installed and in PATH?")
         return False
    except subprocess.CalledProcessError as e:
        logging.error(f"Osmium command failed with exit code {e.returncode}")
        if e.stderr: logging.error(f"STDERR:\n{e.stderr}")
        if e.stdout: logging.error(f"STDOUT:\n{e.stdout}")
        return False
    except Exception as e:
        logging.error(f"An unexpected error occurred during osmium processing: {e}")
        return False


# --- Modified validate_and_clean_geojsonl ---
def validate_and_clean_geojsonl(input_path, output_path):
    """
    Reads a GeoJSON Lines file, skips empty/whitespace lines,
    validates each remaining line as JSON, and writes valid lines to a new file.
    """
    logging.info(f"Validating and cleaning GeoJSON Lines file: {input_path}")
    valid_count = 0
    invalid_count = 0
    skipped_empty_count = 0 # Counter for empty/whitespace lines
    try:
        with open(input_path, 'r', encoding='utf-8') as infile, \
             open(output_path, 'w', encoding='utf-8') as outfile:
            for i, line in enumerate(infile):
                # *** ADDED CHECK FOR EMPTY/WHITESPACE LINES ***
                stripped_line = line.lstrip('\x1e')
                if not stripped_line:
                    skipped_empty_count += 1
                    continue # Skip to the next line

                try:
                    # Attempt to parse the stripped line as JSON
                    # We still write the ORIGINAL line (with newline) if valid
                    json.loads(stripped_line)
                    outfile.write(stripped_line) # Write the original line including newline
                    valid_count += 1
                except json.JSONDecodeError as e:
                    invalid_count += 1
                    if invalid_count <= 10:
                        logging.warning(f"Invalid JSON on line {i+1}: {e}. Line: {stripped_line[:200]}...")
                    elif invalid_count == 11:
                        logging.warning("Further invalid JSON lines will not be logged individually.")
                except Exception as e:
                    logging.error(f"Unexpected error processing line {i+1}: {e}")
                    invalid_count += 1

        logging.info(f"Cleaning complete. Valid lines written: {valid_count}, Invalid lines dropped: {invalid_count}, Empty/whitespace lines skipped: {skipped_empty_count}")
        if valid_count == 0 and (invalid_count > 0 or skipped_empty_count > 0):
             logging.error("No valid JSON lines found/written to the output file.")
             return False
        logging.info(f"Cleaned file written to: {output_path}")
        return True
    except FileNotFoundError:
        logging.error(f"Input file not found for validation: {input_path}")
        return False
    except Exception as e:
        logging.error(f"Failed during GeoJSON validation/cleaning: {e}")
        return False

# --- upload_to_gcs and load_gcs_to_bigquery functions remain the same ---
def upload_to_gcs(bucket_name, source_file_name, destination_blob_name, gcs_client):
    """Uploads a file to the bucket."""
    # (Implementation from previous answer is fine)
    if not os.path.exists(source_file_name):
        logging.error(f"Local source file not found for upload: {source_file_name}")
        return None
    try:
        bucket = gcs_client.get_bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        logging.info(f"Uploading {source_file_name} to gs://{bucket_name}/{destination_blob_name}...")
        blob.upload_from_filename(source_file_name, timeout=600) # Increased timeout for potentially large files
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
    # (Implementation from previous answer is fine)
    if not gcs_uri or not gcs_uri.startswith("gs://"):
        logging.error(f"Invalid GCS URI provided for BigQuery load: {gcs_uri}")
        return False
    logging.info(f"Starting BigQuery load job from {gcs_uri} to {table_id}...")
    load_job = bq_client.load_table_from_uri(
        gcs_uri, table_id, job_config=job_config
    )
    logging.info(f"Load job submitted. Job ID: {load_job.job_id}. Waiting for completion...")
    try:
        load_job.result(timeout=600)
        logging.info(f"BigQuery load job {load_job.job_id} completed successfully.")
        destination_table = bq_client.get_table(table_id)
        logging.info(f"Loaded {destination_table.num_rows} rows into {table_id}.")
        if load_job.errors:
             logging.warning(f"Load job completed with {len(load_job.errors)} non-fatal errors.")
             for error in load_job.errors[:5]:
                  logging.warning(f"  Error detail: {error.get('reason', 'N/A')} - {error.get('message', 'No message')}")
        return True
    except Exception as e:
        logging.error(f"BigQuery load job failed for Job ID {load_job.job_id}: {e}")
        if hasattr(load_job, 'errors') and load_job.errors:
            for error in load_job.errors:
                logging.error(f"  Error detail: {error.get('reason', 'N/A')} - {error.get('message', 'No message')}")
        elif load_job.error_result:
             logging.error(f"  Error result: {load_job.error_result}")
        return False