import os
import logging
from dotenv import load_dotenv
from gcs_api import find_latest_file_in_gcs, read_parquet_from_gcs, process_data, check_existing_keys_in_bigquery, filter_out_duplicates, load_to_bigquery

# --- Configuration Setup ---
# Load environment variables from .env file
load_dotenv()

# Optional: Logging level
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

# File extension to search for
FILE_EXTENSION = ".parquet"

def configure_logging(logging_name="imovirtual_scraping.log"):
    """Configures logging for the script."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(funcName)s() - %(message)s",
        handlers=[
            logging.FileHandler(logging_name),
            logging.StreamHandler(),
        ],
    )

# --- Example Usage (when script is run directly) ---
if __name__ == "__main__":
    configure_logging()
    
    # --- Configuration from Environment Variables ---
    GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
    GCS_SUBFOLDER_PATH = os.getenv("GCS_SUBFOLDER_PATH")
    
    # Print configuration for debugging
    logging.info(f"Using bucket: {GCS_BUCKET_NAME}")
    logging.info(f"Using subfolder path: {GCS_SUBFOLDER_PATH}")
    
    # Validate required environment variables
    if not GCS_BUCKET_NAME or not GCS_SUBFOLDER_PATH:
        logging.error("Exiting: Missing required environment variables: GCS_BUCKET_NAME, GCS_SUBFOLDER_PATH")
        exit(1)
    
    # Find the latest Parquet file in the specified GCS location
    latest_file = find_latest_file_in_gcs(
        bucket_name=GCS_BUCKET_NAME,
        prefix=GCS_SUBFOLDER_PATH,
        extension=FILE_EXTENSION
    )
    
    if not latest_file:
        logging.error("No Parquet files found. Exiting.")
        exit(1)
    
    # Read the Parquet file
    raw_data = read_parquet_from_gcs(latest_file)
    
    # Process the data
    transformed_data = process_data(df=raw_data)

    # After transforming data:
    transformed_data = process_data(df=raw_data)

    # Check for existing keys
    existing_keys = check_existing_keys_in_bigquery(transformed_data)

    # Filter out duplicates
    new_data = filter_out_duplicates(transformed_data, existing_keys)

    # Load to BigQuery
    load_to_bigquery(new_data)
    
    logging.info("Processing completed successfully")