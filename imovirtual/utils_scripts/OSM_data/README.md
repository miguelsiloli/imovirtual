# OSM Data Processing

This folder contains scripts for processing and uploading OpenStreetMap (OSM) data.

## Files

*   `portugal-latest.osm.pbf`: OSM data for Portugal.
*   `tests.py`: Tests for the scripts.
*   `upload_osm_data_to_gcp.py`: Script to upload OSM data to Google Cloud Platform (GCP).
*   `utils.py`: Utility functions for processing OSM data.

## Usage

### Prerequisites

*   **GCP Account:** You need a Google Cloud Platform (GCP) account with the necessary permissions to access Google Cloud Storage (GCS) and BigQuery.
*   **`.env` file:** Create a `.env` file in the same directory as the scripts and set the following environment variables:

    ```
    TYPE="your_gcp_type"
    PROJECT_ID="your_gcp_project_id"
    PRIVATE_KEY_ID="your_gcp_private_key_id"
    PRIVATE_KEY="your_gcp_private_key"
    CLIENT_EMAIL="your_gcp_client_email"
    CLIENT_ID="your_gcp_client_id"
    AUTH_URI="your_gcp_auth_uri"
    TOKEN_URI="your_gcp_token_uri"
    AUTH_PROVIDER_X509_CERT_URL="your_gcp_auth_provider_x509_cert_url"
    CLIENT_X509_CERT_URL="your_gcp_client_x509_cert_url"
    UNIVERSE_DOMAIN="your_gcp_universe_domain"
    GCS_BUCKET_NAME="your_gcs_bucket_name"
    ```

    Replace the placeholder values with your actual GCP credentials and bucket name.
*   **Python Dependencies:** Install the required Python libraries using pip:

    ```bash
    pip install python-dotenv google-cloud-storage google-cloud-bigquery geopandas
    ```
*   **osmium-tool:** Install the `osmium-tool` command-line tool. This is used for processing OSM data.

    ```bash
    # Example installation on Debian/Ubuntu
    sudo apt-get update
    sudo apt-get install osmium-tool
    ```

### Running the script

1.  **Download OSM data:** Download the `portugal-latest.osm.pbf` file from a reputable source (e.g., Geofabrik) and place it in the `imovirtual/utils_scripts/OSM_data` directory.
2.  **Run the `upload_osm_data_to_gcp.py` script:**

    ```bash
    python imovirtual/utils_scripts/OSM_data/upload_osm_data_to_gcp.py
    ```

    The script will process the OSM data, clean it, upload it to GCS, and load it into BigQuery.
3.  **Verify the data in BigQuery:** After the script finishes, verify that the OSM data has been successfully loaded into the specified BigQuery table.

## `utils.py` functions

*   **`process_osm_pbf(source_pbf, output_geojsonl, osm_filter)`:** Uses `osmium-tool` to filter an OSM PBF file and export the results as GeoJSON Lines.
*   **`validate_and_clean_geojsonl(input_path, output_path)`:** Reads a GeoJSON Lines file, skips empty/whitespace lines, validates each remaining line as JSON, and writes valid lines to a new file.
*   **`upload_to_gcs(bucket_name, source_file_name, destination_blob_name, gcs_client)`:** Uploads a file to a GCS bucket.
*   **`load_gcs_to_bigquery(gcs_uri, table_id, job_config, bq_client)`:** Loads data from a GCS URI to a BigQuery table.