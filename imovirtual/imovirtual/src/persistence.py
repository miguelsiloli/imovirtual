import json
import logging
import os
import shutil

from google.cloud import storage
from google.api_core import exceptions as gcp_exceptions


def save_district_data(district: str, data: dict, output_dir: str) -> None:
    output_path = os.path.join(output_dir, f"{district}.json")
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    temp_file_path = os.path.join(output_dir, f".{district}.json.tmp")

    try:
        with open(temp_file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)

        shutil.move(temp_file_path, output_path)
        logging.info(f"Successfully saved data for district {district}")
    except Exception as e:
        logging.error(f"Error saving data for district {district}: {str(e)}")
        if os.path.exists(temp_file_path):
            os.remove(temp_file_path)


def upload_to_gcs(
    bucket_name: str,
    gcs_folder_path: str,
    source_file: str,
    destination_blob_name: str,
    credentials: dict,
) -> None:
    """
    Uploads a file to Google Cloud Storage with enhanced logging.

    Args:
        bucket_name: The name of the GCS bucket.
        gcs_folder_path: The folder path within the bucket (e.g., 'staging/data').
        source_file: The local path of the file to upload.
        destination_blob_name: The desired name of the file in GCS.
        credentials: A dictionary containing the service account credentials.
    """
    full_blob_name = os.path.join(gcs_folder_path, destination_blob_name)
    gcs_uri = f"gs://{bucket_name}/{full_blob_name}"

    # Check if source file exists before proceeding
    if not os.path.exists(source_file):
        logging.error(
            f"[GCS Upload] Source file not found: {source_file}. Cannot upload to {gcs_uri}."
        )
        return  # Exit the function early

    logging.info(f"[GCS Upload] Attempting to upload {source_file} to {gcs_uri}")

    try:
        # 1. Initialize Client
        logging.info(
            f"[GCS Upload] Initializing GCS client for project '{credentials.get('project_id', 'N/A')}'..."
        )
        storage_client = storage.Client.from_service_account_info(credentials)
        logging.info("[GCS Upload] GCS client initialized successfully.")

        # 2. Get Bucket
        logging.info(f"[GCS Upload] Accessing bucket '{bucket_name}'...")
        bucket = storage_client.bucket(bucket_name)
        # You could add a check here, though SDK often raises exceptions later if bucket doesn't exist/no access
        # if not bucket.exists(): # This makes an extra API call
        #     logging.error(f"[GCS Upload] Bucket '{bucket_name}' not found or access denied.")
        #     return

        # 3. Define Blob
        logging.info(f"[GCS Upload] Defining blob object for '{full_blob_name}'...")
        blob = bucket.blob(full_blob_name)
        logging.info(f"[GCS Upload] Blob object created for target: {gcs_uri}")

        # 4. Perform Upload
        logging.info(f"[GCS Upload] Starting upload of local file: {source_file}")
        blob.upload_from_filename(source_file)
        logging.info(f"[GCS Upload] Successfully uploaded {source_file} to {gcs_uri}")

    # Catch more specific GCS exceptions first if needed
    except gcp_exceptions.NotFound as e:
        logging.error(
            f"[GCS Upload] Error: Bucket '{bucket_name}' not found. Details: {str(e)}"
        )
    except gcp_exceptions.Forbidden as e:
        logging.error(
            f"[GCS Upload] Error: Permission denied for {gcs_uri}. Check service account roles ('roles/storage.objectCreator' or 'roles/storage.objectAdmin' needed on the bucket). Details: {str(e)}"
        )
    except gcp_exceptions.GoogleAPICallError as e:
        # Catch other general API errors from Google Cloud
        logging.error(
            f"[GCS Upload] Google API call error during upload to {gcs_uri}: {type(e).__name__} - {str(e)}"
        )
    except FileNotFoundError:
        # This is redundant now due to the check at the beginning, but good practice
        logging.error(
            f"[GCS Upload] Error: Local source file not found during upload attempt: {source_file}"
        )
    except Exception as e:
        # Catch any other unexpected exceptions
        logging.error(
            f"[GCS Upload] An unexpected error occurred during upload to {gcs_uri}: {type(e).__name__} - {str(e)}"
        )
        # Optionally, re-raise the exception if you want the program to stop
        # raise e


def upload_logs_to_gcs(
    credentials: dict, bucket_name: str, gcs_log_folder_path: str, local_log_path: str
):
    """
    Uploads a local log file to a specified Google Cloud Storage path using google-cloud-storage.

    Args:
        credentials_dict: A dictionary containing Google Cloud service account credentials.
        bucket_name: The name of the target GCS bucket.
        gcs_log_folder_path: The folder path within the GCS bucket (e.g., "staging/imovirtual/logs").
        local_log_path: The full path to the local log file to upload.

    Returns:
        None
    """
    # --- 1. Pre-checks ---
    if not os.path.exists(local_log_path):
        logging.error(
            f"[GCS Log Upload] Local log file not found: {local_log_path}. Cannot upload."
        )
        # Removed print statement, relying on logging configuration
        return

    if not bucket_name:
        logging.error("[GCS Log Upload] GCS bucket name not provided.")
        return

    if not credentials or not all(
        k in credentials for k in ["project_id", "private_key", "client_email"]
    ):
        logging.error(
            "[GCS Log Upload] Invalid or incomplete GCS credentials provided."
        )
        return

    # --- 2. Construct GCS Path ---
    log_filename = os.path.basename(local_log_path)
    # Ensure clean path joining for GCS (remove leading/trailing slashes from folder)
    gcs_blob_path = f"{gcs_log_folder_path.strip('/')}/{log_filename}"
    gcs_uri = f"gs://{bucket_name}/{gcs_blob_path}"
    project_id = credentials.get("project_id", "N/A")

    logging.info(
        f"[GCS Log Upload] Attempting to upload '{local_log_path}' to '{gcs_uri}'"
    )

    try:
        # --- 3. Initialize Client ---
        logging.info(
            f"[GCS Log Upload] Initializing GCS client for project '{project_id}'..."
        )
        # Use the client's class method directly for loading credentials
        storage_client = storage.Client.from_service_account_info(credentials)
        logging.info("[GCS Log Upload] GCS client initialized successfully.")

        # --- 4. Get Bucket ---
        logging.info(f"[GCS Log Upload] Accessing bucket '{bucket_name}'...")
        bucket = storage_client.bucket(bucket_name)
        # Note: Bucket existence/access is usually checked implicitly on the next step (blob operation)

        # --- 5. Define Blob ---
        logging.info(f"[GCS Log Upload] Defining blob object for '{gcs_blob_path}'...")
        blob = bucket.blob(gcs_blob_path)
        logging.info(f"[GCS Log Upload] Blob object created for target: {gcs_uri}")

        # --- 6. Perform Upload ---
        logging.info(
            f"[GCS Log Upload] Starting upload of local file: {local_log_path}"
        )
        blob.upload_from_filename(local_log_path)
        logging.info(f"[GCS Log Upload] Successfully uploaded log file to {gcs_uri}")

    # --- 7. Error Handling (aligned with upload_to_gcs) ---
    except gcp_exceptions.NotFound as e:
        logging.error(
            f"[GCS Log Upload] Error: Bucket '{bucket_name}' not found or blob path issue. Details: {str(e)}"
        )
    except gcp_exceptions.Forbidden as e:
        logging.error(
            f"[GCS Log Upload] Error: Permission denied for {gcs_uri}. Check service account roles ('roles/storage.objectCreator' or 'roles/storage.objectAdmin' needed on the bucket). Details: {str(e)}"
        )
    except gcp_exceptions.GoogleAPICallError as e:
        # Catch other general API errors from Google Cloud
        logging.error(
            f"[GCS Log Upload] Google API call error during upload to {gcs_uri}: {type(e).__name__} - {str(e)}"
        )
    except FileNotFoundError:
        # Should be caught by the initial check, but safety first
        logging.error(
            f"[GCS Log Upload] Error: Local source file not found during upload attempt: {local_log_path}"
        )
    except Exception as e:
        # Catch any other unexpected exceptions
        # Use logging.exception to include traceback in the logs for unexpected errors
        logging.exception(
            f"[GCS Log Upload] An unexpected error occurred during log upload to {gcs_uri}: {type(e).__name__} - {str(e)}"
        )
