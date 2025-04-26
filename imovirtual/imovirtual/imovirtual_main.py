import argparse
import os

from dotenv import load_dotenv

from src.graphql_main import fetch_imovirtual_data
from src.utils import configure_logging
from src.get_buildid import get_buildid
from src.persistence import upload_logs_to_gcs, upload_to_gcs
from datetime import datetime

def main():
    # Load environment variables from .env file
    load_dotenv()

    # Define the paths and headers

    date_str = datetime.now().strftime("%Y-%m-%d")
    logging_name = f"imovirtual_scraping_{date_str}.log"
    csv_file_path = 'imovirtual/imovirtual/imovirtual_catalog.csv'
    output_dir = os.path.join('raw', 'imovirtual')
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:127.0) Gecko/20100101 Firefox/127.0",
        "Accept": "multipart/mixed, application/graphql-response+json, application/graphql+json, application/json",
        "Accept-Language": "pt-PT,pt;q=0.8,en;q=0.5,en-US;q=0.3",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Content-Type": "application/json",
        "baggage": "sentry-environment=imovirtualpt2-prd,sentry-release=frontend-platform%40v20240603T121501-imovirtualpt2,sentry-public_key=feffe528c390ea66992a4a05131c3c68,sentry-trace_id=cc8c6f78b99b4e959c670ca3a2b379bd,sentry-transaction=%2Fpt%2Fresultados%2F%5B%5B...searchingCriteria%5D%5D,sentry-sampled=false",
        "Origin": "https://www.imovirtual.com",
        "Alt-Used": "www.imovirtual.com",
        "Connection": "keep-alive",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "Priority": "u=1",
        "TE": "trailers"
    }
    base_url_template = "https://www.imovirtual.com/_next/data/{}/pt/resultados/arrendar/apartamento/{}.json"

    bucket_name = os.environ.get("GCS_BUCKET_NAME")
    gcs_folder_path = os.environ.get("GCS_SUBFOLDER_PATH")

    # Retrieve Google Cloud credentials from environment variables
    credentials = {
        "type": os.getenv("TYPE"),
        "project_id": os.getenv("PROJECT_ID"),
        "private_key_id": os.getenv("PRIVATE_KEY_ID"),
        "private_key": os.getenv("PRIVATE_KEY").replace("\\n", "\n"),
        "client_email": os.getenv("CLIENT_EMAIL"),
        "client_id": os.getenv("CLIENT_ID"),
        "auth_uri": os.getenv("AUTH_URI"),
        "token_uri": os.getenv("TOKEN_URI"),
        "auth_provider_x509_cert_url": os.getenv("AUTH_PROVIDER_X509_CERT_URL"),
        "client_x509_cert_url": os.getenv("CLIENT_X509_CERT_URL")
    }
    configure_logging(logging_name = logging_name)

    source = "imovirtual"
    output_file = os.path.join(output_dir, f"{source}_{date_str}.parquet")
    gcs_target_file = f"{source}_{date_str}.parquet"

    # Fetch data from imovirtual
    print("Fetching data from imovirtual...")
    fetch_imovirtual_data(
        csv_file_path=csv_file_path,
        output_dir=output_dir,
        headers=headers,
        base_url_template=base_url_template,
        get_buildid=get_buildid,
        filename=gcs_target_file,
        workers=4
    )

    upload_to_gcs(
        bucket_name=bucket_name,
        gcs_folder_path=gcs_folder_path,
        source_file=output_file, # name of the file to read data from
        destination_blob_name=gcs_target_file, # name of the file to save as in GCS
        credentials=credentials
    )
    # Upload logging file to GCS
    gcs_log_folder = "staging/imovirtual/logs" # Specific GCS folder for logs
    upload_logs_to_gcs(
        credentials=credentials,
        bucket_name=bucket_name,
        gcs_log_folder_path=gcs_log_folder,
        local_log_path=logging_name # Use the variable defined earlier
    )

if __name__ == "__main__":
   main()
