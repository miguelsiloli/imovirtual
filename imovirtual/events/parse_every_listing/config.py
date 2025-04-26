# config.py
"""
Configuration constants for the Imovirtual scraping flow.
"""

# --- Global Configuration Constants ---
GCS_INPUT_PATH_PREFIX = "staging/imovirtual/"
GCS_OUTPUT_PATH_PREFIX = "staging/imovirtual_listings/"
BASE_URL = 'https://www.imovirtual.com/pt/anuncio/'
REQUEST_TIMEOUT = 30
MIN_SLEEP = 1
MAX_SLEEP = 3

# Note: GCS_BUCKET_NAME is expected from environment variables, not hardcoded here.
# Required environment variables for GCS credentials are listed in utils.py