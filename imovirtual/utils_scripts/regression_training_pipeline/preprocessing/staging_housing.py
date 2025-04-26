import pandas as pd
import logging
import os
import json
from google.oauth2 import service_account # To handle credentials
import pandas_gbq # To interact with BigQuery
from dotenv import load_dotenv # Make sure python-dotenv is installed (pip install python-dotenv)

# --- Credential Loading using dotenv ---
# Load environment variables from .env file in the current directory or parent directories
load_dotenv()
logging.info("Attempting to load credentials from environment variables (set by .env).")

# Construct the service account info dictionary from environment variables
try:
    service_account_info = {
      "type": os.environ['TYPE'], # Or os.getenv('TYPE') which returns None if not found
      "project_id": os.environ['PROJECT_ID'],
      "private_key_id": os.environ['PRIVATE_KEY_ID'],
      # Replace escaped newlines in the private key from env var
      "private_key": os.environ['PRIVATE_KEY'].replace('\\n', '\n'),
      "client_email": os.environ['CLIENT_EMAIL'],
      "client_id": os.environ['CLIENT_ID'],
      "auth_uri": os.environ['AUTH_URI'],
      "token_uri": os.environ['TOKEN_URI'],
      "auth_provider_x509_cert_url": os.environ['AUTH_PROVIDER_X509_CERT_URL'],
      "client_x509_cert_url": os.environ['CLIENT_X509_CERT_URL'],
      "universe_domain": os.environ['UNIVERSE_DOMAIN']
    }
    logging.info("Successfully read credential components from environment variables.")

    # Create credentials object from the constructed dictionary
    gcp_credentials = service_account.Credentials.from_service_account_info(service_account_info)
    logging.info("Successfully created credentials object from service account info.")

except KeyError as e:
    logging.error(f"Missing required environment variable for credentials: {e}. Make sure .env file is present and complete.", exc_info=False)
    gcp_credentials = None # Indicate failure
except Exception as e:
    logging.error(f"Failed to load or process credentials from environment variables: {e}", exc_info=True)
    gcp_credentials = None # Indicate failure


# Configure basic logging (can be moved after credential check if desired)
logging.basicConfig(level=os.getenv('LOG_LEVEL', 'INFO'), format='%(asctime)s - %(levelname)s - %(message)s')


# --- BigQuery Table Details (using environment variables where possible) ---
BQ_PROJECT_ID = os.getenv("BQ_PROJECT_ID", os.getenv("GCP_PROJECT_ID")) # Use specific BQ project or fallback to general GCP project
BQ_HOUSING_DATASET = os.getenv("BQ_DATASET_ID", "staging_housing")
BQ_HOUSING_TABLE = os.getenv("BQ_TABLE_ID", "staging")

if not BQ_PROJECT_ID:
     logging.error("BigQuery Project ID not found in environment variables (BQ_PROJECT_ID or GCP_PROJECT_ID).")
     # Handle appropriately, e.g., exit() or raise error
     exit()

BQ_HOUSING_TABLE_FULL_ID = f"{BQ_PROJECT_ID}.{BQ_HOUSING_DATASET}.{BQ_HOUSING_TABLE}"
logging.info(f"Target BigQuery Table: {BQ_HOUSING_TABLE_FULL_ID}")


# (Rest of the functions: load_data_from_bq, handle_housing_duplicates, etc., remain unchanged)
# ... (paste the function definitions from the previous correct response here) ...
def load_data_from_bq(
    full_table_id: str,
    project_id: str,
    credentials) -> pd.DataFrame:
    """
    Loads data from a specified BigQuery table into a Pandas DataFrame.

    Args:
        full_table_id (str): The full BigQuery table ID (e.g., 'project.dataset.table').
        project_id (str): The GCP project ID (for billing/quota).
        credentials: The google.oauth2.credentials.Credentials object.

    Returns:
        pd.DataFrame: Pandas DataFrame containing the table data.

    Raises:
        ValueError: If credentials are not provided or invalid.
        pandas_gbq.gbq.GenericGBQException: If there's an error querying BigQuery.
    """
    logging.info(f"Attempting to load data from BigQuery table: {full_table_id}")

    if not credentials:
        logging.error("Credentials not provided or failed to load.")
        raise ValueError("Valid GCP credentials are required.")

    try:
        df = pandas_gbq.read_gbq(
            full_table_id,
            project_id=project_id,
            credentials=credentials,
            dialect='standard'
        )
        logging.info(f"Successfully loaded {len(df)} rows from {full_table_id}")
        return df
    except pandas_gbq.gbq.GenericGBQException as e:
        logging.error(f"Error loading data from BigQuery table {full_table_id}: {e}", exc_info=True)
        raise # Re-raise the specific BigQuery exception
    except Exception as e:
        logging.error(f"An unexpected error occurred during BigQuery load: {e}", exc_info=True)
        raise # Re-raise any other exceptions


# --- Other functions remain largely the same, operating on the DataFrame ---
# (handle_housing_duplicates, convert_housing_data_types, etc., copied from previous response for completeness)

def handle_housing_duplicates(df: pd.DataFrame, id_col: str = 'id', sort_col: str = 'dateCreated', keep: str = 'last') -> pd.DataFrame:
    """
    Identifies and removes duplicate listings based on the id_col.
    Uses sort_col (e.g., dateCreated or ingestionDate) to decide which record to keep.
    Addresses potential variations in IDs if necessary (e.g., string vs. numeric representations).

    Args:
        df (pd.DataFrame): Input DataFrame.
        id_col (str): Column name for unique ID. Defaults to 'id'.
        sort_col (str): Column name for sorting to resolve duplicates (e.g., a date column). Defaults to 'dateCreated'.
        keep (str): Which duplicate record to keep ('first' or 'last'). Defaults to 'last'.

    Returns:
        pd.DataFrame: DataFrame with duplicates removed.

    Raises:
        KeyError: If id_col or sort_col are not found in the DataFrame.
    """
    logging.info(f"Handling duplicates based on '{id_col}', sorting by '{sort_col}', keeping '{keep}'.")

    if id_col not in df.columns:
        raise KeyError(f"ID column '{id_col}' not found in DataFrame.")
    if sort_col not in df.columns:
        # Allow sort_col to be None if no sorting is desired, though less robust
        logging.warning(f"Sort column '{sort_col}' not found. Duplicates will be dropped without specific sorting tie-breaking.")
        sort_col = None # Fallback to dropping without sorting first


    initial_rows = len(df)
    logging.info(f"Initial row count: {initial_rows}")

    # Ensure id_col is treated consistently (e.g., as string) to catch variations like '123' vs 123
    try:
        # Check if conversion is needed
        if not pd.api.types.is_string_dtype(df[id_col]) and not pd.api.types.is_object_dtype(df[id_col]):
             df[id_col] = df[id_col].astype(str)
        elif pd.api.types.is_object_dtype(df[id_col]): # Handle potential mixed types in object columns
             df[id_col] = df[id_col].astype(str)
    except Exception as e:
        logging.warning(f"Could not convert id_col '{id_col}' to string for consistent duplicate check: {e}")
        # Proceed anyway, but be aware of potential issues

    # Drop rows where the id_col itself is NaN, as they cannot be identified
    # Need to handle non-string NaNs if conversion to string failed or wasn't applicable
    original_id_nans = df[id_col].isna() | (df[id_col].astype(str).str.lower() == 'nan') | (df[id_col].astype(str) == '')
    df_cleaned_id = df[~original_id_nans].copy() # Keep rows where ID is not NaN or empty string
    rows_dropped_nan_id = initial_rows - len(df_cleaned_id)

    if rows_dropped_nan_id > 0:
        logging.warning(f"Dropped {rows_dropped_nan_id} rows due to missing/empty ID in column '{id_col}'.")


    if sort_col and sort_col in df_cleaned_id.columns:
         # Check if sort_col requires conversion before sorting (e.g., to datetime)
        if not pd.api.types.is_datetime64_any_dtype(df_cleaned_id[sort_col]) and not pd.api.types.is_numeric_dtype(df_cleaned_id[sort_col]):
             logging.warning(f"Sort column '{sort_col}' is not datetime or numeric. Attempting conversion to datetime (coerce errors).")
             # BQ Timestamps often come as dbts objects, need conversion
             df_cleaned_id[sort_col] = pd.to_datetime(df_cleaned_id[sort_col], errors='coerce', utc=True) # Assume UTC from BQ TIMESTAMP
             # Handle potential NaTs created by coercion if necessary before sorting
             # df_cleaned_id = df_cleaned_id.dropna(subset=[sort_col]) # Option: drop rows that couldn't be sorted

        # Sort before dropping duplicates
        logging.info(f"Sorting by '{id_col}' and '{sort_col}' before dropping duplicates.")
        # Handle NaNs in sort_col: na_position='first' or 'last' might be needed depending on desired behavior
        # Ensure id_col is string *before* sorting with it
        df_cleaned_id[id_col] = df_cleaned_id[id_col].astype(str)
        df_sorted = df_cleaned_id.sort_values(by=[id_col, sort_col], ascending=True, na_position='first')
        df_deduplicated = df_sorted.drop_duplicates(subset=[id_col], keep=keep)
    else:
        # Drop duplicates without sorting if sort_col is not provided or not found
        logging.warning(f"Proceeding to drop duplicates based on '{id_col}' without sorting tie-breaker.")
        df_cleaned_id[id_col] = df_cleaned_id[id_col].astype(str) # Ensure ID is str
        df_deduplicated = df_cleaned_id.drop_duplicates(subset=[id_col], keep=keep) # 'keep' might be arbitrary here

    final_rows = len(df_deduplicated)
    rows_dropped = initial_rows - rows_dropped_nan_id - final_rows # Adjusted for NaN ID drops

    logging.info(f"Dropped {rows_dropped} duplicate rows based on '{id_col}'. Final row count: {final_rows}")
    return df_deduplicated.reset_index(drop=True) # Reset index after dropping rows

def convert_housing_data_types(df: pd.DataFrame, type_config: dict) -> pd.DataFrame:
    """
    Converts columns to their appropriate data types. Handles potential errors
    during conversion by coercing problematic values to NaN/NaT/NA.

    Args:
        df (pd.DataFrame): Input DataFrame.
        type_config (dict): Dictionary mapping column names to target pandas/numpy types
                           (e.g., {'totalPriceValue': 'float', 'areaInSquareMeters': 'float',
                                  'dateCreated': 'datetime64[ns, UTC]', 'isPrivateOwner': 'boolean',
                                  'totalPossibleImages': 'Int64'}). Use 'Int64' for nullable integers.

    Returns:
        pd.DataFrame: DataFrame with corrected data types.
    """
    logging.info("Converting data types based on type_config.")
    df_typed = df.copy() # Work on a copy

    for col, target_type in type_config.items():
        if col not in df_typed.columns:
            logging.warning(f"Column '{col}' specified in type_config not found in DataFrame. Skipping.")
            continue

        logging.debug(f"Attempting conversion of column '{col}' to type '{target_type}'.")
        original_dtype = df_typed[col].dtype

        try:
            current_col_data = df_typed[col] # Work with the series

            if 'datetime' in str(target_type):
                # BQ Timestamps might be dbts type, handle it
                is_utc = 'UTC' in str(target_type) or 'utc' in str(target_type)
                df_typed[col] = pd.to_datetime(current_col_data, errors='coerce', utc=is_utc)
            elif target_type in ['float', 'float64', 'float32']:
                df_typed[col] = pd.to_numeric(current_col_data, errors='coerce')
                # Ensure it's actually float after coercion (in case input was all NaN/None)
                if not pd.api.types.is_float_dtype(df_typed[col]):
                     df_typed[col] = df_typed[col].astype(float)
            elif target_type in ['int', 'int64', 'int32', 'Int64']: # Int64 supports NaNs/NA
                # Coerce to float first to handle non-numeric strings gracefully -> NaN
                numeric_col = pd.to_numeric(current_col_data, errors='coerce')
                # Convert to nullable integer type
                try:
                     df_typed[col] = numeric_col.astype(target_type)
                except TypeError: # Handle potential issue converting float NaN to non-nullable int
                     if target_type != 'Int64':
                          logging.warning(f"Cannot convert column '{col}' containing NaNs to non-nullable int type '{target_type}'. Using 'Int64' instead.")
                          df_typed[col] = numeric_col.astype('Int64')
                     else:
                          raise # Re-raise if it fails even for Int64
            elif target_type in ['bool', 'boolean']:
                 # Handle common string/numeric representations of bool, then convert to nullable boolean
                 bool_map = {'true': True, 'false': False, '1': True, '0': False, 1: True, 0: False, 1.0: True, 0.0: False}
                 # Apply mapping only if the column isn't already boolean or numeric
                 if pd.api.types.is_object_dtype(current_col_data) or pd.api.types.is_string_dtype(current_col_data):
                      mapped_col = current_col_data.astype(str).str.lower().map(bool_map)
                      # Use combine_first to fill unmapped values with original (coerced) numeric/bool values
                      # Coerce original to numeric/bool first to handle cases like '1.0' which isn't mapped directly
                      coerced_original = pd.to_numeric(current_col_data, errors='ignore') # Ignore if not numeric
                      if not pd.api.types.is_numeric_dtype(coerced_original): # If not numeric, try bool directly
                            # Attempt direct boolean conversion, handling potential errors
                            try:
                                coerced_original = current_col_data.astype('boolean')
                            except (ValueError, TypeError):
                                # If direct boolean conversion fails, keep original or set to NA
                                coerced_original = pd.Series([pd.NA] * len(current_col_data), index=current_col_data.index) # Fallback to NA
                                logging.debug(f"Could not coerce column {col} directly to boolean, using NA for combine_first fallback.")

                      current_col_data = mapped_col.combine_first(coerced_original)


                 # Convert final result to nullable boolean type, coercing errors to NA
                 df_typed[col] = current_col_data.astype('boolean')

            elif target_type in ['str', 'string', 'object']:
                 # Convert to pandas nullable string type if specified
                 if target_type == 'string':
                     df_typed[col] = current_col_data.astype('string')
                 else: # Keep as object or basic str
                     df_typed[col] = current_col_data.astype(str) # Simple str conversion
            else: # Fallback for other types
                df_typed[col] = current_col_data.astype(target_type, errors='ignore')

            final_dtype = df_typed[col].dtype
            if str(original_dtype) != str(final_dtype): # Compare string representation for clarity
                logging.info(f"Converted column '{col}' from {original_dtype} to {final_dtype}.")
            else:
                 logging.debug(f"Column '{col}' already had or retained dtype {final_dtype} after attempted conversion.")

        except Exception as e:
            logging.error(f"Failed to convert column '{col}' to type '{target_type}': {e}. Leaving as {original_dtype}.", exc_info=False)

    return df_typed

def handle_housing_missing_values_initial(
    df: pd.DataFrame,
    target_col: str = 'totalPriceValue',
    required_cols: list = ['areaInSquareMeters', 'city', 'province'],
    strategy: str = 'drop_target_nan' # Currently only supports this strategy
) -> pd.DataFrame:
    """
    Performs an initial pass on missing values. Critically, drops rows where
    the target_col is missing. Optionally drops rows missing essential required_cols.
    Handles Pandas NA types correctly.

    Args:
        df (pd.DataFrame): Input DataFrame.
        target_col (str): The target variable column name. Defaults to 'totalPriceValue'.
        required_cols (list): List of essential feature columns. Defaults to ['areaInSquareMeters', 'city', 'province'].
        strategy (str): Strategy name (currently only 'drop_target_nan' implemented).

    Returns:
        pd.DataFrame: DataFrame with critical NaNs/NAs handled.

    Raises:
        KeyError: If target_col or any required_col is not found.
        NotImplementedError: If an unsupported strategy is provided.
    """
    logging.info("Handling initial missing values (including NA types).")
    df_handled = df.copy()
    initial_rows = len(df_handled)

    if target_col not in df_handled.columns:
        raise KeyError(f"Target column '{target_col}' not found.")
    for col in required_cols:
        if col not in df_handled.columns:
             raise KeyError(f"Required column '{col}' not found.")

    if strategy == 'drop_target_nan':
        # Drop rows where target is missing (handles np.nan, None, pd.NA)
        df_handled = df_handled.dropna(subset=[target_col])
        rows_after_target_drop = len(df_handled)
        dropped_target = initial_rows - rows_after_target_drop
        if dropped_target > 0:
            logging.info(f"Dropped {dropped_target} rows due to missing target ('{target_col}').")

        # Optionally drop rows where *any* of the required columns are missing
        if required_cols:
            # Before dropping based on required_cols, handle potential empty strings if they should be treated as missing
            for r_col in required_cols:
                 # Check if the column exists and is a string/object type before attempting replace
                 if r_col in df_handled.columns and \
                    (pd.api.types.is_string_dtype(df_handled[r_col]) or pd.api.types.is_object_dtype(df_handled[r_col])):
                      # Replace empty strings or strings containing only whitespace with pd.NA
                      df_handled[r_col] = df_handled[r_col].replace(r'^\s*$', pd.NA, regex=True)
                 elif r_col not in df_handled.columns:
                      logging.warning(f"Required column '{r_col}' for NA check not found.")


            # Now drop rows with NA in any required column
            df_handled = df_handled.dropna(subset=[col for col in required_cols if col in df_handled.columns], how='any') # Filter subset list
            rows_after_req_drop = len(df_handled)
            dropped_req = rows_after_target_drop - rows_after_req_drop
            if dropped_req > 0:
                 logging.info(f"Dropped {dropped_req} additional rows due to missing values or empty strings in required columns: {required_cols}.")
    else:
        raise NotImplementedError(f"Strategy '{strategy}' is not implemented.")

    final_rows = len(df_handled)
    total_dropped = initial_rows - final_rows
    logging.info(f"Initial missing value handling complete. Total rows dropped: {total_dropped}. Final row count: {final_rows}")

    # Use reset_index only if rows were actually dropped
    if total_dropped > 0:
        return df_handled.reset_index(drop=True)
    else:
        return df_handled

def standardize_housing_location_names(df: pd.DataFrame, city_col: str = 'city', province_col: str = 'province') -> pd.DataFrame:
    """
    Cleans and standardizes the city and province columns. Includes trimming
    whitespace, converting to lowercase. Uses pandas nullable string type methods.

    Args:
        df (pd.DataFrame): Input DataFrame.
        city_col (str): Name of the city column. Defaults to 'city'.
        province_col (str): Name of the province column. Defaults to 'province'.

    Returns:
        pd.DataFrame: DataFrame with standardized location names.
    """
    logging.info(f"Standardizing location columns: '{city_col}', '{province_col}'.")
    df_std = df.copy()

    for col in [city_col, province_col]:
        if col not in df_std.columns:
            logging.warning(f"Location column '{col}' not found. Skipping standardization.")
            continue

        # Convert to nullable string type first to use .str accessor robustly
        try:
            # Only convert if not already string or object (might be numeric/etc)
            if not (pd.api.types.is_string_dtype(df_std[col]) or pd.api.types.is_object_dtype(df_std[col])):
                 logging.debug(f"Converting column {col} to string type before standardization.")
                 df_std[col] = df_std[col].astype('string') # Pandas nullable string
            elif pd.api.types.is_object_dtype(df_std[col]): # Convert object to nullable string
                 df_std[col] = df_std[col].astype('string')

            # Apply standardization using .str accessor (handles NA correctly)
            if pd.api.types.is_string_dtype(df_std[col]):
                 df_std[col] = df_std[col].str.lower().str.strip()
                 logging.debug(f"Applied lowercasing and stripping to column '{col}'.")
            else:
                 # This case should ideally not be reached after conversion, but log if it does
                 logging.warning(f"Column '{col}' could not be reliably processed as string type ({df_std[col].dtype}). Standardization might be incomplete.")


        except Exception as e:
            logging.error(f"Error standardizing column '{col}': {e}. Skipping.", exc_info=False)


    return df_std

def clean_housing_categorical_features(df: pd.DataFrame, cat_cols: list = ['estate', 'transaction', 'roomsNumber', 'floorNumber']) -> pd.DataFrame:
    """
    Cleans predefined categorical columns by converting to lowercase nullable string and stripping whitespace.
    Can be extended with specific value mappings if needed.

    Args:
        df (pd.DataFrame): Input DataFrame.
        cat_cols (list): List of categorical column names to clean.

    Returns:
        pd.DataFrame: DataFrame with cleaned categorical features.
    """
    logging.info(f"Cleaning categorical columns: {cat_cols}.")
    df_cleaned = df.copy()

    for col in cat_cols:
        if col not in df_cleaned.columns:
            logging.warning(f"Categorical column '{col}' not found. Skipping cleaning.")
            continue

        # Convert to nullable string type for robust processing
        try:
            # Only convert if necessary
            if not (pd.api.types.is_string_dtype(df_cleaned[col]) or pd.api.types.is_object_dtype(df_cleaned[col])):
                logging.debug(f"Converting categorical column {col} to string type.")
                df_cleaned[col] = df_cleaned[col].astype('string')
            elif pd.api.types.is_object_dtype(df_cleaned[col]): # Convert object to nullable string
                df_cleaned[col] = df_cleaned[col].astype('string')

            # Apply cleaning using .str accessor (handles NA)
            if pd.api.types.is_string_dtype(df_cleaned[col]):
                cleaned_col = df_cleaned[col].str.lower().str.strip()
                # Replace empty strings resulting from stripping with NA
                cleaned_col = cleaned_col.replace(r'^\s*$', pd.NA, regex=True)
                df_cleaned[col] = cleaned_col

                # Placeholder for specific value replacements
                # Example:
                # if col == 'roomsNumber':
                #     room_map = {'one': '1', 'two': '2', 'three': '3', 'four': '4', 'five': '5'}
                #     # Apply mapping carefully, maybe after lower/strip
                #     df_cleaned[col] = df_cleaned[col].replace(room_map)

                logging.debug(f"Applied lowercasing, stripping, and empty string -> NA for categorical column '{col}'.")
            else:
                logging.warning(f"Column '{col}' could not be reliably processed as string type ({df_cleaned[col].dtype}). Cleaning might be incomplete.")

        except Exception as e:
            logging.error(f"Error cleaning categorical column '{col}': {e}. Skipping.", exc_info=False)

    return df_cleaned


# --- Example Usage ---
if __name__ == '__main__':
    # Ensure credentials loaded correctly
    if not gcp_credentials:
        logging.error("GCP Credentials were not loaded. Exiting.")
        exit()

    try:
        # 1. Load from BigQuery
        raw_df = load_data_from_bq(BQ_HOUSING_TABLE_FULL_ID, BQ_PROJECT_ID, gcp_credentials)
        print("\n--- Raw Loaded Data from BigQuery ---")
        print(raw_df.head())
        print(f"Loaded {len(raw_df)} rows.")
        # print(raw_df.info()) # Note BQ types might differ slightly initially

        # --- Proceed with the same processing steps ---

        # 2. Handle Duplicates
        dedup_df = handle_housing_duplicates(raw_df, id_col='id', sort_col='dateCreated', keep='last') # Assuming 'id' and 'dateCreated' exist
        print(f"\n--- After Duplicate Handling ({len(dedup_df)} rows) ---")
        #print(dedup_df.head())

        # 3. Convert Data Types (Adapt type_config based on actual BQ schema)
        dtype_config = {
            'totalPriceValue': 'float',
            'pricePerSquareMeterValue': 'float',
            'areaInSquareMeters': 'float',
            'totalPossibleImages': 'Int64', # Use nullable integer
            'isPrivateOwner': 'boolean',    # Use nullable boolean
            'dateCreated': 'datetime64[ns, UTC]',
            'dateCreatedFirst': 'datetime64[ns, UTC]', # Add if exists
            'ingestionDate': 'datetime64[ns, UTC]'
            # Add other columns as needed
        }
        typed_df = convert_housing_data_types(dedup_df, dtype_config)
        print("\n--- After Type Conversion ---")
        print(typed_df.info())
        # print(typed_df.head())


        # 4. Handle Initial Missing Values
        required = ['areaInSquareMeters', 'city', 'province'] # Adjust if needed
        initial_nan_handled_df = handle_housing_missing_values_initial(
            typed_df,
            target_col='totalPriceValue',
            required_cols=required
        )
        print(f"\n--- After Initial NaN Handling ({len(initial_nan_handled_df)} rows) ---")
        # print(initial_nan_handled_df.head())

        # 5. Standardize Location Names
        std_loc_df = standardize_housing_location_names(initial_nan_handled_df, city_col='city', province_col='province') # Check column names
        print("\n--- After Location Standardization (Sample) ---")
        # print(std_loc_df[['id', 'city', 'province']].head()) # Check 'id' column name

        # 6. Clean Categorical Features
        cat_cols_to_clean = ['estate', 'transaction', 'roomsNumber', 'floorNumber'] # Check column names
        cleaned_df = clean_housing_categorical_features(std_loc_df, cat_cols=cat_cols_to_clean)
        print("\n--- After Categorical Cleaning (Sample) ---")
        # print(cleaned_df[['id'] + [col for col in cat_cols_to_clean if col in cleaned_df.columns]].head()) # Check 'id' and cat cols
        print("\n--- Final Preprocessed DataFrame Head ---")
        print(cleaned_df.head())
        print("\n--- Final Preprocessed DataFrame Info ---")
        print(cleaned_df.info())


    except KeyError as e:
         logging.error(f"A required column is missing: {e}. Please check BigQuery table schema and function arguments.", exc_info=True)
    except Exception as e:
        logging.error(f"An error occurred during the BigQuery pipeline execution: {e}", exc_info=True)