# --- Helper function parse_feature_list is NO LONGER NEEDED for this ---
# --- Keep sanitize_column_name for flatten_features_by_category ---
def sanitize_column_name(name):
    """
    Sanitizes a string to be a valid and clean column name.
    (Keep the same function as before)
    """
    if not isinstance(name, str):
        name = str(name)
    name = unicodedata.normalize('NFKD', name).encode('ascii', 'ignore').decode('ascii')
    name = name.lower()
    name = re.sub(r'[^a-z0-9_]+', '_', name)
    name = name.strip('_')
    if re.match(r'^\d', name):
        name = 'col_' + name
    if not name:
        return '_invalid_name_'
    return name

# --- SIMPLIFIED function to calculate features count ---
def calculate_features_count(df):
    """
    Calculates the number of features based on the 'features' column,
    assuming it contains actual lists or NaN values.
    Creates a new 'features_count' column and drops the original 'features' column.
    """
    print("-> Calculating features count...")
    if 'features' not in df.columns:
        print("   'features' column not found. Skipping.")
        if 'features_count' not in df.columns:
             df['features_count'] = 0
        return df

    # --- Direct Calculation ---
    # Apply a lambda function:
    # - If the value is a list, return its length.
    # - Otherwise (NaN, None, string, other types), return 0.
    print("   Calculating length of lists in 'features' column...")
    df['features_count'] = df['features'].apply(lambda x: len(x) if isinstance(x, list) else 0)

    # --- Debugging ---
    print("   Sample values for 'features_count' after calculation:")
    print(df['features_count'].head())
    print("   Value counts for 'features_count':")
    print(df['features_count'].value_counts().head(10)) # Show more values
    print("   Description of 'features_count':")
    print(df['features_count'].describe())
    # --- End Debugging ---

    print("   Dropping original 'features' column.")
    df.drop('features', axis=1, inplace=True)

    print("   'features_count' calculation complete.")
    return df

# --- Keep flatten_features_by_category as it is ---
# (Make sure it handles potential unhashable items as corrected before)
def flatten_features_by_category(df, sanitize_func):
    """
    Flattens the 'features_by_category' JSON string column into separate binary columns.
    (Keep the robust version from the previous iteration)
    """
    print("-> Flattening 'features_by_category' column...")
    if 'features_by_category' not in df.columns:
        print("   'features_by_category' column not found. Skipping.")
        return df

    all_cat_feature_cols = {}
    print("   Scanning for unique category-feature pairs...")
    for json_str in df['features_by_category'].dropna():
        try:
            categories = json.loads(json_str)
            if not isinstance(categories, list): continue
            for category in categories:
                if not isinstance(category, dict) or 'label' not in category or 'values' not in category: continue
                cat_label = category.get('label', 'unknown_category')
                if not isinstance(category['values'], list): continue

                for feature in category['values']:
                    if not isinstance(feature, (str, int, float, bool, tuple)):
                         feature = str(feature) # Convert unhashable types

                    sanitized_cat = sanitize_func(cat_label)
                    sanitized_feat = sanitize_func(feature)
                    col_name = f"feat_cat_{sanitized_cat}_{sanitized_feat}"

                    if col_name not in all_cat_feature_cols:
                        all_cat_feature_cols[col_name] = (cat_label, feature)

        except (json.JSONDecodeError, TypeError):
            continue

    if not all_cat_feature_cols:
        print("   No valid category-feature pairs found. Skipping.")
        return df

    print(f"   Found {len(all_cat_feature_cols)} unique category-feature pairs.")
    print(f"   Creating {len(all_cat_feature_cols)} new category-feature columns.")

    for col_name in all_cat_feature_cols.keys():
        df[col_name] = 0

    print("   Populating new category-feature columns...")
    for index, row in df.iterrows():
        json_str = row['features_by_category']
        if pd.isna(json_str): continue
        try:
            categories = json.loads(json_str)
            if not isinstance(categories, list): continue

            present_pairs = set()
            for category in categories:
                 if not isinstance(category, dict) or 'label' not in category or 'values' not in category: continue
                 cat_label = category.get('label', 'unknown_category')
                 if not isinstance(category['values'], list): continue

                 for feature in category['values']:
                     if not isinstance(feature, (str, int, float, bool, tuple)):
                          feature = str(feature)
                     present_pairs.add((cat_label, feature))

            for col_name, (original_cat, original_feat) in all_cat_feature_cols.items():
                if not isinstance(original_feat, (str, int, float, bool, tuple)):
                     original_feat = str(original_feat) # Ensure comparison key is hashable

                if (original_cat, original_feat) in present_pairs:
                    df.loc[index, col_name] = 1

        except (json.JSONDecodeError, TypeError):
             continue

    print("   'features_by_category' flattening complete.")
    return df


# --- Updated process_data_for_regression function ---
# (No changes needed here, as it just calls the updated calculate_features_count)
def process_data_for_regression(file_path):
    """
    Reads parquet data, selects relevant columns, calculates features count,
    flattens features_by_category, and prepares the DataFrame for regression.
    """
    print(f"Reading data from: {file_path}")
    try:
        df = pd.read_parquet(file_path, engine='pyarrow')
        # --- Add inspection right after load ---
        if 'features' in df.columns:
             print("--- Inspecting 'features' column after load ---")
             print(f"Dtype: {df['features'].dtype}")
             print("Sample non-null values:")
             print(df['features'].dropna().head())
             print("Value Counts (Top 5 Non-Null Types):")
             print(df['features'].dropna().apply(type).value_counts().head())
             print("--------------------------------------------")
        # --- End inspection ---
    except Exception as e:
        print(f"Error reading Parquet file: {e}")
        return None

    print(f"Initial data shape: {df.shape}")

    # 1. Define relevant columns
    relevant_columns = [
        'ad_id', 'price', 'area', 'area_unit', 'rooms_num_clean',
        'rooms_num_raw', 'bathrooms_num', 'build_year', 'construction_status',
        'floor_no', 'ad_category_name', 'ad_category_type', 'latitude',
        'longitude', 'location_address_city', 'location_address_county',
        'location_address_province', 'location_parish_full', 'features', # Keep for processing
        'features_by_category', 'energy_certificate', 'heating',
        'windows_type', 'advertiser_type', 'image_urls'
    ]

    existing_relevant_columns = [col for col in relevant_columns if col in df.columns]
    missing_cols = set(relevant_columns) - set(existing_relevant_columns)
    if missing_cols:
        print(f"Warning: The following expected columns were not found: {missing_cols}")

    df = df[existing_relevant_columns].copy()
    print("Selected relevant columns.")

    # 2. Derive image_count
    if 'image_urls' in df.columns:
        print("-> Deriving image_count...")
        df['image_count'] = df['image_urls'].apply(lambda x: len(x) if isinstance(x, list) else 0)
        df = df.drop(columns=['image_urls'])
        print("   Derived 'image_count' and dropped 'image_urls'.")
    else:
        print("   'image_urls' column not found, skipping image_count derivation.")
        df['image_count'] = 0

    # 3. Process 'features' column -> Calculate count (using the new simplified function)
    df = calculate_features_count(df)

    # 4. Flatten 'features_by_category'
    df = flatten_features_by_category(df, sanitize_column_name)

    # Drop original 'features_by_category' if it existed and was processed
    if 'features_by_category' in df.columns:
        print("-> Dropping original 'features_by_category' column.")
        df = df.drop(columns=['features_by_category'])

    # 5. Explicitly remove other columns not needed for regression model input
    cols_to_remove_explicitly = [
        'price_suffix', 'price_currency', 'location_address_postal_code',
        'created_at', 'modified_at', 'ad_id'
    ]
    existing_cols_to_remove = [col for col in cols_to_remove_explicitly if col in df.columns]
    existing_cols_to_remove = existing_cols_to_remove.append("features_count") # fuck it
    if existing_cols_to_remove:
        print(f"-> Explicitly removing columns: {existing_cols_to_remove}")
        df = df.drop(columns=existing_cols_to_remove)

    print(f"Final data shape after processing: {df.shape}")
    print("\nSample of processed data:")
    print(df.head())
    print("\nColumns in processed data:")
    print(df.columns.tolist())

    return df

# --- Main Execution ---
if __name__ == "__main__":
    # --- Make sure necessary imports are at the top ---
    import pandas as pd
    import json
    import re
    import unicodedata
    # from ast import literal_eval # Not needed anymore for features
    import numpy as np
    # -------------------------------------------------

    parquet_file_path = 'flattened_imovirtual_data.parquet' # Adjust path if needed
    processed_df = process_data_for_regression(parquet_file_path)

    if processed_df is not None:
        output_file_path = 'processed_regression_data_featcount.parquet'
        print(f"\nSaving processed data to: {output_file_path}")
        try:
            processed_df.to_parquet(output_file_path, engine='pyarrow', index=False)
            print("Processed data saved successfully.")
        except Exception as e:
            print(f"Error saving processed data: {e}")

        print("\nProcessed DataFrame Info:")
        processed_df.info()

        # Check the features_count column again
        if 'features_count' in processed_df.columns:
            print("\nFinal check for 'features_count':")
            print(processed_df['features_count'].value_counts().head(10))
            print(processed_df['features_count'].describe())
        else:
             print("\n'features_count' column was not created.")