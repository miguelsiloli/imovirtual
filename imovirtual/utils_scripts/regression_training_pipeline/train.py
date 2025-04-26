import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from sklearn.preprocessing import LabelEncoder # Can be useful for ordinal if needed differently
import lightgbm as lgb
import warnings

warnings.filterwarnings('ignore', category=UserWarning) # Suppress LightGBM UserWarnings about categorical features

# --- Configuration ---
FILE_PATH = '/home/miguel/Projects/imovirtual/processed_regression_data_featcount.parquet'
TARGET_COLUMN = 'price'

# Columns to remove explicitly (based on user request)
COLUMNS_TO_DROP = [
    'ad_category_type',
    'construction_status', # Note: Might be already removed or filled differently depending on prior scripts
    'features_count',
    'ad_id', # Should already be removed
    'area_unit',
    'rooms_num_raw',
    'location_parish_full',
    'image_count'
]

# Columns to fill NaNs
FILLNA_CONFIG = {
    'floor_no': 0, # Fill with 0
    'build_year': 'unknown', # Fill with placeholder string -> becomes categorical
    'heating': 'unknown',
    'windows_type': 'unknown',
    # 'bathrooms_num': 'unknown' # Decision: Fill with median instead to keep numeric
}

# Explicitly define column types for processing and modeling
# Note: Boolean columns (feat_cat_*) often treated as numeric (0/1) by LGBM
NUMERIC_COLS = [
    'area',
    'bathrooms_num', # Will fill NaNs with median
    'floor_no',      # Will fill NaNs with 0
    'latitude',
    'longitude',
    'rooms_num_clean' # Assuming this is the cleaned numeric version
]

# Define categorical columns (including those created by filling NaNs)
CATEGORICAL_COLS = [
    'location_address_province',
    'location_address_county',
    'location_address_city',
    'advertiser_type',
    'ad_category_name',
    'energy_certificate', # Ordinal - will handle specifically
    'heating',            # Becomes categorical after fillna
    'windows_type',       # Becomes categorical after fillna
    'build_year'          # Becomes categorical after fillna
]

# Define the order for the ordinal feature
ENERGY_CERTIFICATE_ORDER = [
    'isento', 'excluído', 'em processo', 'sem certificado', # Assuming 'sem certificado' is low/neutral
    'g', 'f', 'e', 'd', 'c', 'b', 'b-', 'a', 'a+' # Order from worst to best
]


# --- Data Loading and Cleaning Function ---
def load_and_clean_data(filepath, columns_to_drop, fillna_config, numeric_cols, categorical_cols, target_col):
    """Loads, cleans, and prepares data for LightGBM."""
    print(f"--- Loading data from: {filepath} ---")
    try:
        df = pd.read_parquet(filepath)
        print(f"Initial shape: {df.shape}")
        print(f"Initial columns: {df.columns.tolist()}")
    except FileNotFoundError:
        print(f"Error: File not found at {filepath}")
        return None, None
    except Exception as e:
        print(f"Error reading Parquet file: {e}")
        return None, None

    # 1. Drop specified columns (check existence first)
    print("\n--- Dropping specified columns ---")
    actual_cols_to_drop = [col for col in columns_to_drop if col in df.columns]
    if actual_cols_to_drop:
        print(f"Dropping: {actual_cols_to_drop}")
        df = df.drop(columns=actual_cols_to_drop)
    else:
        print("No columns from the drop list found in the DataFrame.")

    # 2. Fill NaNs
    print("\n--- Filling NaN values ---")
    for col, value in fillna_config.items():
        if col in df.columns:
            print(f"Filling NaNs in '{col}' with '{value}'")
            df[col] = df[col].fillna(value)
        else:
            print(f"Warning: Column '{col}' not found for fillna.")

    # Specific NaN handling for bathrooms_num (using median)
    if 'bathrooms_num' in df.columns:
         # Ensure it's numeric before calculating median
        df['bathrooms_num'] = pd.to_numeric(df['bathrooms_num'], errors='coerce')
        median_bathrooms = df['bathrooms_num'].median()
        print(f"Filling NaNs in 'bathrooms_num' with median ({median_bathrooms})")
        df['bathrooms_num'].fillna(median_bathrooms, inplace=True)
        # Ensure it's integer type if appropriate after filling
        # df['bathrooms_num'] = df['bathrooms_num'].astype(int) # Optional, depends if decimals make sense
    else:
        print("Warning: 'bathrooms_num' column not found for median fill.")

    # 3. Ensure correct dtypes and identify features (X) and target (y)
    print("\n--- Finalizing Feature Set and Target ---")
    # Identify all boolean/feature columns automatically
    feature_cols = [col for col in df.columns if col.startswith('feat_cat_')]
    print(f"Identified {len(feature_cols)} 'feat_cat_*' columns.")

    # Combine all feature columns
    all_feature_cols = numeric_cols + categorical_cols + feature_cols

    # Check if all identified columns exist
    missing_features = [col for col in all_feature_cols if col not in df.columns]
    if missing_features:
        print(f"Warning: The following expected feature columns are missing: {missing_features}")
        # Remove missing columns from lists to avoid errors downstream
        numeric_cols = [col for col in numeric_cols if col in df.columns]
        categorical_cols = [col for col in categorical_cols if col in df.columns]
        feature_cols = [col for col in feature_cols if col in df.columns]
        all_feature_cols = [col for col in all_feature_cols if col in df.columns]


    if target_col not in df.columns:
         print(f"Error: Target column '{target_col}' not found in the DataFrame.")
         return None, None

    # Select features and target
    X = df[all_feature_cols].copy() # Use copy to avoid SettingWithCopyWarning
    y = df[target_col].copy()

    # Convert target to numeric, handling potential errors
    y = pd.to_numeric(y, errors='coerce')
    # Handle NaNs created in target (e.g., from non-numeric values)
    nan_target_count = y.isna().sum()
    if nan_target_count > 0:
        print(f"Warning: {nan_target_count} rows removed due to non-numeric target values ('{target_col}').")
        X = X[y.notna()]
        y = y.dropna()

    # 4. Convert dtypes for LightGBM
    print("\n--- Converting column dtypes for LightGBM ---")
    for col in numeric_cols:
        if col in X.columns:
            print(f"Converting '{col}' to numeric.")
            X[col] = pd.to_numeric(X[col], errors='coerce')
            # Optional: Impute any NaNs created by coercion (e.g., if 'unknown' was forced to numeric)
            if X[col].isnull().any():
                median_val = X[col].median()
                print(f"   Imputing NaNs in '{col}' post-coercion with median ({median_val}).")
                X[col].fillna(median_val, inplace=True)


    for col in categorical_cols:
        if col in X.columns:
            if col == 'energy_certificate':
                print(f"Converting '{col}' to ordered categorical.")
                # Ensure all values in the defined order are present in the column or handle unknowns
                current_categories = X[col].unique()
                full_order = [cat for cat in ENERGY_CERTIFICATE_ORDER if cat in current_categories]
                # Add any missing categories from the data not in the predefined order (handle gracefully)
                missing_from_order = [cat for cat in current_categories if cat not in full_order]
                if missing_from_order:
                    print(f"   Warning: Categories in '{col}' not in defined order: {missing_from_order}. Adding them.")
                    # Decide how to add them (e.g., at the beginning/end or based on some logic)
                    # For simplicity, adding them at the beginning (lowest rank)
                    full_order = missing_from_order + full_order

                cat_dtype = pd.CategoricalDtype(categories=full_order, ordered=True)
                X[col] = X[col].astype(cat_dtype)
            else:
                print(f"Converting '{col}' to standard categorical.")
                X[col] = X[col].astype('category')
        else:
             print(f"Warning: Categorical column '{col}' not found during dtype conversion.")

    # Boolean/feat_cat columns are often handled well as numeric (0/1) by LightGBM
    # No explicit conversion needed unless they are object type
    for col in feature_cols:
         if col in X.columns and X[col].dtype == 'object':
             print(f"Warning: Column '{col}' seems boolean but has object type. Converting to int.")
             X[col] = pd.to_numeric(X[col], errors='coerce').fillna(0).astype(int)
         elif col in X.columns:
             # Ensure they are int if not already (e.g., could be bool)
             X[col] = X[col].astype(int)


    print("\n--- Data Cleaning and Preparation Complete ---")
    print(f"Final features shape: {X.shape}")
    print(f"Target shape: {y.shape}")
    X.info() # Display final dtypes

    return X, y

# --- Training and Evaluation Function ---
def train_evaluate_lightgbm(X, y):
    """Trains and evaluates a LightGBM model."""
    if X is None or y is None:
        print("Cannot train model due to data loading errors.")
        return

    print("\n--- Splitting Data ---")
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    print(f"Train set size: {X_train.shape[0]}, Test set size: {X_test.shape[0]}")

    print("\n--- Training LightGBM Model ---")
    # Initialize LightGBM Regressor
    lgbm = lgb.LGBMRegressor(
        objective='regression', # Specify the learning task
        metric='rmse',          # Evaluation metric (Root Mean Squared Error)
        n_estimators=1000,      # Number of boosting rounds
        learning_rate=0.05,
        num_leaves=31,
        max_depth=-1,           # No limit on tree depth
        random_state=42,
        n_jobs=-1,              # Use all available CPU cores
        colsample_bytree=0.8,   # Subsample ratio of columns when constructing each tree
        subsample=0.8,          # Subsample ratio of the training instance
        reg_alpha=0.12,          # L1 regularization
        reg_lambda=0.1          # L2 regularization
    )

    # Train the model
    # LightGBM can automatically handle pandas 'category' dtype
    lgbm.fit(
        X_train, y_train,
        eval_set=[(X_test, y_test)],
        eval_metric='rmse',
        callbacks=[lgb.early_stopping(100, verbose=True)], # Stop if metric doesn't improve for 100 rounds
        # categorical_feature='auto' # Usually default and works with category dtype
    )

    print("\n--- Evaluating Model ---")
    y_pred_train = lgbm.predict(X_train)
    y_pred_test = lgbm.predict(X_test)

    # Calculate metrics
    rmse_train = np.sqrt(mean_squared_error(y_train, y_pred_train))
    mae_train = mean_absolute_error(y_train, y_pred_train)
    r2_train = r2_score(y_train, y_pred_train)

    rmse_test = np.sqrt(mean_squared_error(y_test, y_pred_test))
    mae_test = mean_absolute_error(y_test, y_pred_test)
    r2_test = r2_score(y_test, y_pred_test)

    print("\n--- Performance Metrics ---")
    print(f"Train RMSE: {rmse_train:.4f}")
    print(f"Test RMSE:  {rmse_test:.4f}")
    print(f"Train MAE:  {mae_train:.4f}")
    print(f"Test MAE:   {mae_test:.4f}")
    print(f"Train R^2:  {r2_train:.4f}")
    print(f"Test R^2:   {r2_test:.4f}")

    # --- Feature Importance ---
    print("\n--- Feature Importances (Top 20) ---")
    importance_df = pd.DataFrame({
        'feature': lgbm.feature_name_,
        'importance': lgbm.feature_importances_
    }).sort_values(by='importance', ascending=False)

    print(importance_df.head(20))

    # Plot feature importance (optional, requires matplotlib)
    try:
        import matplotlib.pyplot as plt
        import seaborn as sns

        plt.figure(figsize=(10, 8))
        sns.barplot(x="importance", y="feature", data=importance_df.head(20))
        plt.title("LightGBM Feature Importance (Top 20)")
        plt.tight_layout()
        plt.savefig("lgbm_feature_importance.png") # Save the plot
        print("\nFeature importance plot saved as 'lgbm_feature_importance.png'")
        # plt.show()
    except ImportError:
        print("\nInstall matplotlib and seaborn to visualize feature importance ('pip install matplotlib seaborn')")

# --- Main Execution ---
if __name__ == "__main__":
    # 1. Load and Clean Data
    X, y = load_and_clean_data(
        FILE_PATH,
        COLUMNS_TO_DROP,
        FILLNA_CONFIG,
        NUMERIC_COLS,
        CATEGORICAL_COLS,
        TARGET_COLUMN
    )

    # 2. Train and Evaluate Model
    if X is not None and y is not None:
        train_evaluate_lightgbm(X, y)
    else:
        print("Model training aborted due to data issues.")