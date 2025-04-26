import json
import pandas as pd
from tqdm import tqdm
import logging
from bs4 import BeautifulSoup
import re
import pyarrow # Ensure pyarrow is installed

# --- Configuration ---
INPUT_JSONL_FILE = 'scraped_imovirtual_raw_data.jsonl'
OUTPUT_PARQUET_FILE = 'flattened_imovirtual_data.parquet'

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# --- Helper Functions ---

def clean_html(raw_html):
    """Removes HTML tags from a string."""
    if not raw_html or not isinstance(raw_html, str):
        return ""
    try:
        soup = BeautifulSoup(raw_html, 'html.parser')
        text = soup.get_text(separator='\n', strip=True)
        # Replace multiple newlines with a single one
        text = re.sub(r'\n{2,}', '\n', text)
        return text
    except Exception as e:
        logging.warning(f"Could not clean HTML: {e}. Raw: {raw_html[:100]}...")
        return raw_html # Return raw if cleaning fails

def clean_surrogates(text):
    """Removes or replaces surrogate characters from a string."""
    if not isinstance(text, str):
        return text
    # Replace invalid surrogates with the standard replacement character '�'
    return text.encode('utf-8', 'replace').decode('utf-8')

def extract_and_flatten(json_data, line_num):
    """Extracts and flattens data from a single parsed JSON object."""
    try:
        # Navigate to the core ad data
        ad_data = json_data.get('props', {}).get('pageProps', {}).get('ad', {})
        if not ad_data:
            logging.warning(f"Line {line_num}: Could not find 'ad' object in JSON.")
            return None

        # --- Extract desired fields ---
        # (Extraction logic remains the same as previous version)
        flat_data = {
            'ad_id': ad_data.get('id'),
            'slug': ad_data.get('slug'),
            'url': ad_data.get('url'),
            'title': ad_data.get('title'), # Already present, might differ from target.Title
            'advertiser_type': ad_data.get('advertiserType'),
            'advert_type': ad_data.get('advertType'),
            'status': ad_data.get('status'),
            'created_at': ad_data.get('createdAt'),
            'modified_at': ad_data.get('modifiedAt'),
            'pushed_up_at': ad_data.get('pushedUpAt'),
            'description_raw': ad_data.get('description'),
            'description_clean': clean_html(ad_data.get('description')),

            # Initialize fields extracted from characteristics/target
            'price': None,
            'price_currency': None,
            'price_suffix': None,
            'area': None,
            'area_unit': 'm²', # Assuming m² based on previous context
            'rooms_num_clean': None, # T4, T3 etc.
            'rooms_num_raw': None, # Numeric value (often bedrooms + living rooms)
            'bathrooms_num': None,
            'floor_no': None,
            'energy_certificate': None,
            'construction_status': None,
            'build_year': None, # Often missing or in target
            'heating': None,
            'windows_type': None,

            # Target fields (some might overlap with characteristics)
            'target_price': ad_data.get('target', {}).get('Price'),
            'target_area': ad_data.get('target', {}).get('Area'),
            'target_bathrooms_num': ad_data.get('target', {}).get('Bathrooms_num', [None])[0], # Take first if list
            'target_city': ad_data.get('target', {}).get('City'),
            'target_city_id': ad_data.get('target', {}).get('City_id'),
            'target_construction_status': ad_data.get('target', {}).get('Construction_status', [None])[0],
            'target_country': ad_data.get('target', {}).get('Country'),
            'target_energy_certificate': ad_data.get('target', {}).get('Energy_certificate', [None])[0],
            'target_floor_no': ad_data.get('target', {}).get('Floor_no', [None])[0],
            'target_heating': ad_data.get('target', {}).get('Heating', [None])[0],
            'target_offer_type': ad_data.get('target', {}).get('OfferType'),
            'target_proper_type': ad_data.get('target', {}).get('ProperType'),
            'target_province': ad_data.get('target', {}).get('Province'),
            'target_rooms_num': ad_data.get('target', {}).get('Rooms_num', [None])[0],
            'target_windows_type': ad_data.get('target', {}).get('Windows_type', [None])[0],
            'target_build_year': ad_data.get('target', {}).get('Build_year'), # Direct extraction if available

            # Features (store as list or comma-separated string)
            'features': ad_data.get('features'), # Keep as list

            # featuresByCategory might be complex, store as JSON string?
            'features_by_category': json.dumps(ad_data.get('featuresByCategory')) if ad_data.get('featuresByCategory') else None,

            # Images (extract large URLs)
            'image_urls': [img.get('large') for img in ad_data.get('images', []) if img.get('large')],

            # Location details
            'latitude': ad_data.get('location', {}).get('coordinates', {}).get('latitude'),
            'longitude': ad_data.get('location', {}).get('coordinates', {}).get('longitude'),
            'location_address_city': ad_data.get('location', {}).get('address', {}).get('city', {}).get('name'),
            'location_address_county': ad_data.get('location', {}).get('address', {}).get('county', {}).get('name'),
            'location_address_province': ad_data.get('location', {}).get('address', {}).get('province', {}).get('name'),
            'location_address_postal_code': ad_data.get('location', {}).get('address', {}).get('postalCode'), # Often null

             # Reverse geocoding info (example: full parish name)
            'location_parish_full': next((loc.get('fullName') for loc in ad_data.get('location', {}).get('reverseGeocoding', {}).get('locations', []) if loc.get('locationLevel') == 'parish'), None),


            # Owner details
            'owner_id': ad_data.get('owner', {}).get('id'),
            'owner_name': ad_data.get('owner', {}).get('name'),
            'owner_type': ad_data.get('owner', {}).get('type'),
            'owner_phones': ad_data.get('owner', {}).get('phones'), # Keep as list

            # SEO details
            'seo_title': ad_data.get('seo', {}).get('title'),
            'seo_description': ad_data.get('seo', {}).get('description'),

            # Ad Category
            'ad_category_id': ad_data.get('adCategory', {}).get('id'),
            'ad_category_name': ad_data.get('adCategory', {}).get('name'),
            'ad_category_type': ad_data.get('adCategory', {}).get('type'),

            # Breadcrumbs (maybe store as JSON string or just the last item)
            'breadcrumbs_json': json.dumps(ad_data.get('breadcrumbs')) if ad_data.get('breadcrumbs') else None,
            'last_breadcrumb_label': ad_data.get('breadcrumbs', [])[-1].get('label') if ad_data.get('breadcrumbs') else None,

            # Tracking Data
            'tracking_price': ad_data.get('adTrackingData', {}).get('ad_price'),
            'tracking_business': ad_data.get('adTrackingData', {}).get('business'),
            'tracking_city_name': ad_data.get('adTrackingData', {}).get('city_name'),
            'tracking_poster_type': ad_data.get('adTrackingData', {}).get('poster_type'),
            'tracking_region_name': ad_data.get('adTrackingData', {}).get('region_name'),
            'tracking_seller_id': ad_data.get('adTrackingData', {}).get('seller_id'),
        }

        # --- Process Characteristics List ---
        # (Characteristics processing logic remains the same)
        for char in ad_data.get('characteristics', []):
            key = char.get('key')
            value = char.get('value')
            localized = char.get('localizedValue')
            currency = char.get('currency')
            suffix = char.get('suffix')

            if key == 'price':
                flat_data['price'] = value
                flat_data['price_currency'] = currency
                flat_data['price_suffix'] = suffix
            elif key == 'm': # Area
                flat_data['area'] = value
            elif key == 'rooms_num':
                flat_data['rooms_num_clean'] = localized # e.g., T4
                flat_data['rooms_num_raw'] = value       # e.g., 5
            elif key == 'bathrooms_num':
                flat_data['bathrooms_num'] = value
            elif key == 'floor_no':
                flat_data['floor_no'] = localized # e.g., 1, Rés do chão
            elif key == 'energy_certificate':
                flat_data['energy_certificate'] = localized # e.g., D
            elif key == 'construction_status':
                flat_data['construction_status'] = localized
            elif key == 'build_year':
                 flat_data['build_year'] = value # Overwrite if found here
            elif key == 'heating':
                flat_data['heating'] = localized
            elif key == 'windows_type':
                flat_data['windows_type'] = localized
            # Add elif for other characteristics you might want ('lift', etc.)

        return flat_data

    except Exception as e:
        logging.error(f"Line {line_num}: Error flattening JSON: {e}. Data sample: {str(json_data)[:200]}...")
        return None

# --- Main Execution ---
if __name__ == "__main__":
    logging.info(f"Reading JSONL from {INPUT_JSONL_FILE}...")
    all_flattened_data = []
    line_count = 0
    processed_count = 0
    error_count = 0

    try:
        with open(INPUT_JSONL_FILE, 'r', encoding='utf-8') as infile:
            # Read all lines first to get total for tqdm
            lines = infile.readlines()
            line_count = len(lines)
            logging.info(f"Found {line_count} lines to process.")

            for i, line in enumerate(tqdm(lines, desc="Processing JSONL lines")):
                line_num = i + 1
                line = line.strip()
                if not line:
                    continue # Skip empty lines

                try:
                    parsed_json = json.loads(line)
                    flattened = extract_and_flatten(parsed_json, line_num)
                    if flattened:
                        all_flattened_data.append(flattened)
                        processed_count += 1
                    else:
                         error_count += 1 # Increment error if flattening returned None
                except json.JSONDecodeError as json_err:
                    logging.error(f"Line {line_num}: Invalid JSON: {json_err}. Skipping line.")
                    error_count += 1
                except Exception as e:
                     logging.error(f"Line {line_num}: Unexpected error processing line: {e}")
                     error_count += 1


    except FileNotFoundError:
        logging.error(f"Input file not found: {INPUT_JSONL_FILE}")
        exit()
    except Exception as e:
        logging.error(f"Error reading or processing file {INPUT_JSONL_FILE}: {e}")
        exit()

    logging.info(f"Finished processing. Successfully flattened {processed_count} records.")
    if error_count > 0:
         logging.warning(f"Encountered errors in {error_count} records during JSON parsing/flattening.")

    # --- Convert to DataFrame and Save to Parquet ---
    if all_flattened_data:
        logging.info("Converting flattened data to Pandas DataFrame...")
        try:
            df = pd.DataFrame(all_flattened_data)

            # --- *** NEW: Clean surrogate characters from string columns *** ---
            logging.info("Cleaning surrogate characters from string columns...")
            string_columns = df.select_dtypes(include=['object']).columns
            for col in tqdm(string_columns, desc="Cleaning columns"):
                # Apply the cleaning function only to object (likely string) columns
                # Ensure NaN values are handled gracefully by the cleaner or skipped
                # Using .astype(str).apply(...) ensures even mixed types are attempted
                # But clean_surrogates already handles non-str types, so direct apply is fine:
                 df[col] = df[col].apply(clean_surrogates)
            logging.info("Surrogate cleaning complete.")
            # --- *** End of Surrogate Cleaning *** ---


            # --- Optional: Data Type Conversions ---
            logging.info("Attempting data type conversions...")
            numeric_cols = ['ad_id', 'price', 'area', 'bathrooms_num', 'rooms_num_raw',
                            'latitude', 'longitude', 'owner_id', 'tracking_price',
                            'target_price', 'target_area', 'target_rooms_num', 'target_bathrooms_num', 'build_year', 'target_build_year']
            datetime_cols = ['created_at', 'modified_at', 'pushed_up_at']

            for col in numeric_cols:
                if col in df.columns:
                    # Use pd.to_numeric which is robust to different types
                    df[col] = pd.to_numeric(df[col], errors='coerce')

            for col in datetime_cols:
                 if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors='coerce')


            logging.info(f"Saving DataFrame with {len(df)} rows and {len(df.columns)} columns to {OUTPUT_PARQUET_FILE}...")
            df.to_parquet(OUTPUT_PARQUET_FILE, index=False, engine='pyarrow', compression='snappy')
            logging.info("Parquet file saved successfully.")

        except ImportError:
             logging.error("`pyarrow` library is required to save to Parquet. Please install it (`pip install pyarrow`).")
        except Exception as e:
             logging.error(f"Failed to create or save DataFrame to Parquet: {e}")
             # Optional: print df.info() here for debugging schema issues
             # print(df.info())
             # Raising the exception again might be helpful for debugging
             # raise e

    else:
        logging.warning("No data was successfully flattened to save.")

    logging.info("Script completed.")