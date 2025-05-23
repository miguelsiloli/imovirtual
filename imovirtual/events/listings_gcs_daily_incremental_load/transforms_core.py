import pandas as pd
import json
import re
from typing import Dict, Any, List, Optional
from bs4 import BeautifulSoup
import datetime
import numpy as np # For handling numpy arrays and types

def normalize_structure(data: Any) -> Any:
    if isinstance(data, np.ndarray):
        return [normalize_structure(item) for item in data.tolist()]
    elif isinstance(data, dict):
        return {k: normalize_structure(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [normalize_structure(item) for item in data]
    elif isinstance(data, (np.int_, np.intc, np.intp, np.int8, np.int16, np.int32, np.int64,
                         np.uint8, np.uint16, np.uint32, np.uint64)):
        return int(data)
    elif isinstance(data, (np.float64, np.float16, np.float32, np.float64)):
        return float(data)
    elif isinstance(data, np.bool_):
        return bool(data)
    elif isinstance(data, (np.str_, np.str_)):
        return str(data)
    elif isinstance(data, np.datetime64):
        return pd.Timestamp(data).to_pydatetime()
    return data

def get_nested_value(data_dict: Optional[Dict[str, Any]], keys: List[str], default: Any = None) -> Any:
    current = data_dict
    if not isinstance(current, dict): return default
    for i, key in enumerate(keys):
        if isinstance(current, dict) and key in current:
            value = current[key]
            if value is not None: current = value
            elif i == len(keys) -1: return None
            else: return default
        else: return default
    return current

def remove_typename_recursive(data: Any) -> Any:
    if isinstance(data, dict):
        return {k: remove_typename_recursive(v) for k, v in data.items() if k != '__typename'}
    elif isinstance(data, list):
        return [remove_typename_recursive(item) for item in data]
    return data

def extract_basic_info(ad_payload: Dict[str, Any]) -> Dict[str, Any]:
    info = {}; raw_id = ad_payload.get('id')
    if isinstance(raw_id, float) and raw_id.is_integer(): info['id'] = int(raw_id)
    elif isinstance(raw_id, int): info['id'] = raw_id
    else: info['id'] = raw_id 
    info['title'] = ad_payload.get('title'); info['url'] = ad_payload.get('url')
    description_html = ad_payload.get('description', '')
    soup = BeautifulSoup(description_html, 'html.parser')
    info['description_text'] = soup.get_text(separator=' ').replace('\n', ' ').strip()
    info['advert_type'] = ad_payload.get('advertType'); info['advertiser_type_detail'] = ad_payload.get('advertiserType')
    info['status'] = ad_payload.get('status'); created_at_str = ad_payload.get('createdAt') 
    info['created_at'] = pd.to_datetime(created_at_str, errors='coerce', utc=False) if created_at_str else None
    if pd.NaT is info['created_at']: info['created_at'] = None 
    modified_at_str = ad_payload.get('modifiedAt') 
    info['modified_at'] = pd.to_datetime(modified_at_str, errors='coerce', utc=False) if modified_at_str else None
    if pd.NaT is info['modified_at']: info['modified_at'] = None 
    info['slug'] = ad_payload.get('slug'); return info

def extract_category_info(ad_payload: Dict[str, Any]) -> Dict[str, Any]:
    info = {}; property_type = get_nested_value(ad_payload, ['category', 'name', 0, 'value'])
    if not property_type: property_type = get_nested_value(ad_payload, ['adCategory', 'name'])
    info['property_type'] = property_type
    info['transaction_type'] = get_nested_value(ad_payload, ['adCategory', 'type']); return info

def parse_characteristics(characteristics_list: Optional[List[Dict[str, Any]]]) -> Dict[str, Any]:
    parsed_chars = {'price': None, 'price_currency': None, 'price_period': None, 'area_m2': None, 'typology': None, 'bedrooms': None, 'energy_certificate': None, 'bathrooms': None}
    if not isinstance(characteristics_list, list): return parsed_chars
    if not characteristics_list: return parsed_chars
    for char_item in characteristics_list:
        if not isinstance(char_item, dict): continue
        key = char_item.get('key'); value_data = char_item.get('value'); localized_value = char_item.get('localizedValue')
        if key == 'price':
            if value_data is not None:
                try: parsed_chars['price'] = float(value_data)
                except (ValueError, TypeError): pass 
            parsed_chars['price_currency'] = char_item.get('currency'); suffix = char_item.get('suffix', '')
            parsed_chars['price_period'] = suffix.replace('/', '').strip() if suffix else None
        elif key == 'm': 
            if value_data is not None:
                try: parsed_chars['area_m2'] = float(value_data)
                except (ValueError, TypeError): pass
        elif key == 'rooms_num': 
            parsed_chars['typology'] = localized_value 
            if localized_value and isinstance(localized_value, str) and localized_value.upper().startswith('T'):
                match = re.match(r"T(\d+)", localized_value.upper())
                if match:
                    try: parsed_chars['bedrooms'] = int(match.group(1))
                    except ValueError: pass
        elif key == 'energy_certificate': parsed_chars['energy_certificate'] = localized_value
        elif key == 'bathrooms_num':
            if value_data is not None:
                try: parsed_chars['bathrooms'] = int(value_data)
                except (ValueError, TypeError): pass
    return parsed_chars

def extract_location_info(ad_payload: Dict[str, Any]) -> Dict[str, Any]:
    info = {}; location_data = ad_payload.get('location', {}) 
    if not isinstance(location_data, dict): location_data = {}
    info['address_city'] = get_nested_value(location_data, ['address', 'city', 'name'])
    info['address_county'] = get_nested_value(location_data, ['address', 'county', 'name'])
    info['address_district'] = get_nested_value(location_data, ['address', 'province', 'name'])
    info['latitude'] = get_nested_value(location_data, ['coordinates', 'latitude'])
    info['longitude'] = get_nested_value(location_data, ['coordinates', 'longitude'])
    raw_map_details = location_data.get('mapDetails', {}); info['map_details'] = remove_typename_recursive(raw_map_details) 
    raw_reverse_geocoding = location_data.get('reverseGeocoding', {}); info['reverse_geocoding'] = remove_typename_recursive(raw_reverse_geocoding)
    return info

def extract_contact_info(ad_payload: Dict[str, Any]) -> Dict[str, Any]:
    info = {}; contact_details = ad_payload.get('contactDetails', {})
    if not isinstance(contact_details, dict): contact_details = {}
    info['contact_name'] = contact_details.get('name'); contact_phones = contact_details.get('phones') 
    if isinstance(contact_phones, list) and contact_phones: info['contact_phone'] = contact_phones[0]
    else: info['contact_phone'] = None
    return info

def extract_image_urls(ad_payload: Dict[str, Any]) -> Dict[str, Any]:
    images_data = ad_payload.get('images'); image_urls = []
    if isinstance(images_data, list):
        for img in images_data:
            if isinstance(img, dict) and img.get('large'): image_urls.append(img.get('large'))
    return {'image_urls': image_urls}

def extract_amenities(ad_payload: Dict[str, Any], description_text: Optional[str]) -> Dict[str, Any]:
    other_features_dict = { 'lift': None, 'garage': None, 'heating': None, 'fireplace': None, 'balcony': None, 'suite': None, 'kitchen_equipment': None }
    additional_info_list = ad_payload.get('additionalInformation')
    if isinstance(additional_info_list, list):
        for info_item in additional_info_list:
            if not isinstance(info_item, dict): continue
            label = info_item.get('label'); values = info_item.get('values') 
            if label == 'lift' and isinstance(values, list) and values:
                if values[0] == '::y': other_features_dict['lift'] = True
                elif values[0] == '::n': other_features_dict['lift'] = False
    if description_text and isinstance(description_text, str) and not description_text.startswith("HTML Description"):
        desc_lower = description_text.lower()
        if "garagem fechada" in desc_lower: other_features_dict['garage'] = "Closed"
        elif "garagem" in desc_lower: other_features_dict['garage'] = "Yes"
        if "aquecimento central" in desc_lower: other_features_dict['heating'] = "Central Heating"
        elif "aquecimento" in desc_lower: other_features_dict['heating'] = "Yes"
        if "lareira" in desc_lower: other_features_dict['fireplace'] = True
        if "varanda" in desc_lower: other_features_dict['balcony'] = True
        if "suite" in desc_lower: other_features_dict['suite'] = True
        if "cozinha semi-equipada" in desc_lower: other_features_dict['kitchen_equipment'] = "Semi-equipped"
        elif "cozinha equipada" in desc_lower: other_features_dict['kitchen_equipment'] = "Equipped"
    return {'other_features': other_features_dict}

def extract_passthrough_features(ad_payload: Dict[str, Any], row_name_for_debug: str = "") -> Dict[str, Any]: # Added row_name for debug
    info = {}

    def process_list_for_array_string(data_list: Optional[List[Any]], field_name: str) -> List[str]:
        if not isinstance(data_list, list):
            # This debug print is key if normalize_structure isn't fully effective
            # print(f"DEBUG (Row {row_name_for_debug}, Field '{field_name}'): process_list_for_array_string received non-list: {type(data_list)}. Returning [].")
            return [] 
        
        processed_list = []
        for i, item in enumerate(data_list):
            if isinstance(item, str):
                processed_list.append(item)
            elif isinstance(item, dict): 
                try:
                    processed_list.append(json.dumps(item, ensure_ascii=False)) 
                except TypeError:
                    # print(f"DEBUG (Row {row_name_for_debug}, Field '{field_name}', Item {i}): Failed to json.dumps dict: {item}. Using str().")
                    processed_list.append(str(item)) 
            elif item is None:
                processed_list.append("") 
            else:
                # print(f"DEBUG (Row {row_name_for_debug}, Field '{field_name}', Item {i}): Unexpected item type {type(item)}. Converting to str: {item}")
                processed_list.append(str(item))
        return processed_list

    features_raw = ad_payload.get('features', []) # Default to empty list
    features_by_category_raw = ad_payload.get('featuresByCategory', []) # Default to empty list
    features_without_category_raw = ad_payload.get('featuresWithoutCategory', []) # Default to empty list

    # --- Add Specific Debug for featuresByCategory ---
    # print(f"DEBUG (Row {row_name_for_debug}, extract_passthrough_features): Type of features_by_category_raw (from ad_payload.get): {type(features_by_category_raw)}")
    # if isinstance(features_by_category_raw, list) and features_by_category_raw:
    #     print(f"DEBUG (Row {row_name_for_debug}, extract_passthrough_features): Type of first item in features_by_category_raw: {type(features_by_category_raw[0])}")
    #     print(f"DEBUG (Row {row_name_for_debug}, extract_passthrough_features): Content of first item: {str(features_by_category_raw[0])[:200]}")
    # --- End Specific Debug ---

    info['features'] = process_list_for_array_string(features_raw, 'features')
    info['featuresByCategory'] = process_list_for_array_string(features_by_category_raw, 'featuresByCategory')
    info['featuresWithoutCategory'] = process_list_for_array_string(features_without_category_raw, 'featuresWithoutCategory')
    
    return info

def process_single_listing_row(row_series: pd.Series, props_column_name: str) -> Optional[Dict[str, Any]]:
    props_content_raw = row_series.get(props_column_name)
    props_content_intermediate = {} 
    row_name = str(row_series.name) # For debug messages

    if isinstance(props_content_raw, str):
        try: props_content_intermediate = json.loads(props_content_raw)
        except (TypeError, json.JSONDecodeError): props_content_intermediate = {} 
    elif isinstance(props_content_raw, dict): props_content_intermediate = props_content_raw 
    else: props_content_intermediate = {} 

    props_content = normalize_structure(props_content_intermediate)
    raw_ad_payload = get_nested_value(props_content, ['pageProps', 'ad'])
    ad_payload = raw_ad_payload 
    if not isinstance(ad_payload, dict): ad_payload = {} 

    final_record = {}
    final_record.update(extract_basic_info(ad_payload))
    final_record.update(extract_category_info(ad_payload))
    characteristics_list = ad_payload.get('characteristics')                                                      
    final_record.update(parse_characteristics(characteristics_list))
    final_record.update(extract_location_info(ad_payload))
    final_record.update(extract_contact_info(ad_payload))
    final_record.update(extract_image_urls(ad_payload))
    final_record.update(extract_amenities(ad_payload, final_record.get('description_text')))
    final_record.update(extract_passthrough_features(ad_payload, row_name_for_debug=row_name)) # Pass row_name

    if final_record.get('bedrooms') is None and final_record.get('description_text') and not final_record.get('description_text', "").startswith("HTML Description"):
        desc_lower_for_rooms = final_record['description_text'].lower()
        if "3 quartos" in desc_lower_for_rooms: final_record['bedrooms'] = 3
        elif "2 quartos" in desc_lower_for_rooms: final_record['bedrooms'] = 2
        elif "1 quarto" in desc_lower_for_rooms: final_record['bedrooms'] = 1
        
    ingestion_date_raw = row_series.get('ingestionDate')
    if ingestion_date_raw:
        try:
            if isinstance(ingestion_date_raw, datetime.date) and not isinstance(ingestion_date_raw, datetime.datetime):
                final_record['ingestionDate'] = ingestion_date_raw
            else: 
                dt_obj = pd.to_datetime(ingestion_date_raw)
                final_record['ingestionDate'] = dt_obj.date() if not pd.isna(dt_obj) else None
        except (ValueError, TypeError): final_record['ingestionDate'] = None
    else: final_record['ingestionDate'] = None
        
    bq_schema_keys = [
        "id", "title", "url", "description_text", "advert_type", "advertiser_type_detail", "status", "created_at", "modified_at", 
        "property_type", "transaction_type", "price", "price_currency", "price_period", "area_m2", "typology", "bedrooms", 
        "energy_certificate", "bathrooms", "address_city", "address_county", "address_district", "latitude", "longitude", 
        "contact_name", "contact_phone", "image_urls", "other_features", "features", "featuresByCategory", 
        "featuresWithoutCategory", "map_details", "reverse_geocoding", "slug", "ingestionDate"
    ]
    complete_record = {key: final_record.get(key) for key in bq_schema_keys}
    
    if not isinstance(complete_record.get('image_urls'), list): complete_record['image_urls'] = []
    if not isinstance(complete_record.get('features'), list): complete_record['features'] = []
    if not isinstance(complete_record.get('featuresByCategory'), list): complete_record['featuresByCategory'] = []
    if not isinstance(complete_record.get('featuresWithoutCategory'), list): complete_record['featuresWithoutCategory'] = []
    if not isinstance(complete_record.get('other_features'), dict):
        complete_record['other_features'] = { 'lift': None, 'garage': None, 'heating': None, 'fireplace': None, 'balcony': None, 'suite': None, 'kitchen_equipment': None }
    if not isinstance(complete_record.get('map_details'), dict): complete_record['map_details'] = {}
    if not isinstance(complete_record.get('reverse_geocoding'), dict): complete_record['reverse_geocoding'] = {}
    return complete_record

def transform_listings_data(input_df: pd.DataFrame, props_column_name: str = 'props') -> pd.DataFrame:
    processed_records = []
    for index, original_row in input_df.iterrows():
        original_row.name = index 
        processed_record = process_single_listing_row(original_row, props_column_name)
        if processed_record: processed_records.append(processed_record)
    output_df = pd.DataFrame(processed_records)
    print(f"\n--- DEBUG PRINT (transform_listings_data) ---")
    print(f"--- Processed {len(processed_records)} records. ---")
    if not output_df.empty:
        print(f"--- DataFrame shape: {output_df.shape} ---")
        cols_to_show = ['id','price', 'typology', 'bedrooms', 'featuresByCategory']
        existing_cols_to_show = [col for col in cols_to_show if col in output_df.columns]
        print(f"\n--- Sample (selected cols): {existing_cols_to_show} ---")
        print(output_df[existing_cols_to_show].head(min(3, len(output_df))).to_string())
    else: print("--- Output DataFrame is empty. ---")
    print("--- END OF DEBUG PRINT ---")
    return output_df