CREATE TABLE imovirtual_listings.staging_imovirtual_listings (
    id INT64,
    title STRING,
    url STRING,
    description_text STRING,
    advert_type STRING,
    advertiser_type_detail STRING,
    status STRING,
    created_at TIMESTAMP,
    modified_at TIMESTAMP,
    property_type STRING,
    transaction_type STRING,
    price FLOAT64,
    price_currency STRING,
    price_period STRING,
    area_m2 FLOAT64,
    typology STRING,
    bedrooms INT64,
    energy_certificate STRING,
    bathrooms INT64,
    address_city STRING,
    address_county STRING,
    address_district STRING,
    latitude FLOAT64,
    longitude FLOAT64,
    contact_name STRING,
    contact_phone STRING,
    image_urls ARRAY<STRING>,
    other_features STRUCT<
        lift BOOLEAN,
        garage STRING,
        heating STRING,
        fireplace BOOLEAN,
        balcony BOOLEAN,
        suite BOOLEAN,
        kitchen_equipment STRING
    >,
    features ARRAY<STRING>,
    featuresByCategory ARRAY<STRING>,
    featuresWithoutCategory ARRAY<STRING>,
    map_details STRUCT<
        radius INT64,
        zoom INT64
    >,
    reverse_geocoding STRUCT<
        locations ARRAY<
            STRUCT<
                fullName STRING,
                fullNameItems ARRAY<STRING>,
                id STRING,
                locationLevel STRING,
                name STRING,
                parentIds ARRAY<STRING>
            >
        >
    >,
    slug STRING,               -- <<< ADDED SLUG
    ingestionDate DATE         -- <<< ADDED ingestionDate (as DATE)
);