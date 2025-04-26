Feature Name	Type	Source/Engineering Step	Keep Status	Notes
id	string	Raw	For Joining	Drop before training
totalPriceValue	float	Raw	TARGET	
areaInSquareMeters	float	Raw	Essential	
roomsNumber_numeric	int	Engineered from roomsNumber	Essential	Standardize text to number
estate	category	Raw	Recommended	Check variance
floorNumber_cat	category	Engineered from floorNumber	Check Quality	Check nulls, standardize
listing_latitude	float	Engineered (Geocoding/Centroid)	Essential	Needed for spatial context
listing_longitude	float	Engineered (Geocoding/Centroid)	Essential	Needed for spatial context
freguesia	category	Enriched (Boundary Join)	Recommended	Fine-grained location
municipio	category	Enriched (Boundary Join)	Recommended	Standard admin location
distrito_ilha	category	Enriched (Boundary Join)	Recommended	Standard province location
nuts3	category	Enriched (Boundary Join)	Recommended	Regional context
count_[category]_[radius]m	int	Engineered (POI Counts)	Essential	Multiple columns generated
isPrivateOwner	bool	Raw	Recommended	
totalPossibleImages	int	Raw	Recommended	
dayofweek_created	int	Engineered from dateCreatedFirst	Check Utility	Day effects might be minor
days_on_market	int	Engineered from dateCreatedFirst-dateIngestion	Recommended	
near_hospital	bool	Engineered from Text	Recommended	
is_centro_historico	bool	Engineered from Text	Recommended	
(Other keyword flags)	bool	Engineered from Text	Recommended	Add based on common valuable terms


WITH RankedHousing AS (
  -- Step 1: Deduplicate housing data by slug, keeping the latest record based on ingestionDate
  SELECT
    slug, province, city, totalPriceValue, areaInSquareMeters, roomsNumber, estate, floorNumber, ingestionDate,
    ROW_NUMBER() OVER (PARTITION BY slug ORDER BY ingestionDate DESC NULLS LAST) as row_num
  FROM
    `poised-space-456813-t0.staging_housing.staging`
  WHERE
    slug IS NOT NULL AND slug != ''
),
LatestHousing AS (
  -- Step 2: Filter to get only the latest record for each slug + Clean the city name once
  SELECT
    slug, province, LOWER(TRIM(city)) AS cleaned_city, -- Clean city name here
    city AS original_city, -- Keep original if needed
    totalPriceValue, areaInSquareMeters, roomsNumber, estate, floorNumber
  FROM RankedHousing
  WHERE row_num = 1
),
JoinAttempt1_Freguesia AS (
  -- Step 3: First attempt - LEFT JOIN housing city to boundary freguesia
  SELECT
    lh.*, -- Select all columns from LatestHousing
    fb.freguesia AS fb_freguesia, -- Alias boundary columns to avoid clashes later
    fb.municipio AS fb_municipio,
    fb.distrito_ilha AS fb_distrito_ilha,
    fb.nuts1 AS fb_nuts1,
    fb.nuts2 AS fb_nuts2,
    fb.nuts3 AS fb_nuts3,
    fb.area_ha AS fb_area_ha,
    fb.perimetro_km AS fb_perimetro_km,
    fb.geometry AS fb_geometry
  FROM
    LatestHousing lh
  LEFT JOIN
    `poised-space-456813-t0.staging_housing.freguesia_boundaries_osm` fb
  ON
    lh.cleaned_city = LOWER(TRIM(fb.freguesia)) -- Use cleaned city name
),
JoinAttempt2_Municipio AS (
  -- Step 4: Second attempt - LEFT JOIN housing city (only for those that failed Attempt 1) to boundary municipio
  SELECT
    j1.slug, -- Only need slug to link back, and boundary info from this join
    fb2.freguesia AS fb2_freguesia,
    fb2.municipio AS fb2_municipio,
    fb2.distrito_ilha AS fb2_distrito_ilha,
    fb2.nuts1 AS fb2_nuts1,
    fb2.nuts2 AS fb2_nuts2,
    fb2.nuts3 AS fb2_nuts3,
    fb2.area_ha AS fb2_area_ha,
    fb2.perimetro_km AS fb2_perimetro_km,
    fb2.geometry AS fb2_geometry
  FROM
    JoinAttempt1_Freguesia j1 -- Start from results of the first attempt
  LEFT JOIN
     `poised-space-456813-t0.staging_housing.freguesia_boundaries_osm` fb2
  ON
    j1.cleaned_city = LOWER(TRIM(fb2.municipio)) -- Join on municipio this time
  WHERE
    j1.fb_freguesia IS NULL -- IMPORTANT: Only perform this join for rows that FAILED the first join (based on a boundary column being NULL)
)
-- Step 5: Combine results, prioritizing the first join attempt
SELECT
  -- Housing data columns (from j1, which contains all housing records)
  j1.slug,
  j1.province,
  j1.original_city AS city, -- Show the original city name
  j1.totalPriceValue,
  j1.areaInSquareMeters,
  j1.roomsNumber,
  j1.estate,
  j1.floorNumber,

  -- Boundary data columns: Use COALESCE to pick from Attempt 1 first, then Attempt 2
  COALESCE(j1.fb_freguesia, j2.fb2_freguesia) AS freguesia,
  COALESCE(j1.fb_municipio, j2.fb2_municipio) AS municipio,
  COALESCE(j1.fb_distrito_ilha, j2.fb2_distrito_ilha) AS distrito_ilha,
  COALESCE(j1.fb_nuts1, j2.fb2_nuts1) AS nuts1,
  COALESCE(j1.fb_nuts2, j2.fb2_nuts2) AS nuts2,
  COALESCE(j1.fb_nuts3, j2.fb2_nuts3) AS nuts3,
  COALESCE(j1.fb_area_ha, j2.fb2_area_ha) AS area_ha,
  COALESCE(j1.fb_perimetro_km, j2.fb2_perimetro_km) AS perimetro_km,
  COALESCE(j1.fb_geometry, j2.fb2_geometry) AS geometry

FROM
  JoinAttempt1_Freguesia j1 -- Start with all results from the first join attempt
LEFT JOIN
  JoinAttempt2_Municipio j2 -- Join the results of the second attempt (only for failures of the first)
ON
  j1.slug = j2.slug; -- Link the second attempt's results back using the unique slug


-- Combined Script: Flatten OSM properties JSON and then filter for relevant POIs.

WITH FlattenedNodes AS (
  -- Step 1: Flatten the required properties from the JSON column
  SELECT
    -- Keep original columns
    geometry,
    id,

    -- Flatten relevant keys using JSON_EXTRACT_SCALAR
    -- These are the columns we will filter on below
    JSON_EXTRACT_SCALAR(properties, '$.amenity') AS amenity,
    JSON_EXTRACT_SCALAR(properties, '$.shop') AS shop,
    JSON_EXTRACT_SCALAR(properties, '$.leisure') AS leisure,
    JSON_EXTRACT_SCALAR(properties, '$.highway') AS highway,
    JSON_EXTRACT_SCALAR(properties, '$.railway') AS railway,
    JSON_EXTRACT_SCALAR(properties, '$.public_transport') AS public_transport,
    JSON_EXTRACT_SCALAR(properties, '$.tourism') AS tourism,
    JSON_EXTRACT_SCALAR(properties, '$.natural') AS natural_, -- Aliasing 'natural' key as 'natural_'
    JSON_EXTRACT_SCALAR(properties, '$.office') AS office,
    JSON_EXTRACT_SCALAR(properties, '$.building') AS building

    -- Add other JSON extractions here *only if* needed for the final SELECT,
    -- even if not used in the WHERE clause below.
    -- For example: JSON_EXTRACT_SCALAR(properties, '$.name') AS name

  FROM
    -- *** Source table with the JSON properties column ***
    `poised-space-456813-t0.staging_housing.osm_pt_nodes`
)
-- Step 2: Select the desired final columns AND apply the filter to the flattened columns
SELECT
  -- Select the columns requested for the final output
  geometry,
  id,
  amenity,
  shop,
  leisure,
  highway,
  railway,
  public_transport,
  tourism,
  natural_, -- Use the alias created in the CTE
  office,
  building
FROM
  FlattenedNodes -- Query the results of the flattening CTE
WHERE
  -- Apply the filtering logic using the newly created columns from the CTE
  (
    amenity IN (
      -- Positive Amenities
      'school', 'university', 'college', 'kindergarten', 'hospital', 'clinic', 'doctors', 'pharmacy',
      'restaurant', 'cafe', 'fast_food', 'bar', 'pub', 'bank', 'atm', 'post_office', 'library',
      'cinema', 'theatre', 'marketplace', 'parking',
      -- Services
      'police', 'fire_station', 'townhall',
      -- Potentially Negative (include for distance calculations later)
      'waste_disposal', 'recycling'
    )
    OR shop IN (
      -- High Impact Shops
      'supermarket', 'convenience', 'mall', 'department_store',
      -- Moderate Impact Shops (Grouped as general commercial activity)
      'bakery', 'butcher', 'clothes', 'hardware'
      -- Add more moderate impact types if needed
    )
    OR leisure IN (
       -- High Impact Leisure
      'park', 'playground', 'sports_centre', 'fitness_centre', 'swimming_pool', 'nature_reserve',
      'dog_park', 'garden',
      -- Moderate Impact Leisure
      'stadium'
    )
    OR highway IN (
      -- Transportation Access
      'bus_stop',
      -- Traffic Indicators (relevant for density/character)
      'traffic_signals', 'crossing',
      -- Road Access/Nuisance (Nodes often represent junctions/ends)
      'motorway_junction'
    )
    OR railway IN (
      -- Transport Access
      'station', 'stop', 'subway_entrance', 'tram_stop',
      -- Potential Nuisance
      'level_crossing'
    )
    OR public_transport IN ( -- Capture broader public transport points
      'station', 'stop_position', 'platform'
    )
    OR tourism IN (
      -- Attractions (density might indicate area character)
      'museum', 'attraction', 'gallery', 'viewpoint',
      -- Accommodation (density indicates commercial/tourism activity)
      'hotel', 'guest_house'
    )
    OR natural_ IN ( -- Filter on the aliased column 'natural_'
      -- Desirable Natural Features (proximity likely matters)
      'beach', 'water', 'wood'
    )
    OR office IS NOT NULL -- Include any node explicitly tagged as an office type
    OR building IN ( -- Include nodes representing specific building types relevant to neighborhood character
       'apartments', 'house', 'residential', 'commercial', 'industrial', 'retail', 'school', 'hospital'
    )
  ); -- Semicolon marks the end of the SQL statement