CREATE OR REPLACE TABLE `poised-space-456813-t0.staging_housing.staging` (
  `id` STRING OPTIONS(description="Unique identifier for the listing from the source system"),
  `title` STRING OPTIONS(description="Title of the listing"),
  `slug` STRING NOT NULL OPTIONS(description="URL-friendly slug for the listing. Part of the intended composite primary key."),
  `estate` STRING OPTIONS(description="Type of real estate (e.g., apartment, house)"),
  `transaction` STRING OPTIONS(description="Type of transaction (e.g., sale, rent)"),
  `developmentId` INT64 OPTIONS(description="ID of the associated development, if any"),
  `developmentTitle` STRING OPTIONS(description="Title of the associated development"),
  `developmentUrl` STRING OPTIONS(description="URL of the associated development"),
  `city` STRING OPTIONS(description="City where the property is located"),
  `province` STRING OPTIONS(description="Province/State where the property is located"),
  `street` STRING OPTIONS(description="Street address"),
  `mapRadius` FLOAT64 OPTIONS(description="Radius for map display (meaning might vary by source)"),
  `isExclusiveOffer` BOOL OPTIONS(description="Flag indicating if it's an exclusive offer"),
  `isPrivateOwner` BOOL OPTIONS(description="Flag indicating if listed by a private owner"),
  `isPromoted` BOOL OPTIONS(description="Flag indicating if the listing is promoted"),
  `source` STRING OPTIONS(description="The data source/website where the listing originated"),
  `agencyId` INT64 OPTIONS(description="ID of the listing agency"),
  `agencyName` STRING OPTIONS(description="Name of the listing agency"),
  `agencySlug` STRING OPTIONS(description="URL-friendly slug for the agency"),
  `agencyType` STRING OPTIONS(description="Type of the agency"),
  `agencyBrandingVisible` BOOL OPTIONS(description="Flag indicating if agency branding is visible"),
  `agencyHighlightedAds` BOOL OPTIONS(description="Flag indicating if the agency highlights ads"),
  `agencyImageUrl` STRING OPTIONS(description="URL for the agency's image/logo"),
  `openDays` STRING OPTIONS(description="Information about open house days (might need parsing)"),
  `totalPriceCurrency` STRING OPTIONS(description="Currency of the total price"),
  `totalPriceValue` FLOAT64 OPTIONS(description="Total price of the property"),
  `pricePerSquareMeterCurrency` STRING OPTIONS(description="Currency of the price per square meter"),
  `pricePerSquareMeterValue` FLOAT64 OPTIONS(description="Price per square meter"),
  `areaInSquareMeters` FLOAT64 OPTIONS(description="Total area of the property in square meters"),
  `terrainAreaInSquareMeters` FLOAT64 OPTIONS(description="Area of the terrain/lot in square meters"),
  `roomsNumber` STRING OPTIONS(description="Number of rooms (kept as STRING for flexibility like '5+')"),
  `hidePrice` BOOL OPTIONS(description="Flag indicating if the price is hidden"),
  `floorNumber` STRING OPTIONS(description="Floor number (kept as STRING for flexibility like 'Ground', '10+')"),
  `dateCreated` TIMESTAMP OPTIONS(description="Timestamp when the listing was created or last updated on the source"),
  `dateCreatedFirst` TIMESTAMP OPTIONS(description="Timestamp when the listing was first seen/created on the source"),
  `shortDescription` STRING OPTIONS(description="A brief description of the listing"),
  `totalPossibleImages` INT64 OPTIONS(description="Number of images associated with the listing"),
  `additionalInfo` JSON OPTIONS(description="Flexible field for storing other structured or semi-structured data"),
  `ingestionDate` DATE NOT NULL OPTIONS(description="Date when the record was ingested into the staging table. Part of the intended composite primary key."),

  -- Define the PRIMARY KEY constraint INSIDE the table definition
  PRIMARY KEY (`ingestionDate`, `slug`) NOT ENFORCED
)
-- Partition by ingestionDate for performance and cost optimization on time-based queries/data management
PARTITION BY `ingestionDate`
-- Cluster by slug for improved performance when filtering or joining on slug within a partition
CLUSTER BY `slug`
OPTIONS (
  description="Staging table for raw housing listing data from various sources. The PRIMARY KEY (ingestionDate, slug) constraint is NOT ENFORCED by BigQuery; uniqueness must be handled by the loading process (e.g., using MERGE statements)."
);