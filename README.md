# Project Structure Explanation

This document provides an overview of the project's directory structure and the purpose of each main directory.

## Directories

### events

This directory contains scripts and workflows related to event-driven data processing.

*   **gcs\_daily\_incremental\_load**: This subdirectory contains scripts for loading data from Google Cloud Storage (GCS) to BigQuery in a daily incremental fashion. It includes:

    *   `gcs_api.py`: Python script for interacting with Google Cloud Storage.
    *   `main.py`: Main script for the GCS daily incremental load process.
    *   `requirements.txt`: Lists the Python dependencies for the script.
    *   `staging_ddl.sql`: SQL script for creating staging tables in BigQuery.
    *   `architecture.md`: Architecture documentation
*   **listings\_gcs\_daily\_incremental\_load**: This subdirectory contains scripts for loading listings data from Google Cloud Storage (GCS) to BigQuery in a daily incremental fashion. It includes:

    *   `main.py`: Main script for the listings GCS daily incremental load process.
    *   `requirements.txt`: Lists the Python dependencies for the script.
    *   `staging_imovirtual_listings.sql`: SQL script for creating staging tables for listings data in BigQuery.
    *   `transforms_core.py`: Python script for data transformation logic.
*   **parse\_every\_listing**: This subdirectory contains scripts for parsing every listing. It includes:

    *   `config.py`: Configuration file for the parsing process.
    *   `main.py`: Main script for parsing listings.
    *   `requirements.txt`: Lists the Python dependencies for the script.
    *   `utils.py`: Utility functions for the parsing process.

### util\_scripts

This directory contains utility scripts for various tasks.

*   **CAOP\_data**: This subdirectory contains scripts for uploading CAOP (Custo de Acessibilidade a Oportunidades e Preços) data to Google Cloud Storage. It includes:

    *   `upload_caop_to_gcp.py`: Python script for uploading CAOP data to Google Cloud Storage.
    *   `utils.py`: Utility functions for the CAOP data upload process.
    *   `README.md`: Documentation for CAOP data.
*   **OSM\_data**: This subdirectory contains scripts for uploading OSM (OpenStreetMap) data to Google Cloud Storage. It includes:

    *   `upload_osm_data_to_gcp.py`: Python script for uploading OSM data to Google Cloud Storage.
    *   `utils.py`: Utility functions for the OSM data upload process.
    *   `README.md`: Documentation for OSM data.
*   **regression\_training\_pipeline**: This subdirectory contains scripts for training a regression model. It includes:

    *   `flatten_raw_jsonl.py`: Python script for flattening raw JSONL data.
    *   `train.py`: Python script for training the regression model.
    *   `unstack_and_filter.py`: Python script for unstacking and filtering data.
    *   **preprocessing**: This subdirectory contains scripts for preprocessing data.

        *   `staging_housing.py`: Python script for staging housing data.
        *   `preprocessing.md`: Documentation for preprocessing.
        *   `init.py`: Initialization file.
    *   `data_structure.md`: Documentation for the data structure.

### imovirtual/imovirtual

This directory contains the core logic for the Imovirtual scraping project.

*   `imovirtual_catalog.csv`: CSV file containing the Imovirtual catalog.
*   `imovirtual_main.py`: Main script for the Imovirtual scraping project.
*   **src**: This subdirectory contains the source code for the Imovirtual scraping project.

    *   `__init__.py`: Initialization file.
    *   `fetching.py`: Python script for fetching data.
    *   `get_buildid.py`: Python script for getting the build ID.
    *   `graphql_catalog.py`: Python script for interacting with the GraphQL catalog.
    *   `graphql_main.py`: Python script for interacting with the GraphQL API.
    *   `graphql_main_refactor_plan.md`: Documentation for refactoring the GraphQL main script.
    *   `page.html`: HTML file.
    *   `parse_data.py`: Python script for parsing data.
    *   `persistence.py`: Python script for data persistence.
    *   `processing.py`: Python script for data processing.
    *   `requirements.txt`: Lists the Python dependencies for the script.
    *   `utils.py`: Utility functions.