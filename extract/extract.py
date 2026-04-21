#!/usr/bin/env python3
"""
Movies in the Park - Data Ingestion Script
Ingests Chicago Park District movie data from 2014-2019 into Neon PostgreSQL
"""

import requests
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values
import json
from typing import Dict, List, Any
import logging
import os
from dotenv import load_dotenv
import requests
import pandas as pd
import re

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# =============================================================================
# CONFIGURATION
# =============================================================================

# Neon PostgreSQL Connection String
CONNECTION_STRING = os.environ.get("CONNECTION_STRING")

# API Endpoints (converted to SODA API format)
# Format: https://data.cityofchicago.org/resource/{dataset_id}.json
API_ENDPOINTS = os.environ.get("API_ENPOINTS")

# SODA API limit (default is 1000, set higher to get all records)
API_LIMIT = 50000


# =============================================================================
# DATA FETCHING
# =============================================================================

def fetch_data_from_api(url: str, year: int) -> List[Dict[str, Any]]:
    """
    Fetch data from Chicago Open Data Portal API.
    
    Args:
        url: The SODA API endpoint URL
        year: The year of the dataset (for logging)
    
    Returns:
        List of records as dictionaries
    """
    logger.info(f"Fetching data for year {year} from {url}")
    
    try:
        # Add limit parameter to get all records
        params = {"$limit": API_LIMIT}
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        logger.info(f"Successfully fetched {len(data)} records for year {year}")
        return data
    
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch data for year {year}: {e}")
        raise


# =============================================================================
# DATABASE OPERATIONS
# =============================================================================

def get_connection():
    """Create and return a database connection."""
    logger.info("Connecting to Neon PostgreSQL...")
    conn = psycopg2.connect(CONNECTION_STRING)
    logger.info("Successfully connected to database")
    return conn


def sanitize_column_name(name: str) -> str:
    """
    Sanitize column names for PostgreSQL.
    
    Args:
        name: Original column name
    
    Returns:
        Sanitized column name
    """
    # Replace spaces and special characters with underscores
    sanitized = name.lower().replace(" ", "_").replace("-", "_")
    # Remove any remaining special characters
    sanitized = ''.join(c if c.isalnum() or c == '_' else '' for c in sanitized)
    # Ensure it doesn't start with a number
    if sanitized and sanitized[0].isdigit():
        sanitized = '_' + sanitized
    return sanitized


def infer_column_type(values: List[Any]) -> str:
    """
    Infer PostgreSQL column type from sample values.
    
    Args:
        values: List of sample values for the column
    
    Returns:
        PostgreSQL data type string
    """
    # Filter out None values
    non_null_values = [v for v in values if v is not None]
    
    if not non_null_values:
        return "TEXT"
    
    # Check if all values are numeric
    numeric_count = 0
    float_count = 0
    
    for val in non_null_values[:100]:  # Check first 100 values
        if isinstance(val, (int, float)):
            numeric_count += 1
            if isinstance(val, float) or (isinstance(val, str) and '.' in str(val)):
                float_count += 1
        elif isinstance(val, str):
            try:
                float(val)
                numeric_count += 1
                if '.' in val:
                    float_count += 1
            except ValueError:
                pass
    
    sample_size = min(len(non_null_values), 100)
    
    if numeric_count == sample_size:
        return "NUMERIC" if float_count > 0 else "INTEGER"
    
    return "TEXT"


def create_table_from_data(
    conn,
    table_name: str,
    data: List[Dict[str, Any]]
) -> List[str]:
    """
    Create a table based on the structure of the data.
    
    Args:
        conn: Database connection
        table_name: Name of the table to create
        data: List of records to analyze for schema
    
    Returns:
        List of column names in order
    """
    if not data:
        raise ValueError(f"No data provided for table {table_name}")
    
    # Collect all unique columns from all records
    all_columns = set()
    for record in data:
        all_columns.update(record.keys())
    
    # Create column definitions
    columns = {}
    for col in all_columns:
        sanitized_name = sanitize_column_name(col)
        values = [record.get(col) for record in data]
        col_type = infer_column_type(values)
        columns[sanitized_name] = col_type
    
    # Build CREATE TABLE statement
    column_defs = [
        f'id SERIAL PRIMARY KEY',
        f'ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP'
    ]
    
    column_names = sorted(columns.keys())
    for col_name in column_names:
        col_type = columns[col_name]
        column_defs.append(f'"{col_name}" {col_type}')
    
    create_sql = f"""
        DROP TABLE IF EXISTS {table_name} CASCADE;
        CREATE TABLE {table_name} (
            {', '.join(column_defs)}
        );
    """
    
    logger.info(f"Creating table: {table_name}")
    
    with conn.cursor() as cur:
        cur.execute(create_sql)
    conn.commit()
    
    logger.info(f"Table {table_name} created with {len(column_names)} data columns")
    return column_names


def insert_data(
    conn,
    table_name: str,
    data: List[Dict[str, Any]],
    column_names: List[str]
):
    """
    Insert data into the table.
    
    Args:
        conn: Database connection
        table_name: Target table name
        data: List of records to insert
        column_names: Ordered list of column names
    """
    if not data:
        logger.warning(f"No data to insert into {table_name}")
        return
    
    logger.info(f"Inserting {len(data)} records into {table_name}")
    
    # Prepare values for insertion
    rows = []
    for record in data:
        row = []
        for col in column_names:
            # Find the original column name that matches the sanitized version
            original_col = None
            for key in record.keys():
                if sanitize_column_name(key) == col:
                    original_col = key
                    break
            
            value = record.get(original_col) if original_col else None
            
            # Handle nested objects (convert to JSON string)
            if isinstance(value, (dict, list)):
                value = json.dumps(value)
            
            row.append(value)
        rows.append(tuple(row))
    
    # Build INSERT statement
    columns_quoted = [f'"{col}"' for col in column_names]
    insert_sql = f"""
        INSERT INTO {table_name} ({', '.join(columns_quoted)})
        VALUES %s
    """
    
    with conn.cursor() as cur:
        execute_values(cur, insert_sql, rows, page_size=1000)
    
    conn.commit()
    logger.info(f"Successfully inserted {len(rows)} records into {table_name}")


# =============================================================================
# MAIN INGESTION LOGIC
# =============================================================================

def ingest_year_data(conn, year: int, url: str):
    """
    Ingest data for a single year.
    
    Args:
        conn: Database connection
        year: Year of the dataset
        url: API endpoint URL
    """
    table_name = f"movies_in_the_park_{year}"
    
    logger.info(f"{'='*60}")
    logger.info(f"Processing: {table_name}")
    logger.info(f"{'='*60}")
    
    # Fetch data from API
    data = fetch_data_from_api(url, year)
    
    if not data:
        logger.warning(f"No data returned for year {year}")
        return
    
    # Create table
    column_names = create_table_from_data(conn, table_name, data)
    
    # Insert data
    insert_data(conn, table_name, data, column_names)
    
    # Verify insertion
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cur.fetchone()[0]
        logger.info(f"Verification: {table_name} contains {count} records")


def create_combined_view(conn):
    """
    Create a combined view of all years' data.
    
    Args:
        conn: Database connection
    """
    logger.info("Creating combined view of all years...")
    
    view_sql = """
        DROP VIEW IF EXISTS movies_in_the_park_all_years CASCADE;
        
        CREATE VIEW movies_in_the_park_all_years AS
        SELECT 2014 as year, * FROM movies_in_the_park_2014
        UNION ALL
        SELECT 2015 as year, * FROM movies_in_the_park_2015
        UNION ALL
        SELECT 2016 as year, * FROM movies_in_the_park_2016
        UNION ALL
        SELECT 2017 as year, * FROM movies_in_the_park_2017
        UNION ALL
        SELECT 2018 as year, * FROM movies_in_the_park_2018
        UNION ALL
        SELECT 2019 as year, * FROM movies_in_the_park_2019;
    """
    
    try:
        with conn.cursor() as cur:
            cur.execute(view_sql)
        conn.commit()
        logger.info("Combined view created successfully")
    except Exception as e:
        logger.warning(f"Could not create combined view (schemas may differ): {e}")
        conn.rollback()



################################################################################################
# Chicago zip code ingestion
# Zip codes -> zip code table
################################################################################################

# List of official Chicago ZCTAs (ZIP Code Tabulation Areas)
CHICAGO_ZIPS = [
    "60601", "60602", "60603", "60604", "60605", "60606", "60607", "60608", "60609", "60610",
    "60611", "60612", "60613", "60614", "60615", "60616", "60617", "60618", "60619", "60620",
    "60621", "60622", "60623", "60624", "60625", "60626", "60627", "60628", "60629", "60630", 
    "60631", "60632", "60633", "60634", "60635", "60636", "60637", "60638", "60639", "60640",
    "60641", "60642", "60643", "60644", "60645", "60646", "60647", "60649", "60651", "60652", 
    "60653", "60654", "60655", "60656", "60657", "60659", "60660", "60661", "60707", "60827"
]

def get_chicago_data(zip_list, release="acs2019_5yr"):
    all_results = []

    match = re.fullmatch(r"acs(\d{4})_5yr", release)
    if not match:
        raise ValueError(
            "Release must look like 'acsYYYY_5yr' (example: acs2019_5yr for 2015-2019 data)."
        )

    year = match.group(1)
    variables = [
        "B01002_001E",  # median age
        "B19013_001E",  # median household income
        "B02001_002E",  # white alone
        "B02001_003E",  # black alone
        "B02001_005E",  # asian alone
        "B02001_007E",  # some other race alone
        "B12001_001E",  # total marital-status universe
        "B12001_004E",  # male now married
        "B12001_013E",  # female now married
        "B13002_002E",  # women who had a birth in last 12 months
        "B15003_001E",  # education total
        "B15003_022E",  # bachelor's
        "B15003_023E",  # master's
        "B15003_024E",  # professional school
        "B15003_025E",  # doctorate
    ]
    
    for z in zip_list:
        if z not in CHICAGO_ZIPS:
            print(f"Skipping {z}: Not a recognized Chicago city ZIP.")
            continue
        
        try:
            url = f"https://api.census.gov/data/{year}/acs/acs5"
            params = {
                "get": ",".join(variables),
                "for": f"zip code tabulation area:{z}",
                "in": "state:17",
            }

            response = requests.get(url, params=params, timeout=20)
            response.raise_for_status()
            payload = response.json()

            if not isinstance(payload, list) or len(payload) < 2:
                raise ValueError(f"Unexpected API response for {z}: {payload}")

            headers = payload[0]
            values = payload[1]
            row_data = dict(zip(headers, values))

            def to_num(key):
                value = row_data.get(key)
                if value in (None, "", "null"):
                    return None
                return float(value)

            row = {'zip_code': z, 'release': release}
            
            # Median Age
            row['MedianAge'] = to_num('B01002_001E')
            
            # Median HH Income
            row['MedianIncome'] = to_num('B19013_001E')
            
            # Predominant Race (Logic: Find highest count)
            races = {
                'White': to_num('B02001_002E') or 0,
                'Black': to_num('B02001_003E') or 0,
                'Asian': to_num('B02001_005E') or 0,
                'Other': to_num('B02001_007E') or 0,
            }
            # Simplistic predominant race check
            row['PredRace'] = max(races, key=races.get)
            
            # Marital Status (% Currently Married)
            total_mar = to_num('B12001_001E')
            married = (to_num('B12001_004E') or 0) + (to_num('B12001_013E') or 0)
            row['PctMarried'] = round((married / total_mar) * 100, 2) if total_mar else None
            
            # Fertility (Births in last 12 months)
            row['BirthRate'] = to_num('B13002_002E')
            
            # Education (% Bachelor's or Higher)
            edu_total = to_num('B15003_001E')
            bach_plus = sum((to_num(code) or 0) for code in ['B15003_022E', 'B15003_023E', 'B15003_024E', 'B15003_025E'])
            row['EduRate'] = round((bach_plus / edu_total) * 100, 2) if edu_total else None
            
            all_results.append(row)
            print(f"Successfully processed {z}")
            
        except Exception as e:
            print(f"Could not process {z}: {e}")
            
    return pd.DataFrame(all_results)


#################################################################################################
# MAIN Function
#################################################################################################

def main():
    """Main entry point for the data ingestion script."""
    logger.info("="*60)
    logger.info("MOVIES IN THE PARK - DATA INGESTION")
    logger.info("="*60)
    
    conn = None
    
    try: #zip code logic (runs independently of main ingestion script)
        df = get_chicago_data(CHICAGO_ZIPS)
    except Exception as e:
        logger.error(f"Failed to retrieve Chicago data: {e}")
        return

    try:
        # Establish database connection
        conn = get_connection()
        
        # Process each year
        for year, url in sorted(API_ENDPOINTS.items()):
            try:
                ingest_year_data(conn, year, url)
            except Exception as e:
                logger.error(f"Failed to ingest data for year {year}: {e}")
                conn.rollback()
                continue
        
        # Create summary table
        logger.info("\n" + "="*60)
        logger.info("INGESTION SUMMARY")
        logger.info("="*60)
        
        with conn.cursor() as cur:
            for year in sorted(API_ENDPOINTS.keys()):
                table_name = f"movies_in_the_park_{year}"
                try:
                    cur.execute(f"SELECT COUNT(*) FROM {table_name}")
                    count = cur.fetchone()[0]
                    logger.info(f"  {table_name}: {count} records")
                except Exception:
                    logger.info(f"  {table_name}: TABLE NOT FOUND")
        
        logger.info("="*60)
        logger.info("DATA INGESTION COMPLETED SUCCESSFULLY")
        logger.info("="*60)
        
    except Exception as e:
        logger.error(f"Fatal error during ingestion: {e}")
        raise
    
    finally:
        if conn:
            conn.close()
            logger.info("Database connection closed")


if __name__ == "__main__":
    main()