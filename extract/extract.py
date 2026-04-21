#!/usr/bin/env python3
"""
Movies in the Park - Data Ingestion Script
Ingests Chicago Park District movie data from 2014-2019 into containerized PostgreSQL
"""
import requests
import psycopg2
from psycopg2.extras import execute_values
import json
from typing import Dict, List, Any
import logging
import os
import time
import pandas as pd
import re

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# =============================================================================
# CONFIGURATION — all values sourced from environment / .env via docker-compose
# =============================================================================
DB_HOST     = os.environ.get("DB_HOST", "postgres")   # matches docker-compose service name
DB_PORT     = os.environ.get("DB_PORT", "5433")
DB_NAME     = os.environ.get("DB_NAME", "movies_db")
DB_USER     = os.environ.get("DB_USER", "postgres")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "postgres")

# API Endpoints — stored as a JSON string in .env
# e.g. API_ENDPOINTS={"2014": "https://...", "2015": "https://..."}
_raw_endpoints = os.environ.get("API_ENDPOINTS", "{}")
try:
    API_ENDPOINTS: Dict[int, str] = {
        int(k): v for k, v in json.loads(_raw_endpoints).items()
    }
except json.JSONDecodeError:
    logger.error("API_ENDPOINTS env var is not valid JSON — defaulting to empty dict")
    API_ENDPOINTS = {}

API_LIMIT = 50000

# =============================================================================
# DATABASE CONNECTION — with retry logic for container startup timing
# =============================================================================
def get_connection(retries: int = 10, delay: int = 5):
    """
    Create and return a PostgreSQL connection.
    Retries several times to handle the case where the postgres container
    is still initializing when the extract container starts.
    """
    for attempt in range(1, retries + 1):
        try:
            logger.info(
                f"Connecting to PostgreSQL at {DB_HOST}:{DB_PORT}/{DB_NAME} "
                f"(attempt {attempt}/{retries})..."
            )
            conn = psycopg2.connect(
                host=DB_HOST,
                port=DB_PORT,
                dbname=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD,
                connect_timeout=10,
            )
            logger.info("Successfully connected to PostgreSQL")
            return conn
        except psycopg2.OperationalError as e:
            logger.warning(f"Connection failed: {e}")
            if attempt < retries:
                logger.info(f"Retrying in {delay}s...")
                time.sleep(delay)
            else:
                logger.error("All connection attempts failed")
                raise

# =============================================================================
# DATA FETCHING
# =============================================================================
def fetch_data_from_api(url: str, year: int) -> List[Dict[str, Any]]:
    """Fetch data from Chicago Open Data Portal SODA API."""
    logger.info(f"Fetching data for year {year} from {url}")
    try:
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
# SCHEMA UTILITIES
# =============================================================================
def sanitize_column_name(name: str) -> str:
    """Sanitize column names for PostgreSQL."""
    sanitized = name.lower().replace(" ", "_").replace("-", "_")
    sanitized = ''.join(c if c.isalnum() or c == '_' else '' for c in sanitized)
    if sanitized and sanitized[0].isdigit():
        sanitized = '_' + sanitized
    return sanitized


def infer_column_type(values: List[Any]) -> str:
    """Infer PostgreSQL column type from sample values."""
    non_null_values = [v for v in values if v is not None]
    if not non_null_values:
        return "TEXT"

    numeric_count = 0
    float_count   = 0

    for val in non_null_values[:100]:
        if isinstance(val, (int, float)):
            numeric_count += 1
            if isinstance(val, float):
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

# =============================================================================
# TABLE OPERATIONS
# =============================================================================
def create_table_from_data(
    conn,
    table_name: str,
    data: List[Dict[str, Any]]
) -> List[str]:
    """Dynamically create a raw staging table from the shape of API data."""
    if not data:
        raise ValueError(f"No data provided for table {table_name}")

    all_columns: set = set()
    for record in data:
        all_columns.update(record.keys())

    columns = {}
    for col in all_columns:
        sanitized   = sanitize_column_name(col)
        values      = [record.get(col) for record in data]
        columns[sanitized] = infer_column_type(values)

    column_defs  = [
        "id SERIAL PRIMARY KEY",
        "ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
    ]
    column_names = sorted(columns.keys())
    for col_name in column_names:
        column_defs.append(f'"{col_name}" {columns[col_name]}')

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
    column_names: List[str],
):
    """Bulk-insert records into a staging table."""
    if not data:
        logger.warning(f"No data to insert into {table_name}")
        return

    logger.info(f"Inserting {len(data)} records into {table_name}")

    rows = []
    for record in data:
        row = []
        for col in column_names:
            original_col = next(
                (k for k in record if sanitize_column_name(k) == col), None
            )
            value = record.get(original_col) if original_col else None
            if isinstance(value, (dict, list)):
                value = json.dumps(value)
            row.append(value)
        rows.append(tuple(row))

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
# PER-YEAR INGESTION
# =============================================================================
def ingest_year_data(conn, year: int, url: str):
    """Fetch, stage, and verify one year of movie data."""
    table_name = f"movies_in_the_park_{year}"
    logger.info("=" * 60)
    logger.info(f"Processing: {table_name}")
    logger.info("=" * 60)

    data = fetch_data_from_api(url, year)
    if not data:
        logger.warning(f"No data returned for year {year}")
        return

    column_names = create_table_from_data(conn, table_name, data)
    insert_data(conn, table_name, data, column_names)

    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cur.fetchone()[0]
        logger.info(f"Verification: {table_name} contains {count} records")

# =============================================================================
# COMBINED VIEW
# =============================================================================
def create_combined_view(conn):
    """Create a UNION ALL view across all per-year staging tables."""
    logger.info("Creating combined view of all years...")
    view_sql = """
        DROP VIEW IF EXISTS movies_in_the_park_all_years CASCADE;
        CREATE VIEW movies_in_the_park_all_years AS
            SELECT 2014 AS year, * FROM movies_in_the_park_2014
            UNION ALL
            SELECT 2015 AS year, * FROM movies_in_the_park_2015
            UNION ALL
            SELECT 2016 AS year, * FROM movies_in_the_park_2016
            UNION ALL
            SELECT 2017 AS year, * FROM movies_in_the_park_2017
            UNION ALL
            SELECT 2018 AS year, * FROM movies_in_the_park_2018
            UNION ALL
            SELECT 2019 AS year, * FROM movies_in_the_park_2019;
    """
    try:
        with conn.cursor() as cur:
            cur.execute(view_sql)
        conn.commit()
        logger.info("Combined view created successfully")
    except Exception as e:
        logger.warning(f"Could not create combined view (schemas may differ): {e}")
        conn.rollback()

# =============================================================================
# CHICAGO ZIP / CENSUS DATA
# =============================================================================
CHICAGO_ZIPS = [
    "60601", "60602", "60603", "60604", "60605", "60606", "60607", "60608", "60609", "60610",
    "60611", "60612", "60613", "60614", "60615", "60616", "60617", "60618", "60619", "60620",
    "60621", "60622", "60623", "60624", "60625", "60626", "60627", "60628", "60629", "60630",
    "60631", "60632", "60633", "60634", "60635", "60636", "60637", "60638", "60639", "60640",
    "60641", "60642", "60643", "60644", "60645", "60646", "60647", "60649", "60651", "60652",
    "60653", "60654", "60655", "60656", "60657", "60659", "60660", "60661", "60707", "60827",
]


def get_chicago_data(zip_list: List[str], release: str = "acs2019_5yr") -> pd.DataFrame:
    """Fetch ACS demographic data for Chicago ZIP codes from the Census API."""
    match = re.fullmatch(r"acs(\d{4})_5yr", release)
    if not match:
        raise ValueError("Release must look like 'acsYYYY_5yr' (e.g. acs2019_5yr).")
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

    all_results = []
    for z in zip_list:
        if z not in CHICAGO_ZIPS:
            logger.warning(f"Skipping {z}: not a recognised Chicago ZIP")
            continue
        try:
            url    = f"https://api.census.gov/data/{year}/acs/acs5"
            params = {
                "get": ",".join(variables),
                "for": f"zip code tabulation area:{z}",
                "in":  "state:17",
            }
            response = requests.get(url, params=params, timeout=20)
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, list) or len(payload) < 2:
                raise ValueError(f"Unexpected API response for {z}: {payload}")

            headers  = payload[0]
            values   = payload[1]
            row_data = dict(zip(headers, values))

            def to_num(key):
                val = row_data.get(key)
                return None if val in (None, "", "null") else float(val)

            races = {
                "White": to_num("B02001_002E") or 0,
                "Black": to_num("B02001_003E") or 0,
                "Asian": to_num("B02001_005E") or 0,
                "Other": to_num("B02001_007E") or 0,
            }
            total_mar = to_num("B12001_001E")
            married   = (to_num("B12001_004E") or 0) + (to_num("B12001_013E") or 0)
            edu_total = to_num("B15003_001E")
            bach_plus = sum(
                (to_num(c) or 0)
                for c in ["B15003_022E", "B15003_023E", "B15003_024E", "B15003_025E"]
            )

            all_results.append({
                "zip_code":    z,
                "release":     release,
                "MedianAge":   to_num("B01002_001E"),
                "MedianIncome":to_num("B19013_001E"),
                "PredRace":    max(races, key=races.get),
                "PctMarried":  round((married / total_mar) * 100, 2) if total_mar else None,
                "BirthRate":   to_num("B13002_002E"),
                "EduRate":     round((bach_plus / edu_total) * 100, 2) if edu_total else None,
            })
            logger.info(f"Census data fetched for ZIP {z}")
        except Exception as e:
            logger.warning(f"Could not process ZIP {z}: {e}")

    return pd.DataFrame(all_results)


def ingest_census_data(conn, df: pd.DataFrame):
    """Write the Census demographics DataFrame into a postgres staging table."""
    if df.empty:
        logger.warning("Census DataFrame is empty — skipping census table creation")
        return

    create_sql = """
        DROP TABLE IF EXISTS chicago_zip_demographics CASCADE;
        CREATE TABLE chicago_zip_demographics (
            id               SERIAL PRIMARY KEY,
            ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            zip_code         TEXT,
            release          TEXT,
            "MedianAge"      NUMERIC,
            "MedianIncome"   NUMERIC,
            "PredRace"       TEXT,
            "PctMarried"     NUMERIC,
            "BirthRate"      NUMERIC,
            "EduRate"        NUMERIC
        );
    """
    with conn.cursor() as cur:
        cur.execute(create_sql)
    conn.commit()
    logger.info("Table chicago_zip_demographics created")

    data_cols = ["zip_code", "release", "MedianAge", "MedianIncome",
                 "PredRace", "PctMarried", "BirthRate", "EduRate"]
    rows = [
        tuple(None if pd.isna(row[c]) else row[c] for c in data_cols)
        for _, row in df.iterrows()
    ]
    insert_sql = """
        INSERT INTO chicago_zip_demographics
            (zip_code, release, "MedianAge", "MedianIncome",
             "PredRace", "PctMarried", "BirthRate", "EduRate")
        VALUES %s
    """
    with conn.cursor() as cur:
        execute_values(cur, insert_sql, rows, page_size=500)
    conn.commit()
    logger.info(f"Inserted {len(rows)} census rows into chicago_zip_demographics")

# =============================================================================
# MAIN
# =============================================================================
def main():
    logger.info("=" * 60)
    logger.info("MOVIES IN THE PARK — DATA INGESTION")
    logger.info("=" * 60)

    conn = None
    try:
        conn = get_connection()

        # --- Chicago census / ZIP demographics ---
        logger.info("Fetching Chicago census data...")
        census_df = get_chicago_data(CHICAGO_ZIPS)
        ingest_census_data(conn, census_df)

        # --- Per-year movie data ---
        if not API_ENDPOINTS:
            logger.error("API_ENDPOINTS is empty — nothing to ingest")
        else:
            for year, url in sorted(API_ENDPOINTS.items()):
                try:
                    ingest_year_data(conn, year, url)
                except Exception as e:
                    logger.error(f"Failed to ingest year {year}: {e}")
                    conn.rollback()

            create_combined_view(conn)

        # --- Summary ---
        logger.info("=" * 60)
        logger.info("INGESTION SUMMARY")
        logger.info("=" * 60)
        with conn.cursor() as cur:
            for year in sorted(API_ENDPOINTS.keys()):
                table = f"movies_in_the_park_{year}"
                try:
                    cur.execute(f"SELECT COUNT(*) FROM {table}")
                    count = cur.fetchone()[0]
                    logger.info(f"  {table}: {count} records")
                except Exception:
                    logger.info(f"  {table}: TABLE NOT FOUND")

        logger.info("=" * 60)
        logger.info("DATA INGESTION COMPLETED SUCCESSFULLY")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"Fatal error during ingestion: {e}")
        raise
    finally:
        if conn:
            conn.close()
            logger.info("Database connection closed")


if __name__ == "__main__":
    main