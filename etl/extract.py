#!/usr/bin/env python3
"""
Movies in the Park - Extract
Fetches raw data from Chicago Open Data + Census APIs.
Normalises column names per year, merges all years into a single raw DataFrame.
Returns raw merged DataFrame to main.py — does NOT write to postgres.
"""
import requests
import json
import logging
import os
import re
import pandas as pd
from typing import Dict, List

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# =============================================================================
# CONFIGURATION
# =============================================================================
_raw_endpoints = os.environ.get("API_ENDPOINTS", "{}")
try:
    API_ENDPOINTS: Dict[int, str] = {
        int(k): v for k, v in json.loads(_raw_endpoints).items()
    }
except json.JSONDecodeError:
    logger.error("API_ENDPOINTS is not valid JSON — check your .env file")
    API_ENDPOINTS = {}

API_LIMIT = 50000

CHICAGO_ZIPS = [
    "60601", "60602", "60603", "60604", "60605", "60606", "60607", "60608", "60609", "60610",
    "60611", "60612", "60613", "60614", "60615", "60616", "60617", "60618", "60619", "60620",
    "60621", "60622", "60623", "60624", "60625", "60626", "60627", "60628", "60629", "60630",
    "60631", "60632", "60633", "60634", "60635", "60636", "60637", "60638", "60639", "60640",
    "60641", "60642", "60643", "60644", "60645", "60646", "60647", "60649", "60651", "60652",
    "60653", "60654", "60655", "60656", "60657", "60659", "60660", "60661", "60707", "60827",
]

# =============================================================================
# PER-YEAR COLUMN RENAME MAPS
# Translates raw API field names into consistent internal names
# Based on merge_movies.py column audit across all 6 years
# =============================================================================
#
#  Internal name   | 2014              | 2015              | 2016              | 2017          | 2018          | 2019
#  ────────────────|───────────────────|───────────────────|───────────────────|───────────────|───────────────|──────────────
#  moviename       | moviename         | moviename         | moviename         | title         | title         | title
#  address         | location_1        | location_1        | location_1        | park_address  | park_address  | park_address
#  rating          | movierating       | movierating       | movierating       | rating        | rating        | rating
#  date            | startdate         | startdate         | (missing)         | date          | date          | date
#  closedcaptioning| movieclosedcaption| movieclosedcaption| movieclosedcaption| cc            | cc            | cc
#  zip             | zipcode           | zipcode           | zipcode           | (missing)     | (missing)     | (missing)
#  park            | location          | parkname          | location          | park          | park          | park
#
YEAR_COLUMN_MAPS: Dict[int, Dict[str, str]] = {
    2014: {
        "moviename":          "moviename",
        "location_1":         "address",
        "movierating":        "rating",
        "startdate":          "date",
        "movieclosedcaption": "closedcaptioning",
        "zipcode":            "zip",
        "location":           "park",
    },
    2015: {
        "moviename":          "moviename",
        "location_1":         "address",
        "movierating":        "rating",
        "startdate":          "date",
        "movieclosedcaption": "closedcaptioning",
        "zipcode":            "zip",
        "parkname":           "park",
    },
    2016: {
        "moviename":          "moviename",
        "location_1":         "address",
        "movierating":        "rating",
        # date missing for 2016 — will be imputed in transform
        "movieclosedcaption": "closedcaptioning",
        "zipcode":            "zip",
        "location":           "park",
    },
    2017: {
        "title":              "moviename",
        "park_address":       "address",
        "rating":             "rating",
        "date":               "date",
        "cc":                 "closedcaptioning",
        # zip missing for 2017 — will be geocoded in transform
        "park":               "park",
    },
    2018: {
        "title":              "moviename",
        "park_address":       "address",
        "rating":             "rating",
        "date":               "date",
        "cc":                 "closedcaptioning",
        # zip missing for 2018 — will be geocoded in transform
        "park":               "park",
    },
    2019: {
        "title":              "moviename",
        "park_address":       "address",
        "rating":             "rating",
        "date":               "date",
        "cc":                 "closedcaptioning",
        # zip missing for 2019 — will be geocoded in transform
        "park":               "park",
    },
}

# These are the only columns we carry forward after normalisation
MERGED_COLUMNS = ["moviename", "address", "rating", "date", "closedcaptioning", "zip", "park", "source_year"]

# =============================================================================
# MOVIES — fetch, normalise, merge
# =============================================================================
def fetch_year(url: str, year: int) -> pd.DataFrame:
    """
    Fetch one year of movie data from the SODA API.
    Returns raw DataFrame with original API column names.
    Returns empty DataFrame on failure so pipeline continues for other years.
    """
    logger.info(f"  Fetching {year} from {url}")
    try:
        response = requests.get(
            url,
            params={"$limit": API_LIMIT},
            timeout=30
        )
        response.raise_for_status()
        df = pd.DataFrame(response.json())
        logger.info(f"  {year}: {len(df):,} rows — columns: {df.columns.tolist()}")
        return df
    except requests.exceptions.RequestException as e:
        logger.error(f"  Failed to fetch {year}: {e}")
        return pd.DataFrame()


def normalise_year(df: pd.DataFrame, year: int) -> pd.DataFrame:
    """
    Normalise a single year's raw DataFrame into consistent internal column names.
    Steps:
      1. Lowercase all column names (handles API inconsistencies)
      2. Apply the per-year rename map
      3. Keep only the known internal columns
      4. Add missing columns as NaN (e.g. date for 2016, zip for 2017-2019)
      5. Tag with source_year
    """
    if df.empty:
        return df

    # Step 1 — lowercase all column names
    df.columns = [col.lower().strip() for col in df.columns]

    # Step 2 — apply year-specific rename map
    rename_map = YEAR_COLUMN_MAPS.get(year, {})
    df = df.rename(columns=rename_map)

    logger.info(f"  {year} columns after normalisation: {df.columns.tolist()}")

    # Step 3 — tag source year
    df["source_year"] = year

    # Step 4 — keep only internal columns, add missing ones as NaN
    internal_cols = ["moviename", "address", "rating", "date",
                     "closedcaptioning", "zip", "park", "source_year"]

    for col in internal_cols:
        if col not in df.columns:
            logger.warning(f"  {year}: column '{col}' not found — filling with NaN")
            df[col] = pd.NA

    df = df[internal_cols]

    return df


def fetch_all_movies() -> pd.DataFrame:
    """
    Fetch all years, normalise column names, concat into one raw merged DataFrame.
    Equivalent to the UNION ALL logic in merge_movies.py but fully in-memory.
    """
    if not API_ENDPOINTS:
        raise RuntimeError("API_ENDPOINTS is empty — check your .env file")

    frames = []
    for year, url in sorted(API_ENDPOINTS.items()):
        raw_df        = fetch_year(url, year)
        normalised_df = normalise_year(raw_df, year)
        if not normalised_df.empty:
            frames.append(normalised_df)
            logger.info(f"  {year}: {len(normalised_df):,} rows normalised and staged for merge")

    if not frames:
        raise RuntimeError("All API fetches failed — no movie data retrieved")

    merged = pd.concat(frames, ignore_index=True)
    logger.info(f"All years merged — {len(merged):,} total rows")
    logger.info(f"Merged columns: {merged.columns.tolist()}")
    return merged

# =============================================================================
# CENSUS — fetch ACS demographics for Chicago ZIPs
# =============================================================================
def fetch_census(zip_list: List[str], release: str = "acs2019_5yr") -> pd.DataFrame:
    """
    Fetch ACS demographic data for Chicago ZIP codes from the Census API.
    Returns a raw DataFrame — no transformation applied.
    """
    match = re.fullmatch(r"acs(\d{4})_5yr", release)
    if not match:
        raise ValueError("Release must look like 'acsYYYY_5yr' e.g. acs2019_5yr")
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
            logger.warning(f"  Skipping {z}: not a recognised Chicago ZIP")
            continue
        try:
            response = requests.get(
                f"https://api.census.gov/data/{year}/acs/acs5",
                params={
                    "get":  ",".join(variables),
                    "for":  f"zip code tabulation area:{z}",
                    "in":   "state:17",
                },
                timeout=20,
            )
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, list) or len(payload) < 2:
                raise ValueError(f"Unexpected response structure for {z}")

            row_data             = dict(zip(payload[0], payload[1]))
            row_data["zip_code"] = z
            row_data["release"]  = release
            all_results.append(row_data)
            logger.info(f"  Census fetched for ZIP {z}")

        except Exception as e:
            logger.warning(f"  Could not fetch census data for {z}: {e}")

    if not all_results:
        logger.warning("No census data retrieved")
        return pd.DataFrame()

    df = pd.DataFrame(all_results)
    logger.info(f"Census fetch complete — {len(df):,} ZIP rows")
    return df


def merge_census(movies_df: pd.DataFrame, census_df: pd.DataFrame) -> pd.DataFrame:
    """
    Left join census demographics onto movie records by zip code.
    Movies without a matching zip code retain all movie columns with NaN for census columns.
    """
    if census_df.empty:
        logger.warning("Census DataFrame is empty — skipping census merge")
        return movies_df

    logger.info("Merging census demographics onto movie records by zip code...")

    # Ensure zip types match for the join
    movies_df["zip"]          = movies_df["zip"].astype(str).str.replace(".0", "", regex=False).str.strip()
    census_df["zip_code"]     = census_df["zip_code"].astype(str).str.strip()

    merged = movies_df.merge(
        census_df,
        left_on="zip",
        right_on="zip_code",
        how="left"             # keep all movie rows even if no census match
    )

    # Drop the redundant zip_code column brought in by the merge
    merged = merged.drop(columns=["zip_code"], errors="ignore")

    matched   = merged["B19013_001E"].notna().sum()
    unmatched = merged["B19013_001E"].isna().sum()
    logger.info(f"Census merge complete — {matched:,} rows matched, {unmatched:,} unmatched")

    return merged

# =============================================================================
# PUBLIC INTERFACE — called by main.py
# =============================================================================
def run_extract() -> pd.DataFrame:
    """
    Entry point called by main.py.

    1. Fetches all 6 years from Chicago SODA API
    2. Normalises column names per year (replaces SQL ALTER TABLE renames)
    3. Merges all years into one DataFrame (replaces SQL UNION ALL)
    4. Fetches Census ACS demographics for Chicago ZIPs
    5. Left joins census data onto movie records by zip code

    Returns:
        raw_df — merged movies + census DataFrame, fully in memory ready to be passed directly to run_transform()
    """
    logger.info("=" * 60)
    logger.info("EXTRACT — fetching and merging API data")
    logger.info("=" * 60)

    movies_df = fetch_all_movies()
    census_df = fetch_census(CHICAGO_ZIPS)
    raw_df    = merge_census(movies_df, census_df)

    logger.info(f"Extract complete — {len(raw_df):,} rows, {len(raw_df.columns)} columns ready for transform")
    logger.info(f"Final columns: {raw_df.columns.tolist()}")

    return raw_df