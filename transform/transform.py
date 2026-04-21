#!/usr/bin/env python3
"""
Movies in the Park - Transform Script
Reads merged_movies.csv from data/, cleans and transforms it,
writes cleaned_merged_movies_final.csv back to data/
"""

import pandas as pd
import numpy as np
import json
import time
import logging
import os
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderServiceError

# =============================================================================
# LOGGING
# =============================================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# =============================================================================
# CONFIGURATION
# =============================================================================
DATA_DIR        = os.environ.get("DATA_DIR", "/app/data")
INPUT_FILE      = os.path.join(DATA_DIR, "merged_movies.csv")
OUTPUT_FILE     = os.path.join(DATA_DIR, "cleaned_merged_movies_final.csv")
GEOCODER_AGENT  = os.environ.get("GEOCODER_AGENT", "movies_in_the_park_transform")
GEOCODER_DELAY  = float(os.environ.get("GEOCODER_DELAY", "1.0"))   # seconds between calls

FINAL_COLUMNS = [
    "Movie Name",
    "Address",
    "Rating",
    "Year",
    "Date",
    "Closed Captioning",
    "Zip",
    "Park Name",
]

# =============================================================================
# STEP 1 — LOAD
# =============================================================================
def load_data(path: str) -> pd.DataFrame:
    """Load raw merged CSV from disk."""
    logger.info(f"Loading data from {path}")
    df = pd.read_csv(path)
    logger.info(f"Loaded {len(df):,} rows, {len(df.columns)} columns")
    return df

# =============================================================================
# STEP 2 — EXTRACT COORDINATES FROM ADDRESS COLUMN
# =============================================================================
def extract_coords(val) -> str | float:
    """
    Parse the raw address column (stored as a dict-like string) and
    return a 'latitude, longitude' string, or NaN on failure.
    """
    try:
        data = json.loads(str(val).replace("'", '"'))
        return f"{data['latitude']}, {data['longitude']}"
    except Exception:
        return np.nan


def add_coords_column(df: pd.DataFrame) -> pd.DataFrame:
    """Add a 'coords' column derived from the raw 'address' column."""
    logger.info("Extracting coordinates from address column...")
    df["coords"] = df["address"].apply(extract_coords)
    missing = df["coords"].isna().sum()
    logger.info(f"Coordinates extracted — {missing:,} rows have no coords")
    return df

# =============================================================================
# STEP 3 — FILL MISSING ZIP CODES VIA REVERSE GEOCODING
# =============================================================================
def build_geolocator() -> Nominatim:
    return Nominatim(user_agent=GEOCODER_AGENT)


def reverse_geocode_zip(geolocator: Nominatim, coords: str, original_zip):
    """
    Attempt a reverse geocode and return the postcode.
    Falls back to original_zip on any error.
    """
    try:
        time.sleep(GEOCODER_DELAY)
        location = geolocator.reverse(coords, timeout=10)
        postcode = location.raw.get("address", {}).get("postcode", original_zip)
        return postcode
    except (GeocoderTimedOut, GeocoderServiceError) as e:
        logger.warning(f"Geocoder error for coords {coords}: {e}")
        return original_zip
    except Exception as e:
        logger.warning(f"Unexpected error for coords {coords}: {e}")
        return original_zip


def fill_missing_zips(df: pd.DataFrame, geolocator: Nominatim) -> pd.DataFrame:
    """Fill NaN zip codes using reverse geocoding on available coords."""
    missing_zip_mask = df["zip"].isna() & df["coords"].notna()
    missing_count = missing_zip_mask.sum()
    logger.info(f"Filling {missing_count:,} missing zip codes via reverse geocoding...")

    for idx in df[missing_zip_mask].index:
        coords = df.at[idx, "coords"]
        df.at[idx, "zip"] = reverse_geocode_zip(
            geolocator, coords, df.at[idx, "zip"]
        )

    still_missing = df["zip"].isna().sum()
    logger.info(f"Zip fill complete — {still_missing:,} zips still missing")
    return df

# =============================================================================
# STEP 4 — BASIC COLUMN CLEANING
# =============================================================================
def clean_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Apply simple, non-geocoding transformations."""
    logger.info("Cleaning columns...")

    # --- Closed Captioning ---
    df["closedcaptioning"] = (
        df["closedcaptioning"]
        .map({"Yes": "Y", "No": "N"})
        .fillna(df["closedcaptioning"])
    )

    # --- Date ---
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    imputed_dates = df["date"].isna().sum()
    df.loc[df["date"].isna(), "date"] = pd.Timestamp("2016-01-01")
    logger.info(f"Imputed {imputed_dates:,} missing dates with 2016-01-01 placeholder")

    # --- Zip — strip trailing '.0' artefacts ---
    df["zip"] = df["zip"].astype(str).str.replace(".0", "", regex=False)

    # --- Convert date to date-only (drop time component) ---
    df["date"] = pd.to_datetime(df["date"]).dt.date

    return df

# =============================================================================
# STEP 5 — RENAME COLUMNS
# =============================================================================
COLUMN_RENAME_MAP = {
    "moviename":       "Movie Name",
    "address":         "Address",
    "rating":          "Rating",
    "date":            "Date",
    "closedcaptioning":"Closed Captioning",
    "zip":             "Zip",
    "park":            "Park Name",
}

def rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Renaming columns...")
    df = df.rename(columns=COLUMN_RENAME_MAP)
    return df

# =============================================================================
# STEP 6 — TYPE COERCION
# =============================================================================
def coerce_types(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Coercing data types...")

    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df["Zip"]  = pd.to_numeric(df["Zip"], errors="coerce").fillna(0).astype(int)

    return df

# =============================================================================
# STEP 7 — BUILD PARK ADDRESS MAP VIA GEOCODING
# =============================================================================
def build_park_address_map(
    df: pd.DataFrame,
    geolocator: Nominatim
) -> dict:
    """
    For every unique park, attempt to resolve a human-readable address using:
      1. Reverse geocode from coords (preferred)
      2. Forward geocode from park name (fallback when coords missing)
    Returns a dict: {park_name: address_string}
    """
    unique_parks = df[["Park Name", "coords"]].drop_duplicates(subset=["Park Name"])
    logger.info(f"Building address map for {len(unique_parks):,} unique parks...")

    park_address_map = {}

    for _, row in unique_parks.iterrows():
        park_name = row["Park Name"]
        coords    = row["coords"]

        # Skip entirely unnamed parks
        if pd.isna(park_name):
            continue

        # --- No coords: forward-geocode by park name ---
        if pd.isna(coords):
            try:
                time.sleep(GEOCODER_DELAY)
                location = geolocator.geocode(park_name, timeout=10)
                if location:
                    park_address_map[park_name] = location.address
                    logger.info(f"Forward geocoded: {park_name}")
                else:
                    logger.warning(f"No result for park name: {park_name}")
            except (GeocoderTimedOut, GeocoderServiceError) as e:
                logger.warning(f"Geocoder error for park '{park_name}': {e}")
            continue

        # --- Coords available: reverse geocode ---
        try:
            time.sleep(GEOCODER_DELAY)
            location = geolocator.reverse(coords, timeout=10)
            park_address_map[park_name] = location.address
            logger.info(f"Reverse geocoded: {park_name}")
        except (GeocoderTimedOut, GeocoderServiceError) as e:
            logger.warning(f"Geocoder error for park '{park_name}' coords '{coords}': {e}")
            park_address_map[park_name] = "Address Not Found"
        except Exception as e:
            logger.warning(f"Unexpected error for park '{park_name}': {e}")
            park_address_map[park_name] = "Address Not Found"

    return park_address_map


def apply_park_addresses(
    df: pd.DataFrame,
    park_address_map: dict
) -> pd.DataFrame:
    """Overwrite Address column using the geocoded park address map."""
    logger.info("Applying geocoded park addresses to Address column...")
    df["Address"] = df["Park Name"].map(park_address_map)
    return df

# =============================================================================
# STEP 8 — REMOVE BLANK MOVIE NAMES
# =============================================================================
def drop_blank_movie_names(df: pd.DataFrame) -> pd.DataFrame:
    initial = len(df)
    df = df.dropna(subset=["Movie Name"])
    df = df[df["Movie Name"].str.strip() != ""]
    removed = initial - len(df)
    logger.info(f"Removed {removed:,} rows with blank Movie Name — {len(df):,} rows remaining")
    return df

# =============================================================================
# STEP 9 — ADD YEAR COLUMN, NULL OUT 2016 PLACEHOLDER DATES
# =============================================================================
def add_year_and_fix_2016(df: pd.DataFrame) -> pd.DataFrame:
    """
    Derive Year from Date.
    Rows imputed with the 2016-01-01 placeholder keep their Year
    but have their Date set to NaT (unknown).
    """
    logger.info("Deriving Year column and clearing 2016 placeholder dates...")
    df["Year"] = pd.to_datetime(df["Date"], errors="coerce").dt.year
    df.loc[df["Year"] == 2016, "Date"] = np.nan

    logger.info("Yearly record counts:")
    for year, count in df["Year"].value_counts().sort_index().items():
        logger.info(f"  {year}: {count:,} records")

    return df

# =============================================================================
# STEP 10 — FILL REMAINING NULLS & SELECT FINAL COLUMNS
# =============================================================================
def finalise(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Filling remaining nulls and selecting final columns...")

    df["Rating"]            = df["Rating"].fillna("NR")
    df["Closed Captioning"] = df["Closed Captioning"].fillna("N")

    df_final = df[FINAL_COLUMNS].copy()
    return df_final

# =============================================================================
# STEP 11 — VALIDATION REPORT
# =============================================================================
def validation_report(df: pd.DataFrame):
    logger.info("=" * 60)
    logger.info("VALIDATION REPORT")
    logger.info("=" * 60)
    logger.info("DATA TYPES:\n" + str(df.dtypes))
    logger.info("MISSING VALUES:\n" + str(df.isna().sum()))
    logger.info("UNIQUE YEARS: " + str(sorted(df["Year"].dropna().unique().tolist())))
    logger.info("SAMPLE (first 5 rows):\n" + str(df.head()))
    logger.info("=" * 60)

# =============================================================================
# SAVE
# =============================================================================
def save_data(df: pd.DataFrame, path: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_csv(path, index=False)
    logger.info(f"Saved {len(df):,} rows to {path}")

# =============================================================================
# MAIN
# =============================================================================
def main():
    logger.info("=" * 60)
    logger.info("MOVIES IN THE PARK - TRANSFORM")
    logger.info("=" * 60)

    geolocator = build_geolocator()

    # Pipeline
    df = load_data(INPUT_FILE)
    df = add_coords_column(df)
    df = fill_missing_zips(df, geolocator)
    df = clean_columns(df)
    df = rename_columns(df)
    df = coerce_types(df)

    park_address_map = build_park_address_map(df, geolocator)
    df = apply_park_addresses(df, park_address_map)

    df = drop_blank_movie_names(df)
    df = add_year_and_fix_2016(df)
    df = finalise(df)

    validation_report(df)
    save_data(df, OUTPUT_FILE)

    logger.info("TRANSFORM COMPLETE")


if __name__ == "__main__":
    main()