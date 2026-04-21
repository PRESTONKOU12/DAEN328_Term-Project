#!/usr/bin/env python3
"""
Movies in the Park - Transform
Receives raw merged DataFrame from extract via main.py.
Cleans and shapes the data.
Returns clean DataFrame to main.py — does NOT read from or write to postgres.
"""
import pandas as pd
import numpy as np
import json
import time
import logging
import os
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderServiceError

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

GEOCODER_AGENT = os.environ.get("GEOCODER_AGENT", "movies_in_the_park_transform")
GEOCODER_DELAY = float(os.environ.get("GEOCODER_DELAY", "1.0"))

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

COLUMN_RENAME_MAP = {
    "moviename":        "Movie Name",
    "address":          "Address",
    "rating":           "Rating",
    "date":             "Date",
    "closedcaptioning": "Closed Captioning",
    "zip":              "Zip",
    "park":             "Park Name",
}

# =============================================================================
# TRANSFORM STEPS
# =============================================================================
def build_geolocator() -> Nominatim:
    return Nominatim(user_agent=GEOCODER_AGENT)


def extract_coords(val) -> str | float:
    """Parse raw address column into 'latitude, longitude' string."""
    try:
        data = json.loads(str(val).replace("'", '"'))
        return f"{data['latitude']}, {data['longitude']}"
    except Exception:
        return np.nan


def add_coords_column(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Extracting coordinates from address column...")
    df["coords"] = df["address"].apply(extract_coords)
    logger.info(f"  {df['coords'].isna().sum():,} rows have no coords")
    return df


def reverse_geocode_zip(geolocator, coords: str, original_zip):
    """Reverse geocode a coordinate pair to a postcode."""
    try:
        time.sleep(GEOCODER_DELAY)
        location = geolocator.reverse(coords, timeout=10)
        return location.raw.get("address", {}).get("postcode", original_zip)
    except (GeocoderTimedOut, GeocoderServiceError) as e:
        logger.warning(f"  Geocoder timeout/error for {coords}: {e}")
        return original_zip
    except Exception as e:
        logger.warning(f"  Unexpected geocoder error: {e}")
        return original_zip


def fill_missing_zips(df: pd.DataFrame, geolocator) -> pd.DataFrame:
    mask = df["zip"].isna() & df["coords"].notna()
    logger.info(f"Filling {mask.sum():,} missing zip codes via reverse geocoding...")
    for idx in df[mask].index:
        df.at[idx, "zip"] = reverse_geocode_zip(
            geolocator, df.at[idx, "coords"], df.at[idx, "zip"]
        )
    logger.info(f"  {df['zip'].isna().sum():,} zips still missing after fill")
    return df


def clean_columns(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Cleaning columns...")

    # Closed Captioning
    df["closedcaptioning"] = (
        df["closedcaptioning"]
        .map({"Yes": "Y", "No": "N"})
        .fillna(df["closedcaptioning"])
    )

    # Date — impute missing with 2016-01-01 placeholder
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    imputed    = df["date"].isna().sum()
    df.loc[df["date"].isna(), "date"] = pd.Timestamp("2016-01-01")
    logger.info(f"  Imputed {imputed:,} missing dates with 2016-01-01 placeholder")

    # Zip — strip .0 artefacts from float conversion
    df["zip"]  = df["zip"].astype(str).str.replace(".0", "", regex=False)

    # Date — drop time component
    df["date"] = pd.to_datetime(df["date"]).dt.date

    return df


def coerce_types(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Coercing types...")
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df["Zip"]  = pd.to_numeric(df["Zip"], errors="coerce").fillna(0).astype(int)
    return df


def build_park_address_map(df: pd.DataFrame, geolocator) -> dict:
    unique_parks = df[["Park Name", "coords"]].drop_duplicates(subset=["Park Name"])
    logger.info(f"Geocoding addresses for {len(unique_parks):,} unique parks...")
    park_address_map = {}

    for _, row in unique_parks.iterrows():
        park_name = row["Park Name"]
        coords    = row["coords"]

        if pd.isna(park_name):
            continue

        # No coords — forward geocode by park name
        if pd.isna(coords):
            try:
                time.sleep(GEOCODER_DELAY)
                location = geolocator.geocode(park_name, timeout=10)
                if location:
                    park_address_map[park_name] = location.address
                    logger.info(f"  Forward geocoded: {park_name}")
                else:
                    logger.warning(f"  No geocode result for: {park_name}")
            except (GeocoderTimedOut, GeocoderServiceError) as e:
                logger.warning(f"  Geocoder error for '{park_name}': {e}")
            continue

        # Coords available — reverse geocode
        try:
            time.sleep(GEOCODER_DELAY)
            location = geolocator.reverse(coords, timeout=10)
            park_address_map[park_name] = location.address
            logger.info(f"  Reverse geocoded: {park_name}")
        except (GeocoderTimedOut, GeocoderServiceError) as e:
            logger.warning(f"  Geocoder error for '{park_name}' @ '{coords}': {e}")
            park_address_map[park_name] = "Address Not Found"
        except Exception as e:
            logger.warning(f"  Unexpected error for '{park_name}': {e}")
            park_address_map[park_name] = "Address Not Found"

    return park_address_map


def drop_blank_movie_names(df: pd.DataFrame) -> pd.DataFrame:
    initial = len(df)
    df = df.dropna(subset=["Movie Name"])
    df = df[df["Movie Name"].str.strip() != ""]
    logger.info(f"Dropped {initial - len(df):,} rows with blank Movie Name")
    return df


def add_year_and_fix_2016(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Deriving Year column, clearing 2016 placeholder dates...")
    df["Year"] = pd.to_datetime(df["Date"], errors="coerce").dt.year
    df.loc[df["Year"] == 2016, "Date"] = np.nan
    logger.info("  Yearly record counts:")
    for year, count in df["Year"].value_counts().sort_index().items():
        logger.info(f"    {year}: {count:,} records")
    return df


def validation_report(df: pd.DataFrame):
    logger.info("=" * 60)
    logger.info("TRANSFORM VALIDATION REPORT")
    logger.info("=" * 60)
    logger.info("DATA TYPES:\n"            + str(df.dtypes))
    logger.info("MISSING VALUES:\n"        + str(df.isna().sum()))
    logger.info("UNIQUE YEARS: "           + str(sorted(df["Year"].dropna().unique().tolist())))
    logger.info("SAMPLE (first 5 rows):\n" + str(df.head()))
    logger.info("=" * 60)

# =============================================================================
# PUBLIC INTERFACE — called by main.py
# =============================================================================
def run_transform(raw_df: pd.DataFrame) -> pd.DataFrame:
    """
    Entry point called by main.py.

    Receives raw merged DataFrame from extract.
    Returns clean DataFrame ready for load.

    Args:
        raw_df: Raw merged DataFrame from run_extract()

    Returns:
        clean_df: Fully cleaned and shaped DataFrame
    """
    logger.info("=" * 60)
    logger.info("TRANSFORM — cleaning raw DataFrame")
    logger.info("=" * 60)

    geolocator = build_geolocator()

    df = raw_df.copy()                                  # never mutate the input
    df = add_coords_column(df)
    df = fill_missing_zips(df, geolocator)
    df = clean_columns(df)
    df = df.rename(columns=COLUMN_RENAME_MAP)
    df = coerce_types(df)

    park_address_map = build_park_address_map(df, geolocator)
    df["Address"] = df["Park Name"].map(park_address_map)

    df = drop_blank_movie_names(df)
    df = add_year_and_fix_2016(df)

    # Fill remaining nulls
    df["Rating"]            = df["Rating"].fillna("NR")
    df["Closed Captioning"] = df["Closed Captioning"].fillna("N")

    # Select and order final columns
    clean_df = df[FINAL_COLUMNS].copy()

    validation_report(clean_df)

    logger.info(f"Transform complete — {len(clean_df):,} clean rows ready for load")
    return clean_df