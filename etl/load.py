#!/usr/bin/env python3
"""
Movies in the Park - Load
Receives clean DataFrame from transform via main.py.
Creates a normalised schema and populates all tables.
Schema:
    ZipCodes  (zip_code, income)
        └── Parks (park_id, park_name, address, zip_code)
                └── Events (event_id, movie_name, park_id, rating, date, closed_captioning)
This is the only script that writes to postgres.
"""
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# =============================================================================
# TABLE DEFINITIONS — normalised schema from db_setup.py
# =============================================================================
CREATE_ZIPCODES_TABLE = """
    CREATE TABLE IF NOT EXISTS ZipCodes (
        zip_code VARCHAR(10) PRIMARY KEY,
        income   INTEGER
    );
"""

CREATE_PARKS_TABLE = """
    CREATE TABLE IF NOT EXISTS Parks (
        park_id   SERIAL PRIMARY KEY,
        park_name VARCHAR(255) NOT NULL,
        address   VARCHAR(255) NOT NULL,
        zip_code  VARCHAR(10)  NOT NULL,
        FOREIGN KEY (zip_code) REFERENCES ZipCodes(zip_code)
    );
"""

CREATE_EVENTS_TABLE = """
    CREATE TABLE IF NOT EXISTS Events (
        event_id          SERIAL PRIMARY KEY,
        movie_name        VARCHAR(255) NOT NULL,
        park_id           INTEGER      NOT NULL,
        rating            VARCHAR(10)  NOT NULL,
        date              DATE,
        closed_captioning BOOLEAN,
        FOREIGN KEY (park_id) REFERENCES Parks(park_id)
    );
"""

# =============================================================================
# TABLE CREATION
# =============================================================================
def create_tables(conn):
    """
    Create all tables in dependency order.
    ZipCodes must exist before Parks (FK on zip_code).
    Parks must exist before Events (FK on park_id).
    Uses IF NOT EXISTS — safe to re-run.
    """
    logger.info("Creating tables...")
    with conn.cursor() as cur:
        logger.info("  Creating ZipCodes table...")
        cur.execute(CREATE_ZIPCODES_TABLE)

        logger.info("  Creating Parks table...")
        cur.execute(CREATE_PARKS_TABLE)

        logger.info("  Creating Events table...")
        cur.execute(CREATE_EVENTS_TABLE)

    conn.commit()
    logger.info("All tables created successfully")

# =============================================================================
# LOAD — ZIPCODES
# =============================================================================
def load_zipcodes(conn, df: pd.DataFrame):
    """
    Populate ZipCodes from unique zip codes in the clean DataFrame.
    Income is populated from census data if available in the DataFrame,
    otherwise defaults to NULL.
    Skips rows with invalid/missing zip codes.
    """
    logger.info("Loading ZipCodes table...")

    # Extract unique zip codes — filter out 0 and invalid entries
    unique_zips = (
        df[["Zip"]]
        .drop_duplicates()
        .dropna()
    )
    unique_zips = unique_zips[unique_zips["Zip"] != 0]

    # Pull income from DataFrame if the census merge added it, else NULL
    income_col_present = "MedianIncome" in df.columns

    rows = []
    for _, row in unique_zips.iterrows():
        zip_code = str(int(row["Zip"])).zfill(5)   # ensure 5-digit string e.g. "60601"
        income   = int(row["MedianIncome"]) if (
            income_col_present and not pd.isna(row.get("MedianIncome"))
        ) else None
        rows.append((zip_code, income))

    insert_sql = """
        INSERT INTO ZipCodes (zip_code, income)
        VALUES %s
        ON CONFLICT (zip_code) DO NOTHING;
    """
    with conn.cursor() as cur:
        execute_values(cur, insert_sql, rows, page_size=500)
    conn.commit()

    logger.info(f"  {len(rows):,} zip codes loaded into ZipCodes")
    return rows

# =============================================================================
# LOAD — PARKS
# =============================================================================
def load_parks(conn, df: pd.DataFrame) -> dict:
    """
    Populate Parks from unique park names in the clean DataFrame.
    Returns a dict mapping park_name → park_id for use when loading Events.
    """
    logger.info("Loading Parks table...")

    unique_parks = (
        df[["Park Name", "Address", "Zip"]]
        .drop_duplicates(subset=["Park Name"])
        .dropna(subset=["Park Name"])
    )

    rows = []
    for _, row in unique_parks.iterrows():
        park_name = str(row["Park Name"]).strip()
        address   = str(row["Address"]).strip() if pd.notna(row["Address"]) else "Address Unknown"
        zip_code  = str(int(row["Zip"])).zfill(5) if row["Zip"] != 0 else "00000"
        rows.append((park_name, address, zip_code))

    insert_sql = """
        INSERT INTO Parks (park_name, address, zip_code)
        VALUES %s
        ON CONFLICT DO NOTHING
        RETURNING park_id, park_name;
    """

    # execute_values does not support RETURNING — insert row by row to capture park_ids
    park_name_to_id = {}
    with conn.cursor() as cur:
        for park_name, address, zip_code in rows:
            cur.execute(
                """
                INSERT INTO Parks (park_name, address, zip_code)
                VALUES (%s, %s, %s)
                ON CONFLICT DO NOTHING
                RETURNING park_id, park_name;
                """,
                (park_name, address, zip_code)
            )
            result = cur.fetchone()
            if result:
                park_name_to_id[result[1]] = result[0]

        # Fetch any parks that already existed (ON CONFLICT DO NOTHING skips RETURNING)
        cur.execute("SELECT park_id, park_name FROM Parks;")
        for park_id, park_name in cur.fetchall():
            park_name_to_id[park_name] = park_id

    conn.commit()
    logger.info(f"  {len(rows):,} parks processed — {len(park_name_to_id):,} in Parks table")
    return park_name_to_id

# =============================================================================
# LOAD — EVENTS
# =============================================================================
def load_events(conn, df: pd.DataFrame, park_name_to_id: dict):
    """
    Populate Events using park_id foreign keys resolved from park_name_to_id.
    Skips rows where park_id cannot be resolved.
    """
    logger.info(f"Loading Events table from {len(df):,} clean rows...")

    rows    = []
    skipped = 0

    for _, row in df.iterrows():
        park_name = str(row["Park Name"]).strip() if pd.notna(row["Park Name"]) else None
        park_id   = park_name_to_id.get(park_name)

        if not park_id:
            logger.warning(f"  Skipping event — no park_id found for '{park_name}'")
            skipped += 1
            continue

        movie_name = str(row["Movie Name"]).strip()
        rating     = str(row["Rating"]).strip()  if pd.notna(row["Rating"])  else "NR"
        date       = row["Date"] if pd.notna(row["Date"]) else None

        # Normalise closed captioning to boolean
        cc_raw = row["Closed Captioning"]
        if pd.isna(cc_raw):
            closed_captioning = False
        elif str(cc_raw).strip().upper() in ("Y", "YES", "TRUE", "1"):
            closed_captioning = True
        else:
            closed_captioning = False

        rows.append((movie_name, park_id, rating, date, closed_captioning))

    insert_sql = """
        INSERT INTO Events (movie_name, park_id, rating, date, closed_captioning)
        VALUES %s;
    """
    with conn.cursor() as cur:
        execute_values(cur, insert_sql, rows, page_size=500)
    conn.commit()

    logger.info(f"  {len(rows):,} events inserted — {skipped:,} skipped")

# =============================================================================
# VERIFY
# =============================================================================
def verify_load(conn):
    """Log row counts for all tables as a post-load sanity check."""
    logger.info("Verifying loaded tables...")
    tables = ["ZipCodes", "Parks", "Events"]
    with conn.cursor() as cur:
        for table in tables:
            try:
                cur.execute(f"SELECT COUNT(*) FROM {table}")
                count = cur.fetchone()[0]
                logger.info(f"  {table}: {count:,} rows")
            except Exception as e:
                logger.warning(f"  Could not verify {table}: {e}")
                conn.rollback()

# =============================================================================
# PUBLIC INTERFACE — called by main.py
# =============================================================================
def run_load_postgres(conn, clean_df: pd.DataFrame):
    """
    Entry point called by main.py.

    Receives the clean movies DataFrame from run_transform().
    Creates the normalised schema and bulk inserts all data.
    Load order respects foreign key constraints:
        1. ZipCodes  (no dependencies)
        2. Parks     (depends on ZipCodes)
        3. Events    (depends on Parks)

    Args:
        conn:     Active psycopg2 connection from main.py
        clean_df: Clean movies DataFrame from run_transform()
    """
    logger.info("=" * 60)
    logger.info("LOAD — writing clean data to postgres")
    logger.info("=" * 60)

    try:
        create_tables(conn)

        load_zipcodes(conn, clean_df)

        park_name_to_id = load_parks(conn, clean_df)

        load_events(conn, clean_df, park_name_to_id)

        verify_load(conn)

        logger.info("=" * 60)
        logger.info("Load complete — all tables written to postgres")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"Load failed: {e}")
        conn.rollback()
        raise

#!/usr/bin/env python3
"""
Movies in the Park - Load (TEST MODE)
Receives clean DataFrame from transform via main.py.
Writes clean DataFrame to CSV in DATA_DIR instead of postgres.
Swap CMD in main.py to run_load_postgres() when ready for production.
"""
import pandas as pd
import logging
import os

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

DATA_DIR    = os.environ.get("DATA_DIR", "/app/data")
OUTPUT_FILE = os.path.join(DATA_DIR, "cleaned_movies_final.csv")

# =============================================================================
# PUBLIC INTERFACE — called by main.py (TESTING PURPOSES)
# =============================================================================
def run_load_csv(clean_df: pd.DataFrame):
    """
    TEST MODE — writes clean DataFrame to CSV instead of postgres.

    Args:
        clean_df: Clean movies DataFrame from run_transform()
    """
    logger.info("=" * 60)
    logger.info("LOAD (TEST MODE) — writing clean DataFrame to CSV")
    logger.info("=" * 60)

    try:
        os.makedirs(DATA_DIR, exist_ok=True)

        clean_df.to_csv(OUTPUT_FILE, index=False)

        logger.info(f"CSV written — {len(clean_df):,} rows saved to {OUTPUT_FILE}")
        logger.info(f"Columns: {clean_df.columns.tolist()}")
        logger.info(f"Sample:\n{clean_df.head()}")
        logger.info("=" * 60)
        logger.info("LOAD COMPLETE")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"Load failed: {e}")
        raise