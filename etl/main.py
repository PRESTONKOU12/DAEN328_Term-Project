#!/usr/bin/env python3
"""
Movies in the Park — ETL Orchestrator
Coordinates extract → transform → load as a pure in-memory pipeline.
Postgres is written to exactly once, at the end of the pipeline.
"""
import logging
import time
import os
import psycopg2

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

DB_HOST     = os.environ.get("DB_HOST")
DB_PORT     = os.environ.get("DB_PORT")
DB_NAME     = os.environ.get("DB_NAME")
DB_USER     = os.environ.get("DB_USER")
DB_PASSWORD = os.environ.get("DB_PASSWORD")


def get_connection(retries: int = 10, delay: int = 5):
    """
    Establish a single shared postgres connection for the full pipeline.
    Retries handle the case where the postgres container is still warming up.
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
                logger.error("All connection attempts exhausted")
                raise


def main():
    logger.info("=" * 60)
    logger.info("MOVIES IN THE PARK — ETL PIPELINE")
    logger.info("=" * 60)

    conn = None

    try:
        conn = get_connection()

        # ------------------------------------------------------------------
        # PHASE 1 — EXTRACT
        # Calls Chicago Open Data + Census APIs
        # Returns a single merged raw DataFrame — nothing written to postgres
        # ------------------------------------------------------------------
        logger.info("PHASE 1 — EXTRACT")
        logger.info("-" * 60)
        from extract import run_extract
        raw_df = run_extract()
        logger.info(f"Extract complete — {len(raw_df):,} raw rows returned")

        # ------------------------------------------------------------------
        # PHASE 2 — TRANSFORM
        # Receives raw merged DataFrame from extract
        # Returns a clean DataFrame — nothing written to postgres
        # ------------------------------------------------------------------
        logger.info("PHASE 2 — TRANSFORM")
        logger.info("-" * 60)
        from transform import run_transform
        clean_df = run_transform(raw_df)
        logger.info(f"Transform complete — {len(clean_df):,} clean rows returned")

        # ------------------------------------------------------------------
        # PHASE 3 — LOAD
        # Receives clean DataFrame from transform
        # First and only write to postgres
        # ------------------------------------------------------------------
        logger.info("PHASE 3 — LOAD")
        logger.info("-" * 60)
        from load import run_load_csv
        run_load_csv(clean_df) #TODO: Change into postgres once testing is confirmed to work
        logger.info("Load complete — clean tables written to postgres")

        logger.info("=" * 60)
        logger.info("ETL PIPELINE COMPLETE")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        if conn:
            conn.rollback()
        raise

    finally:
        if conn:
            conn.close()
            logger.info("Database connection closed")


if __name__ == "__main__":
    main()