# Movies in the Chicago Parks 
Authors:
- Diya Dev
- Preston Kouyoumdjian
- Andres Sanchez
- Will Youngblood 


## Overview:
The goal of this project is to develop insights into different movie events within various parks in Chicago.  With the end goal to derive data-driven insights to answer the following questions: Grouping by zip-code, what genres are the most popular, Can we compare events hosted to income of the neighborhood, racial demographics, etc.

The architecture of the pipeline follows a standard ETL (extract, transform, load) system.  Ingesting data from the chicago data portals' "Movies in the park: (year)" for years 2014 to 2019.  To develop a more nuanced analysis, we used multiple datasets across multiple years. 


## Data sourcing
The main api that we used in this project came from the Chicago open data portal.  These datasets contained information about the events that were hosted throughout the years. 

To supliment our analysis 

Chicago data portal: https://data.cityofchicago.org
- Example link from 2014: https://data.cityofchicago.org/d/cyqk-tzjs

Census data: (Andres please put census api link here)
# Movies in the Park — Data Pipeline

## Architecture & Data Flow

```
╔══════════════════════════════════════════════════════════════════════════════════════════════╗
║                    MOVIES IN THE PARK — ETL PIPELINE & DATABASE SCHEMA                       ║
╚══════════════════════════════════════════════════════════════════════════════════════════════╝

 EXTERNAL SOURCES                  [etl container]                        [postgres container]
 ═══════════════                   ═══════════════                        ════════════════════

 ┌──────────────────────┐
 │  Chicago Open Data   │
 │  SODA API            │
 │                      │          ┌─────────────────────┐
 │  2014 → 260 rows     │          │                     │
 │  2015 → 237 rows     ├─────────►│     extract.py      │
 │  2016 → 243 rows     │ HTTP GET │                     │
 │  2017 → 237 rows     │ JSON     │  Per-year column    │
 │  2018 → 221 rows     │          │  normalisation:     │
 │  2019 → 210 rows     │          │  ┌───────────────┐  │
 └──────────────────────┘          │  │ 2014-2016     │  │
                                   │  │ location_1    │  │
                                   │  │ → address     │  │
                                   │  │ startdate     │  │
                                   │  │ → date        │  │
                                   │  │ movierating   │  │
                                   │  │ → rating      │  │
                                   │  ├───────────────┤  │
                                   │  │ 2017-2019     │  │
                                   │  │ title         │  │
                                   │  │ → moviename   │  │
                                   │  │ park_address  │  │
                                   │  │ → address     │  │
                                   │  │ cc            │  │
                                   │  │ → caption     │  │
                                   │  └───────────────┘  │
                                   │                     │
                                   │  UNION ALL →        │
                                   │  movies_df          │
                                   │  1,408 rows         │
 ┌──────────────────────┐          │                     │
 │  US Census Bureau    │          │  census_df          │
 │  ACS 2019 5yr API    ├─────────►│  58 ZIP rows        │
 │                      │ HTTP GET │                     │
 │  60 Chicago ZIPs     │ JSON     │  Derives:           │
 │  (58 successful)     │          │  • PredRace         │
 │                      │          │  • PctMarried       │
 └──────────────────────┘          │  • EduRate          │
                                   │  • BirthRate        │
                                   │  • MedianAge        │
                                   │  • Population       │
                                   │                     │
                                   └──────────┬──────────┘
                                              │
                                              │  movies_df (raw)
                                              │  census_df (raw)
                                              │  in memory
                                              ▼
                                   ┌─────────────────────┐
                                   │                     │
                                   │    transform.py     │
                                   │                     │
                                   │  movies_df only ──► │
                                   │                     │
                                   │  • extract_coords   │
                                   │    (JSON → lat/lng) │
                                   │                     │
                                   │  • fill_missing_    │
                                   │    zips (geocoder)  │
                                   │                     │
                                   │  • clean_columns    │
                                   │    CC: Yes/No → Y/N │
                                   │    date imputation  │
                                   │    zip cleanup      │
                                   │                     │
                                   │  • build_park_      │
                                   │    address_map      │
                                   │    (Nominatim fwd   │
                                   │     + rev geocode)  │
                                   │                     │
                                   │  • drop blank       │
                                   │    movie names      │
                                   │                     │
                                   │  • derive Year col  │
                                   │    null 2016 dates  │
                                   │                     │
                                   │  Returns:           │
                                   │  clean_movies_df    │
                                   │  ~1,400 rows        │
                                   │                     │
                                   └──────────┬──────────┘
                                              │
                                              │  clean_movies_df (in memory)
                                              │  census_df      (in memory)
                                              ▼
                                   ┌─────────────────────┐
                                   │                     │
                                   │      load.py        │
                                   │                     │
                                   │  FK-safe load order:│
                                   │                     │
                                   │  1. create_tables() │
                                   │  2. ensure_unknown_ │
                                   │     zip() → '00000' │
                                   │  3. load_zipcodes() │
                                   │  4. ensure_all_     │
                                   │     movie_zips()    │
                                   │  5. load_parks()    │
                                   │  6. load_events()   │
                                   │  7. verify_load()   │
                                   │                     │
                                   └──────────┬──────────┘
                                              │
                                              │ psycopg2 INSERT
                                              ▼
╔═════════════════════════════════════════════════════════════════════════════════════════════╗
║  DATABASE SCHEMA                                                                             ║
║                                                                                              ║
║  ZipCodes                                                                                    ║
║  ══════════════════════════════════════════════════════════════════════════════              ║
║  zip_code         VARCHAR(10)   PRIMARY KEY                                                  ║
║  income           INTEGER       avg median household income (Census B19013_001E)             ║
║  predominant_race VARCHAR(20)   majority race per ZIP (derived from B02001_00xE)            ║
║  pct_married      NUMERIC(5,2)  % currently married (derived from B12001_004E + 013E)       ║
║  birth_rate       INTEGER       women with birth last 12 months (B13002_002E)               ║
║  edu_rate         NUMERIC(5,2)  % bachelor's or higher (derived from B15003_022-025E)       ║
║  median_age       NUMERIC(6,2)  median age (B01002_001E)                                    ║
║  population       INTEGER       total ZIP population (B01003_001E)                          ║
║            │                                                                                 ║
║            │ FK zip_code                                                                     ║
║            ▼                                                                                 ║
║  Parks                                                                                       ║
║  ══════════════════════════════════════════════════════════════════════════════              ║
║  park_id          SERIAL        PRIMARY KEY                                                  ║
║  park_name        VARCHAR(255)  NOT NULL                                                     ║
║  address          VARCHAR(255)  geocoded via Nominatim                                       ║
║  zip_code         VARCHAR(10)   FK → ZipCodes.zip_code                                      ║
║            │                                                                                 ║
║            │ FK park_id                                                                      ║
║            ▼                                                                                 ║
║  Events                                                                                      ║
║  ══════════════════════════════════════════════════════════════════════════════              ║
║  event_id          SERIAL        PRIMARY KEY                                                 ║
║  movie_name        VARCHAR(255)  NOT NULL                                                    ║
║  park_id           INTEGER       FK → Parks.park_id                                         ║
║  rating            VARCHAR(10)   NOT NULL  (NR if unknown)                                  ║
║  date              DATE          NULL for 2016 (date unavailable in source API)             ║
║  closed_captioning BOOLEAN       Y/Yes → TRUE  |  N/No → FALSE                             ║
╚═════════════════════════════════════════════════════════════════════════════════════════════╝
                                              │
                                              │ psycopg2 SELECT
                                              ▼
                                   ┌─────────────────────┐
                                   │                     │
                                   │    [streamlit]      │
                                   │                     │
                                   │  • query postgres   │
                                   │  • render charts    │
                                   │  • filter/explore   │
                                   │                     │
                                   └──────────┬──────────┘
                                              │
                                              │ HTTP :8501
                                              ▼
                                   ┌─────────────────────┐
                                   │      BROWSER        │
                                   │  localhost:8501     │
                                   └─────────────────────┘


╔══════════════════════════════════════════════════════════════════════════════════════════════╗
║  STARTUP ORDER          docker-compose up --build                                            ║
║                                                                                              ║
║  [postgres] ──healthy──► [etl] ──completed──► [streamlit]                                   ║
║   empty db               extract                dashboard up                                ║
║                          transform              at :8501                                    ║
║                          load                                                               ║
╚══════════════════════════════════════════════════════════════════════════════════════════════╝


╔══════════════════════════════════════════════════════════════════════════════════════════════╗
║  IN-MEMORY DATA FLOW — no CSV written to disk, postgres written to exactly once              ║
║                                                                                              ║
║  extract.py              transform.py              load.py                                  ║
║  ══════════              ════════════              ═══════                                  ║
║  movies_df  ────────►   clean_movies_df ────────► ZipCodes (census_df)                     ║
║  census_df  ─────────────────────────────────────► Parks   (clean_movies_df)               ║
║                                                    Events  (clean_movies_df)               ║
╚══════════════════════════════════════════════════════════════════════════════════════════════╝
```

## Legend

| Symbol | Meaning |
|--------|---------|
| `──►` | Data flow direction |
| `[service]` | Docker container |
| `FK` | Foreign key constraint |
| `PK` | Primary key |
| `in memory` | Python DataFrame — never written to disk |
| `service_healthy` | Postgres healthcheck gate before ETL connects |
| `service_completed_successfully` | ETL must fully finish before streamlit starts |

## Quickstart
1. Before running any commands, first ensure that you copy the .env example file (renaming to .env) and insert your postgres server password. 

2. Once the .env file is configured it is safe to run 
```bash
docker-compose up --build
```
- <b>Remark:</b> This will perform a full ETL cycle which might take a little bit of time (around 3-5 minutes) to fully ingest, clean, and store.

3. Once the ETL pipeline is complete and the logger declares nothing went wrong with the etl pipeline, the streamlit dashboard will start.
- Open your browser at `http://localhost:8501` to view streamlit.
