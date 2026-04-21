# Movies in the chicago parks 
Authors:
- Diya Dev
- Preston Kouyoumdjian
- Andres Sanchez
- Will Youngblood 


## Overview:
The goal of this project is to develop insights into different movie events within various parks in Chicago.  With the end goal to derive data driven insights to answer the following questions: Grouping by zip-code, what genres are the most popular, Can we compare events hosted to income / zip-code, and many more questions.

The architecture of the pipeline follows a standard ETL (extract, transform, load) system.  Ingesting data from the chicago data portals' "Movies in the park: (year)" for years 2014 to 2019.  To develop a more nuanced analysis, we used multiple datasets across multiple years. 


## Pipeline structure
The steps of our pipeline are as follows
- Extract: 
    - Use rest api to extract 6 years of movie data.
    - Store into individual tables
    - Combine into 1 large database (no feature cleaning)
- Transform: 
    - Clean individual features.
- Load:
    - Seperate clean data into db schema (to ensure 3NF) 


## Architecture & Data Flow

```
╔══════════════════════════════════════════════════════════════════════════════════════════════╗
║                         MOVIES IN THE PARK — DATA PIPELINE                                   ║
╚══════════════════════════════════════════════════════════════════════════════════════════════╝

 EXTERNAL SOURCES                    [etl container]                         STORAGE
 ═══════════════                     ═══════════════                         ═══════

 ┌─────────────────────┐
 │ Chicago Open Data   │
 │ SODA API            │
 │ 2014 → 2019         │
 └──────────┬──────────┘
            │ HTTP GET                ┌──────────────────────┐
            │ JSON                   │                      │
            ▼                        │     extract.py       │
 ┌─────────────────────┐             │                      │
 │ US Census Bureau    │────────────►│  • fetch movies API  │
 │ ACS API             │  HTTP GET   │  • fetch census API  │
 │ ZIP demographics    │  JSON       │  • merge into single │
 └─────────────────────┘             │    raw DataFrame     │
                                     │                      │
                                     └──────────┬───────────┘
                                                │
                                                │  raw DataFrame
                                                │  (in memory)
                                                ▼
                                     ┌──────────────────────┐
                                     │                      │
                                     │    transform.py      │
                                     │                      │
                                     │  • extract coords    │
                                     │  • fill missing ZIPs │
                                     │  • reverse geocode   │
                                     │  • clean columns     │
                                     │  • rename + coerce   │
                                     │  • drop blank rows   │
                                     │  • derive Year col   │
                                     │  • fill nulls        │
                                     │                      │
                                     └──────────┬───────────┘
                                                │
                                                │  clean DataFrame
                                                │  (in memory)
                                                ▼
                                     ┌──────────────────────┐
                                     │                      │
                                     │      load.py         │
                                     │                      │
                                     │  • create ZipCodes   │
                                     │  • create Parks      │
                                     │  • create Events     │
                                     │  • bulk insert all   │
                                     │  • verify row counts │
                                     │                      │
                                     └──────────┬───────────┘
                                                │
                                                │  psycopg2
                                                │  INSERT
                                                ▼
                                     ┌──────────────────────────────────────┐
                                     │           [postgres]                 │
                                     │                                      │
                                     │  ZipCodes                            │
                                     │  ├── zip_code  VARCHAR(10)  PK       │
                                     │  └── income    INTEGER               │
                                     │            │                         │
                                     │            │ FK zip_code             │
                                     │            ▼                         │
                                     │  Parks                               │
                                     │  ├── park_id   SERIAL       PK       │
                                     │  ├── park_name VARCHAR(255)          │
                                     │  ├── address   VARCHAR(255)          │
                                     │  └── zip_code  VARCHAR(10)  FK       │
                                     │            │                         │
                                     │            │ FK park_id              │
                                     │            ▼                         │
                                     │  Events                              │
                                     │  ├── event_id          SERIAL  PK    │
                                     │  ├── movie_name        VARCHAR(255)  │
                                     │  ├── park_id           INTEGER FK    │
                                     │  ├── rating            VARCHAR(10)   │
                                     │  ├── date              DATE          │
                                     │  └── closed_captioning BOOLEAN       │
                                     │                                      │
                                     └──────────────────────────────────────┘
                                                │
                                                │  psycopg2
                                                │  SELECT
                                                ▼
                                     ┌──────────────────────┐
                                     │                      │
                                     │     [streamlit]      │
                                     │                      │
                                     │  • query postgres    │
                                     │  • render charts     │
                                     │  • filter / explore  │
                                     │                      │
                                     └──────────┬───────────┘
                                                │
                                                │ HTTP :8501
                                                ▼
                                     ┌──────────────────────┐
                                     │       BROWSER        │
                                     │   localhost:8501     │
                                     └──────────────────────┘


╔══════════════════════════════════════════════════════════════════════════════════════════════╗
║  STARTUP ORDER          docker-compose up --build                                            ║
║                                                                                              ║
║  [postgres] ──healthy──► [etl] ──completed──► [streamlit]                                   ║
║   empty db               extract                dashboard                                   ║
║                          transform              up at :8501                                 ║
║                          load                                                               ║
╚══════════════════════════════════════════════════════════════════════════════════════════════╝


╔══════════════════════════════════════════════════════════════════════════════════════════════╗
║  IN-MEMORY DATA FLOW                                                                         ║
║                                                                                              ║
║  extract.py                 transform.py                load.py                             ║
║  ══════════                 ════════════                ═══════                             ║
║  fetch APIs                 receives raw_df             receives clean_df                   ║
║       │                          │                           │                              ║
║       │                          │  cleans in memory         │  writes to postgres          ║
║       ▼                          ▼                           ▼                              ║
║  raw_df        ────────►   clean_df         ────────►   ZipCodes                           ║
║  (in memory)               (in memory)                  Parks                              ║
║                                                          Events                             ║
║                                                                                              ║
║  No CSVs written to disk.                                                                    ║
║  Postgres is written to exactly once — at the end of the pipeline.                           ║
╚══════════════════════════════════════════════════════════════════════════════════════════════╝


╔══════════════════════════════════════════════════════════════════════════════════════════════╗
║  NORMALISED SCHEMA — LOAD ORDER                                                              ║
║                                                                                              ║
║  1. ZipCodes   ← no foreign key dependencies, loaded first                                  ║
║       │                                                                                      ║
║  2. Parks      ← depends on ZipCodes (zip_code FK)                                          ║
║       │                                                                                      ║
║  3. Events     ← depends on Parks (park_id FK)                                              ║
╚══════════════════════════════════════════════════════════════════════════════════════════════╝
```

## Legend

| Symbol | Meaning |
|--------|---------|
| `──►` | Data flow direction |
| `[service]` | Docker container |
| `FK` | Foreign key constraint |
| `PK` | Primary key |
| `in memory` | Data lives in a Python DataFrame — never written to disk |
| `service_healthy` | Docker healthcheck gate — postgres must be ready before ETL connects |
| `service_completed_successfully` | ETL must fully finish before streamlit starts |

## Quickstart

```bash
docker-compose up --build
```

Then open your browser at `http://localhost:8501`