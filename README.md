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

 EXTERNAL SOURCES                    CONTAINERS                              STORAGE
 ═══════════════                     ══════════                              ═══════

 ┌─────────────────────┐
 │ Chicago Open Data   │
 │ SODA API            │
 │ (2014–2019 movies)  │
 └──────────┬──────────┘
            │ HTTP GET
            │ JSON
            ▼
 ┌─────────────────────┐            ┌──────────────────────┐
 │ US Census Bureau    │            │                      │             ┌─────────────────────┐
 │ ACS API             │──────────► │   [extract]          │────────────►│   [postgres]        │
 │ (ZIP demographics)  │  HTTP GET  │                      │  psycopg2   │                     │
 └─────────────────────┘  JSON      │  • fetch API data    │  INSERT     │  movies_in_the_     │
                                    │  • infer schema      │             │  park_2014..2019    │
                                    │  • sanitize columns  │             │                     │
                                    │  • bulk insert       │             │  chicago_zip_       │
                                    │                      │             │  demographics       │
                                    └──────────────────────┘             │                     │
                                                                         │  (raw / unclean)    │
                                                                         └─────────────────────┘


                                    ┌──────────────────────┐
                                    │                      │             ┌─────────────────────┐
                                    │   [transform]        │────────────►│   ./data/           │
                                    │                      │  writes     │                     │
                                    │  • extract coords    │  CSV        │  cleaned_merged_    │
                                    │  • fill missing ZIPs │             │  movies_final.csv   │
                                    │  • reverse geocode   │◄────────────│                     │
                                    │  • clean columns     │  reads      │  merged_movies.csv  │
                                    │  • rename + coerce   │  CSV        │  (shared volume)    │
                                    │  • drop blank rows   │             └─────────────────────┘
                                    │  • derive Year col   │
                                    └──────────────────────┘


                                    ┌──────────────────────┐
                                    │                      │◄────────────┐
                                    │   [load]             │  reads      │  ./data/
                                    │                      │  CSV        │  cleaned_merged_
                                    │  • read cleaned CSV  │             │  movies_final.csv
                                    │  • validate schema   │             └─────────────────────
                                    │  • upsert final      │
                                    │    tables            │────────────►┌─────────────────────┐
                                    │                      │  psycopg2   │   [postgres]        │
                                    └──────────────────────┘  INSERT     │                     │
                                                                         │  movies_clean       │
                                                                         │                     │
                                                                         │  chicago_zip_       │
                                                                         │  demographics       │
                                                                         │                     │
                                                                         │  (clean / final)    │
                                                                         └──────────┬──────────┘
                                                                                    │
                                                                                    │ psycopg2
                                                                                    │ SELECT
                                                                                    ▼
                                    ┌──────────────────────┐
                                    │                      │
                                    │   [streamlit]        │
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
                                    │      BROWSER         │
                                    │   localhost:8501     │
                                    └──────────────────────┘


╔══════════════════════════════════════════════════════════════════════════════════════════════╗
║  STARTUP ORDER          docker-compose up --build                                            ║
║                                                                                              ║
║  [postgres] ──healthy──► [extract] ──done──► [transform] ──done──► [load] ──done──►         ║
║   empty db               raw tables          cleaned CSV            final tables             ║
║                                                                                  │           ║
║                                                                              [streamlit]     ║
║                                                                              dashboard up    ║
╚══════════════════════════════════════════════════════════════════════════════════════════════╝


╔══════════════════════════════════════════════════════════════════════════════════════════════╗
║  SHARED VOLUME MAP                                                                           ║
║                                                                                              ║
║   host: ./data  ◄──────────────────────────────────────────────────────────────────────     ║
║                          │ extract        │ transform      │ load                           ║
║                          │ writes raw     │ reads + writes │ reads cleaned                  ║
║                          │ CSVs           │ cleaned CSVs   │ CSVs                           ║
║                  container: /app/data ◄──────────────────────────────────────────────────   ║
╚══════════════════════════════════════════════════════════════════════════════════════════════╝
```

## Legend

| Symbol | Meaning |
|--------|---------|
| `──►` | Data flow direction |
| `[service]` | Docker container |
| `(raw / unclean)` | Staging data, not yet transformed |
| `(clean / final)` | Production-ready data for the dashboard |
| `./data/` | Shared host volume mounted into each container at `/app/data` |
| `service_healthy` | Docker healthcheck gate — postgres must be ready before extract connects |
| `service_completed_successfully` | Each ETL stage must fully finish before the next one starts |

## Quickstart

```bash
docker-compose up --build
```

Then open your browser at `http://localhost:8501`