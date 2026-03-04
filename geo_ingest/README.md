# Open Data GeoDB

Quarterly ingest pipeline for cadastral polygons into MongoDB with GeoJSON
geometry and a `kadastrs` lookup key.

## Requirements

- Python 3.11+
- MongoDB 5+ (or Atlas)
- GDAL (required by GeoPandas/Fiona)

macOS example:

```
brew install gdal
```

## Setup

```
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
```

## Configuration

- Update `config.yaml` or set environment variables from `.env` / `.env.example`.
- `OPEN_DATA_PATH` should point to the folder with the quarterly drop.
- `STORAGE_MODE` controls output: `local`, `mongo`, `duckdb`, or combinations like `local,duckdb`.
- `OUTPUT_PATH` is used for local JSON output (region subfolders).
- `DUCKDB_PATH` controls the local DuckDB file path.
- For Atlas, set `MONGODB_URI` to your cluster connection string and use `STORAGE_MODE=mongo` or `local,mongo`.

## Run

Dry run (scan only):

```
python ingest.py --dry-run
```

Ingest:

```
python ingest.py
```

## JSON → DuckDB migration

If you already have local GeoJSON files and want to build DuckDB without
re-ingesting shapefiles:

```
python ingest.py migrate-json-to-duckdb
```

## API service

Start the lookup API:

```
uvicorn api:app --reload
```

If Atlas IP allow-listing is enabled, add your current IP in the Atlas Network Access settings.

Example request:

```
curl "http://localhost:8000/properties?kadastrs=1234567890"
```

## Frontend

Open the static UI in a browser:

- File: `../apps/frontend/index.html`
- Ensure the API is running at `http://localhost:8000`.
- Mapbox token is configured in `apps/frontend/index.html`.
 

## Data expectations

The ingest scans for complete Shapefile sets (`.shp`, `.shx`, `.dbf`, `.prj`).
Folders with only metadata (`.prj/.cpg/.shp.xml`) are skipped with warnings.
