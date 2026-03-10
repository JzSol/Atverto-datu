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
- `layers` in `config.yaml` defines each dataset (cadastre, biotopi, etc) with its `path` and display defaults.
- `STORAGE_MODE` controls output: `local`, `mongo`, `duckdb`, or combinations like `local,duckdb`.
- `OUTPUT_PATH` is used for local JSON output (region subfolders).
- `DUCKDB_PATH` controls the local DuckDB file path.
- `MAPBOX_ACCESS_TOKEN` is served to frontend by API (`/frontend-config`) so token is not hardcoded in frontend files.
- For Atlas, set `MONGODB_URI` to your cluster connection string and use `STORAGE_MODE=mongo` or `local,mongo`.

## Run

Dry run (scan only):

```
python ingest.py --dry-run
```

Ingest:

```
python ingest.py ingest
```

Ingest a single layer:

```
python ingest.py ingest --layer biotopi
```

DAP layers (from `dap_dati`):

- `biotopi`
- `aizsargajamie_koki`
- `dabas_pieminekli`
- `invazivas_sugas`
- `iadt`
- `sugas`

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

List available layers:

```
curl "http://localhost:8000/layers"
```

Fetch layer features:

```
curl "http://localhost:8000/layers/biotopi/features"
```

## Frontend

Open the static UI in a browser:

- File: `../apps/frontend/index.html`
- Ensure the API is running at `http://localhost:8000`.
- Mapbox token is read from `MAPBOX_ACCESS_TOKEN` via the API.
 

## Data expectations

The ingest scans for complete Shapefile sets (`.shp`, `.shx`, `.dbf`, `.prj`).
Folders with only metadata (`.prj/.cpg/.shp.xml`) are skipped with warnings.
