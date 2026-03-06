# Project Run Guide

## 1) Backend setup (one time)

```bash
cd /Users/JanisMac_mini/Atverto-datu/geo_ingest
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
```

## 2) Configure environment

Edit `geo_ingest/.env` and set at least:

```env
STORAGE_MODE="local,duckdb"
OUTPUT_PATH="../local-data/regions"
DUCKDB_PATH="../local-data/geodb.duckdb"
MAPBOX_ACCESS_TOKEN="your_mapbox_token"
```

If using MongoDB Atlas, also set:

```env
MONGODB_URI="your_mongodb_uri"
MONGODB_DATABASE="open_data"
MONGODB_COLLECTION="properties"
```

## 3) Prepare data (only when needed)

If you already have JSON in `local-data/regions` and want to rebuild DuckDB:

```bash
cd /Users/JanisMac_mini/Atverto-datu/geo_ingest
source .venv/bin/activate
python3 ingest.py migrate-json-to-duckdb
```

## 4) Run API

```bash
cd /Users/JanisMac_mini/Atverto-datu/geo_ingest
source .venv/bin/activate
uvicorn api:app --reload
```

Check API:

```bash
curl http://127.0.0.1:8000/health
```

## 5) Run frontend

In a new terminal:

```bash
cd /Users/JanisMac_mini/Atverto-datu
npm install
npm run dev
```

Open:

- `http://127.0.0.1:5173`

## 6) Test

1. Enter cadastre number (example: `50720060539`).
2. Click search.
3. Map should zoom to property and show geometry.

## 7) Quick fixes

- `Failed to fetch`: API is not running on `127.0.0.1:8000`.
- `Property not found`: data is not ingested for that cadastre.
- Map token error: `MAPBOX_ACCESS_TOKEN` is missing in `geo_ingest/.env`.
