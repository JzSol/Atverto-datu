# App Startup Guide

Short guide to run the backend API and the frontend UI.

## 1) Backend API (Python + FastAPI)

Create a virtual environment and install dependencies:

```
cd /Users/JanisMac_mini/Atverto-datu/geo_ingest
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
```

Configure MongoDB:

- For Atlas, set `MONGODB_URI` in `geo_ingest/.env`.
- Ensure your IP is allow-listed in Atlas Network Access.
- For local JSON only, set `STORAGE_MODE=local` and `OUTPUT_PATH=../local-data/regions`.
- For fast local queries, set `STORAGE_MODE=duckdb` (or `local,duckdb`) and `DUCKDB_PATH=../local-data/geodb.duckdb`.

## JSON → DuckDB migration

If you already have local JSON and want to populate DuckDB without the raw shapefiles:

```
cd /Users/JanisMac_mini/Atverto-datu/geo_ingest
source .venv/bin/activate
python3 ingest.py migrate-json-to-duckdb
```

Start the API:

```
python3 -m uvicorn api:app --reload
```

Health check:

```
curl http://127.0.0.1:8000/health
```

## 2) Frontend UI (static)

Install the frontend dev server:

```
cd /Users/JanisMac_mini/Atverto-datu
npm install
```

Start the UI:

```
npm run dev
```

Open in browser:

- `http://127.0.0.1:5173`
- On another device: `http://<your-mac-ip>:5173`

## 3) What should happen

- The UI will call the API at `http://<same-host>:8000`.
- Searching by `kadastrs` should return a GeoJSON feature and zoom the map.

## Common issues

- **Failed to fetch**: API not running or blocked. Check `uvicorn` logs.
- **SSL handshake failed**: Atlas IP not allow-listed or network blocked.
- **404 not found**: No data ingested yet.
