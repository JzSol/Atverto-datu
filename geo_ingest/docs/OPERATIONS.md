# Quarterly Update Runbook

## 1) Drop data

- Place the quarterly dataset under `geo_ingest/open-data/`.
- Each `nodala####` must include `.shp`, `.shx`, `.dbf`, `.prj`.

## 2) Configure

- Update `config.yaml` or use environment variables / `.env`.
- Set `DROP_COLLECTION=true` if you want a full refresh.
- Set `STORAGE_MODE=local` to write JSON files only.
- Set `STORAGE_MODE=duckdb` (or `local,duckdb`) to build a local DuckDB cache for fast lookups.
- For Atlas, set `MONGODB_URI`, use `STORAGE_MODE=mongo` or `local,mongo`, and ensure your IP is allow-listed.

## 3) Run ingest

```
./run_ingest.sh
```

## 3a) Migrate existing JSON to DuckDB

```
python ingest.py migrate-json-to-duckdb
```

## 4) Verify

- MongoDB collection should have documents with:
  - `kadastrs`
  - `geometry` (GeoJSON, EPSG:4326)
  - `properties`
 - Local mode writes GeoJSON under `local-data/regions/<region>/<nodala>.geojson`.
- DuckDB mode writes `local-data/geodb.duckdb`.

## 5) Start lookup UI (optional)

```
uvicorn api:app --reload
```

- Open `apps/frontend/index.html` in a browser.
- Search by `kadastrs` to view geometry on the map.
- Ensure the Mapbox token is set in `apps/frontend/index.html`.
