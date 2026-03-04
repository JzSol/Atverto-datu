# Atverto Datu

Monorepo for the open-data ingestion pipeline and a simple lookup UI.

## Structure

- `geo_ingest/` — ingestion pipeline + API
- `apps/frontend/` — static Mapbox UI
- `geo_ingest/open-data/` — quarterly data drops (gitignored)
- `local-data/` — local JSON outputs (gitignored)

## Quick start

```
cd geo_ingest
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
uvicorn api:app --reload
```

Serve the UI:

```
cd ..
npm i
npm run dev
```

Then open `http://localhost:5173`.
