from __future__ import annotations

import json
import logging
import os
import re
from dataclasses import dataclass
from time import perf_counter
from pathlib import Path

import yaml
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pymongo import MongoClient
from shapely.geometry import GeometryCollection, MultiPolygon, Polygon, mapping, shape
from shapely.ops import unary_union


@dataclass
class ApiConfig:
    mongodb_uri: str
    database: str
    collection: str
    kadastrs_field: str
    output_path: Path
    duckdb_path: Path
    storage_mode: str
    mapbox_access_token: str
    log_level: str


def load_config(config_path: Path) -> ApiConfig:
    load_dotenv()
    data = {}
    if config_path.exists():
        with config_path.open("r", encoding="utf-8") as handle:
            data = yaml.safe_load(handle) or {}

    base_dir = config_path.parent if config_path else Path.cwd()
    output_raw = os.getenv("OUTPUT_PATH", data.get("output_path", "../local-data/regions"))
    output_path = output_raw if isinstance(output_raw, Path) else Path(output_raw)
    if not output_path.is_absolute():
        output_path = (base_dir / output_path).resolve()
    duckdb_raw = os.getenv("DUCKDB_PATH", data.get("duckdb_path", "../local-data/geodb.duckdb"))
    duckdb_path = duckdb_raw if isinstance(duckdb_raw, Path) else Path(duckdb_raw)
    if not duckdb_path.is_absolute():
        duckdb_path = (base_dir / duckdb_path).resolve()
    storage_mode = os.getenv("STORAGE_MODE", data.get("storage_mode", "local"))

    return ApiConfig(
        mongodb_uri=os.getenv("MONGODB_URI", data.get("mongodb_uri", "mongodb://localhost:27017")),
        database=os.getenv("MONGODB_DATABASE", data.get("database", "open_data")),
        collection=os.getenv("MONGODB_COLLECTION", data.get("collection", "properties")),
        kadastrs_field=os.getenv("KADASTRS_FIELD", data.get("kadastrs_field", "kadastrs")),
        output_path=output_path,
        duckdb_path=duckdb_path,
        storage_mode=str(storage_mode).strip().lower(),
        mapbox_access_token=os.getenv(
            "MAPBOX_ACCESS_TOKEN",
            os.getenv("NEXT_PUBLIC_MAPBOX_ACCESS_TOKEN", data.get("mapbox_access_token", "")),
        ),
        log_level=os.getenv("LOG_LEVEL", data.get("log_level", "INFO")),
    )


def parse_storage_mode(value: str) -> set[str]:
    raw = (value or "").strip().lower()
    if raw in {"", "local"}:
        return {"local"}
    if raw == "both":
        return {"local", "mongo"}
    if raw == "all":
        return {"local", "mongo", "duckdb"}
    parts = re.split(r"[,+]", raw)
    return {part.strip() for part in parts if part.strip()}


def configure_logging(level: str) -> None:
    logging.basicConfig(
        level=level.upper(),
        format="%(asctime)s %(levelname)s %(message)s",
    )


app = FastAPI(title="Open Data GeoDB")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
def startup() -> None:
    config_path = Path(os.getenv("CONFIG_PATH", Path(__file__).resolve().parent / "config.yaml"))
    cfg = load_config(config_path)
    configure_logging(cfg.log_level)
    app.state.config = cfg
    modes = parse_storage_mode(cfg.storage_mode)
    use_mongo = "mongo" in modes
    use_duckdb = "duckdb" in modes

    if use_mongo:
        app.state.client = MongoClient(cfg.mongodb_uri)
        try:
            app.state.client.admin.command("ping")
        except Exception as exc:  # noqa: BLE001 - surface connection failures
            logging.error("MongoDB connection failed: %s", exc)
            raise
        app.state.collection = app.state.client[cfg.database][cfg.collection]
        logging.info("Connected to MongoDB %s/%s", cfg.database, cfg.collection)
    else:
        app.state.client = None
        app.state.collection = None
        logging.info("MongoDB disabled.")

    if use_duckdb:
        import duckdb

        cfg.duckdb_path.parent.mkdir(parents=True, exist_ok=True)
        app.state.duckdb = duckdb.connect(str(cfg.duckdb_path), read_only=False)
        logging.info("Connected to DuckDB %s", cfg.duckdb_path)
    else:
        app.state.duckdb = None
        logging.info("DuckDB disabled.")


@app.on_event("shutdown")
def shutdown() -> None:
    client = getattr(app.state, "client", None)
    if client:
        client.close()
    duckdb_conn = getattr(app.state, "duckdb", None)
    if duckdb_conn:
        duckdb_conn.close()


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.get("/frontend-config")
def frontend_config() -> dict:
    cfg = app.state.config
    return {"mapboxAccessToken": cfg.mapbox_access_token or ""}


def list_regions(output_path: Path) -> list[str]:
    if not output_path.exists():
        return []
    return sorted([p.name for p in output_path.iterdir() if p.is_dir()])


def duckdb_regions(conn) -> list[str]:
    try:
        rows = conn.execute("SELECT DISTINCT region FROM features ORDER BY region").fetchall()
    except Exception:  # noqa: BLE001
        return []
    return [row[0] for row in rows if row and row[0]]


def load_geojson_files(output_path: Path, region: str) -> list[dict]:
    region_dir = output_path / region
    if not region_dir.exists() or not region_dir.is_dir():
        return []
    features: list[dict] = []
    for path in sorted(region_dir.glob("*.geojson")):
        try:
            with path.open("r", encoding="utf-8") as handle:
                data = json.load(handle)
        except Exception as exc:  # noqa: BLE001 - skip unreadable files
            logging.warning("Failed to read %s: %s", path, exc)
            continue
        if data.get("type") == "FeatureCollection":
            features.extend(data.get("features") or [])
        elif data.get("type") == "Feature":
            features.append(data)
    return features


@app.get("/regions")
def regions() -> dict:
    cfg = app.state.config
    duckdb_conn = getattr(app.state, "duckdb", None)
    if duckdb_conn is not None:
        return {"regions": duckdb_regions(duckdb_conn)}
    return {"regions": list_regions(cfg.output_path)}


@app.get("/properties/all")
def get_all_properties(region: str = Query("all")) -> dict:
    cfg = app.state.config
    duckdb_conn = getattr(app.state, "duckdb", None)
    if duckdb_conn is not None:
        if region == "all":
            rows = duckdb_conn.execute(
                "SELECT kadastrs, property_name, region, source_file, properties_json, geometry_json FROM features"
            ).fetchall()
        else:
            rows = duckdb_conn.execute(
                "SELECT kadastrs, property_name, region, source_file, properties_json, geometry_json FROM features WHERE region = ?",
                [region],
            ).fetchall()
        features = []
        for kadastrs, property_name, region_name, source_file, properties_json, geometry_json in rows:
            props = json.loads(properties_json) if properties_json else {}
            props.setdefault("kadastrs", kadastrs)
            if property_name:
                props.setdefault("property_name", property_name)
            if source_file:
                props.setdefault("source_file", source_file)
            features.append(
                {"type": "Feature", "geometry": json.loads(geometry_json), "properties": props}
            )
    else:
        output_path = cfg.output_path
        regions_list = list_regions(output_path)
        if not regions_list:
            raise HTTPException(status_code=404, detail="No local data found.")
        if region != "all" and region not in regions_list:
            raise HTTPException(status_code=404, detail="Region not found.")
        target_regions = regions_list if region == "all" else [region]
        features = []
        for region_name in target_regions:
            features.extend(load_geojson_files(output_path, region_name))

    return {
        "type": "FeatureCollection",
        "properties": {"region": region, "count": len(features)},
        "features": features,
    }


@app.middleware("http")
async def log_requests(request, call_next):
    start = perf_counter()
    response = await call_next(request)
    duration_ms = (perf_counter() - start) * 1000
    logging.info("%s %s -> %s (%.1fms)", request.method, request.url.path, response.status_code, duration_ms)
    return response


def build_kadastrs_query(kadastrs: str) -> dict:
    if kadastrs.isdigit():
        return {"kadastrs": {"$in": [kadastrs, int(kadastrs)]}}
    return {"kadastrs": kadastrs}


def build_outline(features: list[dict]) -> dict | None:
    shapes = []
    for feature in features:
        geometry = feature.get("geometry")
        if not geometry:
            continue
        try:
            shapes.append(shape(geometry))
        except Exception:  # noqa: BLE001 - guard invalid geometries
            continue
    if not shapes:
        return None
    union = unary_union(shapes)
    if isinstance(union, GeometryCollection):
        polys = [g for g in union.geoms if isinstance(g, (Polygon, MultiPolygon))]
        if not polys:
            return None
        union = unary_union(polys)
    if union.is_empty:
        return None
    return mapping(union)


@app.get("/properties")
def get_property(kadastrs: str = Query(..., min_length=1)) -> dict:
    collection = app.state.collection
    query = build_kadastrs_query(kadastrs)
    duckdb_conn = getattr(app.state, "duckdb", None)
    if duckdb_conn is not None:
        rows = duckdb_conn.execute(
            "SELECT kadastrs, property_name, source_file, properties_json, geometry_json FROM features WHERE kadastrs = ?",
            [kadastrs],
        ).fetchall()
        docs = []
        for kadastrs_value, property_name, source_file, properties_json, geometry_json in rows:
            docs.append(
                {
                    "kadastrs": kadastrs_value,
                    "property_name": property_name,
                    "source_file": source_file,
                    "properties": json.loads(properties_json) if properties_json else {},
                    "geometry": json.loads(geometry_json) if geometry_json else None,
                }
            )
    else:
        if collection is None:
            raise HTTPException(status_code=400, detail="No storage backend configured.")
        docs = list(collection.find(query, {"_id": 0}))
    if not docs:
        raise HTTPException(status_code=404, detail="Property not found")

    features = []
    for doc in docs:
        if not doc.get("geometry"):
            continue
        properties = dict(doc.get("properties") or {})
        properties.setdefault("kadastrs", doc.get("kadastrs"))
        if doc.get("property_name"):
            properties.setdefault("property_name", doc.get("property_name"))
        if doc.get("source_file"):
            properties.setdefault("source_file", doc.get("source_file"))
        features.append(
            {
                "type": "Feature",
                "geometry": doc.get("geometry"),
                "properties": properties,
            }
        )

    if len(features) == 1:
        return features[0]

    outline = build_outline(features)
    if outline:
        features.append(
            {
                "type": "Feature",
                "geometry": outline,
                "properties": {"kadastrs": kadastrs, "is_outline": True},
            }
        )

    visible_count = sum(1 for feat in features if not feat.get("properties", {}).get("is_outline"))
    return {
        "type": "FeatureCollection",
        "properties": {"kadastrs": kadastrs, "count": visible_count},
        "features": features,
    }
