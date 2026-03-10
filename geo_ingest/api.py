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
class LayerConfig:
    id: str
    label: str
    path: Path
    geometry_type: str
    display: dict


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
    layers: list[LayerConfig]


def build_layers(data: dict, base_dir: Path, open_data_path: Path) -> list[LayerConfig]:
    raw_layers = data.get("layers") or []
    if not raw_layers:
        raw_layers = [
            {
                "id": "cadastre",
                "label": "Cadastre",
                "path": str(open_data_path),
                "geometry_type": "polygon",
                "display": {},
            }
        ]
    layers: list[LayerConfig] = []
    for layer in raw_layers:
        if not isinstance(layer, dict):
            continue
        layer_id = str(layer.get("id", "")).strip()
        if not layer_id:
            continue
        label = str(layer.get("label") or layer_id).strip()
        raw_path = layer.get("path") or str(open_data_path)
        path = Path(raw_path)
        if not path.is_absolute():
            path = (base_dir / path).resolve()
        geometry_type = str(layer.get("geometry_type") or "polygon")
        display = layer.get("display") or {}
        layers.append(
            LayerConfig(
                id=layer_id,
                label=label,
                path=path,
                geometry_type=geometry_type,
                display=display,
            )
        )
    return layers


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

    open_data_raw = os.getenv("OPEN_DATA_PATH", data.get("open_data_path", "../open-data"))
    open_data_path = open_data_raw if isinstance(open_data_raw, Path) else Path(open_data_raw)
    if not open_data_path.is_absolute():
        open_data_path = (base_dir / open_data_path).resolve()
    layers = build_layers(data, base_dir, open_data_path)

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
        layers=layers,
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


def ensure_duckdb_schema(conn) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS features (
            feature_id TEXT PRIMARY KEY,
            layer_id TEXT,
            kadastrs TEXT,
            property_name TEXT,
            region TEXT,
            source_file TEXT,
            properties_json TEXT,
            geometry_json TEXT
        );
        """
    )
    try:
        columns = [row[1] for row in conn.execute("PRAGMA table_info('features')").fetchall()]
        if "layer_id" not in columns:
            conn.execute("ALTER TABLE features ADD COLUMN layer_id TEXT")
            conn.execute(
                "UPDATE features SET layer_id = 'cadastre' WHERE layer_id IS NULL OR layer_id = ''"
            )
    except Exception:  # noqa: BLE001
        pass
    try:
        conn.execute("CREATE INDEX IF NOT EXISTS features_kadastrs_idx ON features(kadastrs)")
        conn.execute("CREATE INDEX IF NOT EXISTS features_layer_idx ON features(layer_id)")
        conn.execute(
            "CREATE INDEX IF NOT EXISTS features_layer_kadastrs_idx ON features(layer_id, kadastrs)"
        )
    except Exception:  # noqa: BLE001
        pass


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
        ensure_duckdb_schema(app.state.duckdb)
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


def get_layer_config(cfg: ApiConfig, layer_id: str) -> LayerConfig | None:
    for layer in cfg.layers:
        if layer.id == layer_id:
            return layer
    return None


def parse_bbox(raw: str) -> tuple[float, float, float, float]:
    parts = [item.strip() for item in raw.split(",")]
    if len(parts) != 4:
        raise ValueError("bbox must be minx,miny,maxx,maxy")
    minx, miny, maxx, maxy = (float(item) for item in parts)
    return minx, miny, maxx, maxy


def geometry_intersects_bbox(geometry: dict, bbox: tuple[float, float, float, float]) -> bool:
    try:
        geom = shape(geometry)
    except Exception:  # noqa: BLE001
        return False
    minx, miny, maxx, maxy = geom.bounds
    return not (maxx < bbox[0] or maxy < bbox[1] or minx > bbox[2] or miny > bbox[3])


@app.get("/layers")
def layers() -> dict:
    cfg = app.state.config
    duckdb_conn = getattr(app.state, "duckdb", None)
    payload = []
    for layer in cfg.layers:
        has_data = None
        if duckdb_conn is not None:
            try:
                row = duckdb_conn.execute(
                    "SELECT 1 FROM features WHERE layer_id = ? LIMIT 1",
                    [layer.id],
                ).fetchone()
                has_data = bool(row)
            except Exception:  # noqa: BLE001
                has_data = None
        payload.append(
            {
                "id": layer.id,
                "label": layer.label,
                "geometry_type": layer.geometry_type,
                "display": layer.display or {},
                "has_data": has_data,
            }
        )
    return {"layers": payload}


@app.get("/layers/{layer_id}/features")
def layer_features(
    layer_id: str,
    limit: int = Query(2000, ge=1, le=50000),
    bbox: str | None = Query(None, description="minx,miny,maxx,maxy"),
) -> dict:
    cfg = app.state.config
    layer = get_layer_config(cfg, layer_id)
    if not layer:
        raise HTTPException(status_code=404, detail="Layer not found")

    duckdb_conn = getattr(app.state, "duckdb", None)
    if duckdb_conn is None:
        raise HTTPException(status_code=400, detail="Layer data requires DuckDB.")

    rows = duckdb_conn.execute(
        "SELECT properties_json, geometry_json FROM features WHERE layer_id = ? LIMIT ?",
        [layer_id, limit],
    ).fetchall()
    features = []
    for properties_json, geometry_json in rows:
        properties = json.loads(properties_json) if properties_json else {}
        geometry = json.loads(geometry_json) if geometry_json else None
        if not geometry:
            continue
        properties.setdefault("layer_id", layer_id)
        features.append({"type": "Feature", "geometry": geometry, "properties": properties})

    if bbox:
        try:
            bbox_tuple = parse_bbox(bbox)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        features = [feat for feat in features if geometry_intersects_bbox(feat.get("geometry"), bbox_tuple)]

    return {
        "type": "FeatureCollection",
        "properties": {"layer_id": layer_id, "count": len(features)},
        "features": features,
    }


def list_regions(output_path: Path) -> list[str]:
    if not output_path.exists():
        return []
    return sorted([p.name for p in output_path.iterdir() if p.is_dir()])


def duckdb_regions(conn, layer_id: str) -> list[str]:
    try:
        rows = conn.execute(
            "SELECT DISTINCT region FROM features WHERE layer_id = ? ORDER BY region",
            [layer_id],
        ).fetchall()
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
        return {"regions": duckdb_regions(duckdb_conn, "cadastre")}
    return {"regions": list_regions(cfg.output_path)}


@app.get("/properties/all")
def get_all_properties(region: str = Query("all")) -> dict:
    cfg = app.state.config
    duckdb_conn = getattr(app.state, "duckdb", None)
    if duckdb_conn is not None:
        if region == "all":
            rows = duckdb_conn.execute(
                "SELECT kadastrs, property_name, region, source_file, properties_json, geometry_json FROM features WHERE layer_id = ?",
                ["cadastre"],
            ).fetchall()
        else:
            rows = duckdb_conn.execute(
                "SELECT kadastrs, property_name, region, source_file, properties_json, geometry_json FROM features WHERE layer_id = ? AND region = ?",
                ["cadastre", region],
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
            "SELECT kadastrs, property_name, source_file, properties_json, geometry_json FROM features WHERE layer_id = ? AND kadastrs = ?",
            ["cadastre", kadastrs],
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
