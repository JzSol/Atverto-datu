from __future__ import annotations

import json
import logging
import os
import re
from dataclasses import dataclass
from pathlib import Path
from time import perf_counter
from typing import Iterable, List, Tuple

import geopandas as gpd
import numpy as np
import pandas as pd
import typer
import yaml
from dotenv import load_dotenv
from pymongo import MongoClient, UpdateOne
from pymongo.errors import OperationFailure
from shapely.geometry import GeometryCollection, MultiPolygon, Polygon, mapping
from shapely.ops import unary_union
from shapely.validation import make_valid

app = typer.Typer(add_completion=False)


@dataclass
class Config:
    open_data_path: Path
    output_path: Path
    duckdb_path: Path
    storage_mode: str
    mongodb_uri: str
    database: str
    collection: str
    kadastrs_field: str
    batch_size: int
    drop_collection: bool
    log_level: str


def parse_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def parse_int(value: object, default: int) -> int:
    if value is None:
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def resolve_path(base_dir: Path, raw_path: str) -> Path:
    path = Path(raw_path)
    return path if path.is_absolute() else (base_dir / path).resolve()


def load_config(config_path: Path) -> Config:
    load_dotenv()
    data = {}
    if config_path.exists():
        with config_path.open("r", encoding="utf-8") as handle:
            data = yaml.safe_load(handle) or {}

    base_dir = config_path.parent if config_path else Path.cwd()
    open_data_raw = os.getenv("OPEN_DATA_PATH", data.get("open_data_path", "../open-data"))
    open_data_path = resolve_path(base_dir, open_data_raw)

    output_raw = os.getenv("OUTPUT_PATH", data.get("output_path", "../local-data/regions"))
    output_path = resolve_path(base_dir, output_raw)
    duckdb_raw = os.getenv("DUCKDB_PATH", data.get("duckdb_path", "../local-data/geodb.duckdb"))
    duckdb_path = resolve_path(base_dir, duckdb_raw)
    storage_mode = os.getenv("STORAGE_MODE", data.get("storage_mode", "local"))
    storage_mode = str(storage_mode).strip().lower()

    return Config(
        open_data_path=open_data_path,
        output_path=output_path,
        duckdb_path=duckdb_path,
        storage_mode=storage_mode,
        mongodb_uri=os.getenv("MONGODB_URI", data.get("mongodb_uri", "mongodb://localhost:27017")),
        database=os.getenv("MONGODB_DATABASE", data.get("database", "open_data")),
        collection=os.getenv("MONGODB_COLLECTION", data.get("collection", "properties")),
        kadastrs_field=os.getenv("KADASTRS_FIELD", data.get("kadastrs_field", "kadastrs")),
        batch_size=parse_int(os.getenv("BATCH_SIZE", data.get("batch_size", 500)), 500),
        drop_collection=parse_bool(os.getenv("DROP_COLLECTION", data.get("drop_collection", False))),
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


def find_shapefiles(open_data_path: Path) -> Tuple[List[Path], List[Path]]:
    shapefiles: List[Path] = []
    incomplete: List[Path] = []
    for path in open_data_path.rglob("*"):
        if not path.is_file() or path.suffix.lower() != ".shp":
            continue
        required = [
            path.with_suffix(".shx"),
            path.with_suffix(".dbf"),
            path.with_suffix(".prj"),
        ]
        if all(req.exists() for req in required):
            shapefiles.append(path)
        else:
            incomplete.append(path)
    return shapefiles, incomplete


def normalize_value(value: object) -> object:
    if value is None or pd.isna(value):
        return None
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        return float(value)
    if isinstance(value, (np.bool_,)):
        return bool(value)
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()
    return value


def normalize_kadastrs(value: object) -> str | None:
    if value is None or pd.isna(value):
        return None
    if isinstance(value, (np.integer,)):
        return str(int(value))
    if isinstance(value, (np.floating,)):
        as_int = int(value)
        if float(as_int) == float(value):
            return str(as_int)
        return str(value)
    text = str(value).strip()
    return text or None


def extract_property_name(properties: dict) -> str | None:
    for key in ("property_name", "name", "nosaukums", "adm2", "adm1"):
        value = properties.get(key)
        if value not in (None, ""):
            return str(value)
    return None


def fix_geometry(geometry, context: str) -> Polygon | MultiPolygon | None:
    if geometry is None or geometry.is_empty:
        return None
    if not geometry.is_valid:
        try:
            geometry = make_valid(geometry)
        except Exception:  # noqa: BLE001 - fallback for invalid geometries
            geometry = geometry.buffer(0)

    if isinstance(geometry, GeometryCollection):
        polys = [g for g in geometry.geoms if isinstance(g, (Polygon, MultiPolygon))]
        if not polys:
            logging.warning("Skipping geometry collection without polygons: %s", context)
            return None
        geometry = unary_union(polys)

    if not isinstance(geometry, (Polygon, MultiPolygon)):
        logging.warning("Skipping non-polygon geometry: %s (%s)", context, geometry.geom_type)
        return None

    if not geometry.is_valid:
        try:
            geometry = geometry.buffer(0)
        except Exception:  # noqa: BLE001 - fallback for invalid geometries
            logging.warning("Unable to fix invalid geometry: %s", context)
            return None

    if geometry.is_empty:
        return None
    return geometry


def iter_documents(gdf: gpd.GeoDataFrame, kadastrs_field: str) -> Iterable[tuple[int, dict]]:
    for row_index, row in gdf.iterrows():
        props = row.drop(labels="geometry").to_dict()
        normalized = {key: normalize_value(val) for key, val in props.items()}
        kadastrs_value = normalize_kadastrs(normalized.get(kadastrs_field))
        if not kadastrs_value:
            logging.warning("Skipping feature without kadastrs value.")
            continue
        normalized[kadastrs_field] = kadastrs_value
        property_name = extract_property_name(normalized)
        if property_name:
            normalized.setdefault("property_name", property_name)
        geometry = fix_geometry(row.geometry, f"kadastrs={kadastrs_value}")
        if geometry is None:
            continue
        doc = {
            "kadastrs": kadastrs_value,
            "property_name": property_name,
            "properties": normalized,
            "geometry": mapping(geometry),
        }
        yield row_index, doc


def ensure_column(gdf: gpd.GeoDataFrame, column_name: str) -> gpd.GeoDataFrame:
    if column_name in gdf.columns:
        return gdf
    lower_map = {col.lower(): col for col in gdf.columns if isinstance(col, str)}
    if column_name.lower() in lower_map:
        return gdf.rename(columns={lower_map[column_name.lower()]: column_name})
    return gdf


def write_batch(collection, documents: List[dict]) -> int:
    operations = []
    for doc in documents:
        feature_id = doc.get("feature_id")
        if not feature_id:
            logging.warning("Skipping document without feature_id.")
            continue
        operations.append(UpdateOne({"feature_id": feature_id}, {"$set": doc}, upsert=True))
    if not operations:
        return 0
    result = collection.bulk_write(operations, ordered=False)
    return result.inserted_count + result.upserted_count + result.modified_count


def write_geojson_header(handle) -> None:
    handle.write("{\"type\":\"FeatureCollection\",\"features\":[")


def write_geojson_footer(handle) -> None:
    handle.write("]}")


def init_duckdb(db_path: Path):
    import duckdb

    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(db_path))
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS features (
            feature_id TEXT PRIMARY KEY,
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
        conn.execute("CREATE INDEX IF NOT EXISTS features_kadastrs_idx ON features(kadastrs)")
    except Exception:  # noqa: BLE001 - index may already exist
        pass
    return conn


def write_duckdb_batch(conn, rows: list[tuple]) -> int:
    if not rows:
        return 0
    conn.executemany(
        "INSERT OR REPLACE INTO features VALUES (?, ?, ?, ?, ?, ?, ?)",
        rows,
    )
    return len(rows)


def iter_geojson_features(path: Path) -> Iterable[dict]:
    with path.open("r", encoding="utf-8") as handle:
        data = json.load(handle)
    if data.get("type") == "FeatureCollection":
        for feature in data.get("features") or []:
            yield feature
    elif data.get("type") == "Feature":
        yield data


@app.command()
def migrate_json_to_duckdb(
    output_path: Path = typer.Option(None, help="Path to local JSON regions folder."),
    duckdb_path: Path = typer.Option(None, help="Path to DuckDB file."),
    batch_size: int = typer.Option(500, help="Batch size for inserts."),
) -> None:
    config_path = Path(__file__).resolve().parent / "config.yaml"
    cfg = load_config(config_path)
    output_root = output_path or cfg.output_path
    db_path = duckdb_path or cfg.duckdb_path

    if not output_root.exists():
        logging.error("Output path does not exist: %s", output_root)
        raise typer.Exit(code=1)

    configure_logging(cfg.log_level)
    conn = init_duckdb(db_path)

    total = 0
    for region_dir in sorted([p for p in output_root.iterdir() if p.is_dir()]):
        for geojson_file in sorted(region_dir.glob("*.geojson")):
            rows: list[tuple] = []
            for feature in iter_geojson_features(geojson_file):
                props = feature.get("properties") or {}
                kadastrs = normalize_kadastrs(props.get("kadastrs"))
                if not kadastrs:
                    continue
                feature_id = props.get("feature_id") or f"{geojson_file.name}:{kadastrs}"
                rows.append(
                    (
                        feature_id,
                        kadastrs,
                        props.get("property_name"),
                        region_dir.name,
                        props.get("source_file") or geojson_file.name,
                        json.dumps(props, ensure_ascii=True, separators=(",", ":")),
                        json.dumps(feature.get("geometry"), ensure_ascii=True, separators=(",", ":")),
                    )
                )
                if len(rows) >= batch_size:
                    total += write_duckdb_batch(conn, rows)
                    rows = []
            if rows:
                total += write_duckdb_batch(conn, rows)

            logging.info("Migrated %s", geojson_file)

    logging.info("Migration complete. Rows written: %s", total)

def process_shapefile(
    shapefile: Path,
    collection,
    kadastrs_field: str,
    batch_size: int,
    output_root: Path | None,
    use_local: bool,
    use_mongo: bool,
    duckdb_conn,
) -> int:
    logging.info("Reading %s", shapefile)
    read_start = perf_counter()
    gdf = gpd.read_file(shapefile)
    read_elapsed = perf_counter() - read_start
    if gdf.empty:
        logging.info("No features in %s", shapefile)
        return 0
    logging.info("Loaded %s features from %s in %.1fs", len(gdf), shapefile, read_elapsed)

    gdf = ensure_column(gdf, kadastrs_field)
    if gdf.crs:
        try:
            gdf = gdf.to_crs(epsg=4326)
        except Exception as exc:  # noqa: BLE001 - surface reprojection failures
            logging.warning("CRS transform failed for %s: %s", shapefile, exc)
    else:
        logging.warning("Missing CRS for %s; skipping reprojection", shapefile)

    inserted = 0
    processed = 0
    batch: List[dict] = []

    output_handle = None
    wrote_feature = False
    if output_root and use_local:
        region = shapefile.parent.name
        output_dir = output_root / region
        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = output_dir / f"{shapefile.stem}.geojson"
        output_handle = output_path.open("w", encoding="utf-8")
        write_geojson_header(output_handle)
    mongo_written = 0
    duckdb_written = 0
    duckdb_rows: list[tuple] = []

    for row_index, doc in iter_documents(gdf, kadastrs_field):
        doc["source_file"] = shapefile.name
        props = doc.get("properties") or {}
        raw_id = props.get("objectid_1") or props.get("id")
        if raw_id not in (None, ""):
            feature_id = f"{shapefile.name}:{raw_id}"
        else:
            feature_id = f"{shapefile.name}:row-{row_index}"
        doc["feature_id"] = feature_id
        processed += 1

        if output_handle:
            feature_properties = dict(doc.get("properties") or {})
            feature_properties.setdefault("kadastrs", doc.get("kadastrs"))
            if doc.get("property_name"):
                feature_properties.setdefault("property_name", doc.get("property_name"))
            if doc.get("feature_id"):
                feature_properties.setdefault("feature_id", doc.get("feature_id"))
            if doc.get("source_file"):
                feature_properties.setdefault("source_file", doc.get("source_file"))
            feature = {
                "type": "Feature",
                "geometry": doc.get("geometry"),
                "properties": feature_properties,
            }
            if wrote_feature:
                output_handle.write(",")
            output_handle.write(json.dumps(feature, ensure_ascii=True))
            wrote_feature = True

        if duckdb_conn is not None:
            region = shapefile.parent.name
            duckdb_rows.append(
                (
                    doc.get("feature_id"),
                    doc.get("kadastrs"),
                    doc.get("property_name"),
                    region,
                    doc.get("source_file"),
                    json.dumps(doc.get("properties") or {}, ensure_ascii=True, separators=(",", ":")),
                    json.dumps(doc.get("geometry"), ensure_ascii=True, separators=(",", ":")),
                )
            )
            if len(duckdb_rows) >= batch_size:
                duckdb_written += write_duckdb_batch(duckdb_conn, duckdb_rows)
                duckdb_rows = []

        if use_mongo and collection is not None:
            batch.append(doc)
            if len(batch) >= batch_size:
                batch_inserted = write_batch(collection, batch)
                inserted += batch_inserted
                mongo_written += batch_inserted
                logging.info(
                    "Processed %s features (mongo=%s, duckdb=%s) for %s",
                    processed,
                    mongo_written,
                    duckdb_written,
                    shapefile.name,
                )
                batch = []

    if use_mongo and batch:
        batch_inserted = write_batch(collection, batch)
        inserted += batch_inserted
        mongo_written += batch_inserted
        logging.info(
            "Processed %s features (mongo=%s, duckdb=%s) for %s",
            processed,
            mongo_written,
            duckdb_written,
            shapefile.name,
        )

    if duckdb_conn is not None and duckdb_rows:
        duckdb_written += write_duckdb_batch(duckdb_conn, duckdb_rows)

    if output_handle:
        write_geojson_footer(output_handle)
        output_handle.close()

    return inserted + duckdb_written


def prepare_indexes(collection) -> None:
    for index in collection.list_indexes():
        if index.get("key") == {"kadastrs": 1} and index.get("unique"):
            try:
                collection.drop_index(index["name"])
                logging.info("Dropped unique index on kadastrs.")
            except OperationFailure as exc:
                logging.warning("Unable to drop kadastrs index: %s", exc)
    collection.create_index("kadastrs")
    try:
        collection.create_index("feature_id", unique=True)
    except OperationFailure as exc:
        logging.warning("Unique index on feature_id failed: %s", exc)
    collection.create_index([("geometry", "2dsphere")])


@app.command()
def ingest(
    config: Path = typer.Option(None, help="Path to config file."),
    dry_run: bool = typer.Option(False, help="Only scan for shapefiles."),
) -> None:
    config_path = config or (Path(__file__).resolve().parent / "config.yaml")
    cfg = load_config(config_path)
    configure_logging(cfg.log_level)

    if not cfg.open_data_path.exists():
        logging.error("Open data path does not exist: %s", cfg.open_data_path)
        raise typer.Exit(code=1)

    shapefiles, incomplete = find_shapefiles(cfg.open_data_path)
    if incomplete:
        logging.warning("Found %s shapefiles missing sidecar files.", len(incomplete))
        for item in incomplete[:10]:
            logging.warning("Incomplete shapefile: %s", item)
        if len(incomplete) > 10:
            logging.warning("... and %s more.", len(incomplete) - 10)

    logging.info("Valid shapefiles: %s", len(shapefiles))
    if dry_run:
        logging.info("Dry run enabled; exiting.")
        return

    modes = parse_storage_mode(cfg.storage_mode)
    use_local = "local" in modes
    use_mongo = "mongo" in modes
    use_duckdb = "duckdb" in modes

    collection = None
    if use_mongo:
        client = MongoClient(cfg.mongodb_uri)
        collection = client[cfg.database][cfg.collection]
        if cfg.drop_collection:
            logging.warning("Dropping collection %s.%s", cfg.database, cfg.collection)
            collection.drop()
        prepare_indexes(collection)

    output_root = None
    if use_local:
        output_root = cfg.output_path
        output_root.mkdir(parents=True, exist_ok=True)

    duckdb_conn = None
    if use_duckdb:
        duckdb_conn = init_duckdb(cfg.duckdb_path)

    total = 0
    for shapefile in shapefiles:
        total += process_shapefile(
            shapefile=shapefile,
            collection=collection,
            kadastrs_field=cfg.kadastrs_field,
            batch_size=cfg.batch_size,
            output_root=output_root,
            use_local=use_local,
            use_mongo=use_mongo,
            duckdb_conn=duckdb_conn,
        )

    logging.info("Ingested/updated %s records.", total)


if __name__ == "__main__":
    app()
