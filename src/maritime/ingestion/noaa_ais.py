from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Callable
from urllib.parse import quote

import httpx
import polars as pl

from maritime.config import Settings
from maritime.network import PoliteHttpClient
from maritime.storage.parquet import atomic_write, merge_by_key


SOURCE_ID = "noaa_marinecadastre"
DATASET_ID = "historical_ais_routes"
ROUTE_SCHEMA = {
    "source": pl.String,
    "mmsi": pl.String,
    # Stored as timezone-naive UTC, matching the existing live-route table.
    "observed_at": pl.Datetime,
    "vessel_name": pl.String,
    "latitude": pl.Float64,
    "longitude": pl.Float64,
    "speed_knots": pl.Float64,
    "course_degrees": pl.Float64,
}


@dataclass(frozen=True)
class GeographicBounds:
    south: float
    west: float
    north: float
    east: float

    def __post_init__(self):
        values = (self.south, self.west, self.north, self.east)
        if not all(math.isfinite(value) for value in values):
            raise ValueError("bounds must contain finite numbers")
        if not (-90 <= self.south < self.north <= 90):
            raise ValueError("bounds must satisfy -90 <= south < north <= 90")
        if not (-180 <= self.west < self.east <= 180):
            raise ValueError("bounds must satisfy -180 <= west < east <= 180")

    @classmethod
    def parse(cls, value: str) -> "GeographicBounds":
        try:
            south, west, north, east = (float(part.strip()) for part in value.split(","))
        except (TypeError, ValueError):
            raise ValueError("bounds must be south,west,north,east") from None
        return cls(south, west, north, east)

    def as_dict(self) -> dict[str, float]:
        return {
            "south": self.south,
            "west": self.west,
            "north": self.north,
            "east": self.east,
        }


def empty_routes() -> pl.DataFrame:
    return pl.DataFrame(schema=ROUTE_SCHEMA)


def normalize_noaa_routes(frame: pl.DataFrame, sample_minutes: int) -> pl.DataFrame:
    """Normalize an ERDDAP response and retain one observation per time bucket."""
    if frame.is_empty():
        return empty_routes()
    if sample_minutes < 1:
        raise ValueError("sample_minutes must be at least 1")
    aliases = {
        "MMSI": "mmsi",
        "time": "observed_at",
        "Lat": "latitude",
        "Lon": "longitude",
        "SOG": "speed_knots",
        "COG": "course_degrees",
        "VesselName": "vessel_name",
    }
    missing = set(aliases) - set(frame.columns)
    if missing:
        raise ValueError(f"NOAA response is missing columns: {', '.join(sorted(missing))}")
    normalized = frame.rename(aliases).select(
        pl.lit(SOURCE_ID).alias("source"),
        pl.col("mmsi").cast(pl.String).str.strip_chars(),
        pl.col("observed_at")
        .cast(pl.String)
        .str.to_datetime(strict=False, time_zone="UTC")
        .dt.replace_time_zone(None),
        pl.col("vessel_name").cast(pl.String).str.strip_chars().replace("", None),
        pl.col("latitude").cast(pl.Float64, strict=False),
        pl.col("longitude").cast(pl.Float64, strict=False),
        pl.col("speed_knots").cast(pl.Float64, strict=False),
        pl.col("course_degrees").cast(pl.Float64, strict=False),
    ).filter(
        pl.col("mmsi").is_not_null()
        & pl.col("observed_at").is_not_null()
        & pl.col("latitude").is_between(-90, 90)
        & pl.col("longitude").is_between(-180, 180)
    )
    if normalized.is_empty():
        return empty_routes()
    seconds = sample_minutes * 60
    return (
        normalized.sort(["mmsi", "observed_at"])
        .with_columns(
            (pl.col("observed_at").dt.epoch("s") // seconds).alias("_sample_bucket")
        )
        .unique(["mmsi", "_sample_bucket"], keep="first", maintain_order=True)
        .drop("_sample_bucket")
        .sort(["mmsi", "observed_at"])
    )


class NoaaHistoricalAisProvider:
    """Explicit, bounded access to NOAA/MarineCadastre historical AIS via ERDDAP."""

    def __init__(self, settings: Settings, client: PoliteHttpClient):
        self.settings = settings
        self.client = client
        self.cfg = settings.sources["noaa_historical_ais"]

    def query_url(
        self,
        start: datetime,
        end: datetime,
        bounds: GeographicBounds,
    ) -> str:
        dataset = self.cfg["dataset_id_template"].format(year=start.year)
        variables = "MMSI,time,Lat,Lon,SOG,COG,VesselName"
        start_value = start.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        end_value = end.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        query = (
            f"{variables}&time>={start_value}&time<{end_value}"
            f"&Lat>={bounds.south}&Lat<={bounds.north}"
            f"&Lon>={bounds.west}&Lon<={bounds.east}"
        )
        encoded = quote(query, safe=",&=.-_:T")
        return f"{self.cfg['erddap_base_url'].rstrip('/')}/{dataset}.csv?{encoded}"

    async def _chunk(
        self,
        start: datetime,
        end: datetime,
        bounds: GeographicBounds,
        sample_minutes: int,
    ) -> pl.DataFrame:
        cache_dir = self.settings.data_dir / "cache/downloads/noaa_ais"
        stamp = start.strftime("%Y%m%dT%H%M%S")
        target = cache_dir / f"{stamp}.csv"
        maximum_bytes = int(self.cfg["maximum_download_megabytes_per_chunk"]) * 1024 * 1024
        try:
            await self.client.download(
                self.query_url(start, end, bounds),
                target,
                maximum_bytes=maximum_bytes,
                headers={"Accept": "text/csv", "User-Agent": "MaritimeIntelligenceGlobe/0.1"},
            )
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 404:
                return empty_routes()
            raise
        try:
            raw = pl.read_csv(
                target,
                skip_rows_after_header=1,
                infer_schema_length=10_000,
                schema_overrides={"MMSI": pl.String, "VesselName": pl.String},
                null_values=["NaN", "null", ""],
            )
            return normalize_noaa_routes(raw, sample_minutes)
        finally:
            target.unlink(missing_ok=True)

    async def day(
        self,
        selected: date,
        bounds: GeographicBounds,
        sample_minutes: int | None = None,
        progress: Callable[[int, int], None] | None = None,
    ) -> pl.DataFrame:
        sample_minutes = int(sample_minutes or self.cfg["sample_minutes"])
        chunk_hours = max(1, min(24, int(self.cfg["request_chunk_hours"])))
        start = datetime.combine(selected, time.min, tzinfo=timezone.utc)
        finish = start + timedelta(days=1)
        frames: list[pl.DataFrame] = []
        cursor = start
        total_chunks = math.ceil(24 / chunk_hours)
        completed_chunks = 0
        while cursor < finish:
            chunk_end = min(finish, cursor + timedelta(hours=chunk_hours))
            frame = await self._chunk(cursor, chunk_end, bounds, sample_minutes)
            if not frame.is_empty():
                frames.append(frame)
            cursor = chunk_end
            completed_chunks += 1
            if progress:
                progress(completed_chunks, total_chunks)
        if not frames:
            return empty_routes()
        return (
            pl.concat(frames, how="vertical_relaxed")
            .unique(["source", "mmsi", "observed_at"], keep="last")
            .sort(["mmsi", "observed_at"])
        )


class NoaaHistoricalAisImporter:
    def __init__(self, settings: Settings, database, client: PoliteHttpClient):
        self.settings = settings
        self.database = database
        self.client = client
        self.provider = NoaaHistoricalAisProvider(settings, client)

    def partition_path(self, selected: date) -> Path:
        return (
            self.settings.data_dir
            / "silver/noaa_ais_routes"
            / f"date={selected.isoformat()}"
            / "routes.parquet"
        )

    async def import_day(
        self,
        selected: date,
        bounds: GeographicBounds,
        sample_minutes: int | None = None,
        progress: Callable[[int, int], None] | None = None,
    ) -> dict:
        now = datetime.now(timezone.utc)
        sample_minutes = int(
            sample_minutes or self.settings.sources["noaa_historical_ais"]["sample_minutes"]
        )
        try:
            incoming = await self.provider.day(
                selected, bounds, sample_minutes, progress=progress
            )
            target = self.partition_path(selected)
            if incoming.is_empty():
                added = updated = total = 0
            else:
                combined, added, updated = merge_by_key(
                    target, incoming, ["source", "mmsi", "observed_at"]
                )
                atomic_write(combined, target)
                total = combined.height
            local_dates = self.database.historical_route_dates()
            self.database.upsert_manifest(
                {
                    "source": SOURCE_ID,
                    "dataset": DATASET_ID,
                    "last_successful_refresh": now,
                    "local_max_date": local_dates["latest_available_date"],
                    "upstream_max_date": None,
                    "last_checked": now,
                    "rows_downloaded": incoming.height,
                    "rows_inserted": added,
                    "rows_updated": updated,
                    "status": "ok" if not incoming.is_empty() else "no_data",
                    "error_message": None,
                    "metadata": {
                        "bounds": bounds.as_dict(),
                        "sample_minutes": sample_minutes,
                        "partition": str(target.relative_to(self.settings.root)),
                        "total_partition_rows": total,
                        "license": "CC0-1.0",
                        "coverage": self.settings.sources["noaa_historical_ais"][
                            "coverage_label"
                        ],
                    },
                }
            )
            return {
                "date": selected,
                "downloaded": incoming.height,
                "added": added,
                "updated": updated,
                "total": total,
            }
        except Exception as exc:
            self.database.upsert_manifest(
                {
                    "source": SOURCE_ID,
                    "dataset": DATASET_ID,
                    "last_checked": now,
                    "status": "failed",
                    "error_message": str(exc),
                    "metadata": {
                        "bounds": bounds.as_dict(),
                        "sample_minutes": sample_minutes,
                    },
                }
            )
            raise
