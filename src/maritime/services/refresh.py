from __future__ import annotations

import asyncio
import logging
import uuid
from dataclasses import dataclass, asdict
from datetime import date, datetime, timedelta, timezone
import polars as pl

from maritime.analytics import calculate_analytics
from maritime.config import Settings
from maritime.ingestion.nga import NgaWorldPortIndexProvider, enrich_ports_with_wpi
from maritime.ingestion.portwatch import PortWatchProvider
from maritime.network import PoliteHttpClient
from maritime.storage.database import MaritimeDatabase
from maritime.storage.parquet import atomic_write, merge_by_key

log = logging.getLogger(__name__)


def incremental_window(local_max: date | None, upstream_max: date, revision_lookback_days: int, bootstrap_days: int) -> tuple[date, date]:
    """Return the revision-aware inclusive download window."""
    start = local_max - timedelta(days=revision_lookback_days) if local_max else upstream_max - timedelta(days=bootstrap_days - 1)
    return start, upstream_max


def refresh_windows(
    local_min: date | None,
    local_max: date | None,
    upstream_max: date,
    revision_lookback_days: int,
    requested_history_days: int | None = None,
    default_bootstrap_days: int = 120,
) -> list[tuple[date, date]]:
    """Plan non-overlapping bootstrap/backfill and recent-revision windows.

    An explicit history request means "retain at least this many days through the
    upstream maximum." Existing history is never discarded or downloaded again.
    The ordinary refresh path remains limited to its recent revision window.
    """
    if requested_history_days is not None and requested_history_days < 1:
        raise ValueError("requested history days must be at least 1")
    if local_min is None or local_max is None:
        days = requested_history_days or default_bootstrap_days
        return [(upstream_max - timedelta(days=days - 1), upstream_max)]

    windows: list[tuple[date, date]] = []
    if requested_history_days is not None:
        requested_start = upstream_max - timedelta(days=requested_history_days - 1)
        missing_history_end = local_min - timedelta(days=1)
        if requested_start <= missing_history_end:
            windows.append((requested_start, missing_history_end))

    recent_start = max(local_min, local_max - timedelta(days=revision_lookback_days))
    if recent_start <= upstream_max:
        windows.append((recent_start, upstream_max))
    return windows


def split_date_window(start: date, end: date, maximum_days: int = 31) -> list[tuple[date, date]]:
    """Split an inclusive date window into bounded, non-overlapping batches."""
    if maximum_days < 1:
        raise ValueError("maximum_days must be at least 1")
    if start > end:
        return []
    batches: list[tuple[date, date]] = []
    cursor = start
    while cursor <= end:
        batch_end = min(end, cursor + timedelta(days=maximum_days - 1))
        batches.append((cursor, batch_end))
        cursor = batch_end + timedelta(days=1)
    return batches


@dataclass
class Job:
    job_id: str | None = None
    state: str = "idle"
    stage: str = "Ready"
    started_at: datetime | None = None
    completed_at: datetime | None = None
    rows_added: int = 0
    rows_updated: int = 0
    newest_local_date: date | None = None
    newest_source_date: date | None = None
    oldest_local_date: date | None = None
    oldest_requested_date: date | None = None
    error: str | None = None


class MaritimeRefreshService:
    """Explicit and exclusive boundary for refresh-time external access."""
    def __init__(self, settings: Settings, database: MaritimeDatabase, on_data_changed=None):
        self.settings, self.database = settings, database
        self.on_data_changed = on_data_changed
        self._guard = asyncio.Lock()
        self._task: asyncio.Task | None = None
        dates = database.dates()
        self.job = Job(
            newest_local_date=dates["latest_available_date"],
            oldest_local_date=dates["earliest_available_date"],
        )

    def status(self):
        return asdict(self.job)

    async def start(self, bootstrap_days: int | None = None):
        if bootstrap_days is not None and bootstrap_days < 1:
            raise ValueError("bootstrap_days must be at least 1")
        async with self._guard:
            if self._task and not self._task.done():
                return self.status()
            dates = self.database.dates()
            self.job = Job(
                job_id=str(uuid.uuid4()), state="running", stage="Checking sources…",
                started_at=datetime.now(timezone.utc),
                newest_local_date=dates["latest_available_date"],
                oldest_local_date=dates["earliest_available_date"],
            )
            self._task = asyncio.create_task(self._run(bootstrap_days))
            return self.status()

    async def wait(self):
        if self._task:
            await self._task
        return self.status()

    def _stage(self, value: str):
        self.job.stage = value
        log.info(value)

    async def _enrich_ports_from_wpi(
        self,
        ports: pl.DataFrame,
        client: PoliteHttpClient,
    ) -> pl.DataFrame:
        """Use a small cached WPI extract without making it a PortWatch dependency."""
        cfg = self.settings.sources.get("nga_world_port_index", {})
        if not cfg.get("enabled", False):
            return ports
        cache_path = self.settings.data_dir / "silver/nga_world_port_index.parquet"
        ttl = timedelta(days=float(cfg.get("refresh_ttl_days", 7)))
        cache_is_fresh = False
        if cache_path.exists():
            modified = datetime.fromtimestamp(cache_path.stat().st_mtime, timezone.utc)
            cache_is_fresh = datetime.now(timezone.utc) - modified < ttl

        if not cache_is_fresh:
            self._stage("Refreshing NGA World Port Index metadata…")
            try:
                wpi = await NgaWorldPortIndexProvider(self.settings, client).download()
                minimum_records = int(cfg.get("minimum_records", 1000))
                if wpi.height < minimum_records:
                    raise RuntimeError(
                        f"NGA WPI validation failed: only {wpi.height} records"
                    )
                if wpi["wpi_object_id"].n_unique() != wpi.height:
                    raise RuntimeError("NGA WPI validation failed: duplicate object IDs")
                valid_coordinates = (
                    pl.col("wpi_latitude").is_between(-90, 90).fill_null(False)
                    & pl.col("wpi_longitude").is_between(-180, 180).fill_null(False)
                )
                invalid_coordinates = wpi.select((~valid_coordinates).sum()).item()
                if invalid_coordinates:
                    raise RuntimeError(
                        f"NGA WPI validation failed: {invalid_coordinates} invalid coordinates"
                    )
                atomic_write(wpi, cache_path)
                now = datetime.now(timezone.utc)
                self.database.upsert_manifest({
                    "source": "nga",
                    "dataset": "world_port_index",
                    "last_successful_refresh": now,
                    "last_checked": now,
                    "rows_downloaded": wpi.height,
                    "status": "ok",
                    "error_message": None,
                    "metadata": {
                        "cache_path": "data/silver/nga_world_port_index.parquet",
                        "refresh_ttl_days": cfg.get("refresh_ttl_days", 7),
                    },
                })
            except Exception as exc:
                log.warning("NGA WPI enrichment refresh failed; using cached metadata if available: %s", exc)
                self.database.upsert_manifest({
                    "source": "nga",
                    "dataset": "world_port_index",
                    "last_checked": datetime.now(timezone.utc),
                    "status": "stale" if cache_path.exists() else "unavailable",
                    "error_message": str(exc),
                })

        if not cache_path.exists():
            return ports
        try:
            wpi = pl.read_parquet(cache_path)
            self._stage("Matching PortWatch ports to NGA metadata…")
            return enrich_ports_with_wpi(
                ports,
                wpi,
                name_maximum_distance_km=float(cfg.get("name_maximum_distance_km", 50)),
                proximity_maximum_distance_km=float(cfg.get("proximity_maximum_distance_km", 8)),
                ambiguity_margin_km=float(cfg.get("ambiguity_margin_km", 3)),
            )
        except Exception as exc:
            log.warning("Cached NGA WPI enrichment could not be applied: %s", exc)
            return ports

    async def _run(self, bootstrap_days: int | None):
        client = PoliteHttpClient(self.settings.network)
        provider = PortWatchProvider(self.settings, client)
        activity_path = self.settings.data_dir / "silver/port_daily_activity.parquet"
        ports_path = self.settings.data_dir / "silver/ports.parquet"
        analytics_path = self.settings.data_dir / "gold/port_daily_anomalies.parquet"
        try:
            self._stage("Checking PortWatch…")
            upstream = await provider.latest_date()
            if upstream is None:
                raise RuntimeError("PortWatch did not report an available date")
            self.job.newest_source_date = upstream
            local_dates = self.database.dates()
            local_min = local_dates["earliest_available_date"]
            local_max = local_dates["latest_available_date"]
            lookback = int(self.settings.sources["portwatch"]["revision_lookback_days"])
            default_days = max(
                120, int(self.settings.analytics["baseline"]["rolling_days"]) + 31
            )
            windows = refresh_windows(
                local_min, local_max, upstream, lookback,
                requested_history_days=bootstrap_days,
                default_bootstrap_days=default_days,
            )
            fetch_windows = [
                batch
                for start, end in windows
                for batch in split_date_window(start, end)
            ]
            if bootstrap_days is not None:
                self.job.oldest_requested_date = upstream - timedelta(days=bootstrap_days - 1)
            self._stage("Checking port metadata…")
            ports = await provider.ports()
            if ports.height < 100:
                raise RuntimeError(f"Port metadata validation failed: only {ports.height} valid ports")
            ports = await self._enrich_ports_from_wpi(ports, client)
            atomic_write(ports, ports_path)
            incoming_parts: list[pl.DataFrame] = []
            staged_paths = []
            if fetch_windows:
                stage_root = (
                    self.settings.data_dir / "cache/backfills" /
                    f"portwatch_{fetch_windows[0][0]}_{fetch_windows[-1][1]}"
                )
            else:
                stage_root = self.settings.data_dir / "cache/backfills/portwatch_empty"
            for index, (start, end) in enumerate(fetch_windows, start=1):
                kind = "Backfilling historical" if local_min and end < local_min else "Downloading recent"
                stage_path = stage_root / f"{start}_{end}.parquet"
                staged_paths.append(stage_path)
                if stage_path.exists():
                    self._stage(
                        f"Loading checkpointed maritime observations… {start} to {end} "
                        f"({index}/{len(fetch_windows)})"
                    )
                    part = pl.read_parquet(stage_path)
                else:
                    self._stage(
                        f"{kind} maritime observations… {start} to {end} "
                        f"({index}/{len(fetch_windows)})"
                    )
                    part = await provider.activity(start, end, self._stage)
                    atomic_write(part, stage_path)
                if not part.is_empty():
                    incoming_parts.append(part)
            incoming = (
                pl.concat(incoming_parts, how="diagonal_relaxed")
                .unique(["date", "port_id"], keep="last")
                if incoming_parts
                else pl.DataFrame()
            )
            if incoming.is_empty():
                self._stage("No additional maritime observations available")
                self.database.refresh_views()
                if self.on_data_changed:
                    self.on_data_changed()
                self.job.state = "complete"
                return
            if incoming.select(pl.col("port_calls").is_null().mean()).item() > .8:
                raise RuntimeError("Source validation failed: unexpected port_calls null rate")
            combined, added, updated = merge_by_key(activity_path, incoming, ["date","port_id"])
            self._stage("Validating and updating local Parquet…")
            atomic_write(combined, activity_path)
            self._stage("Calculating activity scores…")
            cfg = self.settings.analytics
            analytics = calculate_analytics(combined, cfg["baseline"]["rolling_days"], cfg["baseline"]["minimum_history_days"], cfg["activity_score"]["volume_weight"], cfg["activity_score"]["port_calls_weight"], cfg["activity_score"]["z_clip"])
            atomic_write(analytics, analytics_path)
            self.database.refresh_views()
            if self.on_data_changed:
                self.on_data_changed()
            self.job.rows_added, self.job.rows_updated = added, updated
            self.job.newest_local_date = upstream
            self.job.oldest_local_date = combined["date"].min()
            now = datetime.now(timezone.utc)
            self.database.upsert_manifest({"source":"portwatch","dataset":"daily_ports","last_successful_refresh":now,"local_max_date":upstream,"upstream_max_date":upstream,"last_checked":now,"rows_downloaded":incoming.height,"rows_inserted":added,"rows_updated":updated,"status":"ok","error_message":None,"metadata":{"windows":[{"start":str(start),"end":str(end)} for start,end in windows],"requested_history_days":bootstrap_days}})
            for stage_path in staged_paths:
                stage_path.unlink(missing_ok=True)
            try:
                stage_root.rmdir()
            except OSError:
                pass
            self._stage("Complete")
            self.job.state = "complete"
        except Exception as exc:
            log.exception("Refresh failed; preserved existing local data")
            self.job.state, self.job.stage, self.job.error = "failed", "Could not retrieve requested data", str(exc)
            local_dates = self.database.dates()
            self.job.newest_local_date = local_dates["latest_available_date"]
            self.job.oldest_local_date = local_dates["earliest_available_date"]
            self.database.upsert_manifest({"source":"portwatch","dataset":"daily_ports","last_checked":datetime.now(timezone.utc),"status":"error","error_message":str(exc),"metadata":{}})
        finally:
            self.job.completed_at = datetime.now(timezone.utc)
            await client.aclose()
