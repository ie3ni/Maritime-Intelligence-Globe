from __future__ import annotations

import asyncio
import logging
import math
import time
import uuid
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone

from maritime.config import Settings
from maritime.ingestion.noaa_bulk import NoaaBulkDailyImporter, weekly_sample_dates
from maritime.network import PoliteHttpClient
from maritime.storage.database import MaritimeDatabase


log = logging.getLogger(__name__)


@dataclass
class NoaaWeeklyImportJob:
    job_id: str | None = None
    state: str = "idle"
    stage: str = "Ready"
    started_at: datetime | None = None
    completed_at: datetime | None = None
    current_date: date | None = None
    completed_dates: int = 0
    total_dates: int = 0
    skipped_dates: int = 0
    rows_added: int = 0
    rows_updated: int = 0
    source_bytes_downloaded: int = 0
    parquet_bytes_written: int = 0
    average_seconds_per_date: float | None = None
    estimated_seconds_remaining: int | None = None
    estimated_completion_at: datetime | None = None
    error: str | None = None


class NoaaWeeklyHistoryService:
    """One resumable national weekly-history job with per-day source cleanup."""

    def __init__(self, settings: Settings, database: MaritimeDatabase):
        self.settings = settings
        self.database = database
        self._guard = asyncio.Lock()
        self._task: asyncio.Task | None = None
        self._cancel_requested = False
        self._completed_dates: set[date] = set()
        self._download_durations: list[float] = []
        dates = self._dates()
        self._sync_completed_dates(dates)
        complete = bool(dates) and len(self._completed_dates) == len(dates)
        self.job = NoaaWeeklyImportJob(
            state="complete" if complete else "idle",
            stage=(
                f"Complete · {len(self._completed_dates)} weekly dates available"
                if complete else
                f"Ready · {len(self._completed_dates)}/{len(dates)} weekly dates available"
            ),
            completed_dates=len(self._completed_dates),
            total_dates=len(dates),
            parquet_bytes_written=self._local_parquet_bytes(),
        )

    def _dates(self) -> list[date]:
        config = self.settings.sources["noaa_historical_ais"]
        return weekly_sample_dates(
            date.fromisoformat(str(config["source_earliest_date"])),
            date.fromisoformat(str(config["source_latest_date"])),
            int(config.get("weekly_sample_weekday", 2)),
        )

    def _sync_completed_dates(self, scheduled: list[date]) -> None:
        local = set(self.database.historical_route_dates()["available_dates"])
        self._completed_dates = local.intersection(scheduled)

    def _local_parquet_bytes(self) -> int:
        root = self.settings.data_dir / "silver/noaa_ais_routes"
        return sum(
            path.stat().st_size
            for path in root.glob("date=*/routes.parquet")
            if path.is_file()
        )

    def _update_eta(self) -> None:
        if self.job.state != "running" or not self._download_durations:
            self.job.average_seconds_per_date = None
            self.job.estimated_seconds_remaining = None
            self.job.estimated_completion_at = None
            return
        average = sum(self._download_durations) / len(self._download_durations)
        remaining = max(0, self.job.total_dates - self.job.completed_dates)
        seconds = int(math.ceil(average * remaining))
        self.job.average_seconds_per_date = round(average, 1)
        self.job.estimated_seconds_remaining = seconds
        self.job.estimated_completion_at = datetime.now(timezone.utc) + timedelta(seconds=seconds)

    def status(self) -> dict:
        self._update_eta()
        return asdict(self.job)

    async def start(self) -> dict:
        async with self._guard:
            if self._task and not self._task.done():
                return self.status()
            self._cancel_requested = False
            dates = self._dates()
            self._sync_completed_dates(dates)
            self._download_durations = []
            if dates and len(self._completed_dates) == len(dates):
                self.job = NoaaWeeklyImportJob(
                    job_id=str(uuid.uuid4()),
                    state="complete",
                    stage=f"Complete · {len(dates)} weekly dates available · all source ZIP/CSV files discarded",
                    started_at=datetime.now(timezone.utc),
                    completed_at=datetime.now(timezone.utc),
                    current_date=dates[-1],
                    completed_dates=len(dates),
                    total_dates=len(dates),
                    skipped_dates=len(dates),
                    parquet_bytes_written=self._local_parquet_bytes(),
                    estimated_seconds_remaining=0,
                )
                return self.status()
            self.job = NoaaWeeklyImportJob(
                job_id=str(uuid.uuid4()),
                state="running",
                stage="Preparing resumable weekly U.S. history…",
                started_at=datetime.now(timezone.utc),
                completed_dates=len(self._completed_dates),
                total_dates=len(dates),
                parquet_bytes_written=self._local_parquet_bytes(),
            )
            self._task = asyncio.create_task(self._run(dates))
            return self.status()

    async def cancel(self) -> dict:
        if self._task and not self._task.done():
            self._cancel_requested = True
            self.job.stage = "Cancellation requested · stopping after the current date"
        return self.status()

    async def wait(self) -> dict:
        if self._task:
            await self._task
        return self.status()

    async def stop(self) -> None:
        await self.cancel()
        if self._task:
            await self._task

    async def _run(self, dates: list[date]) -> None:
        config = self.settings.sources["noaa_historical_ais"]
        sample_minutes = int(config.get("weekly_sample_minutes", 30))
        client = PoliteHttpClient(self.settings.network)
        importer = NoaaBulkDailyImporter(self.settings, self.database, client)
        try:
            for selected in dates:
                if self._cancel_requested:
                    self.job.state = "cancelled"
                    self.job.stage = "Weekly import paused · completed dates were preserved"
                    break
                self.job.current_date = selected
                if importer.is_complete(selected):
                    self._completed_dates.add(selected)
                    self.job.completed_dates = len(self._completed_dates)
                    self.job.skipped_dates += 1
                    self.job.stage = f"Already complete · {selected}"
                    continue
                self.job.stage = f"Downloading national NOAA AIS · {selected}"
                started = time.monotonic()
                result = await importer.import_day(selected, sample_minutes)
                self._download_durations.append(max(.001, time.monotonic() - started))
                self.job.rows_added += result["added"]
                self.job.rows_updated += result["updated"]
                self.job.source_bytes_downloaded += result["source_bytes"]
                self.job.parquet_bytes_written = max(
                    self._local_parquet_bytes(),
                    self.job.parquet_bytes_written + result["parquet_bytes"],
                )
                self._completed_dates.add(selected)
                self.job.completed_dates = len(self._completed_dates)
                self._update_eta()
                self.job.stage = f"Compacted {selected} · source files discarded"
            else:
                self.job.state = "complete"
                self.job.stage = (
                    f"Complete · {self.job.completed_dates} weekly dates available · "
                    "all source ZIP/CSV files discarded"
                )
                self.job.estimated_seconds_remaining = 0
                self.job.estimated_completion_at = datetime.now(timezone.utc)
        except Exception as exc:
            log.exception("NOAA weekly U.S. history import failed")
            self.job.state = "failed"
            self.job.stage = (
                "Weekly history paused at the failed date; "
                "completed dates remain resumable"
            )
            self.job.error = str(exc)
        finally:
            self.job.completed_at = datetime.now(timezone.utc)
            await client.aclose()
