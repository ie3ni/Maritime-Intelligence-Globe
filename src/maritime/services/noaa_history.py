from __future__ import annotations

import asyncio
import logging
import math
import uuid
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone

from maritime.config import Settings
from maritime.ingestion.noaa_ais import GeographicBounds, NoaaHistoricalAisImporter
from maritime.network import PoliteHttpClient
from maritime.storage.database import MaritimeDatabase


log = logging.getLogger(__name__)


@dataclass
class NoaaImportJob:
    job_id: str | None = None
    state: str = "idle"
    stage: str = "Ready"
    started_at: datetime | None = None
    completed_at: datetime | None = None
    start_date: date | None = None
    end_date: date | None = None
    current_date: date | None = None
    completed_chunks: int = 0
    total_chunks: int = 0
    rows_added: int = 0
    rows_updated: int = 0
    error: str | None = None


class NoaaHistoricalImportService:
    """One guarded, cancellable NOAA import job inside the local application."""

    def __init__(self, settings: Settings, database: MaritimeDatabase):
        self.settings = settings
        self.database = database
        self._guard = asyncio.Lock()
        self._task: asyncio.Task | None = None
        self.job = NoaaImportJob()

    def status(self) -> dict:
        return asdict(self.job)

    async def start(
        self,
        start: date,
        end: date,
        bounds: GeographicBounds,
        sample_minutes: int,
    ) -> dict:
        config = self.settings.sources["noaa_historical_ais"]
        if not config.get("enabled", True):
            raise ValueError("NOAA historical AIS importing is disabled")
        if end < start:
            raise ValueError("end date must be on or after start date")
        earliest = date.fromisoformat(str(config["source_earliest_date"]))
        latest = date.fromisoformat(str(config["source_latest_date"]))
        if start < earliest or end > latest:
            raise ValueError(
                f"NOAA route imports must be between {earliest} and {latest}"
            )
        days = (end - start).days + 1
        maximum_days = int(config["maximum_days_per_run"])
        if days > maximum_days:
            raise ValueError(f"imports are limited to {maximum_days} days per run")
        if sample_minutes < 1 or sample_minutes > 60:
            raise ValueError("sample_minutes must be between 1 and 60")
        async with self._guard:
            if self._task and not self._task.done():
                return self.status()
            chunks_per_day = math.ceil(
                24 / max(1, min(24, int(config["request_chunk_hours"])))
            )
            self.job = NoaaImportJob(
                job_id=str(uuid.uuid4()),
                state="running",
                stage="Preparing bounded NOAA request…",
                started_at=datetime.now(timezone.utc),
                start_date=start,
                end_date=end,
                total_chunks=days * chunks_per_day,
            )
            self._task = asyncio.create_task(
                self._run(start, end, bounds, sample_minutes, chunks_per_day)
            )
            return self.status()

    async def cancel(self) -> dict:
        async with self._guard:
            if self._task and not self._task.done():
                self._task.cancel()
        if self._task:
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        return self.status()

    async def wait(self) -> dict:
        if self._task:
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        return self.status()

    async def stop(self) -> None:
        await self.cancel()

    async def _run(
        self,
        start: date,
        end: date,
        bounds: GeographicBounds,
        sample_minutes: int,
        chunks_per_day: int,
    ) -> None:
        client = PoliteHttpClient(self.settings.network)
        importer = NoaaHistoricalAisImporter(self.settings, self.database, client)
        try:
            selected = start
            day_index = 0
            while selected <= end:
                self.job.current_date = selected
                completed_before = day_index * chunks_per_day

                def progress(completed: int, total: int) -> None:
                    self.job.completed_chunks = completed_before + completed
                    self.job.stage = (
                        f"Downloading NOAA routes for {selected} · "
                        f"chunk {completed}/{total}"
                    )

                result = await importer.import_day(
                    selected,
                    bounds,
                    sample_minutes,
                    progress=progress,
                )
                self.job.rows_added += result["added"]
                self.job.rows_updated += result["updated"]
                day_index += 1
                selected += timedelta(days=1)
            self.job.completed_chunks = self.job.total_chunks
            self.job.state = "complete"
            self.job.stage = (
                f"Complete · {self.job.rows_added:,} route points added"
            )
        except asyncio.CancelledError:
            self.job.state = "cancelled"
            self.job.stage = "NOAA import cancelled; completed date partitions were preserved"
            raise
        except Exception as exc:
            log.exception("NOAA historical AIS import failed")
            self.job.state = "failed"
            self.job.stage = "NOAA historical route import failed"
            self.job.error = str(exc)
        finally:
            self.job.completed_at = datetime.now(timezone.utc)
            await client.aclose()
