from __future__ import annotations

from datetime import date, datetime
from typing import Any

from pydantic import BaseModel, Field


class PortSnapshot(BaseModel):
    port_id: str
    name: str
    country: str
    latitude: float
    longitude: float
    activity_score: float
    volume_z_score: float | None = None
    calls_z_score: float | None = None
    volume_change_pct: float | None = None
    port_calls: float | None = None
    total_volume: float | None = None
    importance: float = 1
    port_function: str = "cargo_or_mixed"
    port_function_label: str = "Cargo / mixed-use port"
    classification_basis: str = "Insufficient typed-call history"
    classification_confidence: str = "low"
    tanker_call_share: float | None = None
    typed_call_count: int = 0


class GlobeSnapshot(BaseModel):
    date: date
    ports: list[PortSnapshot]


class LivePortSnapshot(BaseModel):
    port_id: str
    name: str
    country: str
    latitude: float
    longitude: float
    port_calls: float
    unique_vessels: float
    calls_expected: float | None = None
    baseline_days: int | None = None
    calls_z_score: float | None = None
    activity_score: float | None = None
    last_observation_at: datetime | None = None
    importance: float = 1
    port_function: str = "cargo_or_mixed"
    port_function_label: str = "Cargo / mixed-use port"
    classification_basis: str = "Insufficient typed-call history"
    classification_confidence: str = "low"
    tanker_call_share: float | None = None
    typed_call_count: int = 0


class LiveGlobeSnapshot(BaseModel):
    date: date
    source: str = "aisstream"
    provisional: bool = True
    ports: list[LivePortSnapshot]


class RefreshStatus(BaseModel):
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


class NoaaWeeklyImportStatus(BaseModel):
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


class ManifestRecord(BaseModel):
    source: str
    dataset: str
    last_successful_refresh: datetime | None = None
    local_max_date: date | None = None
    upstream_max_date: date | None = None
    last_checked: datetime | None = None
    etag: str | None = None
    last_modified: str | None = None
    source_version: str | None = None
    rows_downloaded: int = 0
    rows_inserted: int = 0
    rows_updated: int = 0
    status: str = "never"
    error_message: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
