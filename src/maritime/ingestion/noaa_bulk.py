from __future__ import annotations

import asyncio
import csv
import json
import os
import shutil
import tempfile
import zipfile
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import duckdb

from maritime.config import Settings
from maritime.ingestion.noaa_ais import DATASET_ID, ROUTE_SCHEMA, SOURCE_ID
from maritime.network import PoliteHttpClient


def weekly_sample_dates(start: date, end: date, weekday: int = 2) -> list[date]:
    """Return one date per ISO week; weekday uses Monday=0 through Sunday=6."""
    if end < start:
        raise ValueError("end date must be on or after start date")
    if weekday < 0 or weekday > 6:
        raise ValueError("weekday must be between 0 and 6")
    first = start + timedelta(days=(weekday - start.weekday()) % 7)
    return [
        first + timedelta(days=offset)
        for offset in range(0, (end - first).days + 1, 7)
    ]


def _identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _bulk_columns(csv_path: Path) -> dict[str, str]:
    with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        header = next(csv.reader(handle), None)
    if not header:
        raise ValueError("NOAA daily CSV has no header")
    available = {value.strip().lower(): value.strip() for value in header}

    def required(*candidates: str) -> str:
        for candidate in candidates:
            if candidate.lower() in available:
                return available[candidate.lower()]
        raise ValueError(f"NOAA daily CSV is missing {candidates[0]}")

    return {
        "mmsi": required("MMSI"),
        "observed_at": required("BaseDateTime", "time"),
        "latitude": required("LAT", "Lat", "latitude"),
        "longitude": required("LON", "Lon", "longitude"),
        "speed_knots": required("SOG"),
        "course_degrees": required("COG"),
        "vessel_name": required("VesselName"),
    }


class NoaaBulkDailyImporter:
    """Download, compact, validate, and immediately discard one national AIS day."""

    def __init__(self, settings: Settings, database, client: PoliteHttpClient):
        self.settings = settings
        self.database = database
        self.client = client
        self.cfg = settings.sources["noaa_historical_ais"]
        self._cleanup_orphan_sources()

    def _cleanup_orphan_sources(self) -> None:
        """Remove at most one interrupted day's source artifacts before resuming."""
        cache_root = self.settings.data_dir / "cache/downloads/noaa_ais/weekly"
        if not cache_root.exists():
            return
        for pattern in ("*.zip", "*.zip.part"):
            for artifact in cache_root.glob(pattern):
                artifact.unlink(missing_ok=True)
        for workspace in cache_root.iterdir():
            if workspace.is_dir():
                shutil.rmtree(workspace, ignore_errors=True)

    def partition_path(self, selected: date) -> Path:
        return (
            self.settings.data_dir
            / "silver/noaa_ais_routes"
            / f"date={selected.isoformat()}"
            / "routes.parquet"
        )

    def completion_marker(self, selected: date) -> Path:
        return self.partition_path(selected).with_name("weekly-national.complete.json")

    def is_complete(self, selected: date) -> bool:
        target = self.partition_path(selected)
        marker = self.completion_marker(selected)
        if not target.exists() or not marker.exists():
            return False
        try:
            payload = json.loads(marker.read_text(encoding="utf-8"))
            return payload.get("date") == selected.isoformat() and target.stat().st_size > 0
        except (OSError, ValueError, TypeError):
            return False

    def archive_url(self, selected: date) -> str:
        root = str(self.cfg["bulk_base_url"]).rstrip("/")
        return f"{root}/{selected.year}/AIS_{selected:%Y_%m_%d}.zip"

    def _extract_csv(self, archive: Path, directory: Path) -> Path:
        maximum = int(self.cfg["maximum_expanded_gigabytes_per_day"]) * 1024**3
        with zipfile.ZipFile(archive) as zipped:
            members = [item for item in zipped.infolist() if item.filename.lower().endswith(".csv")]
            if not members:
                raise ValueError("NOAA daily archive contains no CSV file")
            member = max(members, key=lambda item: item.file_size)
            if member.file_size > maximum:
                raise RuntimeError(
                    f"Expanded NOAA day exceeds the configured {maximum:,}-byte safety limit"
                )
            target = directory / "source.csv"
            with zipped.open(member) as source, target.open("wb") as destination:
                shutil.copyfileobj(source, destination, length=8 * 1024 * 1024)
        return target

    def _compact_csv(self, csv_path: Path, output: Path, sample_minutes: int) -> int:
        columns = _bulk_columns(csv_path)
        q = {key: _identifier(value) for key, value in columns.items()}
        seconds = sample_minutes * 60
        temporary_directory = csv_path.parent / "duckdb-temp"
        temporary_directory.mkdir(exist_ok=True)
        connection = duckdb.connect()
        try:
            connection.execute("SET threads = 2")
            connection.execute("SET preserve_insertion_order = false")
            connection.execute("SET memory_limit = '2GB'")
            connection.execute("SET temp_directory = ?", [str(temporary_directory)])
            source_path = str(csv_path).replace("'", "''")
            output_path = str(output).replace("'", "''")
            query = f"""
                COPY (
                    WITH normalized AS (
                        SELECT
                            trim(CAST({q['mmsi']} AS VARCHAR)) AS mmsi,
                            try_cast({q['observed_at']} AS TIMESTAMP) AS observed_at,
                            nullif(trim(CAST({q['vessel_name']} AS VARCHAR)), '') AS vessel_name,
                            try_cast({q['latitude']} AS DOUBLE) AS latitude,
                            try_cast({q['longitude']} AS DOUBLE) AS longitude,
                            try_cast({q['speed_knots']} AS DOUBLE) AS speed_knots,
                            try_cast({q['course_degrees']} AS DOUBLE) AS course_degrees
                        FROM read_csv_auto(
                            '{source_path}', header=true, all_varchar=true,
                            ignore_errors=true, sample_size=20000
                        )
                    ), sampled AS (
                        SELECT
                            mmsi,
                            min(observed_at) AS observed_at,
                            first(vessel_name ORDER BY observed_at) AS vessel_name,
                            first(latitude ORDER BY observed_at) AS latitude,
                            first(longitude ORDER BY observed_at) AS longitude,
                            first(speed_knots ORDER BY observed_at) AS speed_knots,
                            first(course_degrees ORDER BY observed_at) AS course_degrees
                        FROM normalized
                        WHERE
                            mmsi IS NOT NULL AND mmsi <> ''
                            AND observed_at IS NOT NULL
                            AND latitude BETWEEN -90 AND 90
                            AND longitude BETWEEN -180 AND 180
                        GROUP BY mmsi, floor(epoch(observed_at) / {seconds})
                    )
                    SELECT
                        '{SOURCE_ID}'::VARCHAR AS source,
                        mmsi::VARCHAR AS mmsi,
                        observed_at::TIMESTAMP AS observed_at,
                        vessel_name::VARCHAR AS vessel_name,
                        latitude::DOUBLE AS latitude,
                        longitude::DOUBLE AS longitude,
                        speed_knots::DOUBLE AS speed_knots,
                        course_degrees::DOUBLE AS course_degrees
                    FROM sampled
                    ORDER BY mmsi, observed_at
                ) TO '{output_path}' (FORMAT PARQUET, COMPRESSION ZSTD)
            """
            connection.execute(query)
            return int(
                connection.execute(
                    "SELECT count(*) FROM read_parquet(?, hive_partitioning=false)", [str(output)]
                ).fetchone()[0]
            )
        finally:
            connection.close()

    def _merge_and_validate(self, incoming: Path, selected: date) -> tuple[int, int, int]:
        target = self.partition_path(selected)
        target.parent.mkdir(parents=True, exist_ok=True)
        replacement = target.with_suffix(".parquet.tmp")
        connection = duckdb.connect()
        try:
            if target.exists():
                target_sql = str(target).replace("'", "''")
                incoming_sql = str(incoming).replace("'", "''")
                replacement_sql = str(replacement).replace("'", "''")
                existing_count = int(
                    connection.execute(
                        "SELECT count(*) FROM read_parquet(?, hive_partitioning=false)", [str(target)]
                    ).fetchone()[0]
                )
                overlap = int(
                    connection.execute(
                        """
                        SELECT count(*) FROM read_parquet(?, hive_partitioning=false) incoming
                        INNER JOIN read_parquet(?, hive_partitioning=false) existing
                        USING (source, mmsi, observed_at)
                        """,
                        [str(incoming), str(target)],
                    ).fetchone()[0]
                )
                connection.execute(
                    f"""
                    COPY (
                        SELECT * EXCLUDE (_priority, _row)
                        FROM (
                            SELECT *, row_number() OVER (
                                PARTITION BY source, mmsi, observed_at
                                ORDER BY _priority DESC
                            ) AS _row
                            FROM (
                                SELECT *, 0 AS _priority FROM read_parquet('{target_sql}', hive_partitioning=false)
                                UNION ALL BY NAME
                                SELECT *, 1 AS _priority FROM read_parquet('{incoming_sql}', hive_partitioning=false)
                            )
                        ) WHERE _row = 1
                        ORDER BY mmsi, observed_at
                    ) TO '{replacement_sql}' (FORMAT PARQUET, COMPRESSION ZSTD)
                    """
                )
                incoming_count = int(
                    connection.execute(
                        "SELECT count(*) FROM read_parquet(?, hive_partitioning=false)", [str(incoming)]
                    ).fetchone()[0]
                )
                added, updated = incoming_count - overlap, overlap
                expected = existing_count + added
            else:
                shutil.copyfile(incoming, replacement)
                expected = int(
                    connection.execute(
                        "SELECT count(*) FROM read_parquet(?, hive_partitioning=false)", [str(incoming)]
                    ).fetchone()[0]
                )
                added, updated = expected, 0
            actual = int(
                connection.execute(
                    "SELECT count(*) FROM read_parquet(?, hive_partitioning=false)",
                    [str(replacement)],
                ).fetchone()[0]
            )
            described = connection.execute(
                "DESCRIBE SELECT * FROM read_parquet(?, hive_partitioning=false)", [str(replacement)]
            ).fetchall()
            actual_columns = [row[0] for row in described]
            expected_columns = list(ROUTE_SCHEMA)
            if actual != expected or actual_columns != expected_columns:
                raise RuntimeError(
                    "Compacted NOAA Parquet validation failed: "
                    f"rows {actual}/{expected}, columns {actual_columns}/{expected_columns}"
                )
            os.replace(replacement, target)
            return added, updated, actual
        finally:
            replacement.unlink(missing_ok=True)
            connection.close()

    def convert_archive(self, selected: date, archive: Path, sample_minutes: int) -> dict:
        cache_root = self.settings.data_dir / "cache/downloads/noaa_ais/weekly"
        cache_root.mkdir(parents=True, exist_ok=True)
        workspace = Path(
            tempfile.mkdtemp(prefix=f"{selected.isoformat()}-", dir=cache_root)
        )
        marker_temp: Path | None = None
        try:
            csv_path = self._extract_csv(archive, workspace)
            incoming = workspace / "sampled.parquet"
            downloaded_rows = self._compact_csv(csv_path, incoming, sample_minutes)
            added, updated, total = self._merge_and_validate(incoming, selected)
            target = self.partition_path(selected)
            marker = self.completion_marker(selected)
            marker_temp = marker.with_suffix(".json.tmp")
            marker_temp.write_text(
                json.dumps(
                    {
                        "date": selected.isoformat(),
                        "sample_minutes": sample_minutes,
                        "rows": total,
                        "completed_at": datetime.now(timezone.utc).isoformat(),
                    },
                    sort_keys=True,
                ),
                encoding="utf-8",
            )
            os.replace(marker_temp, marker)
            return {
                "date": selected,
                "downloaded": downloaded_rows,
                "added": added,
                "updated": updated,
                "total": total,
                "parquet_bytes": target.stat().st_size,
            }
        finally:
            if marker_temp:
                marker_temp.unlink(missing_ok=True)
            shutil.rmtree(workspace, ignore_errors=True)

    async def import_day(self, selected: date, sample_minutes: int) -> dict:
        cache_root = self.settings.data_dir / "cache/downloads/noaa_ais/weekly"
        cache_root.mkdir(parents=True, exist_ok=True)
        archive = cache_root / f"AIS_{selected:%Y_%m_%d}.zip"
        maximum = int(self.cfg["maximum_download_megabytes_per_day"]) * 1024**2
        try:
            await self.client.download(
                self.archive_url(selected),
                archive,
                maximum_bytes=maximum,
                headers={"User-Agent": "MaritimeIntelligenceGlobe/0.1"},
            )
            source_bytes = archive.stat().st_size
            result = await asyncio.to_thread(
                self.convert_archive, selected, archive, sample_minutes
            )
            result["source_bytes"] = source_bytes
            now = datetime.now(timezone.utc)
            local_dates = self.database.historical_route_dates()
            self.database.upsert_manifest(
                {
                    "source": SOURCE_ID,
                    "dataset": DATASET_ID,
                    "last_successful_refresh": now,
                    "local_max_date": local_dates["latest_available_date"],
                    "upstream_max_date": date.fromisoformat(
                        str(self.cfg["source_latest_date"])
                    ),
                    "last_checked": now,
                    "rows_downloaded": result["downloaded"],
                    "rows_inserted": result["added"],
                    "rows_updated": result["updated"],
                    "status": "ok",
                    "error_message": None,
                    "metadata": {
                        "import_mode": "weekly_national_bulk",
                        "sample_minutes": sample_minutes,
                        "source_bytes_discarded": source_bytes,
                        "parquet_bytes": result["parquet_bytes"],
                        "license": "CC0-1.0",
                        "coverage": self.cfg["coverage_label"],
                    },
                }
            )
            return result
        finally:
            archive.unlink(missing_ok=True)
            archive.with_suffix(".zip.part").unlink(missing_ok=True)
