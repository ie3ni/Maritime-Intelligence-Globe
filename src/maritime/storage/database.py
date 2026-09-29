from __future__ import annotations

import json
import math
import re
import threading
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import duckdb

from maritime.config import Settings


_GAS_TERMINAL_PATTERN = re.compile(
    r"\b(?:lng|lpg|liquefied natural gas|liquefied petroleum gas)\b", re.IGNORECASE
)
_OIL_TERMINAL_PATTERN = re.compile(r"\b(?:oil|petroleum|crude)\b", re.IGNORECASE)


def _affirmative(value: Any) -> bool:
    return str(value or "").strip().lower() in {"yes", "y", "true", "1"}


def classify_port_function(
    port_id: str,
    name: str,
    cargo_calls: float,
    tanker_calls: float,
    minimum_typed_calls: int = 20,
    tanker_dominant_share: float = 0.8,
    wpi_name: str | None = None,
    wpi_alternate_name: str | None = None,
    wpi_facility_oil_terminal: str | None = None,
    wpi_facility_lng_terminal: str | None = None,
    wpi_oil_terminal_depth_m: float | None = None,
    wpi_lng_terminal_depth_m: float | None = None,
) -> dict[str, Any]:
    """Classify a port from explicit identity, WPI evidence, and full call history."""
    cargo = max(0.0, float(cargo_calls or 0))
    tanker = max(0.0, float(tanker_calls or 0))
    typed_calls = cargo + tanker
    tanker_share = tanker / typed_calls if typed_calls else None
    normalized_name = name or ""
    wpi_identity = " ".join(filter(None, (wpi_name, wpi_alternate_name)))
    wpi_oil_facility = _affirmative(wpi_facility_oil_terminal)
    wpi_lng_facility = _affirmative(wpi_facility_lng_terminal)
    wpi_oil_depth = float(wpi_oil_terminal_depth_m or 0) > 0
    wpi_lng_depth = float(wpi_lng_terminal_depth_m or 0) > 0
    wpi_dedicated_oil = bool(
        _OIL_TERMINAL_PATTERN.search(wpi_identity) and (wpi_oil_facility or wpi_oil_depth)
    )
    wpi_dedicated_lng = bool(
        _GAS_TERMINAL_PATTERN.search(wpi_identity) and (wpi_lng_facility or wpi_lng_depth)
    )

    if _GAS_TERMINAL_PATTERN.search(normalized_name) or wpi_dedicated_lng:
        confirmed = wpi_lng_facility or wpi_lng_depth
        function, label, basis, confidence = (
            "lng_lpg_terminal",
            "LNG / LPG terminal",
            "Explicit LNG/LPG identity; confirmed by NGA WPI" if confirmed
            else "Explicit LNG/LPG terminal name",
            "high",
        )
    elif (
        port_id.lower().startswith("fso")
        or _OIL_TERMINAL_PATTERN.search(normalized_name)
        or wpi_dedicated_oil
    ):
        confirmed = wpi_oil_facility or wpi_oil_depth
        function, label, basis, confidence = (
            "oil_terminal",
            "Oil terminal",
            "Explicit oil/offshore-terminal identity; confirmed by NGA WPI" if confirmed
            else "Explicit oil/offshore-terminal identity",
            "high",
        )
    elif typed_calls >= minimum_typed_calls and tanker_share is not None and tanker_share >= tanker_dominant_share:
        function, label, basis, confidence = (
            "tanker_dominant",
            "Tanker-dominant port",
            f"{tanker_share:.0%} tanker calls across full stored history",
            "medium",
        )
    else:
        function, label = "cargo_or_mixed", "Cargo / mixed-use port"
        if typed_calls >= minimum_typed_calls and tanker_share is not None:
            basis = f"{tanker_share:.0%} tanker calls across full stored history"
            reported = []
            if wpi_oil_facility:
                reported.append("oil-terminal")
            if wpi_lng_facility:
                reported.append("LNG-terminal")
            if reported:
                basis += (
                    f"; NGA WPI reports {' and '.join(reported)} facilities, not a dedicated terminal identity"
                )
            confidence = "medium"
        else:
            reported = []
            if wpi_oil_facility:
                reported.append("oil-terminal")
            if wpi_lng_facility:
                reported.append("LNG-terminal")
            if reported:
                basis = (
                    f"NGA WPI reports {' and '.join(reported)} facilities, but not a dedicated terminal identity"
                )
                confidence = "medium"
            else:
                basis = "Insufficient typed-call history for a specialized classification"
                confidence = "low"

    return {
        "port_function": function,
        "port_function_label": label,
        "classification_basis": basis,
        "classification_confidence": confidence,
        "tanker_call_share": round(tanker_share, 4) if tanker_share is not None else None,
        "typed_call_count": int(round(typed_calls)),
    }


class MaritimeDatabase:
    """DuckDB repository. This module never imports or calls the network layer."""

    def __init__(self, settings: Settings):
        self.settings = settings
        settings.database_path.parent.mkdir(parents=True, exist_ok=True)
        for layer in ("bronze", "silver", "gold", "cache/downloads"):
            (settings.data_dir / layer).mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._port_profiles_revision: tuple[Any, ...] | None = None
        self._port_profiles: dict[str, dict[str, Any]] = {}
        self.initialize()

    @contextmanager
    def connection(self):
        with self._lock:
            conn = duckdb.connect(str(self.settings.database_path))
            try:
                yield conn
            finally:
                conn.close()

    def initialize(self):
        with self.connection() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS ingestion_manifest (
                    source VARCHAR, dataset VARCHAR, last_successful_refresh TIMESTAMP,
                    local_max_date DATE, upstream_max_date DATE, last_checked TIMESTAMP,
                    etag VARCHAR, last_modified VARCHAR, source_version VARCHAR,
                    rows_downloaded BIGINT DEFAULT 0, rows_inserted BIGINT DEFAULT 0,
                    rows_updated BIGINT DEFAULT 0, status VARCHAR, error_message VARCHAR,
                    metadata JSON, PRIMARY KEY(source, dataset)
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS ais_port_calls (
                    event_id VARCHAR PRIMARY KEY,
                    call_date DATE NOT NULL,
                    port_id VARCHAR NOT NULL,
                    mmsi VARCHAR NOT NULL,
                    vessel_name VARCHAR,
                    vessel_type VARCHAR,
                    arrived_at TIMESTAMP NOT NULL,
                    last_seen_at TIMESTAMP NOT NULL,
                    confidence VARCHAR NOT NULL DEFAULT 'provisional',
                    source VARCHAR NOT NULL DEFAULT 'aisstream'
                )
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS ais_port_calls_date_idx ON ais_port_calls(call_date)")
            conn.execute("CREATE INDEX IF NOT EXISTS ais_port_calls_port_idx ON ais_port_calls(port_id)")
            conn.execute("""
                CREATE TABLE IF NOT EXISTS ais_route_points (
                    mmsi VARCHAR NOT NULL,
                    observed_at TIMESTAMP NOT NULL,
                    vessel_name VARCHAR,
                    latitude DOUBLE NOT NULL,
                    longitude DOUBLE NOT NULL,
                    speed_knots DOUBLE,
                    course_degrees DOUBLE,
                    message_type VARCHAR NOT NULL,
                    PRIMARY KEY(mmsi, observed_at)
                )
            """)
            conn.execute(
                "CREATE INDEX IF NOT EXISTS ais_route_points_time_idx "
                "ON ais_route_points(observed_at)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS ais_route_points_vessel_idx "
                "ON ais_route_points(mmsi)"
            )
            self.refresh_views(conn)

    def refresh_views(self, conn=None):
        owns = conn is None
        if owns:
            conn = duckdb.connect(str(self.settings.database_path))
        try:
            paths = {
                "ports": self.settings.data_dir / "silver/ports.parquet",
                "activity": self.settings.data_dir / "silver/port_daily_activity.parquet",
                "analytics": self.settings.data_dir / "gold/port_daily_anomalies.parquet",
            }
            schemas = {
                "ports": "SELECT CAST(NULL AS VARCHAR) AS port_id, CAST(NULL AS VARCHAR) AS \"name\", CAST(NULL AS VARCHAR) AS country, CAST(NULL AS DOUBLE) AS latitude, CAST(NULL AS DOUBLE) AS longitude, CAST(NULL AS VARCHAR) AS un_locode, CAST(NULL AS VARCHAR) AS region, CAST(NULL AS VARCHAR) AS harbor_type, CAST(NULL AS VARCHAR) AS harbor_size, CAST(NULL AS DOUBLE) AS importance WHERE false",
                "activity": "SELECT CAST(NULL AS DATE) AS date, CAST(NULL AS VARCHAR) AS port_id, CAST(NULL AS DOUBLE) AS port_calls, CAST(NULL AS DOUBLE) AS cargo_calls, CAST(NULL AS DOUBLE) AS tanker_calls, CAST(NULL AS DOUBLE) AS import_volume, CAST(NULL AS DOUBLE) AS export_volume, CAST(NULL AS DOUBLE) AS total_volume, CAST(NULL AS VARCHAR) AS source, CAST(NULL AS VARCHAR) AS retrieved_at WHERE false",
                "analytics": "SELECT CAST(NULL AS DATE) AS date, CAST(NULL AS VARCHAR) AS port_id, CAST(NULL AS DOUBLE) AS port_calls, CAST(NULL AS DOUBLE) AS total_volume, CAST(NULL AS DOUBLE) AS calls_expected, CAST(NULL AS DOUBLE) AS volume_expected, CAST(NULL AS DOUBLE) AS calls_z_score, CAST(NULL AS DOUBLE) AS volume_z_score, CAST(NULL AS DOUBLE) AS activity_score, CAST(NULL AS DOUBLE) AS change_7d_pct, CAST(NULL AS DOUBLE) AS change_30d_pct WHERE false",
            }
            for name, path in paths.items():
                sql = f"SELECT * FROM read_parquet('{str(path).replace(chr(39), chr(39)*2)}')" if path.exists() else schemas[name]
                conn.execute(f"CREATE OR REPLACE VIEW {name} AS {sql}")
            self._port_profiles_revision = None
            self._port_profiles = {}
        finally:
            if owns:
                conn.close()

    def dates(self) -> dict[str, Any]:
        with self.connection() as conn:
            row = conn.execute("SELECT min(date), max(date), count(DISTINCT date) FROM analytics").fetchone()
            available = [r[0] for r in conn.execute("SELECT DISTINCT date FROM analytics ORDER BY date").fetchall()]
        return {"earliest_available_date": row[0], "latest_available_date": row[1], "date_count": row[2], "available_dates": available}

    def snapshot(self, selected: date) -> list[dict[str, Any]]:
        with self.connection() as conn:
            rows = conn.execute("""
                SELECT p.port_id, p.name, p.country, p.latitude, p.longitude,
                       a.activity_score, a.volume_z_score, a.calls_z_score,
                       a.change_7d_pct, a.port_calls, a.total_volume, p.importance
                FROM analytics a JOIN ports p USING(port_id)
                WHERE a.date = ? ORDER BY p.importance DESC, a.activity_score DESC
            """, [selected]).fetchall()
            names = [d[0] for d in conn.description]
        return self._with_port_profiles([self._record(names, r) for r in rows])

    def port(self, port_id: str, selected: date | None = None):
        with self.connection() as conn:
            row = conn.execute("SELECT * FROM ports WHERE port_id = ?", [port_id]).fetchone()
            if not row:
                return None
            result = dict(zip([d[0] for d in conn.description], row))
            if selected:
                stat = conn.execute("SELECT * FROM analytics WHERE port_id=? AND date=?", [port_id, selected]).fetchone()
                if stat:
                    result["activity"] = self._record([d[0] for d in conn.description], stat)
        result.update(self.port_profiles().get(port_id, self._default_port_profile()))
        return result

    def history(self, port_id: str, start: date | None, end: date | None):
        clauses, args = ["port_id = ?"], [port_id]
        if start: clauses.append("date >= ?"); args.append(start)
        if end: clauses.append("date <= ?"); args.append(end)
        with self.connection() as conn:
            rows = conn.execute(f"SELECT date, activity_score, port_calls, total_volume, calls_expected, volume_expected, calls_z_score, volume_z_score, change_7d_pct, change_30d_pct FROM analytics WHERE {' AND '.join(clauses)} ORDER BY date", args).fetchall()
            names = [d[0] for d in conn.description]
        return [self._record(names, r) for r in rows]

    def top_anomalies(self, selected: date, limit: int):
        with self.connection() as conn:
            rows = conn.execute("""
                SELECT p.port_id, p.name, p.country, p.latitude, p.longitude,
                       a.activity_score, a.volume_z_score, a.calls_z_score,
                       a.change_7d_pct, a.port_calls, a.total_volume, p.importance
                FROM analytics a JOIN ports p USING(port_id)
                WHERE a.date = ? ORDER BY abs(a.activity_score - 50) DESC, p.importance DESC LIMIT ?
            """, [selected, limit]).fetchall()
            names = [d[0] for d in conn.description]
        return self._with_port_profiles([self._record(names, row) for row in rows])

    @staticmethod
    def _default_port_profile() -> dict[str, Any]:
        return {
            "port_function": "cargo_or_mixed",
            "port_function_label": "Cargo / mixed-use port",
            "classification_basis": "Insufficient typed-call history for a specialized classification",
            "classification_confidence": "low",
            "tanker_call_share": None,
            "typed_call_count": 0,
        }

    def _profile_revision(self) -> tuple[Any, ...]:
        revision: list[Any] = []
        for relative_path in ("silver/ports.parquet", "silver/port_daily_activity.parquet"):
            path = self.settings.data_dir / relative_path
            try:
                stat = path.stat()
                revision.extend((stat.st_mtime_ns, stat.st_size, stat.st_ino))
            except FileNotFoundError:
                revision.extend((0, 0, 0))
        return tuple(revision)

    def port_profiles(self) -> dict[str, dict[str, Any]]:
        """Return stable profiles calculated once from the complete local history."""
        revision = self._profile_revision()
        if revision == self._port_profiles_revision:
            return self._port_profiles

        cfg = self.settings.analytics.get("port_classification", {})
        minimum_typed_calls = int(cfg.get("minimum_typed_calls", 20))
        tanker_dominant_share = float(cfg.get("tanker_dominant_share", 0.8))
        with self.connection() as conn:
            port_columns = {row[1] for row in conn.execute("PRAGMA table_info('ports')").fetchall()}
            optional_columns = (
                "wpi_name",
                "wpi_alternate_name",
                "wpi_facility_oil_terminal",
                "wpi_facility_lng_terminal",
                "wpi_oil_terminal_depth_m",
                "wpi_lng_terminal_depth_m",
            )
            expressions = [
                f"p.{column}" if column in port_columns else f"NULL AS {column}"
                for column in optional_columns
            ]
            rows = conn.execute(f"""
                SELECT p.port_id, p.name,
                       coalesce(a.cargo_calls, 0) AS cargo_calls,
                       coalesce(a.tanker_calls, 0) AS tanker_calls,
                       {', '.join(expressions)}
                FROM ports p
                LEFT JOIN (
                    SELECT port_id, sum(cargo_calls) AS cargo_calls,
                           sum(tanker_calls) AS tanker_calls
                    FROM activity GROUP BY port_id
                ) a USING(port_id)
            """).fetchall()
            names = [description[0] for description in conn.description]
        records = [self._record(names, row) for row in rows]
        profiles = {
            record["port_id"]: classify_port_function(
                record["port_id"],
                record["name"],
                record["cargo_calls"],
                record["tanker_calls"],
                minimum_typed_calls,
                tanker_dominant_share,
                wpi_name=record["wpi_name"],
                wpi_alternate_name=record["wpi_alternate_name"],
                wpi_facility_oil_terminal=record["wpi_facility_oil_terminal"],
                wpi_facility_lng_terminal=record["wpi_facility_lng_terminal"],
                wpi_oil_terminal_depth_m=record["wpi_oil_terminal_depth_m"],
                wpi_lng_terminal_depth_m=record["wpi_lng_terminal_depth_m"],
            )
            for record in records
        }
        self._port_profiles = profiles
        self._port_profiles_revision = revision
        return profiles

    def _with_port_profiles(self, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        profiles = self.port_profiles()
        for record in records:
            record.update(profiles.get(record["port_id"], self._default_port_profile()))
        return records

    def status(self):
        dates = self.dates()
        with self.connection() as conn:
            port_count = conn.execute("SELECT count(*) FROM ports").fetchone()[0]
            manifests = conn.execute("SELECT source,dataset,last_successful_refresh,local_max_date,upstream_max_date,last_checked,status,error_message FROM ingestion_manifest ORDER BY source,dataset").fetchall()
        dates["port_count"] = port_count
        dates["sources"] = [dict(zip(("source","dataset","last_successful_refresh","local_max_date","upstream_max_date","last_checked","status","error_message"), row)) for row in manifests]
        return dates

    def all_ports(self) -> list[dict[str, Any]]:
        with self.connection() as conn:
            rows = conn.execute(
                "SELECT port_id, name, country, latitude, longitude, importance FROM ports"
            ).fetchall()
            names = [description[0] for description in conn.description]
        return [self._record(names, row) for row in rows]

    def record_ais_port_call(self, values: dict[str, Any]) -> bool:
        """Persist one deduplicated, provisional AIS-derived port call."""
        columns = [
            "event_id", "call_date", "port_id", "mmsi", "vessel_name",
            "vessel_type", "arrived_at", "last_seen_at", "confidence", "source",
        ]
        prepared = [values.get(column) for column in columns]
        with self.connection() as conn:
            before = conn.execute(
                "SELECT count(*) FROM ais_port_calls WHERE event_id = ?", [values["event_id"]]
            ).fetchone()[0]
            conn.execute(
                f"INSERT INTO ais_port_calls ({','.join(columns)}) VALUES ({','.join('?' for _ in columns)}) "
                "ON CONFLICT(event_id) DO UPDATE SET "
                "last_seen_at=excluded.last_seen_at, vessel_name=coalesce(excluded.vessel_name, ais_port_calls.vessel_name), "
                "vessel_type=coalesce(excluded.vessel_type, ais_port_calls.vessel_type)",
                prepared,
            )
        return before == 0

    def record_ais_route_points(self, points: list[dict[str, Any]]) -> int:
        """Persist a sampled route batch in one short DuckDB transaction."""
        if not points:
            return 0
        columns = (
            "mmsi", "observed_at", "vessel_name", "latitude", "longitude",
            "speed_knots", "course_degrees", "message_type",
        )
        prepared = [tuple(point.get(column) for column in columns) for point in points]
        with self.connection() as conn:
            conn.executemany(
                f"INSERT INTO ais_route_points ({','.join(columns)}) "
                f"VALUES ({','.join('?' for _ in columns)}) "
                "ON CONFLICT(mmsi, observed_at) DO UPDATE SET "
                "vessel_name=coalesce(excluded.vessel_name, ais_route_points.vessel_name), "
                "latitude=excluded.latitude, longitude=excluded.longitude, "
                "speed_knots=excluded.speed_knots, course_degrees=excluded.course_degrees, "
                "message_type=excluded.message_type",
                prepared,
            )
        return len(prepared)

    def prune_ais_route_points(self, before: datetime) -> None:
        with self.connection() as conn:
            conn.execute("DELETE FROM ais_route_points WHERE observed_at < ?", [before])

    def maintain_ais_storage(
        self, route_before: datetime, port_call_before: date
    ) -> dict[str, int]:
        """Bound live AIS tables and checkpoint DuckDB in one serialized pass."""
        with self.connection() as conn:
            route_count = conn.execute(
                "SELECT count(*) FROM ais_route_points WHERE observed_at < ?",
                [route_before],
            ).fetchone()[0]
            call_count = conn.execute(
                "SELECT count(*) FROM ais_port_calls WHERE call_date < ?",
                [port_call_before],
            ).fetchone()[0]
            conn.execute("DELETE FROM ais_route_points WHERE observed_at < ?", [route_before])
            conn.execute("DELETE FROM ais_port_calls WHERE call_date < ?", [port_call_before])
            conn.execute("CHECKPOINT")
        return {
            "route_points_removed": int(route_count),
            "port_calls_removed": int(call_count),
        }

    def ais_routes(
        self,
        hours: int = 6,
        maximum_vessels: int = 800,
        maximum_points_per_vessel: int = 40,
    ) -> list[dict[str, Any]]:
        """Return bounded, recent routes from local storage only."""
        since = datetime.now(timezone.utc) - timedelta(hours=hours)
        with self.connection() as conn:
            rows = conn.execute("""
                WITH active_vessels AS (
                    SELECT mmsi, max(observed_at) AS last_observed_at
                    FROM ais_route_points
                    WHERE observed_at >= ?
                    GROUP BY mmsi
                    ORDER BY last_observed_at DESC
                    LIMIT ?
                ), ranked_points AS (
                    SELECT p.*,
                           row_number() OVER (
                               PARTITION BY p.mmsi ORDER BY p.observed_at DESC
                           ) AS point_rank
                    FROM ais_route_points p
                    JOIN active_vessels a USING(mmsi)
                    WHERE p.observed_at >= ?
                )
                SELECT mmsi, observed_at, vessel_name, latitude, longitude,
                       speed_knots, course_degrees
                FROM ranked_points
                WHERE point_rank <= ?
                ORDER BY mmsi, observed_at
            """, [since, maximum_vessels, since, maximum_points_per_vessel]).fetchall()
        routes: dict[str, dict[str, Any]] = {}
        for mmsi, observed_at, vessel_name, latitude, longitude, speed, course in rows:
            route = routes.setdefault(str(mmsi), {
                "mmsi": str(mmsi),
                "vessel_name": vessel_name,
                "last_observed_at": observed_at,
                "speed_knots": speed,
                "course_degrees": course,
                "points": [],
            })
            route["vessel_name"] = vessel_name or route["vessel_name"]
            route["last_observed_at"] = observed_at
            route["speed_knots"] = speed
            route["course_degrees"] = course
            route["points"].append({
                "observed_at": observed_at,
                "latitude": latitude,
                "longitude": longitude,
            })
        return list(routes.values())

    def historical_route_dates(self) -> dict[str, Any]:
        """Return locally imported historical-route dates without contacting NOAA."""
        root = self.settings.data_dir / "silver/noaa_ais_routes"
        available: list[date] = []
        for path in root.glob("date=*/routes.parquet"):
            try:
                available.append(date.fromisoformat(path.parent.name.removeprefix("date=")))
            except ValueError:
                continue
        available.sort()
        config = self.settings.sources.get("noaa_historical_ais", {})
        return {
            "earliest_available_date": available[0] if available else None,
            "latest_available_date": available[-1] if available else None,
            "date_count": len(available),
            "available_dates": available,
            "source": "noaa_marinecadastre",
            "source_label": config.get("source_label", "NOAA Historical AIS"),
            "coverage_label": config.get("coverage_label", "U.S. waters only"),
        }

    def historical_routes(
        self,
        selected: date,
        maximum_vessels: int = 800,
        maximum_points_per_vessel: int = 120,
    ) -> list[dict[str, Any]]:
        """Read a bounded route replay from one local NOAA date partition."""
        path = (
            self.settings.data_dir
            / "silver/noaa_ais_routes"
            / f"date={selected.isoformat()}"
            / "routes.parquet"
        )
        if not path.exists():
            return []
        with self.connection() as conn:
            rows = conn.execute(
                """
                WITH points AS (
                    SELECT * FROM read_parquet(?)
                    WHERE latitude BETWEEN -90 AND 90
                      AND longitude BETWEEN -180 AND 180
                ), active_vessels AS (
                    SELECT mmsi, max(observed_at) AS last_observed_at
                    FROM points GROUP BY mmsi
                    ORDER BY last_observed_at DESC LIMIT ?
                ), ranked_points AS (
                    SELECT p.*,
                           row_number() OVER (
                               PARTITION BY p.mmsi ORDER BY p.observed_at DESC
                           ) AS point_rank
                    FROM points p JOIN active_vessels a USING(mmsi)
                )
                SELECT mmsi, observed_at, vessel_name, latitude, longitude,
                       speed_knots, course_degrees
                FROM ranked_points
                WHERE point_rank <= ?
                ORDER BY mmsi, observed_at
                """,
                [str(path), maximum_vessels, maximum_points_per_vessel],
            ).fetchall()
        routes: dict[str, dict[str, Any]] = {}
        for mmsi, observed_at, vessel_name, latitude, longitude, speed, course in rows:
            route = routes.setdefault(
                str(mmsi),
                {
                    "mmsi": str(mmsi),
                    "vessel_name": vessel_name,
                    "last_observed_at": observed_at,
                    "speed_knots": speed,
                    "course_degrees": course,
                    "points": [],
                },
            )
            route["vessel_name"] = vessel_name or route["vessel_name"]
            route["last_observed_at"] = observed_at
            route["speed_knots"] = speed
            route["course_degrees"] = course
            route["points"].append(
                {
                    "observed_at": observed_at,
                    "latitude": latitude,
                    "longitude": longitude,
                }
            )
        return list(routes.values())

    def ais_route_status(self, active_minutes: int = 30) -> dict[str, Any]:
        cutoff = datetime.now(timezone.utc) - timedelta(minutes=active_minutes)
        with self.connection() as conn:
            point_count, latest = conn.execute(
                "SELECT count(*), max(observed_at) FROM ais_route_points"
            ).fetchone()
            active = conn.execute(
                "SELECT count(DISTINCT mmsi) FROM ais_route_points WHERE observed_at >= ?",
                [cutoff],
            ).fetchone()[0]
        return {
            "route_point_count": point_count,
            "active_route_vessel_count": active,
            "last_route_observation_at": latest,
        }

    def recent_ais_calls(self, since: datetime) -> dict[tuple[str, str], datetime]:
        with self.connection() as conn:
            rows = conn.execute(
                "SELECT mmsi, port_id, max(arrived_at) FROM ais_port_calls "
                "WHERE arrived_at >= ? GROUP BY mmsi, port_id",
                [since],
            ).fetchall()
        return {(str(mmsi), port_id): arrived_at.replace(tzinfo=timezone.utc) if arrived_at.tzinfo is None else arrived_at for mmsi, port_id, arrived_at in rows}

    def ais_dates(self) -> dict[str, Any]:
        with self.connection() as conn:
            first, latest, count = conn.execute(
                "SELECT min(call_date), max(call_date), count(DISTINCT call_date) FROM ais_port_calls"
            ).fetchone()
            available = [row[0] for row in conn.execute(
                "SELECT DISTINCT call_date FROM ais_port_calls ORDER BY call_date"
            ).fetchall()]
        return {
            "earliest_available_date": first,
            "latest_available_date": latest,
            "date_count": count,
            "available_dates": available,
        }

    def ais_snapshot(self, selected: date) -> list[dict[str, Any]]:
        """Return locally aggregated provisional calls and a calls-only score.

        Missing days are included as zeroes when computing the baseline. A score is
        withheld until seven prior collection days exist, avoiding a fabricated
        anomaly signal during initial collection.
        """
        with self.connection() as conn:
            first = conn.execute("SELECT min(call_date) FROM ais_port_calls").fetchone()[0]
            if first is None or selected < first:
                return []
            rows = conn.execute("""
                WITH selected_calls AS (
                    SELECT port_id, count(*)::DOUBLE AS port_calls,
                           count(DISTINCT mmsi)::DOUBLE AS unique_vessels,
                           max(last_seen_at) AS last_observation_at
                    FROM ais_port_calls WHERE call_date = ? GROUP BY port_id
                ), prior_daily AS (
                    SELECT p.port_id, days.day,
                           count(c.event_id)::DOUBLE AS port_calls
                    FROM ports p
                    CROSS JOIN generate_series(?, ? - INTERVAL 1 DAY, INTERVAL 1 DAY) AS days(day)
                    LEFT JOIN ais_port_calls c
                      ON c.port_id = p.port_id AND c.call_date = CAST(days.day AS DATE)
                    GROUP BY p.port_id, days.day
                ), baseline AS (
                    SELECT port_id, avg(port_calls) AS calls_expected,
                           stddev_samp(port_calls) AS calls_std,
                           count(*) AS baseline_days
                    FROM prior_daily GROUP BY port_id
                )
                SELECT p.port_id, p.name, p.country, p.latitude, p.longitude,
                       s.port_calls, s.unique_vessels, b.calls_expected,
                       b.baseline_days,
                       CASE WHEN b.baseline_days >= 7 AND b.calls_std > 0
                            THEN greatest(-3.5, least(3.5, (s.port_calls-b.calls_expected)/b.calls_std))
                            ELSE NULL END AS calls_z_score,
                       CASE WHEN b.baseline_days >= 7 AND b.calls_std > 0
                            THEN round(greatest(0, least(100, 50 + 50 *
                                 greatest(-3.5, least(3.5, (s.port_calls-b.calls_expected)/b.calls_std))/3.5)), 1)
                            ELSE NULL END AS activity_score,
                       s.last_observation_at, p.importance
                FROM selected_calls s
                JOIN ports p USING(port_id)
                LEFT JOIN baseline b USING(port_id)
                ORDER BY s.port_calls DESC, p.importance DESC
            """, [selected, first, selected]).fetchall()
            names = [description[0] for description in conn.description]
        return self._with_port_profiles([self._record(names, row) for row in rows])

    def ais_port_history(self, port_id: str, start: date | None, end: date | None) -> list[dict[str, Any]]:
        first = start
        last = end
        with self.connection() as conn:
            if first is None or last is None:
                bounds = conn.execute(
                    "SELECT min(call_date), max(call_date) FROM ais_port_calls WHERE port_id = ?", [port_id]
                ).fetchone()
                first = first or bounds[0]
                last = last or bounds[1]
            if first is None or last is None:
                return []
            rows = conn.execute("""
                SELECT CAST(days.day AS DATE) AS date,
                       count(c.event_id)::DOUBLE AS port_calls,
                       count(DISTINCT c.mmsi)::DOUBLE AS unique_vessels
                FROM generate_series(?, ?, INTERVAL 1 DAY) AS days(day)
                LEFT JOIN ais_port_calls c
                  ON c.port_id = ? AND c.call_date = CAST(days.day AS DATE)
                GROUP BY days.day ORDER BY days.day
            """, [first, last, port_id]).fetchall()
            names = [description[0] for description in conn.description]
        return [self._record(names, row) for row in rows]

    def ais_status(self) -> dict[str, Any]:
        dates = self.ais_dates()
        with self.connection() as conn:
            count, vessels, last_seen = conn.execute(
                "SELECT count(*), count(DISTINCT mmsi), max(last_seen_at) FROM ais_port_calls"
            ).fetchone()
        dates.update({"port_call_count": count, "unique_vessel_count": vessels, "last_observation_at": last_seen})
        return dates

    def upsert_manifest(self, values: dict[str, Any]):
        columns = list(values)
        assignments = ",".join(f"{c}=excluded.{c}" for c in columns if c not in ("source", "dataset"))
        prepared = [json.dumps(v) if c == "metadata" else v for c, v in values.items()]
        with self.connection() as conn:
            conn.execute(f"INSERT INTO ingestion_manifest ({','.join(columns)}) VALUES ({','.join('?' for _ in columns)}) ON CONFLICT(source,dataset) DO UPDATE SET {assignments}", prepared)
    @staticmethod
    def _record(names, row):
        return {name: (None if isinstance(value, float) and not math.isfinite(value) else value) for name, value in zip(names, row)}
