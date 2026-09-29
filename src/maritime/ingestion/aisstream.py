from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import math
import os
import random
from collections import deque
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

import websockets

from maritime.config import Settings
from maritime.storage.database import MaritimeDatabase

log = logging.getLogger(__name__)


@dataclass
class VisitState:
    port_id: str
    entered_at: datetime
    last_seen_at: datetime
    call_recorded: bool = False


@dataclass
class RoutePoint:
    mmsi: str
    observed_at: datetime
    vessel_name: str | None
    latitude: float
    longitude: float
    speed_knots: float | None
    course_degrees: float | None
    message_type: str


@dataclass
class CollectorStatus:
    state: str = "stopped"
    stage: str = "Live AIS is off"
    configured: bool = False
    connected_at: datetime | None = None
    last_message_at: datetime | None = None
    messages_received: int = 0
    calls_detected: int = 0
    reconnects: int = 0
    subscription_box_count: int = 0
    subscription_coverage_pct: float = 0
    messages_per_minute: int = 0
    megabytes_per_minute: float = 0
    route_points_persisted: int = 0
    active_route_vessels: int = 0
    pending_route_points: int = 0
    last_route_flush_at: datetime | None = None
    error: str | None = None


class SafetyLimitExceeded(RuntimeError):
    """Stop rather than consume a stream broader than the configured budget."""


def build_port_subscription_boxes(
    ports: list[dict[str, Any]], padding_km: float = 12, tile_degrees: float = 5,
) -> list[list[list[float]]]:
    """Create compact NW/SE boxes covering every local port plus padding.

    Ports are grouped into coarse tiles, but each emitted rectangle is tightened
    to the actual ports in that tile. This keeps the number of subscription
    regions manageable without subscribing to empty oceans or inland areas.
    """
    if not ports:
        return []
    groups: dict[tuple[int, int], list[tuple[float, float, float, float]]] = {}
    for port in ports:
        latitude, longitude = float(port["latitude"]), float(port["longitude"])
        lat_pad = padding_km / 111.0
        lon_pad = padding_km / (111.0 * max(abs(math.cos(math.radians(latitude))), 0.08))
        key = (
            math.floor((latitude + 90) / tile_degrees),
            math.floor((longitude + 180) / tile_degrees),
        )
        groups.setdefault(key, []).append(
            (latitude - lat_pad, latitude + lat_pad, longitude - lon_pad, longitude + lon_pad)
        )
    boxes: list[list[list[float]]] = []
    for bounds in groups.values():
        south = max(-90.0, min(item[0] for item in bounds))
        north = min(90.0, max(item[1] for item in bounds))
        raw_west = min(item[2] for item in bounds)
        raw_east = max(item[3] for item in bounds)
        west, east = max(-180.0, raw_west), min(180.0, raw_east)
        boxes.append([[round(north, 5), round(west, 5)], [round(south, 5), round(east, 5)]])
        if raw_east > 180:
            boxes.append([[round(north, 5), -180.0], [round(south, 5), round(raw_east - 360, 5)]])
        if raw_west < -180:
            boxes.append([[round(north, 5), round(raw_west + 360, 5)], [round(south, 5), 180.0]])
    return boxes


def approximate_subscription_coverage_pct(boxes: list[list[list[float]]]) -> float:
    """Return a conservative spherical area estimate for status/guardrails."""
    area_fraction = 0.0
    for (north, west), (south, east) in boxes:
        longitude_fraction = max(0.0, east - west) / 360.0
        latitude_fraction = (math.sin(math.radians(north)) - math.sin(math.radians(south))) / 2.0
        area_fraction += longitude_fraction * latitude_fraction
    return round(min(100.0, area_fraction * 100), 3)


class PortGrid:
    """Small dependency-free geographic index for point-based port geofences."""

    def __init__(self, ports: list[dict[str, Any]], radius_km: float):
        self.radius_km = radius_km
        self.ports = {port["port_id"]: port for port in ports}
        self.cells: dict[tuple[int, int], list[dict[str, Any]]] = {}
        for port in ports:
            key = (math.floor(float(port["latitude"])), math.floor(float(port["longitude"])))
            self.cells.setdefault(key, []).append(port)

    @staticmethod
    def _distance_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        radius = 6371.0088
        p1, p2 = math.radians(lat1), math.radians(lat2)
        dp = math.radians(lat2 - lat1)
        dl = math.radians(lon2 - lon1)
        a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
        return 2 * radius * math.asin(math.sqrt(a))

    def nearest(self, latitude: float, longitude: float) -> dict[str, Any] | None:
        lat_span = max(1, math.ceil(self.radius_km / 111.0))
        lon_km = 111.0 * max(abs(math.cos(math.radians(latitude))), 0.08)
        lon_span = max(1, math.ceil(self.radius_km / lon_km))
        lat_cell, lon_cell = math.floor(latitude), math.floor(longitude)
        nearest_port, nearest_distance = None, self.radius_km
        for lat_offset in range(-lat_span, lat_span + 1):
            for lon_offset in range(-lon_span, lon_span + 1):
                for port in self.cells.get((lat_cell + lat_offset, lon_cell + lon_offset), []):
                    distance = self._distance_km(
                        latitude, longitude, float(port["latitude"]), float(port["longitude"])
                    )
                    if distance <= nearest_distance:
                        nearest_port, nearest_distance = port, distance
        return nearest_port


class AisStreamCollector:
    """Explicitly started global AIS ingestion with sampled local tracks.

    The frontend never connects to AISStream. It reads the resulting local DuckDB
    events through FastAPI, preserving the application's local-first query path.
    Incoming events are read continuously but coalesced and batch-written so the
    database never receives one transaction per position report.
    """

    def __init__(self, settings: Settings, database: MaritimeDatabase):
        self.settings = settings
        self.database = database
        self.config = settings.sources.get("aisstream", {})
        self.api_key = os.getenv("AISSTREAM_API_KEY", "").strip()
        self.status_value = CollectorStatus(configured=bool(self.api_key))
        self._task: asyncio.Task | None = None
        self._stop = asyncio.Event()
        self._visits: dict[str, VisitState] = {}
        self._last_processed: dict[str, datetime] = {}
        self._traffic_window: deque[tuple[datetime, int]] = deque()
        self._traffic_bytes = 0
        self._last_cache_prune = datetime.now(timezone.utc)
        self._last_route_prune = datetime.now(timezone.utc)
        maintenance_hours = float(self.config.get("maintenance_interval_hours", 24))
        self._last_database_maintenance = (
            datetime.now(timezone.utc) - timedelta(hours=maintenance_hours)
        )
        self._last_manifest_write: datetime | None = None
        self._subscription_boxes: list[list[list[float]]] = []
        self._pending_route_points: dict[str, RoutePoint] = {}
        self._last_route_points: dict[str, RoutePoint] = {}
        self._route_flush_lock = asyncio.Lock()
        route_status = database.ais_route_status()
        self.status_value.route_points_persisted = int(route_status["route_point_count"])
        self.status_value.active_route_vessels = int(route_status["active_route_vessel_count"])
        cooldown_hours = float(self.config.get("same_port_cooldown_hours", 6))
        self._cooldown = timedelta(hours=cooldown_hours)
        self._recent_calls = database.recent_ais_calls(datetime.now(timezone.utc) - self._cooldown)
        self._grid = PortGrid(database.all_ports(), float(self.config.get("port_radius_km", 8)))

    def status(self) -> dict[str, Any]:
        result = asdict(self.status_value)
        result.update(self.database.ais_status())
        return result

    async def start(self) -> dict[str, Any]:
        self.api_key = os.getenv("AISSTREAM_API_KEY", "").strip()
        self.status_value.configured = bool(self.api_key)
        if not self.api_key:
            self.status_value.state = "configuration_required"
            self.status_value.stage = "Add an AISStream API key to start live collection"
            self.status_value.error = None
            return self.status()
        if self._task and not self._task.done():
            return self.status()
        ports = self.database.all_ports()
        self._grid = PortGrid(ports, float(self.config.get("port_radius_km", 8)))
        subscription_mode = str(self.config.get("subscription_mode", "global")).lower()
        if subscription_mode == "global":
            if not self.config.get("allow_global_subscription", False):
                self.status_value.state = "configuration_required"
                self.status_value.stage = "Global AIS requires explicit configuration"
                return self.status()
            self._subscription_boxes = [[[90.0, -180.0], [-90.0, 180.0]]]
        else:
            self._subscription_boxes = build_port_subscription_boxes(
                ports,
                float(self.config.get("subscription_padding_km", 12)),
                float(self.config.get("subscription_tile_degrees", 5)),
            )
        maximum_boxes = int(self.config.get("maximum_subscription_boxes", 500))
        coverage_pct = approximate_subscription_coverage_pct(self._subscription_boxes)
        maximum_coverage_pct = float(self.config.get("maximum_subscription_coverage_pct", 100))
        if not self._subscription_boxes:
            self.status_value.state = "configuration_required"
            self.status_value.stage = "Live AIS needs a local port catalog"
            return self.status()
        if len(self._subscription_boxes) > maximum_boxes:
            self.status_value.state = "configuration_required"
            self.status_value.stage = "Generated AIS regions exceed the configured safety limit"
            self.status_value.error = f"{len(self._subscription_boxes)} boxes exceeds limit {maximum_boxes}"
            return self.status()
        if coverage_pct > maximum_coverage_pct:
            self.status_value.state = "configuration_required"
            self.status_value.stage = "Generated AIS regions exceed the configured area safety limit"
            self.status_value.error = f"{coverage_pct:g}% coverage exceeds limit {maximum_coverage_pct:g}%"
            return self.status()
        self._visits.clear()
        self._last_processed.clear()
        self._pending_route_points.clear()
        self._last_route_points.clear()
        self._traffic_window.clear()
        self._traffic_bytes = 0
        self._stop = asyncio.Event()
        route_status = self.database.ais_route_status()
        self.status_value = CollectorStatus(
            state="starting", stage="Connecting to the live AIS feed…", configured=True,
            subscription_box_count=len(self._subscription_boxes),
            subscription_coverage_pct=coverage_pct,
            route_points_persisted=int(route_status["route_point_count"]),
            active_route_vessels=int(route_status["active_route_vessel_count"]),
        )
        self._task = asyncio.create_task(self._run())
        return self.status()

    async def stop(self) -> dict[str, Any]:
        self._stop.set()
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        await self._flush_routes()
        self.status_value.state = "stopped"
        self.status_value.stage = "Live AIS is off"
        self._write_manifest("stopped", force=True)
        return self.status()

    async def _run(self):
        backoff = float(self.config.get("reconnect_initial_seconds", 5))
        maximum_backoff = float(self.config.get("reconnect_maximum_seconds", 300))
        maximum_failures = int(self.config.get("maximum_consecutive_failures", 8))
        consecutive_failures = 0
        while not self._stop.is_set():
            try:
                await self._consume()
                backoff = float(self.config.get("reconnect_initial_seconds", 5))
                consecutive_failures = 0
            except asyncio.CancelledError:
                raise
            except SafetyLimitExceeded as exc:
                self.status_value.state = "paused"
                self.status_value.stage = "Live AIS paused by the local traffic safety limit"
                self.status_value.error = str(exc)
                self._write_manifest("paused", str(exc), force=True)
                return
            except Exception as exc:
                log.warning("AISStream connection interrupted: %s", exc)
                consecutive_failures += 1
                error_text = str(exc).lower()
                authentication_failure = any(
                    marker in error_text
                    for marker in ("api key", "authentication", "unauthorized", "forbidden", "401", "403")
                )
                if authentication_failure or consecutive_failures >= maximum_failures:
                    self.status_value.state = "paused"
                    self.status_value.stage = (
                        "Live AIS paused; check the API key" if authentication_failure
                        else "Live AIS paused after repeated connection failures"
                    )
                    self.status_value.error = str(exc)
                    self._write_manifest("paused", str(exc), force=True)
                    return
                self.status_value.state = "reconnecting"
                self.status_value.stage = "Live AIS interrupted; reconnecting…"
                self.status_value.error = str(exc)
                self.status_value.reconnects += 1
                self._write_manifest("reconnecting", str(exc), force=True)
                try:
                    await asyncio.wait_for(self._stop.wait(), timeout=backoff * random.uniform(0.8, 1.2))
                except TimeoutError:
                    pass
                backoff = min(backoff * 2, maximum_backoff)

    async def _consume(self):
        endpoint = self.config.get("websocket_url", "wss://stream.aisstream.io/v0/stream")
        async with websockets.connect(
            endpoint, compression="deflate", max_size=262_144, max_queue=32,
            ping_interval=20, ping_timeout=20, open_timeout=10,
        ) as socket:
            subscription = {
                "APIKey": self.api_key,
                "BoundingBoxes": self._subscription_boxes,
                "FilterMessageTypes": [
                    "PositionReport", "StandardClassBPositionReport",
                    "ExtendedClassBPositionReport",
                ],
            }
            await socket.send(json.dumps(subscription))
            confirmation_raw = await asyncio.wait_for(socket.recv(), timeout=10)
            self._record_traffic(len(confirmation_raw))
            confirmation = json.loads(
                confirmation_raw.decode("utf-8") if isinstance(confirmation_raw, bytes) else confirmation_raw
            )
            if confirmation.get("MessageType") != "SubscriptionConfirmation":
                raise RuntimeError("AISStream did not confirm the global subscription")
            compression_enabled = self._deep_value(confirmation.get("Message") or {}, "CompressionEnabled")
            if compression_enabled is not True:
                raise SafetyLimitExceeded("AISStream compression was not negotiated")
            self.status_value.state = "running"
            self.status_value.stage = "Receiving global AIS; routes and port calls are provisional"
            self.status_value.connected_at = datetime.now(timezone.utc)
            self.status_value.error = None
            self._write_manifest("running", force=True)
            flush_task = asyncio.create_task(self._route_flush_loop())
            try:
                async for raw in socket:
                    if self._stop.is_set():
                        break
                    self._record_traffic(len(raw))
                    event = json.loads(raw.decode("utf-8") if isinstance(raw, bytes) else raw)
                    await self.process_message(event)
            finally:
                flush_task.cancel()
                try:
                    await flush_task
                except asyncio.CancelledError:
                    pass
                await self._flush_routes()
            if not self._stop.is_set():
                raise RuntimeError("AISStream connection closed unexpectedly")

    async def process_message(self, event: dict[str, Any], observed_at: datetime | None = None):
        """Process one decoded message; public for deterministic fixture tests."""
        now = observed_at or datetime.now(timezone.utc)
        self.status_value.messages_received += 1
        self.status_value.last_message_at = now
        metadata = event.get("MetaData") or {}
        mmsi = metadata.get("MMSI") or self._deep_value(event.get("Message") or {}, "UserID")
        if mmsi is None:
            return
        mmsi = str(mmsi)
        minimum_interval = timedelta(
            seconds=float(self.config.get("process_each_vessel_every_seconds", 60))
        )
        last_processed = self._last_processed.get(mmsi)
        if last_processed is not None and now - last_processed < minimum_interval:
            return
        self._last_processed[mmsi] = now
        self._prune_caches(now)
        latitude, longitude = metadata.get("Latitude"), metadata.get("Longitude")
        if latitude is None or longitude is None:
            latitude = self._deep_value(event.get("Message") or {}, "Latitude")
            longitude = self._deep_value(event.get("Message") or {}, "Longitude")
        try:
            latitude, longitude = float(latitude), float(longitude)
        except (TypeError, ValueError):
            return
        if not (-90 <= latitude <= 90 and -180 <= longitude <= 180):
            return
        speed = self._float_value(self._deep_value(event.get("Message") or {}, "Sog"))
        course = self._float_value(self._deep_value(event.get("Message") or {}, "Cog"))
        self._consider_route_point(RoutePoint(
            mmsi=mmsi,
            observed_at=now,
            vessel_name=self._string_value(metadata.get("ShipName")),
            latitude=latitude,
            longitude=longitude,
            speed_knots=speed,
            course_degrees=course,
            message_type=str(event.get("MessageType") or "PositionReport"),
        ))
        if len(self._pending_route_points) >= int(
            self.config.get("route_batch_max_points", 1_500)
        ):
            await self._flush_routes()
        port = self._grid.nearest(latitude, longitude)
        if port is None:
            self._visits.pop(mmsi, None)
            return
        state = self._visits.get(mmsi)
        max_gap = timedelta(minutes=float(self.config.get("maximum_observation_gap_minutes", 30)))
        if state is None or state.port_id != port["port_id"] or now - state.last_seen_at > max_gap:
            state = VisitState(port_id=port["port_id"], entered_at=now, last_seen_at=now)
            self._visits[mmsi] = state
            return
        state.last_seen_at = now
        if speed is None:
            return
        dwell = timedelta(minutes=float(self.config.get("minimum_dwell_minutes", 20)))
        max_speed = float(self.config.get("maximum_dwell_speed_knots", 3))
        if state.call_recorded or now - state.entered_at < dwell or speed > max_speed:
            return
        previous = self._recent_calls.get((mmsi, state.port_id))
        if previous is not None and now - previous < self._cooldown:
            state.call_recorded = True
            return
        vessel_name = self._string_value(metadata.get("ShipName"))
        event_id = hashlib.sha256(
            f"{mmsi}|{state.port_id}|{state.entered_at.isoformat()}".encode("utf-8")
        ).hexdigest()[:32]
        inserted = self.database.record_ais_port_call({
            "event_id": event_id,
            "call_date": state.entered_at.date(),
            "port_id": state.port_id,
            "mmsi": mmsi,
            "vessel_name": vessel_name,
            "vessel_type": None,
            "arrived_at": state.entered_at,
            "last_seen_at": now,
            "confidence": "provisional_point_geofence",
            "source": "aisstream",
        })
        state.call_recorded = True
        self._recent_calls[(mmsi, state.port_id)] = state.entered_at
        if inserted:
            self.status_value.calls_detected += 1
            self._write_manifest("running")

    def _consider_route_point(self, point: RoutePoint) -> None:
        previous = self._last_route_points.get(point.mmsi)
        should_keep = previous is None
        if previous is not None:
            elapsed = (point.observed_at - previous.observed_at).total_seconds()
            minimum_interval = float(self.config.get("route_minimum_interval_seconds", 120))
            force_interval = float(self.config.get("route_force_interval_seconds", 600))
            if elapsed >= force_interval:
                should_keep = True
            elif elapsed >= minimum_interval:
                distance = PortGrid._distance_km(
                    previous.latitude, previous.longitude, point.latitude, point.longitude
                )
                course_change = self._course_change(
                    previous.course_degrees, point.course_degrees
                )
                should_keep = (
                    distance >= float(self.config.get("route_minimum_distance_km", 1))
                    or course_change
                    >= float(self.config.get("route_minimum_course_change_degrees", 15))
                )
        if not should_keep:
            return
        self._last_route_points[point.mmsi] = point
        self._pending_route_points[point.mmsi] = point
        self.status_value.active_route_vessels = len(self._last_route_points)
        self.status_value.pending_route_points = len(self._pending_route_points)

    async def _route_flush_loop(self) -> None:
        interval = float(self.config.get("route_batch_interval_seconds", 15))
        while not self._stop.is_set():
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=interval)
            except TimeoutError:
                await self._flush_routes()
                await self._prune_route_storage(datetime.now(timezone.utc))

    async def _flush_routes(self) -> None:
        async with self._route_flush_lock:
            if not self._pending_route_points:
                return
            batch = list(self._pending_route_points.values())
            self._pending_route_points.clear()
            self.status_value.pending_route_points = 0
            values = [asdict(point) for point in batch]
            try:
                persisted = await asyncio.to_thread(
                    self.database.record_ais_route_points, values
                )
            except Exception:
                for point in batch:
                    current = self._pending_route_points.get(point.mmsi)
                    if current is None or current.observed_at < point.observed_at:
                        self._pending_route_points[point.mmsi] = point
                self.status_value.pending_route_points = len(self._pending_route_points)
                raise
            self.status_value.route_points_persisted += persisted
            self.status_value.last_route_flush_at = datetime.now(timezone.utc)
            self._write_manifest("running")

    async def _prune_route_storage(self, now: datetime) -> None:
        if now - self._last_route_prune < timedelta(hours=1):
            return
        retention = timedelta(hours=float(self.config.get("route_retention_hours", 72)))
        await asyncio.to_thread(self.database.prune_ais_route_points, now - retention)
        self._last_route_prune = now
        maintenance_interval = timedelta(
            hours=float(self.config.get("maintenance_interval_hours", 24))
        )
        if now - self._last_database_maintenance < maintenance_interval:
            return
        port_call_retention = timedelta(
            days=float(self.config.get("port_call_retention_days", 180))
        )
        try:
            result = await asyncio.to_thread(
                self.database.maintain_ais_storage,
                now - retention,
                (now - port_call_retention).date(),
            )
            log.info(
                "AIS storage maintenance removed %s route points and %s port calls",
                result["route_points_removed"],
                result["port_calls_removed"],
            )
            self._last_database_maintenance = now
        except Exception:
            # Maintenance must never stop the live collector. The hourly route
            # prune already succeeded, and this bounded pass can retry later.
            log.exception("AIS storage maintenance failed; live collection continues")

    def _record_traffic(self, message_bytes: int):
        now = datetime.now(timezone.utc)
        self._traffic_window.append((now, message_bytes))
        self._traffic_bytes += message_bytes
        cutoff = now - timedelta(minutes=1)
        while self._traffic_window and self._traffic_window[0][0] < cutoff:
            _, expired_bytes = self._traffic_window.popleft()
            self._traffic_bytes -= expired_bytes
        self.status_value.messages_per_minute = len(self._traffic_window)
        self.status_value.megabytes_per_minute = round(self._traffic_bytes / 1_048_576, 2)
        maximum_messages = int(self.config.get("maximum_messages_per_minute", 12_000))
        maximum_megabytes = float(self.config.get("maximum_megabytes_per_minute", 8))
        if len(self._traffic_window) > maximum_messages:
            raise SafetyLimitExceeded(
                f"Received more than {maximum_messages:,} messages in one minute"
            )
        if self._traffic_bytes > maximum_megabytes * 1_048_576:
            raise SafetyLimitExceeded(
                f"Received more than {maximum_megabytes:g} MB in one minute"
            )

    def _prune_caches(self, now: datetime):
        if now - self._last_cache_prune < timedelta(minutes=5):
            return
        cutoff = now - timedelta(hours=float(self.config.get("cache_ttl_hours", 6)))
        self._visits = {
            mmsi: state for mmsi, state in self._visits.items() if state.last_seen_at >= cutoff
        }
        self._last_processed = {
            mmsi: seen_at for mmsi, seen_at in self._last_processed.items() if seen_at >= cutoff
        }
        recent_cutoff = now - self._cooldown
        self._recent_calls = {
            key: arrived_at for key, arrived_at in self._recent_calls.items() if arrived_at >= recent_cutoff
        }
        self._last_route_points = {
            mmsi: point
            for mmsi, point in self._last_route_points.items()
            if point.observed_at >= cutoff
        }
        self.status_value.active_route_vessels = len(self._last_route_points)
        self._last_cache_prune = now

    def _write_manifest(self, status: str, error: str | None = None, force: bool = False):
        now = datetime.now(timezone.utc)
        interval = timedelta(seconds=float(self.config.get("manifest_write_interval_seconds", 60)))
        if not force and self._last_manifest_write is not None and now - self._last_manifest_write < interval:
            return
        local_max = self.database.ais_dates()["latest_available_date"]
        values = {
            "source": "aisstream",
            "dataset": "provisional_port_calls",
            "local_max_date": local_max,
            "last_checked": now,
            "rows_downloaded": self.status_value.messages_received,
            "rows_inserted": self.database.ais_status()["port_call_count"],
            "rows_updated": 0,
            "status": status,
            "error_message": error,
            "metadata": {
                "method": "point geofence + dwell",
                "radius_km": float(self.config.get("port_radius_km", 8)),
                "minimum_dwell_minutes": float(self.config.get("minimum_dwell_minutes", 20)),
                "subscription_box_count": len(self._subscription_boxes),
                "subscription_coverage_pct": approximate_subscription_coverage_pct(
                    self._subscription_boxes
                ),
                "message_types": [
                    "PositionReport", "StandardClassBPositionReport", "ExtendedClassBPositionReport"
                ],
                "route_sampling": {
                    "minimum_interval_seconds": float(
                        self.config.get("route_minimum_interval_seconds", 120)
                    ),
                    "force_interval_seconds": float(
                        self.config.get("route_force_interval_seconds", 600)
                    ),
                    "retention_hours": float(self.config.get("route_retention_hours", 72)),
                    "batch_interval_seconds": float(
                        self.config.get("route_batch_interval_seconds", 15)
                    ),
                },
                "port_call_retention_days": float(
                    self.config.get("port_call_retention_days", 180)
                ),
                "maintenance_interval_hours": float(
                    self.config.get("maintenance_interval_hours", 24)
                ),
                "messages_per_minute": self.status_value.messages_per_minute,
                "megabytes_per_minute": self.status_value.megabytes_per_minute,
                "route_points_persisted": self.status_value.route_points_persisted,
                "provisional": True,
            },
        }
        if status == "running":
            values["last_successful_refresh"] = now
            values["upstream_max_date"] = now.date()
        self.database.upsert_manifest(values)
        self._last_manifest_write = now

    @staticmethod
    def _deep_value(value: Any, key: str) -> Any:
        if isinstance(value, dict):
            if key in value:
                return value[key]
            for nested in value.values():
                found = AisStreamCollector._deep_value(nested, key)
                if found is not None:
                    return found
        elif isinstance(value, list):
            for nested in value:
                found = AisStreamCollector._deep_value(nested, key)
                if found is not None:
                    return found
        return None

    @staticmethod
    def _string_value(value: Any) -> str | None:
        if value is None:
            return None
        text = str(value).strip()
        return text or None

    @staticmethod
    def _float_value(value: Any) -> float | None:
        try:
            number = float(value)
        except (TypeError, ValueError):
            return None
        return number if math.isfinite(number) else None

    @staticmethod
    def _course_change(previous: float | None, current: float | None) -> float:
        if previous is None or current is None:
            return 0
        difference = abs((current - previous) % 360)
        return min(difference, 360 - difference)
