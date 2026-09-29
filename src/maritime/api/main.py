from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import date
from functools import lru_cache

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from maritime.config import load_config
from maritime.ingestion.aisstream import AisStreamCollector
from maritime.models import (
    GlobeSnapshot,
    LiveGlobeSnapshot,
    LivePortSnapshot,
    NoaaWeeklyImportStatus,
    PortSnapshot,
    RefreshStatus,
)
from maritime.services.noaa_weekly_history import NoaaWeeklyHistoryService
from maritime.services.refresh import MaritimeRefreshService
from maritime.storage.database import MaritimeDatabase

settings = load_config()
database = MaritimeDatabase(settings)
ais_collector = AisStreamCollector(settings, database)
noaa_import_service = NoaaWeeklyHistoryService(settings, database)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    yield
    await ais_collector.stop()
    await noaa_import_service.stop()


app = FastAPI(title="Maritime Intelligence Globe API", version="0.1.0", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["http://127.0.0.1:5173","http://localhost:5173"], allow_methods=["*"], allow_headers=["*"])


@app.get("/api/health")
def health():
    return {"status":"ok", "mode":"local-first"}


@app.get("/api/dates")
def dates():
    return database.dates()


def analytics_revision() -> tuple[int, int, int]:
    path = database.settings.data_dir / "gold/port_daily_anomalies.parquet"
    try:
        stat = path.stat()
    except FileNotFoundError:
        return (0, 0, 0)
    return (stat.st_mtime_ns, stat.st_size, stat.st_ino)


@lru_cache(maxsize=48)
def cached_snapshot(selected: date, _revision: tuple[int, int, int]):
    return database.snapshot(selected)


refresh_service = MaritimeRefreshService(settings, database, cached_snapshot.cache_clear)


@app.get("/api/globe/snapshot", response_model=GlobeSnapshot)
def globe_snapshot(date_: date = Query(alias="date")):
    rows = cached_snapshot(date_, analytics_revision())
    return GlobeSnapshot(date=date_, ports=[PortSnapshot(**row) for row in rows])


@app.get("/api/ports/{port_id}")
def port(port_id: str, date_: date | None = Query(default=None, alias="date")):
    result = database.port(port_id, date_)
    if not result: raise HTTPException(404, "Port not found")
    return result


@app.get("/api/ports/{port_id}/history")
def port_history(port_id: str, start: date | None = None, end: date | None = None):
    if start and end and start > end: raise HTTPException(422, "start must be on or before end")
    return {"port_id":port_id, "start":start, "end":end, "observations":database.history(port_id, start, end)}


@app.get("/api/anomalies/top")
def top_anomalies(date_: date = Query(alias="date"), limit: int = Query(10, ge=1, le=100)):
    return {"date":date_, "ports":database.top_anomalies(date_, limit)}


@app.get("/api/data/status")
def data_status():
    return database.status()


@app.get("/api/live/status")
def live_status():
    return ais_collector.status()


@app.post("/api/live/start", status_code=202)
async def start_live_collection():
    return await ais_collector.start()


@app.post("/api/live/stop")
async def stop_live_collection():
    return await ais_collector.stop()


@app.get("/api/live/snapshot", response_model=LiveGlobeSnapshot)
def live_snapshot(date_: date = Query(alias="date")):
    rows = database.ais_snapshot(date_)
    return LiveGlobeSnapshot(date=date_, ports=[LivePortSnapshot(**row) for row in rows])


@app.get("/api/live/routes")
def live_routes(
    hours: int = Query(default=6, ge=1, le=24),
    limit: int = Query(default=800, ge=1, le=2000),
    points: int = Query(default=40, ge=2, le=120),
):
    """Return a bounded local snapshot of recently sampled vessel tracks."""
    config = settings.sources.get("aisstream", {})
    bounded_hours = min(hours, int(config.get("route_api_hours", 6)))
    bounded_limit = min(limit, int(config.get("route_api_maximum_vessels", 800)))
    bounded_points = min(
        points, int(config.get("route_api_maximum_points_per_vessel", 40))
    )
    routes = database.ais_routes(bounded_hours, bounded_limit, bounded_points)
    return {
        "hours": bounded_hours,
        "vessel_count": len(routes),
        "provisional": True,
        "routes": routes,
    }


@app.get("/api/historical/routes/dates")
def historical_route_dates():
    """List only the NOAA route partitions already present on local storage."""
    return database.historical_route_dates()


@app.get("/api/historical/routes")
def historical_routes(
    date_: date = Query(alias="date"),
    limit: int = Query(default=800, ge=1, le=2000),
    points: int = Query(default=120, ge=2, le=240),
):
    """Return a bounded, local NOAA historical route replay for one UTC date."""
    config = settings.sources.get("noaa_historical_ais", {})
    bounded_limit = min(limit, int(config.get("route_api_maximum_vessels", 800)))
    bounded_points = min(
        points, int(config.get("route_api_maximum_points_per_vessel", 120))
    )
    routes = database.historical_routes(date_, bounded_limit, bounded_points)
    return {
        "date": date_,
        "vessel_count": len(routes),
        "provisional": False,
        "historical": True,
        "source": "noaa_marinecadastre",
        "source_label": config.get("source_label", "NOAA Historical AIS"),
        "coverage_label": config.get("coverage_label", "U.S. waters only"),
        "routes": routes,
    }


@app.post(
    "/api/historical/import",
    response_model=NoaaWeeklyImportStatus,
    status_code=202,
)
async def start_historical_import():
    """Start or resume the fixed weekly national NOAA history job."""
    return await noaa_import_service.start()


@app.get(
    "/api/historical/import/status",
    response_model=NoaaWeeklyImportStatus,
)
def historical_import_status():
    return noaa_import_service.status()


@app.post(
    "/api/historical/import/cancel",
    response_model=NoaaWeeklyImportStatus,
)
async def cancel_historical_import():
    return await noaa_import_service.cancel()


@app.get("/api/live/ports/{port_id}/history")
def live_port_history(port_id: str, start: date | None = None, end: date | None = None):
    if start and end and start > end:
        raise HTTPException(422, "start must be on or before end")
    return {
        "port_id": port_id,
        "start": start,
        "end": end,
        "provisional": True,
        "observations": database.ais_port_history(port_id, start, end),
    }


@app.post("/api/refresh", response_model=RefreshStatus, status_code=202)
async def refresh():
    cached_snapshot.cache_clear()
    return await refresh_service.start()


@app.get("/api/refresh/status", response_model=RefreshStatus)
def refresh_status():
    return refresh_service.status()


if settings.frontend_dist.exists():
    assets = settings.frontend_dist / "assets"
    cesium = settings.frontend_dist / "cesium"
    if assets.exists(): app.mount("/assets", StaticFiles(directory=assets), name="assets")
    if cesium.exists(): app.mount("/cesium", StaticFiles(directory=cesium), name="cesium")

    @app.get("/{path:path}", include_in_schema=False)
    def frontend(path: str):
        candidate = settings.frontend_dist / path
        return FileResponse(candidate if path and candidate.is_file() else settings.frontend_dist / "index.html")
