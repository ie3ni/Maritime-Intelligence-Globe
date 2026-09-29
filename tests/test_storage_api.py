from datetime import date, datetime, timedelta, timezone
from io import BytesIO
from zipfile import ZIP_DEFLATED, ZipFile

import polars as pl
import pytest
from fastapi.testclient import TestClient

from maritime.api import main as api_module
from maritime.config import Settings, load_config
from maritime.ingestion.aisstream import (
    AisStreamCollector,
    SafetyLimitExceeded,
    approximate_subscription_coverage_pct,
    build_port_subscription_boxes,
)
from maritime.ingestion.noaa_ais import (
    GeographicBounds,
    NoaaHistoricalAisProvider,
    normalize_noaa_routes,
)
from maritime.ingestion.noaa_bulk import NoaaBulkDailyImporter, weekly_sample_dates
from maritime.services.refresh import MaritimeRefreshService
from maritime.services.noaa_history import NoaaHistoricalImportService
from maritime.services.noaa_weekly_history import NoaaWeeklyHistoryService
from maritime.storage.database import MaritimeDatabase, classify_port_function
from maritime.storage.parquet import atomic_write


def make_database(tmp_path):
    base = load_config()
    settings = Settings(root=tmp_path,host="127.0.0.1",port=8000,data_dir=tmp_path/"data",database_path=tmp_path/"database/maritime.duckdb",frontend_dist=tmp_path/"dist",app=base.app,network=base.network,sources=base.sources,storage=base.storage,analytics=base.analytics,visualization=base.visualization)
    ports=pl.DataFrame({"port_id":["NLRTM"],"name":["Rotterdam"],"country":["Netherlands"],"latitude":[51.95],"longitude":[4.14],"un_locode":["NLRTM"],"region":["Europe"],"harbor_type":[None],"harbor_size":["Large"],"importance":[100.0]})
    analytics=pl.DataFrame({"date":[date(2026,9,9)],"port_id":["NLRTM"],"port_calls":[40.0],"import_volume":[600.0],"export_volume":[400.0],"total_volume":[1000.0],"calls_expected":[30.0],"volume_expected":[800.0],"calls_z_score":[1.2],"volume_z_score":[1.8],"activity_score":[76.0],"change_7d_pct":[8.0],"change_30d_pct":[12.0]})
    atomic_write(ports,settings.data_dir/"silver/ports.parquet");atomic_write(analytics,settings.data_dir/"gold/port_daily_anomalies.parquet")
    return MaritimeDatabase(settings)


def test_local_only_query_endpoints(monkeypatch,tmp_path):
    database=make_database(tmp_path)
    api_module.database=database
    api_module.cached_snapshot.cache_clear()

    async def forbidden_network(*args,**kwargs):
        raise AssertionError("normal query attempted external HTTP")
    monkeypatch.setattr("httpx.AsyncClient.get",forbidden_network)
    client=TestClient(api_module.app)
    paths=["/api/dates","/api/globe/snapshot?date=2026-09-09","/api/ports/NLRTM?date=2026-09-09","/api/ports/NLRTM/history?start=2026-09-09&end=2026-09-09","/api/anomalies/top?date=2026-09-09&limit=5","/api/live/snapshot?date=2026-09-09","/api/live/routes","/api/live/ports/NLRTM/history?start=2026-09-09&end=2026-09-09","/api/historical/routes/dates","/api/historical/routes?date=2026-09-09","/api/historical/import/status"]
    for path in paths:
        response=client.get(path)
        assert response.status_code==200,path

    activity=client.get("/api/ports/NLRTM?date=2026-09-09").json()["activity"]
    assert activity["import_volume"]==600.0
    assert activity["export_volume"]==400.0
    assert activity["total_volume"]==1000.0


def test_health(tmp_path):
    response=TestClient(api_module.app).get("/api/health")
    assert response.json()=={"status":"ok","mode":"local-first"}


def test_snapshot_cache_tracks_local_analytics_revision(tmp_path):
    database=make_database(tmp_path)
    api_module.database=database
    api_module.cached_snapshot.cache_clear()
    selected=date(2026,9,9)
    assert api_module.globe_snapshot(selected).ports[0].activity_score==76

    analytics_path=database.settings.data_dir/"gold/port_daily_anomalies.parquet"
    analytics=pl.read_parquet(analytics_path).with_columns(pl.lit(55.0).alias("activity_score"))
    atomic_write(analytics,analytics_path)
    database.refresh_views()

    assert api_module.globe_snapshot(selected).ports[0].activity_score==55


def test_port_function_classification_uses_explicit_identity_before_call_mix():
    assert classify_port_function("fso7", "Offshore Oil Terminal", 50, 1)["port_function"] == "oil_terminal"
    assert classify_port_function("gas1", "North LNG Terminal", 50, 1)["port_function"] == "lng_lpg_terminal"
    assert classify_port_function("port1", "General Port", 2, 18)["port_function"] == "tanker_dominant"
    assert classify_port_function("port2", "General Port", 15, 5)["port_function"] == "cargo_or_mixed"


def test_port_function_uses_wpi_only_for_dedicated_terminal_evidence():
    dedicated = classify_port_function(
        "port3", "North Facility", 40, 2,
        wpi_name="North Oil Terminal",
        wpi_facility_oil_terminal="Yes",
        wpi_oil_terminal_depth_m=18.0,
    )
    assert dedicated["port_function"] == "oil_terminal"
    assert "NGA WPI" in dedicated["classification_basis"]

    mixed = classify_port_function(
        "port4", "General Port", 15, 5,
        wpi_name="General Port",
        wpi_facility_oil_terminal="Yes",
        wpi_oil_terminal_depth_m=18.0,
    )
    assert mixed["port_function"] == "cargo_or_mixed"
    assert "not a dedicated terminal" in mixed["classification_basis"]


def test_snapshot_port_shape_profile_uses_complete_stored_history(tmp_path):
    database=make_database(tmp_path)
    activity=pl.DataFrame({
        "date":[date(2019,1,1),date(2026,9,9)],"port_id":["NLRTM","NLRTM"],
        "port_calls":[10.0,10.0],"cargo_calls":[1.0,1.0],"tanker_calls":[9.0,9.0],
        "import_volume":[0.0,0.0],"export_volume":[0.0,0.0],"total_volume":[0.0,0.0],
        "source":["fixture","fixture"],"retrieved_at":["fixture","fixture"],
    })
    atomic_write(activity,database.settings.data_dir/"silver/port_daily_activity.parquet")
    database.refresh_views()

    port=database.snapshot(date(2026,9,9))[0]
    assert port["port_function"]=="tanker_dominant"
    assert port["typed_call_count"]==20
    assert port["tanker_call_share"]==0.9
    assert "full stored history" in port["classification_basis"]


async def test_aisstream_derives_and_deduplicates_provisional_call(monkeypatch,tmp_path):
    database=make_database(tmp_path)
    monkeypatch.setenv("AISSTREAM_API_KEY","fixture-key")
    collector=AisStreamCollector(database.settings,database)
    entered=datetime(2026,9,11,12,0,tzinfo=timezone.utc)
    message={
        "MessageType":"PositionReport",
        "MetaData":{"MMSI":244123456,"ShipName":"TEST CARGO","Latitude":51.95,"Longitude":4.14},
        "Message":{"PositionReport":{"Sog":0.4}},
    }
    await collector.process_message(message,entered)
    await collector.process_message(message,entered+timedelta(minutes=21))
    await collector.process_message(message,entered+timedelta(minutes=25))
    snapshot=database.ais_snapshot(date(2026,9,11))
    assert len(snapshot)==1
    assert snapshot[0]["port_id"]=="NLRTM"
    assert snapshot[0]["port_calls"]==1
    assert snapshot[0]["activity_score"] is None
    assert database.ais_status()["port_call_count"]==1


async def test_wpi_failure_never_blocks_portwatch_metadata(monkeypatch, tmp_path):
    database = make_database(tmp_path)
    service = MaritimeRefreshService(database.settings, database)
    ports = pl.read_parquet(database.settings.data_dir / "silver/ports.parquet")

    async def unavailable(_self):
        raise RuntimeError("fixture NGA outage")

    monkeypatch.setattr("maritime.ingestion.nga.NgaWorldPortIndexProvider.download", unavailable)
    result = await service._enrich_ports_from_wpi(ports, object())

    assert result.equals(ports)
    status = database.status()
    manifest = next(source for source in status["sources"] if source["source"] == "nga")
    assert manifest["status"] == "unavailable"


def test_ais_calls_only_score_waits_for_seven_prior_days(tmp_path):
    database=make_database(tmp_path)
    first=date(2026,9,1)
    for offset in range(8):
        calls=5 if offset==7 else (1 if offset % 2 == 0 else 2)
        for index in range(calls):
            arrived=datetime(2026,9,1,12,0)+timedelta(days=offset,minutes=index)
            database.record_ais_port_call({
                "event_id":f"{offset}-{index}","call_date":arrived.date(),"port_id":"NLRTM",
                "mmsi":f"244{offset:03}{index:03}","vessel_name":"FIXTURE","vessel_type":"cargo",
                "arrived_at":arrived,"last_seen_at":arrived+timedelta(minutes=20),
                "confidence":"fixture","source":"aisstream",
            })
    assert database.ais_snapshot(first)[0]["activity_score"] is None
    latest=database.ais_snapshot(first+timedelta(days=7))[0]
    assert latest["baseline_days"]==7
    assert latest["activity_score"] is not None
    assert latest["activity_score"]>50


def test_port_subscription_boxes_are_focused_and_cover_every_port():
    ports=[
        {"port_id":"A","latitude":51.95,"longitude":4.14},
        {"port_id":"B","latitude":1.26,"longitude":103.84},
        {"port_id":"C","latitude":-33.86,"longitude":151.20},
        {"port_id":"D","latitude":37.77,"longitude":-122.42},
        {"port_id":"E","latitude":0.0,"longitude":179.98},
    ]
    boxes=build_port_subscription_boxes(ports,padding_km=12,tile_degrees=5)
    assert boxes
    assert boxes != [[[-90,-180],[90,180]]]
    assert approximate_subscription_coverage_pct(boxes)<1
    for port in ports:
        assert any(
            south<=port["latitude"]<=north and west<=port["longitude"]<=east
            for (north,west),(south,east) in boxes
        )


def test_collector_pauses_at_configured_traffic_budget(tmp_path):
    database=make_database(tmp_path)
    database.settings.sources["aisstream"]["maximum_messages_per_minute"]=2
    collector=AisStreamCollector(database.settings,database)
    collector._record_traffic(100)
    collector._record_traffic(100)
    with pytest.raises(SafetyLimitExceeded):
        collector._record_traffic(100)


async def test_global_route_points_are_sampled_and_batch_persisted(monkeypatch, tmp_path):
    database = make_database(tmp_path)
    monkeypatch.setenv("AISSTREAM_API_KEY", "fixture-key")
    database.settings.sources["aisstream"]["process_each_vessel_every_seconds"] = 60
    database.settings.sources["aisstream"]["route_minimum_interval_seconds"] = 120
    database.settings.sources["aisstream"]["route_minimum_distance_km"] = 1
    collector = AisStreamCollector(database.settings, database)
    first = datetime.now(timezone.utc) - timedelta(minutes=10)

    def position(latitude, longitude, course):
        return {
            "MessageType": "PositionReport",
            "MetaData": {
                "MMSI": 244123456,
                "ShipName": "TEST CARGO",
                "Latitude": latitude,
                "Longitude": longitude,
            },
            "Message": {"PositionReport": {"Sog": 14.2, "Cog": course}},
        }

    await collector.process_message(position(40.0, -30.0, 90), first)
    await collector._flush_routes()
    await collector.process_message(position(40.0001, -30.0001, 91), first + timedelta(seconds=61))
    await collector.process_message(position(40.03, -29.96, 110), first + timedelta(seconds=121))
    await collector._flush_routes()

    routes = database.ais_routes(hours=1, maximum_vessels=10, maximum_points_per_vessel=10)
    assert len(routes) == 1
    assert routes[0]["mmsi"] == "244123456"
    assert len(routes[0]["points"]) == 2
    assert routes[0]["points"][-1]["longitude"] == -29.96
    assert database.ais_route_status()["route_point_count"] == 2
    assert collector.status_value.pending_route_points == 0


def test_ais_storage_maintenance_prunes_expired_rows_and_keeps_recent_data(tmp_path):
    database = make_database(tmp_path)
    now = datetime.now(timezone.utc)
    old = now - timedelta(days=200)
    recent = now - timedelta(days=2)
    for label, observed in (("old", old), ("recent", recent)):
        database.record_ais_port_call({
            "event_id": label,
            "call_date": observed.date(),
            "port_id": "NLRTM",
            "mmsi": f"244-{label}",
            "vessel_name": "FIXTURE",
            "vessel_type": "cargo",
            "arrived_at": observed,
            "last_seen_at": observed + timedelta(minutes=20),
            "confidence": "fixture",
            "source": "aisstream",
        })
        database.record_ais_route_points([{
            "mmsi": f"244-{label}",
            "observed_at": observed,
            "vessel_name": "FIXTURE",
            "latitude": 40.0,
            "longitude": -74.0,
            "speed_knots": 10.0,
            "course_degrees": 90.0,
            "message_type": "PositionReport",
        }])

    result = database.maintain_ais_storage(
        now - timedelta(hours=72),
        (now - timedelta(days=180)).date(),
    )

    assert result == {"route_points_removed": 1, "port_calls_removed": 1}
    assert database.ais_status()["port_call_count"] == 1
    assert database.ais_route_status()["route_point_count"] == 1


def test_noaa_query_is_bounded_and_normalization_downsamples(tmp_path):
    database = make_database(tmp_path)
    provider = NoaaHistoricalAisProvider(database.settings, object())
    bounds = GeographicBounds.parse("39.5,-75.5,41.5,-72.5")
    start = datetime(2024, 6, 1, 0, 0, tzinfo=timezone.utc)
    url = provider.query_url(start, start + timedelta(hours=1), bounds)
    assert "AIS2024_AIS.csv" in url
    assert "Lat%3E=39.5" in url
    assert "Lon%3C=-72.5" in url
    assert "time%3C2024-06-01T01:00:00Z" in url

    raw = pl.DataFrame({
        "MMSI": ["123456789", "123456789", "123456789", "987654321"],
        "time": [
            "2024-06-01T00:01:00Z", "2024-06-01T00:08:00Z",
            "2024-06-01T00:12:00Z", "2024-06-01T00:03:00Z",
        ],
        "Lat": [40.0, 40.1, 40.2, 41.0],
        "Lon": [-74.0, -73.9, -73.8, -73.0],
        "SOG": [10.0, 10.2, 10.4, 8.0],
        "COG": [90.0, 91.0, 92.0, 180.0],
        "VesselName": ["TEST", "TEST", "TEST", "OTHER"],
    })
    normalized = normalize_noaa_routes(raw, sample_minutes=10)
    assert normalized.height == 3
    assert normalized.filter(pl.col("mmsi") == "123456789").height == 2
    assert normalized["source"].unique().to_list() == ["noaa_marinecadastre"]


def test_historical_route_partitions_are_local_date_aware(tmp_path):
    database = make_database(tmp_path)
    selected = date(2024, 6, 1)
    partition = (
        database.settings.data_dir
        / "silver/noaa_ais_routes"
        / "date=2024-06-01"
        / "routes.parquet"
    )
    frame = pl.DataFrame({
        "source": ["noaa_marinecadastre"] * 3,
        "mmsi": ["123456789"] * 3,
        "observed_at": [
            datetime(2024, 6, 1, 0, 0),
            datetime(2024, 6, 1, 0, 10),
            datetime(2024, 6, 1, 0, 20),
        ],
        "vessel_name": ["TEST"] * 3,
        "latitude": [40.0, 40.1, 40.2],
        "longitude": [-74.0, -73.9, -73.8],
        "speed_knots": [10.0, 10.1, 10.2],
        "course_degrees": [90.0, 91.0, 92.0],
    })
    atomic_write(frame, partition)

    dates = database.historical_route_dates()
    assert dates["available_dates"] == [selected]
    assert dates["coverage_label"] == "U.S. waters only"
    routes = database.historical_routes(selected, maximum_vessels=10, maximum_points_per_vessel=2)
    assert len(routes) == 1
    assert len(routes[0]["points"]) == 2
    assert routes[0]["points"][-1]["longitude"] == -73.8

    api_module.database = database
    client = TestClient(api_module.app)
    assert client.get("/api/historical/routes/dates").json()["available_dates"] == [
        "2024-06-01"
    ]
    payload = client.get("/api/historical/routes?date=2024-06-01&limit=10&points=2").json()
    assert payload["source"] == "noaa_marinecadastre"
    assert payload["coverage_label"] == "U.S. waters only"
    assert payload["vessel_count"] == 1


async def test_noaa_ui_import_job_is_guarded_bounded_and_reports_progress(monkeypatch, tmp_path):
    database = make_database(tmp_path)
    service = NoaaHistoricalImportService(database.settings, database)

    async def fixture_import(_self, selected, bounds, sample_minutes, progress=None):
        assert bounds == GeographicBounds(39.5, -75.5, 41.5, -72.5)
        assert sample_minutes == 10
        if progress:
            progress(12, 24)
            progress(24, 24)
        return {"date": selected, "downloaded": 3, "added": 3, "updated": 0, "total": 3}

    monkeypatch.setattr(
        "maritime.ingestion.noaa_ais.NoaaHistoricalAisImporter.import_day",
        fixture_import,
    )
    first = await service.start(
        date(2023, 6, 1),
        date(2023, 6, 1),
        GeographicBounds(39.5, -75.5, 41.5, -72.5),
        10,
    )
    duplicate = await service.start(
        date(2023, 6, 1),
        date(2023, 6, 1),
        GeographicBounds(39.5, -75.5, 41.5, -72.5),
        10,
    )
    assert duplicate["job_id"] == first["job_id"]
    complete = await service.wait()
    assert complete["state"] == "complete"
    assert complete["completed_chunks"] == complete["total_chunks"] == 24
    assert complete["rows_added"] == 3

    with pytest.raises(ValueError, match="limited to 7 days"):
        await service.start(
            date(2023, 6, 1),
            date(2023, 6, 8),
            GeographicBounds(39.5, -75.5, 41.5, -72.5),
            10,
        )

    with pytest.raises(ValueError, match="between 2018-01-01 and 2023-12-31"):
        await service.start(
            date(2017, 12, 31),
            date(2017, 12, 31),
            GeographicBounds(39.5, -75.5, 41.5, -72.5),
            10,
        )


def test_weekly_noaa_schedule_uses_one_wednesday_for_all_six_years():
    dates = weekly_sample_dates(date(2018, 1, 1), date(2023, 12, 31))
    assert len(dates) == 313
    assert dates[0] == date(2018, 1, 3)
    assert dates[-1] == date(2023, 12, 27)
    assert all(selected.weekday() == 2 for selected in dates)


async def test_bulk_noaa_day_compacts_to_parquet_and_discards_sources(tmp_path):
    database = make_database(tmp_path)
    selected = date(2023, 6, 7)
    csv_text = """MMSI,BaseDateTime,LAT,LON,SOG,COG,VesselName
366001111,2023-06-07T00:01:00,40.0,-74.0,10.0,90.0,ALPHA
366001111,2023-06-07T00:10:00,40.1,-73.9,10.5,91.0,ALPHA
366001111,2023-06-07T00:31:00,40.2,-73.8,11.0,92.0,ALPHA
366002222,2023-06-07T00:05:00,29.0,-94.0,8.0,180.0,BETA
"""
    buffer = BytesIO()
    with ZipFile(buffer, "w", compression=ZIP_DEFLATED) as archive:
        archive.writestr(f"AIS_{selected:%Y_%m_%d}.csv", csv_text)

    class FixtureClient:
        async def download(self, _url, target, **_kwargs):
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(buffer.getvalue())
            return {}

    importer = NoaaBulkDailyImporter(database.settings, database, FixtureClient())
    result = await importer.import_day(selected, 30)

    assert result["downloaded"] == 3
    assert result["added"] == 3
    assert importer.is_complete(selected)
    routes = pl.read_parquet(importer.partition_path(selected))
    assert routes.height == 3
    assert dict(routes.schema) == {
        "source": pl.String,
        "mmsi": pl.String,
        "observed_at": pl.Datetime,
        "vessel_name": pl.String,
        "latitude": pl.Float64,
        "longitude": pl.Float64,
        "speed_knots": pl.Float64,
        "course_degrees": pl.Float64,
    }
    cache = database.settings.data_dir / "cache/downloads/noaa_ais/weekly"
    assert not list(cache.glob("*.zip"))
    assert not list(cache.glob("*.part"))
    assert not list(cache.glob("*/source.csv"))

    repeated = await importer.import_day(selected, 30)
    assert repeated["added"] == 0
    assert repeated["updated"] == 3
    assert pl.read_parquet(importer.partition_path(selected)).height == 3


async def test_weekly_history_job_is_guarded_and_resumes_completed_dates(monkeypatch, tmp_path):
    database = make_database(tmp_path)
    service = NoaaWeeklyHistoryService(database.settings, database)
    dates = [date(2023, 6, 7), date(2023, 6, 14)]
    monkeypatch.setattr(service, "_dates", lambda: dates)
    monkeypatch.setattr(
        "maritime.ingestion.noaa_bulk.NoaaBulkDailyImporter.is_complete",
        lambda _self, selected: selected == dates[0],
    )

    async def fixture_import(_self, selected, sample_minutes):
        assert selected == dates[1]
        assert sample_minutes == 30
        return {
            "date": selected,
            "downloaded": 4,
            "added": 4,
            "updated": 0,
            "total": 4,
            "source_bytes": 1000,
            "parquet_bytes": 100,
        }

    monkeypatch.setattr(
        "maritime.ingestion.noaa_bulk.NoaaBulkDailyImporter.import_day",
        fixture_import,
    )
    first = await service.start()
    duplicate = await service.start()
    assert duplicate["job_id"] == first["job_id"]
    complete = await service.wait()
    assert complete["state"] == "complete"
    assert complete["completed_dates"] == complete["total_dates"] == 2
    assert complete["skipped_dates"] == 1
    assert complete["source_bytes_downloaded"] == 1000
    assert complete["parquet_bytes_written"] == 100


def test_weekly_history_status_survives_restart_from_local_partitions(tmp_path):
    database = make_database(tmp_path)
    selected = date(2023, 6, 7)
    database.settings.sources["noaa_historical_ais"]["source_earliest_date"] = selected.isoformat()
    database.settings.sources["noaa_historical_ais"]["source_latest_date"] = selected.isoformat()
    partition = (
        database.settings.data_dir
        / "silver/noaa_ais_routes"
        / f"date={selected.isoformat()}"
        / "routes.parquet"
    )
    atomic_write(pl.DataFrame({
        "source": ["noaa_marinecadastre"],
        "mmsi": ["123456789"],
        "observed_at": [datetime(2023, 6, 7, 12, 0)],
        "vessel_name": ["FIXTURE"],
        "latitude": [40.0],
        "longitude": [-74.0],
        "speed_knots": [10.0],
        "course_degrees": [90.0],
    }), partition)

    status = NoaaWeeklyHistoryService(database.settings, database).status()

    assert status["state"] == "complete"
    assert status["completed_dates"] == status["total_dates"] == 1
    assert status["parquet_bytes_written"] == partition.stat().st_size
