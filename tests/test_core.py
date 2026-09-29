from datetime import date, datetime, timezone

import polars as pl

from maritime.analytics import calculate_analytics, score_from_z
from maritime.config import load_config
from maritime.ingestion.nga import NgaWorldPortIndexProvider, WPI_SCHEMA, enrich_ports_with_wpi
from maritime.network import retry_after_seconds
from maritime.services.refresh import incremental_window, refresh_windows, split_date_window
from maritime.storage.parquet import atomic_write, merge_by_key


def test_score_is_centered_and_weighted():
    assert score_from_z(None, None) == 50
    assert score_from_z(3.5, 3.5) == 100
    assert score_from_z(-3.5, -3.5) == 0
    assert score_from_z(2, -2, .75, .25) > 50


def test_score_preserves_component_weights_when_one_signal_is_unavailable():
    assert score_from_z(None, 0.9354143466934853) == 54.7
    assert score_from_z(1.0, None) == 59.3


def test_retry_after_seconds_and_http_date():
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    assert retry_after_seconds("7", now) == 7
    assert retry_after_seconds("Thu, 01 Jan 2026 00:00:11 GMT", now) == 11
    assert retry_after_seconds("bad", now) is None


def test_revision_lookback_window():
    assert incremental_window(date(2026, 9, 8), date(2026, 9, 10), 14, 120) == (date(2026, 8, 25), date(2026, 9, 10))
    assert incremental_window(None, date(2026, 9, 10), 14, 120)[0] == date(2026, 5, 14)


def test_backfill_plans_only_missing_history_plus_recent_revisions():
    assert refresh_windows(
        date(2026, 5, 8), date(2026, 9, 4), date(2026, 9, 4), 14,
        requested_history_days=365,
    ) == [
        (date(2025, 9, 5), date(2026, 5, 7)),
        (date(2026, 8, 21), date(2026, 9, 4)),
    ]


def test_large_backfill_windows_are_split_into_bounded_batches():
    assert split_date_window(date(2026, 1, 1), date(2026, 3, 5), 31) == [
        (date(2026, 1, 1), date(2026, 1, 31)),
        (date(2026, 2, 1), date(2026, 3, 3)),
        (date(2026, 3, 4), date(2026, 3, 5)),
    ]
    assert split_date_window(date(2026, 3, 5), date(2026, 3, 4), 31) == []
    assert refresh_windows(
        date(2026, 5, 8), date(2026, 9, 4), date(2026, 9, 4), 14,
    ) == [(date(2026, 8, 21), date(2026, 9, 4))]
    assert refresh_windows(None, None, date(2026, 9, 4), 14, 120) == [
        (date(2026, 5, 8), date(2026, 9, 4))
    ]


def test_backfill_merge_is_idempotent_and_preserves_existing_rows(tmp_path):
    target = tmp_path / "activity.parquet"
    existing = pl.DataFrame({
        "date": [date(2026, 5, 8), date(2026, 5, 9)],
        "port_id": ["A", "A"],
        "port_calls": [10.0, 11.0],
    })
    backfill = pl.DataFrame({
        "date": [date(2026, 5, 7), date(2026, 5, 8)],
        "port_id": ["A", "A"],
        "port_calls": [9.0, 10.0],
    })
    atomic_write(existing, target)
    combined, added, updated = merge_by_key(target, backfill, ["date", "port_id"])
    assert (added, updated, combined.height) == (1, 1, 3)
    atomic_write(combined, target)
    repeated, added, updated = merge_by_key(target, backfill, ["date", "port_id"])
    assert (added, updated, repeated.height) == (0, 2, 3)


def test_baselines_require_history_and_score():
    frame = pl.DataFrame({"date":pl.date_range(date(2026,1,1),date(2026,2,9),eager=True),"port_id":["P"]*40,"port_calls":[10.0]*39+[30.0],"total_volume":[100.0]*39+[400.0]})
    result = calculate_analytics(frame, rolling_days=30, minimum_history_days=10)
    assert result["activity_score"][-1] > 50
    assert result["calls_expected"][3] is None


def test_wpi_enrichment_prefers_stable_locode_and_keeps_provenance():
    ports = pl.DataFrame({
        "port_id": ["port1", "port2"],
        "name": ["Rotterdam", "Unmatched Anchorage"],
        "country": ["Netherlands", "Nowhere"],
        "latitude": [51.95, 0.0],
        "longitude": [4.14, 0.0],
        "un_locode": ["NLRTM", None],
        "region": ["Europe", "Ocean"],
        "harbor_type": [None, None],
        "harbor_size": [None, None],
        "importance": [100.0, 1.0],
    })
    record = {column: None for column in WPI_SCHEMA}
    record.update({
        "wpi_object_id": 1,
        "wpi_number": "12345",
        "wpi_name": "Port of Rotterdam",
        "wpi_un_locode": "NL RTM",
        "wpi_latitude": 51.94,
        "wpi_longitude": 4.13,
        "wpi_harbor_type": "Coastal (Breakwater)",
        "wpi_harbor_size": "Large",
        "wpi_facility_container": "Yes",
        "wpi_source": "NGA World Port Index",
        "wpi_retrieved_at": "2026-09-26T00:00:00+00:00",
    })
    wpi = pl.DataFrame([record], schema=WPI_SCHEMA)

    enriched = enrich_ports_with_wpi(ports, wpi)

    assert enriched["wpi_match_method"].to_list() == ["un_locode", None]
    assert enriched["wpi_match_confidence"].to_list() == ["high", None]
    assert enriched["harbor_type"].to_list() == ["Coastal (Breakwater)", None]
    assert enriched["harbor_size"].to_list() == ["Large", None]
    assert enriched["wpi_source"].to_list() == ["NGA World Port Index", None]


async def test_wpi_adapter_discovers_schema_and_requests_only_needed_fields():
    class Response:
        def __init__(self, body):
            self.body = body

        def json(self):
            return self.body

    class Client:
        def __init__(self):
            self.calls = []

        async def get(self, url, **kwargs):
            self.calls.append((url, kwargs.get("params", {})))
            if not url.endswith("/query"):
                names = [
                    "OBJECTID", "wpinumber", "main_port_", "alternate_", "unlocode",
                    "countryCode", "harbor_typ", "harbor_siz", "harbor_use",
                    "oil_termin", "lng_termin", "maxvessell", "maxvesselb",
                    "maxvesseld", "fac_oilter", "fac_lngter", "fac_contai",
                    "fac_liquid", "fac_solidb", "fac_breakb",
                ]
                return Response({
                    "objectIdField": "OBJECTID",
                    "fields": [{"name": name, "type": "esriFieldTypeOID" if name == "OBJECTID" else "esriFieldTypeString"} for name in names],
                })
            return Response({
                "features": [{
                    "attributes": {
                        "OBJECTID": 7, "wpinumber": "12345", "main_port_": "Rotterdam",
                        "unlocode": "NLRTM", "fac_contai": "Yes",
                    },
                    "geometry": {"x": 4.14, "y": 51.95},
                }],
                "exceededTransferLimit": False,
            })

    client = Client()
    result = await NgaWorldPortIndexProvider(load_config(), client).download()

    assert result.height == 1
    assert result["wpi_un_locode"][0] == "NLRTM"
    assert result["wpi_facility_container"][0] == "Yes"
    assert client.calls[1][1]["outFields"] != "*"
    assert "fac_contai" in client.calls[1][1]["outFields"]
