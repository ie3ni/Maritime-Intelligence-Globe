from __future__ import annotations

import math
import re
import unicodedata
from datetime import datetime, timezone
from difflib import SequenceMatcher
from typing import Any

import polars as pl

from maritime.config import Settings
from maritime.network import PoliteHttpClient


WPI_SCHEMA = {
    "wpi_object_id": pl.Int64,
    "wpi_number": pl.String,
    "wpi_name": pl.String,
    "wpi_alternate_name": pl.String,
    "wpi_un_locode": pl.String,
    "wpi_country_code": pl.String,
    "wpi_latitude": pl.Float64,
    "wpi_longitude": pl.Float64,
    "wpi_harbor_type": pl.String,
    "wpi_harbor_size": pl.String,
    "wpi_harbor_use": pl.String,
    "wpi_oil_terminal_depth_m": pl.Float64,
    "wpi_lng_terminal_depth_m": pl.Float64,
    "wpi_max_vessel_length_m": pl.Float64,
    "wpi_max_vessel_beam_m": pl.Float64,
    "wpi_max_vessel_draft_m": pl.Float64,
    "wpi_facility_oil_terminal": pl.String,
    "wpi_facility_lng_terminal": pl.String,
    "wpi_facility_container": pl.String,
    "wpi_facility_liquid_bulk": pl.String,
    "wpi_facility_solid_bulk": pl.String,
    "wpi_facility_breakbulk": pl.String,
    "wpi_source": pl.String,
    "wpi_retrieved_at": pl.String,
}

MATCH_SCHEMA = {
    "wpi_match_method": pl.String,
    "wpi_match_confidence": pl.String,
    "wpi_match_distance_km": pl.Float64,
    **WPI_SCHEMA,
}

_FIELD_CANDIDATES = {
    "wpi_number": ("wpinumber",),
    "name": ("main_port_name", "main_port_"),
    "alternate_name": ("alternate_name", "alternate_"),
    "un_locode": ("unlocode",),
    "country_code": ("wpi_cc", "countrycode"),
    "harbor_type": ("harbor_type_code", "harbor_typ"),
    "harbor_size": ("harbor_size_code", "harbor_siz"),
    "harbor_use": ("harbor_use_code", "harbor_use"),
    "oil_terminal_depth": ("oil_terminal_depth", "oil_termin"),
    "lng_terminal_depth": ("lng_terminal_depth", "lng_termin"),
    "max_vessel_length": ("maxvessellength", "maxvessell"),
    "max_vessel_beam": ("maxvesselbeam", "maxvesselb"),
    "max_vessel_draft": ("maxvesseldraft", "maxvesseld"),
    "facility_oil": ("fac_oilterm", "fac_oilter"),
    "facility_lng": ("fac_lngterm", "fac_lngter"),
    "facility_container": ("fac_container", "fac_contai"),
    "facility_liquid_bulk": ("fac_liquidbulk", "fac_liquid"),
    "facility_solid_bulk": ("fac_solidbulk", "fac_solidb"),
    "facility_breakbulk": ("fac_breakbulk", "fac_breakb"),
}

_GENERIC_NAME_WORDS = {
    "port", "ports", "harbor", "harbour", "terminal", "terminals", "the", "of",
}


def _attribute(attributes: dict[str, Any], name: str) -> Any:
    lookup = {key.lower(): value for key, value in attributes.items()}
    return lookup.get(name.lower())


def _attribute_any(attributes: dict[str, Any], names: tuple[str, ...]) -> Any:
    for name in names:
        value = _attribute(attributes, name)
        if value is not None:
            return value
    return None


def _string(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _number(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _integer(value: Any) -> int | None:
    number = _number(value)
    return int(number) if number is not None else None


def _normalize_locode(value: Any) -> str:
    return re.sub(r"[^A-Z0-9]", "", str(value or "").upper())


def _normalize_name(value: Any, remove_generic: bool = False) -> str:
    decomposed = unicodedata.normalize("NFKD", str(value or ""))
    text = "".join(character for character in decomposed if not unicodedata.combining(character))
    words = re.findall(r"[a-z0-9]+", text.lower())
    if remove_generic:
        words = [word for word in words if word not in _GENERIC_NAME_WORDS]
    return " ".join(words)


def _distance_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    radius = 6371.0088
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)
    value = (
        math.sin(delta_phi / 2) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2) ** 2
    )
    return radius * 2 * math.atan2(math.sqrt(value), math.sqrt(max(0.0, 1 - value)))


def _name_similarity(port_name: str, record: dict[str, Any]) -> float:
    left = _normalize_name(port_name, remove_generic=True)
    scores = []
    for candidate in (record.get("wpi_name"), record.get("wpi_alternate_name")):
        right = _normalize_name(candidate, remove_generic=True)
        if not left or not right:
            continue
        if left == right:
            scores.append(1.0)
        elif left in right or right in left:
            scores.append(0.9)
        else:
            scores.append(SequenceMatcher(None, left, right).ratio())
    return max(scores, default=0.0)


def _record_distance(port: dict[str, Any], record: dict[str, Any]) -> float:
    latitude, longitude = record.get("wpi_latitude"), record.get("wpi_longitude")
    if latitude is None or longitude is None:
        return math.inf
    return _distance_km(
        float(port["latitude"]), float(port["longitude"]), float(latitude), float(longitude)
    )


def _unambiguous_nearest(
    port: dict[str, Any],
    candidates: list[dict[str, Any]],
    maximum_distance_km: float | None,
    ambiguity_margin_km: float,
) -> tuple[dict[str, Any], float] | None:
    ranked = sorted((_record_distance(port, record), index, record) for index, record in enumerate(candidates))
    if not ranked or not math.isfinite(ranked[0][0]):
        return None
    nearest_distance, _, nearest = ranked[0]
    if maximum_distance_km is not None and nearest_distance > maximum_distance_km:
        return None
    if len(ranked) > 1 and ranked[1][0] - nearest_distance < ambiguity_margin_km:
        return None
    return nearest, nearest_distance


def _match_port(
    port: dict[str, Any],
    records: list[dict[str, Any]],
    by_locode: dict[str, list[dict[str, Any]]],
    by_name: dict[str, list[dict[str, Any]]],
    name_maximum_distance_km: float,
    proximity_maximum_distance_km: float,
    ambiguity_margin_km: float,
) -> tuple[dict[str, Any], str, str, float] | None:
    locode = _normalize_locode(port.get("un_locode"))
    if locode and locode in by_locode:
        match = _unambiguous_nearest(port, by_locode[locode], None, ambiguity_margin_km)
        if match:
            record, distance = match
            return record, "un_locode", "high", distance

    name_keys = {
        _normalize_name(port.get("name")),
        _normalize_name(port.get("name"), remove_generic=True),
    } - {""}
    name_candidates: dict[int, dict[str, Any]] = {}
    for key in name_keys:
        for record in by_name.get(key, []):
            name_candidates[id(record)] = record
    if name_candidates:
        match = _unambiguous_nearest(
            port, list(name_candidates.values()), name_maximum_distance_km, ambiguity_margin_km
        )
        if match:
            record, distance = match
            return record, "name_coordinate", "high", distance

    nearby = []
    for record in records:
        distance = _record_distance(port, record)
        if distance <= proximity_maximum_distance_km:
            nearby.append((_name_similarity(str(port.get("name") or ""), record), distance, record))
    nearby.sort(key=lambda item: (-item[0], item[1]))
    if not nearby or nearby[0][0] < 0.65:
        return None
    if len(nearby) > 1:
        score_gap = nearby[0][0] - nearby[1][0]
        distance_gap = nearby[1][1] - nearby[0][1]
        if score_gap < 0.1 and distance_gap < ambiguity_margin_km:
            return None
    score, distance, record = nearby[0]
    confidence = "high" if score >= 0.9 and distance <= 3 else "medium"
    return record, "name_proximity", confidence, distance


def enrich_ports_with_wpi(
    ports: pl.DataFrame,
    wpi: pl.DataFrame,
    *,
    name_maximum_distance_km: float = 50,
    proximity_maximum_distance_km: float = 8,
    ambiguity_margin_km: float = 3,
) -> pl.DataFrame:
    """Attach conservative WPI matches without inventing links for ambiguous ports."""
    if ports.is_empty():
        return ports
    records = wpi.to_dicts() if not wpi.is_empty() else []
    by_locode: dict[str, list[dict[str, Any]]] = {}
    by_name: dict[str, list[dict[str, Any]]] = {}
    for record in records:
        locode = _normalize_locode(record.get("wpi_un_locode"))
        if locode:
            by_locode.setdefault(locode, []).append(record)
        for value in (record.get("wpi_name"), record.get("wpi_alternate_name")):
            for key in (_normalize_name(value), _normalize_name(value, remove_generic=True)):
                if key:
                    by_name.setdefault(key, []).append(record)

    matches = []
    for port in ports.to_dicts():
        match = _match_port(
            port, records, by_locode, by_name,
            name_maximum_distance_km, proximity_maximum_distance_km, ambiguity_margin_km,
        )
        row = {column: None for column in MATCH_SCHEMA}
        if match:
            record, method, confidence, distance = match
            row.update(record)
            row.update(
                wpi_match_method=method,
                wpi_match_confidence=confidence,
                wpi_match_distance_km=round(distance, 3),
            )
        matches.append(row)

    match_frame = pl.DataFrame(matches, schema=MATCH_SCHEMA)
    enriched = pl.concat([ports, match_frame], how="horizontal_extend")
    return enriched.with_columns(
        pl.coalesce([pl.col("harbor_type"), pl.col("wpi_harbor_type")]).alias("harbor_type"),
        pl.coalesce([pl.col("harbor_size"), pl.col("wpi_harbor_size")]).alias("harbor_size"),
    )


class NgaWorldPortIndexProvider:
    """Public NGA WPI metadata adapter, called only by bootstrap/refresh."""

    def __init__(self, settings: Settings, client: PoliteHttpClient):
        self.cfg = settings.sources["nga_world_port_index"]
        self.client = client

    async def download(self) -> pl.DataFrame:
        query_url = self.cfg["layer_url"]
        metadata_url = query_url.removesuffix("/query")
        metadata_response = await self.client.get(metadata_url, params={"f": "json"})
        metadata = metadata_response.json()
        if "error" in metadata:
            raise RuntimeError(f"NGA WPI metadata request failed: {metadata['error']}")
        fields = metadata.get("fields", [])
        if not fields:
            raise RuntimeError("NGA WPI metadata did not contain a field schema")
        field_names = {str(field.get("name", "")).lower(): field.get("name") for field in fields}
        oid_field = metadata.get("objectIdField") or next(
            (field.get("name") for field in fields if field.get("type") == "esriFieldTypeOID"),
            "OBJECTID",
        )
        requested = [oid_field]
        requested.extend(
            actual
            for candidates in _FIELD_CANDIDATES.values()
            for candidate in candidates
            if (actual := field_names.get(candidate))
        )
        page_size = int(self.cfg.get("page_size", 3000))
        retrieved_at = datetime.now(timezone.utc).isoformat()
        offset, rows, seen = 0, [], set()
        while True:
            response = await self.client.get(query_url, params={
                "f": "json", "where": "1=1", "outFields": ",".join(dict.fromkeys(requested)),
                "returnGeometry": "true", "outSR": 4326, "resultOffset": offset,
                "resultRecordCount": page_size, "orderByFields": oid_field,
            })
            body = response.json()
            if "error" in body:
                raise RuntimeError(f"NGA WPI query failed: {body['error']}")
            features = body.get("features", [])
            if not features:
                break
            for feature in features:
                attributes = feature.get("attributes", {})
                geometry = feature.get("geometry", {})
                object_id = _integer(_attribute(attributes, str(oid_field)))
                if object_id is None:
                    raise RuntimeError("NGA WPI returned a record without an object ID")
                if object_id in seen:
                    raise RuntimeError("NGA WPI pagination repeated an object ID")
                seen.add(object_id)
                rows.append({
                    "wpi_object_id": object_id,
                    "wpi_number": _string(_attribute_any(attributes, _FIELD_CANDIDATES["wpi_number"])),
                    "wpi_name": _string(_attribute_any(attributes, _FIELD_CANDIDATES["name"])),
                    "wpi_alternate_name": _string(_attribute_any(attributes, _FIELD_CANDIDATES["alternate_name"])),
                    "wpi_un_locode": _string(_attribute_any(attributes, _FIELD_CANDIDATES["un_locode"])),
                    "wpi_country_code": _string(_attribute_any(attributes, _FIELD_CANDIDATES["country_code"])),
                    "wpi_latitude": _number(geometry.get("y")),
                    "wpi_longitude": _number(geometry.get("x")),
                    "wpi_harbor_type": _string(_attribute_any(attributes, _FIELD_CANDIDATES["harbor_type"])),
                    "wpi_harbor_size": _string(_attribute_any(attributes, _FIELD_CANDIDATES["harbor_size"])),
                    "wpi_harbor_use": _string(_attribute_any(attributes, _FIELD_CANDIDATES["harbor_use"])),
                    "wpi_oil_terminal_depth_m": _number(_attribute_any(attributes, _FIELD_CANDIDATES["oil_terminal_depth"])),
                    "wpi_lng_terminal_depth_m": _number(_attribute_any(attributes, _FIELD_CANDIDATES["lng_terminal_depth"])),
                    "wpi_max_vessel_length_m": _number(_attribute_any(attributes, _FIELD_CANDIDATES["max_vessel_length"])),
                    "wpi_max_vessel_beam_m": _number(_attribute_any(attributes, _FIELD_CANDIDATES["max_vessel_beam"])),
                    "wpi_max_vessel_draft_m": _number(_attribute_any(attributes, _FIELD_CANDIDATES["max_vessel_draft"])),
                    "wpi_facility_oil_terminal": _string(_attribute_any(attributes, _FIELD_CANDIDATES["facility_oil"])),
                    "wpi_facility_lng_terminal": _string(_attribute_any(attributes, _FIELD_CANDIDATES["facility_lng"])),
                    "wpi_facility_container": _string(_attribute_any(attributes, _FIELD_CANDIDATES["facility_container"])),
                    "wpi_facility_liquid_bulk": _string(_attribute_any(attributes, _FIELD_CANDIDATES["facility_liquid_bulk"])),
                    "wpi_facility_solid_bulk": _string(_attribute_any(attributes, _FIELD_CANDIDATES["facility_solid_bulk"])),
                    "wpi_facility_breakbulk": _string(_attribute_any(attributes, _FIELD_CANDIDATES["facility_breakbulk"])),
                    "wpi_source": "NGA World Port Index",
                    "wpi_retrieved_at": retrieved_at,
                })
            offset += len(features)
            if len(features) < page_size and not body.get("exceededTransferLimit", False):
                break
        return pl.DataFrame(rows, schema=WPI_SCHEMA)
