from __future__ import annotations

import json
import os
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Callable

import polars as pl

from maritime.config import Settings
from maritime.network import PoliteHttpClient


class PortWatchProvider:
    def __init__(self, settings: Settings, client: PoliteHttpClient):
        self.settings = settings
        self.client = client
        self.cfg = settings.sources["portwatch"]

    async def _query_pages(self, url: str, where: str, fields: str, progress: Callable[[str], None] | None = None, checkpoint_key: str | None = None):
        page_size = int(self.cfg.get("page_size", 1000))
        offset, seen_ids, records = 0, set(), []
        checkpoint_dir = self.settings.data_dir / "cache/downloads" / checkpoint_key if checkpoint_key else None
        if checkpoint_dir:
            checkpoint_dir.mkdir(parents=True, exist_ok=True)
        while True:
            checkpoint = checkpoint_dir / f"page_{offset:09d}.json" if checkpoint_dir else None
            if checkpoint and checkpoint.exists():
                payload = json.loads(checkpoint.read_text(encoding="utf-8"))
            else:
                response = await self.client.get(url, params={
                    "f": "json", "where": where, "outFields": fields, "returnGeometry": "false",
                    "resultOffset": offset, "resultRecordCount": page_size, "orderByFields": "ObjectId",
                })
                payload = response.json()
                if checkpoint:
                    temporary = checkpoint.with_suffix(".tmp")
                    temporary.write_text(json.dumps(payload), encoding="utf-8")
                    os.replace(temporary, checkpoint)
            if "error" in payload:
                raise RuntimeError(f"PortWatch query failed: {payload['error']}")
            features = payload.get("features", [])
            if not features:
                break
            ids = [f["attributes"].get("ObjectId") for f in features]
            if any(value in seen_ids for value in ids if value is not None):
                raise RuntimeError("PortWatch pagination repeated an object ID")
            seen_ids.update(value for value in ids if value is not None)
            records.extend(f["attributes"] for f in features)
            offset += len(features)
            if progress:
                progress(f"Downloading maritime observations… {len(records):,} rows")
            if len(features) < page_size or not payload.get("exceededTransferLimit", False):
                break
        if checkpoint_dir:
            for checkpoint in checkpoint_dir.glob("page_*.json"):
                checkpoint.unlink()
            checkpoint_dir.rmdir()
        return records

    async def latest_date(self) -> date | None:
        response = await self.client.get(self.cfg["activity_url"], params={
            "f":"json", "where":"1=1", "outStatistics": json.dumps([{"statisticType":"max","onStatisticField":"date","outStatisticFieldName":"max_date"}]), "returnGeometry":"false"
        })
        values = response.json().get("features", [])
        if not values:
            return None
        raw = values[0]["attributes"].get("max_date")
        return date.fromisoformat(str(raw)[:10]) if raw else None

    async def ports(self) -> pl.DataFrame:
        rows = await self._query_pages(self.cfg["ports_url"], "1=1", "portid,portname,country,ISO3,continent,lat,lon,vessel_count_total,LOCODE,ObjectId")
        if not rows:
            raise RuntimeError("PortWatch returned no ports")
        return pl.DataFrame(rows).select(
            pl.col("portid").cast(pl.String).alias("port_id"), pl.col("portname").cast(pl.String).alias("name"),
            pl.col("country").cast(pl.String), pl.col("lat").cast(pl.Float64).alias("latitude"), pl.col("lon").cast(pl.Float64).alias("longitude"),
            pl.col("LOCODE").cast(pl.String).alias("un_locode"), pl.col("continent").cast(pl.String).alias("region"),
            pl.lit(None, dtype=pl.String).alias("harbor_type"), pl.lit(None, dtype=pl.String).alias("harbor_size"),
            pl.col("vessel_count_total").cast(pl.Float64, strict=False).fill_null(1).alias("importance"),
        ).filter(pl.col("latitude").is_between(-90,90) & pl.col("longitude").is_between(-180,180) & pl.col("port_id").is_not_null()).unique("port_id")

    async def activity(self, start: date, end: date, progress=None) -> pl.DataFrame:
        where = f"date >= DATE '{start.isoformat()}' AND date <= DATE '{end.isoformat()}'"
        fields = "date,portid,portcalls,portcalls_cargo,portcalls_tanker,import,export,ObjectId"
        rows = await self._query_pages(self.cfg["activity_url"], where, fields, progress, f"portwatch_{start}_{end}")
        if not rows:
            return pl.DataFrame(schema={"date":pl.Date,"port_id":pl.String,"port_calls":pl.Float64,"cargo_calls":pl.Float64,"tanker_calls":pl.Float64,"import_volume":pl.Float64,"export_volume":pl.Float64,"total_volume":pl.Float64,"source":pl.String,"retrieved_at":pl.String})
        retrieved = datetime.now(timezone.utc).isoformat()
        return pl.DataFrame(rows).select(
            pl.col("date").cast(pl.String).str.slice(0,10).str.to_date().alias("date"), pl.col("portid").cast(pl.String).alias("port_id"),
            pl.col("portcalls").cast(pl.Float64, strict=False).alias("port_calls"), pl.col("portcalls_cargo").cast(pl.Float64, strict=False).alias("cargo_calls"),
            pl.col("portcalls_tanker").cast(pl.Float64, strict=False).alias("tanker_calls"), pl.col("import").cast(pl.Float64, strict=False).alias("import_volume"),
            pl.col("export").cast(pl.Float64, strict=False).alias("export_volume"),
            (pl.col("import").cast(pl.Float64, strict=False).fill_null(0)+pl.col("export").cast(pl.Float64, strict=False).fill_null(0)).alias("total_volume"),
            pl.lit("IMF PortWatch").alias("source"), pl.lit(retrieved).alias("retrieved_at"),
        ).filter(pl.col("date").is_not_null() & pl.col("port_id").is_not_null()).unique(["date","port_id"], keep="last")
