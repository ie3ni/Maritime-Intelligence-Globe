from __future__ import annotations

import argparse
import asyncio
import logging
from datetime import date, timedelta

from maritime.config import load_config
from maritime.ingestion.noaa_ais import GeographicBounds, NoaaHistoricalAisImporter
from maritime.network import PoliteHttpClient
from maritime.storage.database import MaritimeDatabase


def iso_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError:
        raise argparse.ArgumentTypeError("date must use YYYY-MM-DD") from None


def bounds_value(value: str) -> GeographicBounds:
    try:
        return GeographicBounds.parse(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from None


async def import_range(
    start: date,
    end: date,
    bounds: GeographicBounds,
    sample_minutes: int | None,
) -> None:
    settings = load_config()
    config = settings.sources["noaa_historical_ais"]
    if not config.get("enabled", True):
        raise SystemExit("NOAA historical AIS importing is disabled in config/sources.yaml")
    if end < start:
        raise SystemExit("--end must be on or after --start")
    day_count = (end - start).days + 1
    maximum_days = int(config["maximum_days_per_run"])
    if day_count > maximum_days:
        raise SystemExit(
            f"A single import is limited to {maximum_days} days; split this request into smaller ranges"
        )

    client = PoliteHttpClient(settings.network)
    importer = NoaaHistoricalAisImporter(
        settings, MaritimeDatabase(settings), client
    )
    try:
        selected = start
        while selected <= end:
            print(f"Importing NOAA historical AIS for {selected} within {bounds.as_dict()}…")
            result = await importer.import_day(selected, bounds, sample_minutes)
            print(
                f"{selected}: {result['added']:,} added, {result['updated']:,} updated, "
                f"{result['total']:,} stored"
            )
            selected += timedelta(days=1)
    finally:
        await client.aclose()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=(
            "Explicitly import a bounded NOAA/MarineCadastre historical AIS subset. "
            "Coordinates use south,west,north,east."
        )
    )
    parser.add_argument("--start", required=True, type=iso_date)
    parser.add_argument("--end", type=iso_date)
    parser.add_argument(
        "--bounds",
        required=True,
        type=bounds_value,
        metavar="SOUTH,WEST,NORTH,EAST",
        help="required geographic subset; antimeridian-spanning bounds must be split",
    )
    parser.add_argument(
        "--sample-minutes",
        type=int,
        default=None,
        help="retain at most one point per vessel per interval (default from config: 10)",
    )
    args = parser.parse_args()
    if args.sample_minutes is not None and args.sample_minutes < 1:
        parser.error("--sample-minutes must be at least 1")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    asyncio.run(
        import_range(
            args.start,
            args.end or args.start,
            args.bounds,
            args.sample_minutes,
        )
    )
