from __future__ import annotations

import argparse
import asyncio
import logging

from maritime.config import load_config
from maritime.services.refresh import MaritimeRefreshService
from maritime.storage.database import MaritimeDatabase


async def main(days: int):
    settings = load_config()
    service = MaritimeRefreshService(settings, MaritimeDatabase(settings))
    await service.start(bootstrap_days=days)
    result = await service.wait()
    print(result)
    if result["state"] != "complete": raise SystemExit(1)


def positive_days(value: str) -> int:
    days = int(value)
    if days < 1:
        raise argparse.ArgumentTypeError("days must be at least 1")
    return days


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download official PortWatch history into local Parquet/DuckDB")
    parser.add_argument(
        "--days", "--backfill-days", dest="days", type=positive_days, default=120,
        help=(
            "desired total history through the latest source date; safely extends "
            "existing local history without deleting it (default: 120)"
        ),
    )
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    asyncio.run(main(args.days))
