from __future__ import annotations

import os
from pathlib import Path

import polars as pl


def atomic_write(frame: pl.DataFrame, target: Path):
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_suffix(target.suffix + ".tmp")
    frame.write_parquet(temporary, compression="zstd")
    os.replace(temporary, target)


def merge_by_key(existing_path: Path, incoming: pl.DataFrame, keys: list[str]) -> tuple[pl.DataFrame, int, int]:
    if not existing_path.exists():
        return incoming.sort(keys), incoming.height, 0
    existing = pl.read_parquet(existing_path)
    incoming_keys = set(tuple(row) for row in incoming.select(keys).iter_rows())
    existing_keys = set(tuple(row) for row in existing.select(keys).iter_rows())
    updated = len(incoming_keys & existing_keys)
    added = len(incoming_keys - existing_keys)
    combined = pl.concat([existing, incoming], how="diagonal_relaxed").unique(keys, keep="last").sort(keys)
    return combined, added, updated

