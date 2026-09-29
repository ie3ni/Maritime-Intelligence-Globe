from __future__ import annotations

import math

import polars as pl


def score_from_z(volume_z: float | None, calls_z: float | None, volume_weight: float = .65, calls_weight: float = .35, z_clip: float = 3.5) -> float:
    pairs = [(volume_z, volume_weight), (calls_z, calls_weight)]
    positive_weights = [(z, w) for z, w in pairs if w > 0]
    total_weight = sum(w for _, w in positive_weights)
    if total_weight <= 0:
        return 50.0
    # A missing/non-varying component is neutral. Preserve the configured
    # weights instead of promoting the remaining component to 100% influence.
    z = sum(
        (float(value) if value is not None and math.isfinite(float(value)) else 0.0) * weight
        for value, weight in positive_weights
    ) / total_weight
    return round(max(0.0, min(100.0, 50.0 + 50.0 * max(-z_clip, min(z_clip, z)) / z_clip)), 1)


def calculate_analytics(frame: pl.DataFrame, rolling_days: int = 56, minimum_history_days: int = 30, volume_weight: float = .65, calls_weight: float = .35, z_clip: float = 3.5) -> pl.DataFrame:
    if frame.is_empty():
        return frame
    data = frame.sort(["port_id", "date"]).with_columns(
        pl.col("port_calls").rolling_mean(7, min_samples=1).over("port_id").alias("calls_7d"),
        pl.col("total_volume").rolling_mean(7, min_samples=1).over("port_id").alias("volume_7d"),
        pl.col("port_calls").rolling_mean(rolling_days, min_samples=minimum_history_days).over("port_id").alias("calls_expected"),
        pl.col("port_calls").rolling_std(rolling_days, min_samples=minimum_history_days).over("port_id").alias("calls_std"),
        pl.col("total_volume").rolling_mean(rolling_days, min_samples=minimum_history_days).over("port_id").alias("volume_expected"),
        pl.col("total_volume").rolling_std(rolling_days, min_samples=minimum_history_days).over("port_id").alias("volume_std"),
        pl.col("total_volume").pct_change(7).over("port_id").mul(100).alias("change_7d_pct"),
        pl.col("total_volume").pct_change(30).over("port_id").mul(100).alias("change_30d_pct"),
    ).with_columns(
        pl.when(pl.col("calls_std") > 0).then((pl.col("calls_7d") - pl.col("calls_expected")) / pl.col("calls_std")).otherwise(None).alias("calls_z_score"),
        pl.when(pl.col("volume_std") > 0).then((pl.col("volume_7d") - pl.col("volume_expected")) / pl.col("volume_std")).otherwise(None).alias("volume_z_score"),
    )
    return data.with_columns(
        pl.struct(["volume_z_score", "calls_z_score"]).map_elements(
            lambda x: score_from_z(x["volume_z_score"], x["calls_z_score"], volume_weight, calls_weight, z_clip), return_dtype=pl.Float64
        ).alias("activity_score")
    )
