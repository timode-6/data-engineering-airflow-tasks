"""Daily and monthly figures for a month of trips (docs/dynamodb.md)"""

from __future__ import annotations

import pandas as pd

MIN_DISTANCE_M = 10
MIN_DURATION_S = 10
MAX_DURATION_S = 5 * 3600
MAX_SPEED_KMH = 30

DAY = "%Y-%m-%d"
MONTH = "%Y-%m"
AVERAGES = ("avg_distance_m", "avg_duration_s", "avg_speed_kmh", "avg_temperature_c")


def with_validity(frame: pd.DataFrame) -> pd.DataFrame:
    """frame plus speed_kmh, and valid: whether the trip counts towards averages"""
    distance, duration = frame["distance_m"], frame["duration_s"]
    # Multiplying first keeps 5000 m in 600 s at exactly 30.0 km/h, the limit
    speed = (distance * 3.6 / duration).where(duration > 0)
    valid = (
        (distance >= MIN_DISTANCE_M)
        & (duration >= MIN_DURATION_S)
        & (duration <= MAX_DURATION_S)
        & (speed <= MAX_SPEED_KMH)
    )
    return frame.assign(speed_kmh=speed, valid=valid)


def period_labels(times: pd.Series, period: str) -> pd.Series:
    """Each time's day or month as text, period being DAY or MONTH"""
    codes, days = pd.factorize(times.dt.normalize())
    labels = days.strftime(period).to_numpy()[codes]
    return pd.Series(labels, index=times.index, name=times.name)


def period_metrics(trips: pd.DataFrame, period: str) -> pd.DataFrame:
    """One row per DAY or MONTH, indexed by e.g. "2019-06-06", from with_validity's frame"""
    key = period_labels(trips["departure"], period)
    valid = trips["valid"]
    good, good_key = trips[valid], key[valid]
    result = pd.DataFrame(
        {
            "trips": trips.groupby(key).size(),
            "valid_trips": valid.groupby(key).sum(),
            "avg_distance_m": good["distance_m"].groupby(good_key).mean(),
            "avg_duration_s": good["duration_s"].groupby(good_key).mean(),
            "avg_speed_kmh": good["speed_kmh"].groupby(good_key).mean(),
        }
    )
    readings = trips.dropna(subset=["temperature_c"])

    hourly = readings["temperature_c"].groupby(readings["departure"].dt.floor("h")).mean()
    hour_key = hourly.index.strftime(period)
    result["avg_temperature_c"] = hourly.groupby(hour_key).mean()
    result["temperature_hours"] = hourly.groupby(hour_key).size()
    result["temperature_hours"] = result["temperature_hours"].fillna(0).astype(int)
    result.index.name = "period"
    return result


def days_per_month(trips: pd.DataFrame) -> pd.Series:
    """How many days of each month had trips"""
    days = period_labels(trips["departure"], DAY)
    return days.groupby(period_labels(trips["departure"], MONTH)).nunique()
