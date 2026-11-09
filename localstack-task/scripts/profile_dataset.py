"""Profile the Helsinki city bikes dataset; docs/dataset_profile.txt is its output
Reads the Kaggle CSV, or its zip, in chunks and prints what the design relies
on: rows per month and their order, what the columns really hold (speed
units, durations, zero distances, missing ids), station ids against names,
hourly temperature, and which columns make a row unique

    python scripts/profile_dataset.py data/source/archive.zip
    python scripts/profile_dataset.py data/source/database.csv --chunk-rows 200000
"""

from __future__ import annotations

import argparse
import re
import sys
import time
import warnings
import zipfile
from collections import Counter
from collections.abc import Iterator
from contextlib import ExitStack
from pathlib import Path
from typing import IO

import numpy as np
import pandas as pd

DEPARTURE, RETURN = "departure", "return"
DISTANCE, DURATION = "distance (m)", "duration (sec.)"
SPEED, TEMPERATURE = "avg_speed (km/h)", "Air temperature (degC)"
EXPECTED_COLUMNS = [
    DEPARTURE, RETURN, "departure_id", "departure_name", "return_id", "return_name",
    DISTANCE, DURATION, SPEED,
    "departure_latitude", "departure_longitude", "return_latitude", "return_longitude",
    TEMPERATURE,
]  # fmt: skip
NUMERIC_COLUMNS = [c for c in EXPECTED_COLUMNS if c not in (DEPARTURE, RETURN) and "name" not in c]
STATION_COLUMNS = [("departure_id", "departure_name"), ("return_id", "return_name")]
CANDIDATE_KEYS = {
    "departure + departure_id": [DEPARTURE, "departure_id"],
    "departure + both ids + return": [DEPARTURE, "departure_id", "return_id", RETURN],
}
QUANTILES = [0, 0.001, 0.01, 0.05, 0.5, 0.95, 0.99, 0.999, 1]
SKIPPED_LINE = re.compile(r"Skipping line \d+")
DAY_SECONDS = 24 * 3600


def open_sources(path: Path, stack: ExitStack) -> Iterator[tuple[str, int, IO[bytes]]]:
    """Yield (name, uncompressed size, binary handle) for the CSV, or each CSV in a zip"""
    if not zipfile.is_zipfile(path):
        yield path.name, path.stat().st_size, stack.enter_context(path.open("rb"))
        return
    archive = stack.enter_context(zipfile.ZipFile(path))
    members = [m for m in archive.infolist() if m.filename.lower().endswith(".csv")]
    if not members:
        raise SystemExit(f"{path} has no .csv inside: {archive.namelist()}")
    for member in sorted(members, key=lambda m: m.filename):
        yield member.filename, member.file_size, stack.enter_context(archive.open(member))


def floats(values: pd.Series) -> np.ndarray:
    """Finite values as float32: plenty for quantiles, half the memory of float64"""
    array = values.to_numpy(dtype="float64", na_value=np.nan)
    return array[np.isfinite(array)].astype("float32")


def label(value: object) -> str:
    """Station ids are floats in the file (111.0); print the whole ones as integers"""
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value)


def share(name: str, count: int, total: int | None = None) -> str:
    """One aligned report line: a count, and its share of total when given"""
    line = f"  {name:<52} {count:>11,}"
    if total is not None:
        line += f"  {count / max(total, 1):>7.2%}"
    return line


class DatasetProfile:
    """Accumulates statistics chunk by chunk; report() prints them"""

    def __init__(self) -> None:
        self.files: list[tuple[str, int, int]] = []  # name, bytes, rows
        self.rows = 0
        self.skipped_lines = 0
        self.headers: dict[str, list[str]] = {}
        self.missing: Counter[str] = Counter()
        self.unparsable: Counter[str] = Counter()
        self.examples: dict[str, list[str]] = {}
        self.departure_range: list[pd.Timestamp] = []
        self.return_range: list[pd.Timestamp] = []
        self.per_month: Counter[str] = Counter()
        self.per_day: Counter[pd.Timestamp] = Counter()
        self.backward_steps = 0
        self.backward_months = 0
        self.last_departure: pd.Timestamp | None = None
        self.counts: Counter[str] = Counter()
        self.samples: dict[str, list[np.ndarray]] = {
            "distance (m)": [], "duration (s)": [], "speed (km/h, recomputed)": [],
            "temperature (C)": [], "return - departure - duration (s)": [],
        }  # fmt: skip
        self.missing_temperature_by_month: Counter[str] = Counter()
        self.hour_temperatures: list[pd.DataFrame] = []
        self.station_pairs: Counter[tuple[float, str]] = Counter()
        self.station_coordinates: list[pd.DataFrame] = []
        self.row_hashes: list[np.ndarray] = []
        self.key_hashes: dict[str, list[np.ndarray]] = {name: [] for name in CANDIDATE_KEYS}

    # --- update

    def update(self, chunk: pd.DataFrame) -> None:
        """Fold one chunk of raw string columns into the running statistics"""
        self.rows += len(chunk)
        departure = pd.to_datetime(chunk[DEPARTURE], format="ISO8601", errors="coerce")
        returned = pd.to_datetime(chunk[RETURN], format="ISO8601", errors="coerce")
        numbers = {c: pd.to_numeric(chunk[c], errors="coerce") for c in NUMERIC_COLUMNS}

        self._missing_values(chunk, {DEPARTURE: departure, RETURN: returned, **numbers})
        self._time(departure, returned)
        self._durations(chunk, departure, returned, numbers)
        self._speed(chunk, numbers)
        self._temperature(departure, numbers[TEMPERATURE])
        self._stations(chunk, numbers)
        self._keys(chunk)

    def _missing_values(self, chunk: pd.DataFrame, parsed: dict[str, pd.Series]) -> None:
        for column in EXPECTED_COLUMNS:
            self.missing[column] += int(chunk[column].isna().sum())
        for column, values in parsed.items():
            bad = values.isna() & chunk[column].notna()
            self.unparsable[column] += int(bad.sum())
            if bad.any() and len(self.examples.get(column, [])) < 3:
                self.examples.setdefault(column, []).extend(chunk.loc[bad, column].head(3))

    def _time(self, departure: pd.Series, returned: pd.Series) -> None:
        known = departure.dropna()
        if known.empty:
            return
        self.departure_range += [known.min(), known.max()]
        if returned.notna().any():
            self.return_range += [returned.min(), returned.max()]
        self.per_month.update(known.dt.strftime("%Y-%m").value_counts().to_dict())
        self.per_day.update(known.dt.normalize().value_counts().to_dict())

        steps = known.diff()
        months = known.dt.year * 12 + known.dt.month
        self.backward_steps += int((steps < pd.Timedelta(0)).sum())
        self.backward_months += int((months.diff() < 0).sum())
        if self.last_departure is not None and known.iloc[0] < self.last_departure:
            self.backward_steps += 1
            previous = self.last_departure.year * 12 + self.last_departure.month
            self.backward_months += int(months.iloc[0] < previous)
        self.last_departure = known.iloc[-1]

    def _durations(
        self,
        chunk: pd.DataFrame,
        departure: pd.Series,
        returned: pd.Series,
        numbers: dict[str, pd.Series],
    ) -> None:
        distance, duration = numbers[DISTANCE], numbers[DURATION]
        elapsed = (returned - departure).dt.total_seconds()
        gap = elapsed - duration
        c = self.counts
        c["gap known"] += int(gap.notna().sum())
        c["gap within 1 s"] += int((gap.abs() <= 1).sum())
        c["gap within 60 s"] += int((gap.abs() <= 60).sum())
        c["gap over 1 h"] += int((gap > 3600).sum())
        c["duration longer than elapsed by >60 s"] += int((gap < -60).sum())
        c["return before departure"] += int((elapsed < 0).sum())
        c["elapsed over 1 day"] += int((elapsed > DAY_SECONDS).sum())
        if (gap > 3600).any() and len(self.examples.get("gap", [])) < 3:
            columns = [DEPARTURE, RETURN, DURATION, DISTANCE]
            rows = chunk.loc[gap > 3600, columns].head(3)
            self.examples.setdefault("gap", []).extend(", ".join(r) for r in rows.to_numpy())

        c["distance <= 0"] += int((distance <= 0).sum())
        c["distance 0 < d < 10"] += int(((distance > 0) & (distance < 10)).sum())
        c["distance > 20 km"] += int((distance > 20_000).sum())
        c["distance > 100 km"] += int((distance > 100_000).sum())
        c["duration <= 0"] += int((duration <= 0).sum())
        c["duration 0 < t < 10"] += int(((duration > 0) & (duration < 10)).sum())
        c["duration > 5 h"] += int((duration > 5 * 3600).sum())
        c["duration > 1 day"] += int((duration > DAY_SECONDS).sum())
        c["distance <= 0 and duration >= 60"] += int(((distance <= 0) & (duration >= 60)).sum())

        self.samples["distance (m)"].append(floats(distance))
        self.samples["duration (s)"].append(floats(duration))
        self.samples["return - departure - duration (s)"].append(floats(gap))

    def _speed(self, chunk: pd.DataFrame, numbers: dict[str, pd.Series]) -> None:
        distance, duration, given = numbers[DISTANCE], numbers[DURATION], numbers[SPEED]
        both = (distance > 0) & (duration > 0) & given.notna()
        km_per_min = distance / 1000 / (duration / 60)
        km_per_h = distance / duration * 3.6
        c = self.counts
        c["speed checked"] += int(both.sum())
        c["speed equals km/min"] += int((both & np.isclose(given, km_per_min, rtol=1e-6)).sum())
        c["speed equals km/h"] += int((both & np.isclose(given, km_per_h, rtol=1e-6)).sum())
        c["speed infinite"] += int(np.isinf(given).sum())
        speed = km_per_h.where((distance > 0) & (duration > 0))
        c["speed > 30 km/h"] += int((speed > 30).sum())
        c["speed > 50 km/h"] += int((speed > 50).sum())
        c["speed < 1 km/h (distance > 0)"] += int((speed < 1).sum())
        self.samples["speed (km/h, recomputed)"].append(floats(speed))

    def _temperature(self, departure: pd.Series, temperature: pd.Series) -> None:
        self.samples["temperature (C)"].append(floats(temperature))
        absent = temperature.isna() & departure.notna()
        if absent.any():
            months = departure[absent].dt.strftime("%Y-%m").value_counts()
            self.missing_temperature_by_month.update(months.to_dict())
        hours = pd.DataFrame({"hour": departure.dt.floor("h"), "value": temperature})
        self.hour_temperatures.append(hours.dropna().drop_duplicates())

    def _stations(self, chunk: pd.DataFrame, numbers: dict[str, pd.Series]) -> None:
        c = self.counts
        for id_column, name_column in STATION_COLUMNS:
            ids = numbers[id_column]
            c[f"{id_column} not a whole number"] += int((ids.notna() & (ids % 1 != 0)).sum())
            c[f"{id_column} missing, name present"] += int(
                (ids.isna() & chunk[name_column].notna()).sum()
            )
            pairs = pd.DataFrame({"id": ids, "name": chunk[name_column]}).value_counts(dropna=False)
            for (station_id, name), count in pairs.items():
                self.station_pairs[(station_id, name)] += int(count)
            prefix = id_column.removesuffix("_id")
            coordinates = pd.DataFrame({
                "id": ids,
                "lat": numbers[f"{prefix}_latitude"],
                "lon": numbers[f"{prefix}_longitude"],
            })  # fmt: skip
            self.station_coordinates.append(coordinates.dropna().drop_duplicates())

    def _keys(self, chunk: pd.DataFrame) -> None:
        self.row_hashes.append(pd.util.hash_pandas_object(chunk, index=False).to_numpy())
        for name, columns in CANDIDATE_KEYS.items():
            hashes = pd.util.hash_pandas_object(chunk[columns], index=False).to_numpy()
            self.key_hashes[name].append(hashes)

    # --- report

    def report(self, out: IO[str] = sys.stdout) -> None:
        """Print every section; each one is a few lines the design can be checked against"""
        if self.rows == 0:
            print("No rows read.", file=out)
            return
        sections = [
            self._report_structure, self._report_missing, self._report_time,
            self._report_months, self._report_durations, self._report_speed,
            self._report_quantiles, self._report_temperature, self._report_stations,
            self._report_keys,
        ]  # fmt: skip
        for section in sections:
            print(file=out)
            for line in section():
                print(line, file=out)

    def _report_structure(self) -> Iterator[str]:
        yield "STRUCTURE"
        for name, size, rows in self.files:
            yield f"  {name}: {size:,} bytes, {rows:,} rows, ~{size / max(rows, 1):.0f} bytes/row"
        yield f"  rows read            {self.rows:>12,}"
        yield f"  lines skipped        {self.skipped_lines:>12,}  (wrong number of fields)"
        for name, header in self.headers.items():
            extra = [c for c in header if c not in EXPECTED_COLUMNS]
            absent = [c for c in EXPECTED_COLUMNS if c not in header]
            if extra or absent:
                yield f"  {name}: header differs; extra {extra}, missing {absent}"

    def _report_missing(self) -> Iterator[str]:
        yield "MISSING / UNPARSABLE (non-empty but not a number or timestamp)"
        for column in EXPECTED_COLUMNS:
            missing, bad = self.missing[column], self.unparsable[column]
            if missing or bad:
                line = f"  {column:<26} missing {missing:>10,}   unparsable {bad:>8,}"
                if column in self.examples:
                    line += f"   e.g. {self.examples[column][:3]}"
                yield line
        if not any(self.missing.values()) and not any(self.unparsable.values()):
            yield "  none"

    def _report_time(self) -> Iterator[str]:
        yield "TIME"
        if not self.departure_range:
            yield "  no parsable departures"
            return
        yield f"  departure  {min(self.departure_range)}  ..  {max(self.departure_range)}"
        if self.return_range:
            yield f"  return     {min(self.return_range)}  ..  {max(self.return_range)}"
        yield f"  rows departing earlier than the row before: {self.backward_steps:,}"
        yield f"  times the month goes backwards:              {self.backward_months:,}"
        days = pd.Series(self.per_day).sort_index()
        yield (
            f"  days with trips {len(days):,}; trips per day: median {days.median():,.0f}, "
            f"max {days.max():,} on {days.idxmax().date()}"
        )

    def _report_months(self) -> Iterator[str]:
        months = pd.Series(self.per_month).sort_index()
        yield f"ROWS PER MONTH ({len(months)} month{'' if len(months) == 1 else 's'})"
        cells = [f"{month} {count:>9,}" for month, count in months.items()]
        for start in range(0, len(cells), 4):
            yield "  " + "    ".join(cells[start : start + 4])

    def _report_durations(self) -> Iterator[str]:
        c = self.counts
        yield "RETURN TIMESTAMP vs DURATION   (gap = return - departure - duration)"
        for name in ["gap within 1 s", "gap within 60 s", "gap over 1 h"]:
            yield share(name, c[name], c["gap known"])
        if "gap" in self.examples:
            yield "    e.g. (departure, return, duration, distance):"
            yield from (f"      {example}" for example in self.examples["gap"][:3])
        for name in [
            "duration longer than elapsed by >60 s",
            "return before departure",
            "elapsed over 1 day",
        ]:
            yield share(name, c[name], self.rows)
        yield "DISTANCE / DURATION EDGES"
        for name in [
            "distance <= 0",
            "distance 0 < d < 10",
            "distance > 20 km",
            "distance > 100 km",
            "duration <= 0",
            "duration 0 < t < 10",
            "duration > 5 h",
            "duration > 1 day",
            "distance <= 0 and duration >= 60",
        ]:
            yield share(name, c[name], self.rows)

    def _report_speed(self) -> Iterator[str]:
        c = self.counts
        yield "AVG_SPEED COLUMN (rows with distance > 0 and duration > 0)"
        checked = c["speed checked"]
        yield share("equals distance/duration in km/min", c["speed equals km/min"], checked)
        yield share("equals distance/duration in km/h", c["speed equals km/h"], checked)
        yield share("infinite values", c["speed infinite"], self.rows)
        for name in ["speed > 30 km/h", "speed > 50 km/h", "speed < 1 km/h (distance > 0)"]:
            yield share(f"recomputed {name}", c[name], self.rows)

    def _report_quantiles(self) -> Iterator[str]:
        header = "".join(f"{f'p{q * 100:g}':>10}" for q in QUANTILES)
        yield f"QUANTILES{'':<27}{header}"
        for name, parts in self.samples.items():
            values = np.concatenate(parts) if parts else np.array([], dtype="float32")
            if values.size == 0:
                continue
            row = "".join(f"{v:>10.1f}" for v in np.quantile(values, QUANTILES))
            yield f"  {name:<34}{row}   mean {values.mean():.2f}"

    def _report_temperature(self) -> Iterator[str]:
        yield "TEMPERATURE"
        hours = pd.concat(self.hour_temperatures).drop_duplicates()
        values_per_hour = hours.groupby("hour")["value"].nunique()
        several = values_per_hour[values_per_hour > 1]
        yield f"  hours with trips {len(values_per_hour):,}"
        yield f"  hours with more than one temperature value {len(several):,}"
        if len(several):
            hour = several.index[0]
            yield f"    e.g. {hour}: {sorted(hours.loc[hours['hour'] == hour, 'value'])[:5]}"
        missing = pd.Series(self.missing_temperature_by_month).sort_values(ascending=False)
        yield f"  rows without temperature {int(missing.sum()):,}"
        if len(missing):
            yield f"    months with the most: {missing.head(5).to_dict()}"

    def _report_stations(self) -> Iterator[str]:
        c = self.counts
        yield "STATIONS (departure and return columns together)"
        pairs = pd.DataFrame(
            [(i, n, k) for (i, n), k in self.station_pairs.items()], columns=["id", "name", "trips"]
        )
        named = pairs.dropna(subset=["id", "name"])
        yield f"  distinct ids {named['id'].nunique():,}"
        yield f"  distinct names {named['name'].nunique():,}"
        for key, other, title in [
            ("id", "name", "ids with several names"),
            ("name", "id", "names with several ids"),
        ]:
            spread = named.groupby(key)[other].nunique()
            multi = spread[spread > 1].index
            yield f"  {title}: {len(multi):,}"
            for value in list(multi)[:4]:
                variants = named.loc[named[key] == value].sort_values("trips", ascending=False)
                listed = ", ".join(
                    f"{label(v)} ({t:,})"
                    for v, t in zip(variants[other], variants["trips"], strict=True)
                )
                yield f"    {label(value)}: {listed}"
        for id_column, _ in STATION_COLUMNS:
            for problem in ("not a whole number", "missing, name present"):
                yield share(f"{id_column} {problem}", c[f"{id_column} {problem}"], self.rows)
        coordinates = pd.concat(self.station_coordinates).drop_duplicates()
        moved = coordinates.groupby("id").size()
        yield f"  ids with more than one coordinate pair {int((moved > 1).sum()):,}"

    def _report_keys(self) -> Iterator[str]:
        yield "UNIQUENESS (64-bit row hashes; chance collisions are negligible at this size)"
        rows = np.concatenate(self.row_hashes)
        _, copies = np.unique(rows, return_counts=True)
        yield share("identical rows (extra copies)", int((copies - 1).sum()), self.rows)
        yield f"    most copies of one row {copies.max()}"
        for name, parts in self.key_hashes.items():
            hashes = np.concatenate(parts)
            yield share(f"rows repeating {name}", hashes.size - np.unique(hashes).size, self.rows)


def profile(path: Path, chunk_rows: int) -> DatasetProfile:
    """Stream every CSV in path through a DatasetProfile, with progress on stderr"""
    result = DatasetProfile()
    started = time.monotonic()
    with ExitStack() as stack, warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always", pd.errors.ParserWarning)
        for name, size, handle in open_sources(path, stack):
            rows_before = result.rows
            reader = pd.read_csv(
                handle,
                dtype=str,
                keep_default_na=False,
                na_values=[""],
                chunksize=chunk_rows,
                on_bad_lines="warn",
                encoding="utf-8-sig",
            )
            for chunk in reader:
                result.headers.setdefault(name, list(chunk.columns))
                missing = [c for c in EXPECTED_COLUMNS if c not in chunk.columns]
                if missing:
                    raise SystemExit(f"{name} lacks {missing}; its header is {list(chunk.columns)}")
                result.update(chunk)
                messages = (str(warning.message) for warning in caught)
                result.skipped_lines += sum(len(SKIPPED_LINE.findall(m)) for m in messages)
                caught.clear()
                rate = result.rows / max(time.monotonic() - started, 1e-9)
                print(f"\r  {result.rows:,} rows, {rate:,.0f} rows/s", end="", file=sys.stderr)
            result.files.append((name, size, result.rows - rows_before))
    print(f"\r  {result.rows:,} rows in {time.monotonic() - started:.0f} s", file=sys.stderr)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("path", type=Path, help="the Kaggle zip, or the CSV inside it")
    parser.add_argument("--chunk-rows", type=int, default=250_000, help="rows per chunk")
    args = parser.parse_args()
    profile(args.path, args.chunk_rows).report()


if __name__ == "__main__":
    main()
