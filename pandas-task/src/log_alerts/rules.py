"""Alert rules.

A rule receives the log rows chunk by chunk and returns the alerts each chunk
triggers. It keeps what it needs from earlier chunks itself, so a burst of
errors split across two journals is still one burst.

The rules to run are listed in rules.toml, one [[rules]] table each, and the
table's `type` picks the class that implements it (RULE_TYPES below):

- another rule of an existing type is another [[rules]] table;
- a new type is a subclass of Rule, added to RULE_TYPES.
"""

from __future__ import annotations

import logging
import tomllib
from abc import ABC, abstractmethod
from collections.abc import Iterator, Mapping
from pathlib import Path
from typing import Any, ClassVar

import numpy as np
import pandas as pd

from log_alerts.alerts import Alert, to_utc
from log_alerts.journal import COLUMNS, TIME_COLUMN

logger = logging.getLogger(__name__)

NO_TIMES = np.empty(0)


class Rule(ABC):
    """One alert rule."""

    def __init__(self, name: str, description: str) -> None:
        self.name = name
        self.description = description

    @abstractmethod
    def evaluate(self, events: pd.DataFrame) -> list[Alert]:
        """Take the next chunk of log rows and return the alerts it triggers."""

    @classmethod
    @abstractmethod
    def from_config(cls, name: str, description: str, options: Mapping[str, Any]) -> Rule:
        """Build the rule from the rest of its [[rules]] table."""


class ThresholdRule(Rule):
    """More than `threshold` matching rows in less than `window` seconds.

    A row matches when each column in `where` has one of the listed values.
    With `group_by`, rows are counted separately for each value of that column,
    and rows without a value are ignored.

    The window slides rather than following the clock: a row completes a
    burst when the `threshold` matching rows before it are all less than
    `window` older. After an alert the rule stays quiet for that group until
    `cooldown` seconds have passed (one window by default), so a burst raises
    one alert instead of one per row.

    Times are the rows' `date`, never the clock, so replaying old logs raises
    the same alerts. Rows may arrive up to `lateness` seconds behind the newest
    row seen before them; anything later is dropped with a warning.
    """

    OPTIONS: ClassVar[set[str]] = {
        "where",
        "group_by",
        "window",
        "threshold",
        "cooldown",
        "lateness",
    }
    DEFAULT_LATENESS: ClassVar[float] = 300.0

    def __init__(
        self,
        name: str,
        description: str,
        *,
        window: float,
        threshold: int,
        where: Mapping[str, str | list[str]] | None = None,
        group_by: str | None = None,
        cooldown: float | None = None,
        lateness: float = DEFAULT_LATENESS,
    ) -> None:
        super().__init__(name, description)
        self.where = {column: _as_tuple(values) for column, values in (where or {}).items()}
        for column in [*self.where, *([group_by] if group_by else [])]:
            if column not in COLUMNS:
                raise ValueError(f"rule {name!r}: {column!r} is not a log column")
        if isinstance(threshold, bool) or not isinstance(threshold, int) or threshold < 1:
            raise ValueError(f"rule {name!r}: threshold must be a whole number of at least 1")
        if window <= 0:
            raise ValueError(f"rule {name!r}: window must be longer than zero")
        self.window = float(window)
        self.threshold = threshold
        self.group_by = group_by
        self.cooldown = self.window if cooldown is None else float(cooldown)
        self.lateness = float(lateness)
        if self.cooldown < 0 or self.lateness < 0:
            raise ValueError(f"rule {name!r}: cooldown and lateness cannot be negative")

        # Only these columns are copied out of each chunk
        self._columns = [TIME_COLUMN, *([group_by] if group_by else [])]
        # Matching row times per group, sorted, and when each group last alerted
        self._recent: dict[str | None, np.ndarray] = {}
        self._last_alert: dict[str | None, float] = {}
        self._newest = float("-inf")

    @classmethod
    def from_config(cls, name: str, description: str, options: Mapping[str, Any]) -> ThresholdRule:
        unknown = set(options) - cls.OPTIONS
        if unknown:
            raise ValueError(f"rule {name!r}: unknown option {', '.join(sorted(unknown))}")
        missing = {"window", "threshold"} - set(options)
        if missing:
            raise ValueError(f"rule {name!r}: missing {', '.join(sorted(missing))}")
        cooldown = options.get("cooldown")
        return cls(
            name,
            description,
            window=_seconds(name, "window", options["window"]),
            threshold=options["threshold"],
            where=options.get("where"),
            group_by=options.get("group_by"),
            cooldown=None if cooldown is None else _seconds(name, "cooldown", cooldown),
            lateness=_seconds(name, "lateness", options.get("lateness", cls.DEFAULT_LATENESS)),
        )

    @property
    def buffered_rows(self) -> int:
        """How many matching rows the rule keeps from earlier chunks."""
        return sum(len(times) for times in self._recent.values())

    def evaluate(self, events: pd.DataFrame) -> list[Alert]:
        matched = events.loc[self._matches(events), self._columns]
        if matched.empty:
            return []
        too_late = matched[TIME_COLUMN] <= self._newest - self.lateness
        if too_late.any():
            logger.warning(
                "%s: dropped %d rows that arrived more than %.0f s late",
                self.name,
                too_late.sum(),
                self.lateness,
            )
            matched = matched.loc[~too_late]
            if matched.empty:
                return []

        alerts = []
        for key, times in self._groups(matched):
            alerts.extend(self._check(key, np.sort(times.to_numpy(dtype=float))))
        self._newest = max(self._newest, float(matched[TIME_COLUMN].max()))
        self._forget(older_than=self._newest - self.lateness - self.window)
        return sorted(alerts, key=lambda alert: alert.last_event)

    def _matches(self, events: pd.DataFrame) -> pd.Series:
        mask = pd.Series(True, index=events.index)
        for column, values in self.where.items():
            mask &= events[column].isin(values)
        return mask

    def _groups(self, matched: pd.DataFrame) -> Iterator[tuple[str | None, pd.Series]]:
        if self.group_by is None:
            yield None, matched[TIME_COLUMN]
        else:
            yield from matched.groupby(self.group_by, sort=False)[TIME_COLUMN]

    def _check(self, key: str | None, new: np.ndarray) -> list[Alert]:
        """Add one group's new row times and return the alerts they cause."""
        old = self._recent.get(key, NO_TIMES)
        if len(old) == 0 or new[0] >= old[-1]:
            times = np.concatenate([old, new])  # the usual case: rows arrive in order
            first_new = len(old)
        else:
            times = np.sort(np.concatenate([old, new]), kind="stable")
            first_new = int(np.searchsorted(times, new[0]))
        self._recent[key] = times

        # Row i completes a burst if the `threshold` rows before it are less
        # than a window older. Only windows that end at or after the first new
        # row can have changed.
        n = self.threshold
        ends = np.arange(max(n, first_new), len(times))
        bursts = ends[times[ends] - times[ends - n] < self.window]
        return self._alerts_for(key, times, bursts)

    def _alerts_for(self, key: str | None, times: np.ndarray, bursts: np.ndarray) -> list[Alert]:
        """One alert per burst, then quiet for `cooldown`."""
        alerts = []
        burst_times = times[bursts]
        last = self._last_alert.get(key)
        position = 0
        while position < len(bursts):
            end = burst_times[position]
            if last is not None and end < last + self.cooldown:
                # Jump to the first burst after the quiet period
                position = int(np.searchsorted(burst_times, last + self.cooldown))
                continue
            first = int(np.searchsorted(times, end - self.window, side="right"))
            alerts.append(
                Alert(
                    rule=self.name,
                    description=self.description,
                    count=int(bursts[position]) - first + 1,
                    first_event=to_utc(times[first]),
                    last_event=to_utc(end),
                    group_by=self.group_by,
                    key=key,
                )
            )
            last = end
            position += 1
        if last is not None:
            self._last_alert[key] = last
        return alerts

    def _forget(self, older_than: float) -> None:
        """Drop row times no window can reach any more, and expired quiet periods."""
        for key, times in list(self._recent.items()):
            kept = times[np.searchsorted(times, older_than, side="right") :]
            if len(kept):
                self._recent[key] = kept
            else:
                del self._recent[key]
        for key, last in list(self._last_alert.items()):
            if last + self.cooldown <= older_than:
                del self._last_alert[key]


RULE_TYPES: dict[str, type[Rule]] = {
    "threshold": ThresholdRule,
}


def load_rules(path: Path) -> list[Rule]:
    """Build the rules listed in a rules.toml file."""
    with path.open("rb") as file:
        tables = tomllib.load(file).get("rules", [])
    rules = []
    for table in tables:
        options = dict(table)
        name = options.pop("name", None)
        kind = options.pop("type", None)
        if not name or not kind:
            raise ValueError(f"{path}: every rule needs a name and a type")
        description = options.pop("description", name)
        rule_class = RULE_TYPES.get(kind)
        if rule_class is None:
            known = ", ".join(sorted(RULE_TYPES))
            raise ValueError(f"rule {name!r}: unknown type {kind!r} (known: {known})")
        rules.append(rule_class.from_config(name, description, options))

    if not rules:
        raise ValueError(f"{path} defines no rules")
    names = [rule.name for rule in rules]
    duplicates = sorted({name for name in names if names.count(name) > 1})
    if duplicates:
        raise ValueError(f"{path}: rule names must be unique, repeated: {', '.join(duplicates)}")
    return rules


def _as_tuple(values: Any) -> tuple[str, ...]:
    # The journal columns are text, so `error_code = 203` in rules.toml means "203"
    # and `test_mode = true` means "True", as the app writes it
    if isinstance(values, list | tuple):
        return tuple(str(value) for value in values)
    return (str(values),)


def _seconds(rule: str, option: str, value: Any) -> float:
    """A duration from rules.toml: a number of seconds, or text such as "1min" or "1h"."""
    if isinstance(value, bool):
        raise ValueError(f"rule {rule!r}: {option} must be a duration, got {value!r}")
    if isinstance(value, int | float):
        return float(value)
    try:
        return pd.Timedelta(value).total_seconds()
    except ValueError:
        raise ValueError(f"rule {rule!r}: {option} must be a duration, got {value!r}") from None
