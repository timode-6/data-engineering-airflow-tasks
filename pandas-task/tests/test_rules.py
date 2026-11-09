from __future__ import annotations

import logging
import random
from pathlib import Path

import pandas as pd
import pytest

from helpers import START, errors_at, make_events, make_row, spread
from log_alerts.alerts import to_utc
from log_alerts.rules import ThresholdRule, load_rules

RULES_FILE = Path(__file__).resolve().parent.parent / "rules.toml"


def per_minute(**options) -> ThresholdRule:
    return ThresholdRule(
        "per_minute",
        "More than 10 errors in less than one minute",
        where={"severity": "Error"},
        window=60,
        threshold=10,
        **options,
    )


def per_bundle_per_hour(**options) -> ThresholdRule:
    return ThresholdRule(
        "per_bundle_per_hour",
        "More than 10 errors in less than one hour for one bundle_id",
        where={"severity": "Error"},
        group_by="bundle_id",
        window=3600,
        threshold=10,
        **options,
    )


def test_eleven_errors_in_less_than_a_minute_raise_one_alert():
    alerts = per_minute().evaluate(errors_at(*spread(11, 59.9)))

    assert len(alerts) == 1
    assert alerts[0].count == 11
    assert alerts[0].first_event == to_utc(START)
    assert alerts[0].last_event == to_utc(START + 59.9)
    assert alerts[0].key is None


def test_eleven_errors_spread_over_a_full_minute_do_not_alert():
    assert per_minute().evaluate(errors_at(*spread(11, 60))) == []


def test_ten_errors_do_not_alert_however_close():
    assert per_minute().evaluate(errors_at(*spread(10, 1))) == []


def test_rows_outside_where_are_not_counted():
    events = pd.concat([errors_at(*spread(10, 30)), errors_at(31, severity="Success")])

    assert per_minute().evaluate(events) == []


def test_burst_split_across_two_chunks_alerts_once():
    rule = per_minute()

    assert rule.evaluate(errors_at(0, 3, 6, 9, 12, 15)) == []
    assert [alert.count for alert in rule.evaluate(errors_at(18, 21, 24, 27, 30))] == [11]


def test_after_an_alert_the_rule_stays_quiet_for_one_window():
    # one error a second for three minutes
    alerts = per_minute().evaluate(errors_at(*range(180)))

    assert [alert.last_event for alert in alerts] == [to_utc(START + s) for s in (10, 70, 130)]
    assert [alert.count for alert in alerts] == [11, 60, 60]


def test_chunk_size_does_not_change_the_alerts():
    rule = per_minute()
    seconds = list(range(180))

    alerts = []
    for start in range(0, len(seconds), 7):
        alerts += rule.evaluate(errors_at(*seconds[start : start + 7]))

    assert [alert.last_event for alert in alerts] == [to_utc(START + s) for s in (10, 70, 130)]
    assert [alert.count for alert in alerts] == [11, 60, 60]


def test_bundles_are_counted_separately():
    game = errors_at(*spread(11, 3000), bundle_id="com.example.game")  # 11 in 50 minutes
    pet = errors_at(*spread(10, 3000), bundle_id="com.example.pet")  # 10 in 50 minutes

    alerts = per_bundle_per_hour().evaluate(pd.concat([game, pet]))

    assert [(alert.group_by, alert.key, alert.count) for alert in alerts] == [
        ("bundle_id", "com.example.game", 11)
    ]


def test_rows_without_a_bundle_id_are_ignored_by_a_per_bundle_rule():
    assert per_bundle_per_hour().evaluate(errors_at(*spread(11, 60), bundle_id="")) == []


def test_rows_that_arrive_a_little_late_still_count():
    rule = per_minute()
    assert rule.evaluate(errors_at(*range(100, 110))) == []  # ten errors

    alerts = rule.evaluate(errors_at(95))  # the eleventh, 14 s behind

    assert [(alert.count, alert.last_event) for alert in alerts] == [(11, to_utc(START + 109))]


def test_rows_later_than_lateness_are_dropped_with_a_warning(caplog):
    rule = per_minute(lateness=30)
    rule.evaluate(errors_at(*range(1000, 1010)))

    with caplog.at_level(logging.WARNING, logger="log_alerts.rules"):
        alerts = rule.evaluate(errors_at(900))  # 109 s behind the newest row

    assert alerts == []
    assert "dropped 1 rows" in caplog.text


def test_memory_holds_one_window_plus_lateness():
    rule = per_minute(lateness=30)

    for minute in range(10):  # ten minutes, one error a second
        rule.evaluate(errors_at(*range(minute * 60, minute * 60 + 60)))

    assert rule.buffered_rows <= 60 + 30 + 1


def brute_force_alerts(times, keys, window, threshold, cooldown):
    """The rule's definition, checked the slow way: count every window row by row."""
    alerts = []
    for key in sorted(set(keys)):
        group = sorted(t for t, k in zip(times, keys, strict=True) if k == key)
        last_alert = None
        for i, end in enumerate(group):
            count = sum(1 for t in group[: i + 1] if t > end - window)
            quiet = last_alert is not None and end < last_alert + cooldown
            if count > threshold and not quiet:
                alerts.append((key, to_utc(end), count))
                last_alert = end
    return sorted(alerts)


@pytest.mark.parametrize("seed", range(40))
def test_matches_a_brute_force_count_on_random_data(seed):
    rng = random.Random(seed)
    window = rng.choice([5.0, 30.0, 60.0])
    threshold = rng.randint(1, 12)
    cooldown = rng.choice([0.0, 10.0, window, 90.0])
    seconds = sorted(rng.uniform(0, 300) for _ in range(rng.randint(0, 300)))
    keys = [rng.choice(["com.a", "com.b", "com.c"]) for _ in seconds]
    rule = ThresholdRule(
        "random", "random", where={"severity": "Error"}, group_by="bundle_id",
        window=window, threshold=threshold, cooldown=cooldown,
    )  # fmt: skip

    alerts = []
    position = 0
    while position < len(seconds):  # random chunk sizes, rows shuffled inside each chunk
        size = rng.randint(1, 60)
        rows = list(
            zip(seconds[position : position + size], keys[position : position + size], strict=True)
        )
        rng.shuffle(rows)
        alerts += rule.evaluate(make_events(make_row(date=START + s, bundle_id=k) for s, k in rows))
        position += size

    expected = brute_force_alerts([START + s for s in seconds], keys, window, threshold, cooldown)
    assert sorted((a.key, a.last_event, a.count) for a in alerts) == expected


def test_project_rules_are_the_two_task_rules():
    per_minute_rule, per_bundle_rule = load_rules(RULES_FILE)

    assert per_minute_rule.name == "fatal_errors_per_minute"
    assert per_minute_rule.where == {"severity": ("Error",)}
    assert (per_minute_rule.group_by, per_minute_rule.window) == (None, 60)
    assert (per_minute_rule.threshold, per_minute_rule.cooldown) == (10, 60)
    assert per_bundle_rule.name == "fatal_errors_per_bundle_per_hour"
    assert (per_bundle_rule.group_by, per_bundle_rule.window) == ("bundle_id", 3600)
    assert per_bundle_rule.threshold == 10


@pytest.mark.parametrize(
    ("table", "message"),
    [
        ('type = "rate"', "unknown type 'rate'"),
        ('window = "1min"\nthreshold = 10', "needs a name and a type"),
        ('type = "threshold"\nwindow = "1min"', "missing threshold"),
        ('type = "threshold"\nwindow = "soon"\nthreshold = 10', "window must be a duration"),
        ('type = "threshold"\nwindow = "1min"\nthreshold = 0', "threshold must be"),
        ('type = "threshold"\nwindow = "1min"\nthreshold = 10\nthreshhold = 5', "threshhold"),
        ('type = "threshold"\nwindow = "1min"\nthreshold = 10\ngroup_by = "bundle"', "'bundle'"),
        (
            'type = "threshold"\nwindow = "1min"\nthreshold = 10\nwhere = { level = "Error" }',
            "level",
        ),
    ],
)
def test_mistakes_in_rules_toml_are_reported(tmp_path, table, message):
    path = tmp_path / "rules.toml"
    path.write_text(f'[[rules]]\nname = "broken"\n{table}\n', encoding="utf-8")

    with pytest.raises(ValueError, match=message):
        load_rules(path)


def test_rule_names_must_be_unique(tmp_path):
    rule = '[[rules]]\nname = "twice"\ntype = "threshold"\nwindow = 60\nthreshold = 10\n'
    path = tmp_path / "rules.toml"
    path.write_text(rule + rule, encoding="utf-8")

    with pytest.raises(ValueError, match="repeated: twice"):
        load_rules(path)
