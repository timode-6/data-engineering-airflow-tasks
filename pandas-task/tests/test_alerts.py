from __future__ import annotations

from helpers import make_alert


def test_summary_of_an_alert_over_all_rows():
    assert make_alert().summary() == (
        "More than 10 fatal errors in less than one minute: "
        "23 from 2021-09-12 10:31:20 to 2021-09-12 10:32:01 UTC"
    )


def test_subject_names_the_group():
    alert = make_alert(group_by="bundle_id", key="com.example.game")

    assert alert.subject == (
        "More than 10 fatal errors in less than one minute (bundle_id = com.example.game)"
    )


def test_to_dict_is_ready_for_json():
    data = make_alert(group_by="bundle_id", key="com.example.game").to_dict()

    assert data["first_event"] == "2021-09-12T10:31:20+00:00"
    assert data["last_event"] == "2021-09-12T10:32:01.500000+00:00"
    assert (data["rule"], data["key"], data["count"]) == (
        "fatal_errors_per_minute",
        "com.example.game",
        23,
    )
