from __future__ import annotations

import json
import logging

import pytest

from helpers import make_alert
from log_alerts import notifiers
from log_alerts.notifiers import (
    EmailNotifier,
    JsonLinesNotifier,
    LogNotifier,
    Notifier,
    notify_all,
)


class FakeSMTP:
    """Stands in for smtplib.SMTP and keeps what would have been sent."""

    connections: list[FakeSMTP] = []

    def __init__(self, host: str, port: int, timeout: float) -> None:
        self.address = (host, port)
        self.messages = []
        FakeSMTP.connections.append(self)

    def __enter__(self) -> FakeSMTP:
        return self

    def __exit__(self, *exc_info: object) -> None:
        return None

    def send_message(self, message) -> None:
        self.messages.append(message)


@pytest.fixture
def smtp(monkeypatch) -> list[FakeSMTP]:
    FakeSMTP.connections = []
    monkeypatch.setattr(notifiers.smtplib, "SMTP", FakeSMTP)
    return FakeSMTP.connections


def email_notifier() -> EmailNotifier:
    return EmailNotifier(
        "mailpit", 1025, "alerts@example.com", ["oncall@example.com", "dev@example.com"]
    )


def test_email_sends_one_message_per_alert_over_one_connection(smtp):
    email_notifier().send([make_alert(), make_alert(key="com.example.game", group_by="bundle_id")])

    assert len(smtp) == 1
    assert smtp[0].address == ("mailpit", 1025)
    subjects = [message["Subject"] for message in smtp[0].messages]
    assert subjects == [
        "[alert] More than 10 fatal errors in less than one minute",
        "[alert] More than 10 fatal errors in less than one minute (bundle_id = com.example.game)",
    ]
    assert smtp[0].messages[0]["To"] == "oncall@example.com, dev@example.com"


def test_email_body_has_the_details():
    alert = make_alert(group_by="bundle_id", key="com.example.game")

    body = email_notifier().message(alert).get_content()

    assert "Rule:           fatal_errors_per_minute\n" in body
    assert "Matching rows:  23\n" in body
    assert "First row:      2021-09-12 10:31:20 UTC\n" in body
    assert "Last row:       2021-09-12 10:32:01 UTC\n" in body
    assert "bundle_id:      com.example.game\n" in body


def test_email_needs_a_recipient():
    with pytest.raises(ValueError, match="recipient"):
        EmailNotifier("mailpit", 1025, "alerts@example.com", [])


def test_json_file_gets_one_line_per_alert(tmp_path):
    path = tmp_path / "alerts" / "alerts.jsonl"
    notifier = JsonLinesNotifier(path)

    notifier.send([make_alert(), make_alert(count=11)])
    notifier.send([make_alert(group_by="bundle_id", key="com.example.game")])

    records = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
    assert [(record["count"], record["key"]) for record in records] == [
        (23, None),
        (11, None),
        (23, "com.example.game"),
    ]


def test_log_notifier_writes_the_summary(caplog):
    with caplog.at_level(logging.WARNING, logger="log_alerts.notifiers"):
        LogNotifier().send([make_alert()])

    assert "ALERT More than 10 fatal errors in less than one minute: 23 from" in caplog.text


class BrokenNotifier(Notifier):
    name = "broken"

    def send(self, alerts) -> None:
        raise ConnectionRefusedError("no route to the mail server")


def test_a_channel_that_is_down_does_not_stop_the_others(tmp_path, caplog):
    path = tmp_path / "alerts.jsonl"

    with caplog.at_level(logging.ERROR, logger="log_alerts.notifiers"):
        notify_all([BrokenNotifier(), JsonLinesNotifier(path)], [make_alert()])

    assert len(path.read_text(encoding="utf-8").splitlines()) == 1
    assert "broken: could not send 1 alerts" in caplog.text


def test_nothing_is_sent_without_alerts(smtp, tmp_path):
    path = tmp_path / "alerts.jsonl"

    notify_all([email_notifier(), JsonLinesNotifier(path)], [])

    assert smtp == []
    assert not path.exists()
