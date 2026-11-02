"""Sending alerts.

Every alert goes through all notifiers: the log, a JSON Lines file and email.
A new channel, such as Slack or Telegram, is another Notifier subclass.
"""

from __future__ import annotations

import json
import logging
import smtplib
from abc import ABC, abstractmethod
from collections.abc import Sequence
from email.message import EmailMessage
from email.utils import format_datetime
from pathlib import Path
from typing import ClassVar

from log_alerts.alerts import Alert

logger = logging.getLogger(__name__)


class Notifier(ABC):
    """One way of sending alerts."""

    name: ClassVar[str]

    @abstractmethod
    def send(self, alerts: Sequence[Alert]) -> None:
        """Send the alerts. Raises OSError when the channel is unavailable."""


class LogNotifier(Notifier):
    """Writes each alert to the log as a warning."""

    name = "log"

    def send(self, alerts: Sequence[Alert]) -> None:
        for alert in alerts:
            logger.warning("ALERT %s", alert.summary())


class JsonLinesNotifier(Notifier):
    """Appends each alert to a file as one line of JSON: the record of every alert."""

    name = "json file"

    def __init__(self, path: Path) -> None:
        self.path = path

    def send(self, alerts: Sequence[Alert]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("a", encoding="utf-8") as file:
            for alert in alerts:
                file.write(json.dumps(alert.to_dict(), ensure_ascii=False) + "\n")


class EmailNotifier(Notifier):
    """Sends one email per alert, all of a batch over one SMTP connection."""

    name = "email"

    def __init__(
        self,
        host: str,
        port: int,
        sender: str,
        recipients: Sequence[str],
        timeout: float = 10.0,
    ) -> None:
        if not recipients:
            raise ValueError("email alerts need at least one recipient")
        self.host = host
        self.port = port
        self.sender = sender
        self.recipients = list(recipients)
        self.timeout = timeout

    def send(self, alerts: Sequence[Alert]) -> None:
        with smtplib.SMTP(self.host, self.port, timeout=self.timeout) as smtp:
            for alert in alerts:
                smtp.send_message(self.message(alert))

    def message(self, alert: Alert) -> EmailMessage:
        message = EmailMessage()
        message["Subject"] = f"[alert] {alert.subject}"
        message["From"] = self.sender
        message["To"] = ", ".join(self.recipients)
        message["Date"] = format_datetime(alert.detected_at)
        lines = [
            alert.subject,
            "",
            f"Rule:           {alert.rule}",
            f"Matching rows:  {alert.count}",
            f"First row:      {alert.first_event:%Y-%m-%d %H:%M:%S} UTC",
            f"Last row:       {alert.last_event:%Y-%m-%d %H:%M:%S} UTC",
        ]
        if alert.key is not None:
            lines.append(f"{alert.group_by + ':':<16}{alert.key}")
        lines.append(f"Detected at:    {alert.detected_at:%Y-%m-%d %H:%M:%S} UTC")
        message.set_content("\n".join(lines) + "\n")
        return message


def notify_all(notifiers: Sequence[Notifier], alerts: Sequence[Alert]) -> None:
    """Send the alerts through every notifier.

    A channel that is down is logged and skipped, so the others still get the
    alerts: an SMTP outage does not lose them from the log or the JSON file.
    """
    if not alerts:
        return
    for notifier in notifiers:
        try:
            notifier.send(alerts)
        except OSError as error:  # smtplib's errors are OSErrors too
            logger.error("%s: could not send %d alerts: %s", notifier.name, len(alerts), error)
