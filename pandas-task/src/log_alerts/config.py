"""Settings, read from environment variables so Docker Compose can set them.

Every setting has a default that works when the services run from the
project folder, so none of the variables is required.
"""

from __future__ import annotations

import os
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    """Processor settings; from_env shows the variable behind each one."""

    incoming_dir: Path = Path("data/incoming")
    processed_dir: Path = Path("data/processed")
    failed_dir: Path = Path("data/failed")
    alerts_file: Path = Path("data/alerts/alerts.jsonl")
    rules_file: Path = Path("rules.toml")
    poll_seconds: float = 1.0
    report_seconds: float = 10.0
    chunk_rows: int = 100_000
    smtp_host: str = ""  # email is sent only when this is set
    smtp_port: int = 1025
    email_from: str = "log-alerts@example.com"
    email_to: tuple[str, ...] = ("oncall@example.com",)
    log_level: str = "INFO"

    @classmethod
    def from_env(cls, env: Mapping[str, str] = os.environ) -> Settings:
        default = cls()
        settings = cls(
            incoming_dir=_read(env, "INCOMING_DIR", default.incoming_dir, Path),
            processed_dir=_read(env, "PROCESSED_DIR", default.processed_dir, Path),
            failed_dir=_read(env, "FAILED_DIR", default.failed_dir, Path),
            alerts_file=_read(env, "ALERTS_FILE", default.alerts_file, Path),
            rules_file=_read(env, "RULES_FILE", default.rules_file, Path),
            poll_seconds=_read(env, "POLL_SECONDS", default.poll_seconds, float),
            report_seconds=_read(env, "REPORT_SECONDS", default.report_seconds, float),
            chunk_rows=_read(env, "CHUNK_ROWS", default.chunk_rows, int),
            smtp_host=_read(env, "SMTP_HOST", default.smtp_host, str),
            smtp_port=_read(env, "SMTP_PORT", default.smtp_port, int),
            email_from=_read(env, "ALERT_EMAIL_FROM", default.email_from, str),
            email_to=_read(env, "ALERT_EMAIL_TO", default.email_to, _addresses),
            log_level=_read(env, "LOG_LEVEL", default.log_level, str.upper),
        )
        if settings.poll_seconds <= 0 or settings.report_seconds <= 0:
            raise ValueError("POLL_SECONDS and REPORT_SECONDS must be greater than zero")
        if settings.chunk_rows < 1:
            raise ValueError("CHUNK_ROWS must be at least 1")
        return settings


@dataclass(frozen=True)
class ReplaySettings:
    """Replayer settings; from_env shows the variable behind each one."""

    source_file: Path = Path("data/source/data.csv")
    incoming_dir: Path = Path("data/incoming")
    staging_dir: Path = Path("data/replay")
    journal_seconds: float = 60.0
    speed: float = 3600.0  # seconds of log time per real second, 0 for no waiting
    chunk_rows: int = 100_000
    log_level: str = "INFO"

    @classmethod
    def from_env(cls, env: Mapping[str, str] = os.environ) -> ReplaySettings:
        default = cls()
        settings = cls(
            source_file=_read(env, "SOURCE_FILE", default.source_file, Path),
            incoming_dir=_read(env, "INCOMING_DIR", default.incoming_dir, Path),
            staging_dir=_read(env, "STAGING_DIR", default.staging_dir, Path),
            journal_seconds=_read(env, "JOURNAL_SECONDS", default.journal_seconds, float),
            speed=_read(env, "SPEED", default.speed, float),
            chunk_rows=_read(env, "CHUNK_ROWS", default.chunk_rows, int),
            log_level=_read(env, "LOG_LEVEL", default.log_level, str.upper),
        )
        if settings.journal_seconds < 1:
            raise ValueError("JOURNAL_SECONDS must be at least 1")
        if settings.speed < 0:
            raise ValueError("SPEED cannot be negative")
        if settings.chunk_rows < 1:
            raise ValueError("CHUNK_ROWS must be at least 1")
        return settings


def _read[T](env: Mapping[str, str], name: str, default: T, convert: Callable[[str], T]) -> T:
    value = env.get(name, "").strip()
    if not value:
        return default
    try:
        return convert(value)
    except ValueError:
        raise ValueError(f"{name} has an invalid value: {value!r}") from None


def _addresses(value: str) -> tuple[str, ...]:
    """Comma-separated email addresses."""
    addresses = tuple(address.strip() for address in value.split(",") if address.strip())
    if not addresses:
        raise ValueError(value)
    return addresses
