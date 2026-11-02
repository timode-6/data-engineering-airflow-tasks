"""The alert a rule raises."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime


def to_utc(seconds: float) -> datetime:
    """A log time, in seconds since the Unix epoch, as a UTC datetime."""
    return datetime.fromtimestamp(seconds, tz=UTC)


@dataclass(frozen=True)
class Alert:
    """A rule's threshold was crossed.

    `count` matching rows fall between `first_event` and `last_event`, both
    log times. `key` is the value of the rule's `group_by` column, such as a
    bundle_id, or None for a rule over all rows.
    """

    rule: str
    description: str
    count: int
    first_event: datetime
    last_event: datetime
    group_by: str | None = None
    key: str | None = None
    detected_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    @property
    def subject(self) -> str:
        """The description, plus the group for a grouped rule."""
        if self.key is None:
            return self.description
        return f"{self.description} ({self.group_by} = {self.key})"

    def summary(self) -> str:
        """One line for logs and messages."""
        return (
            f"{self.subject}: {self.count} from {self.first_event:%Y-%m-%d %H:%M:%S} "
            f"to {self.last_event:%Y-%m-%d %H:%M:%S} UTC"
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "rule": self.rule,
            "description": self.description,
            "group_by": self.group_by,
            "key": self.key,
            "count": self.count,
            "first_event": self.first_event.isoformat(),
            "last_event": self.last_event.isoformat(),
            "detected_at": self.detected_at.isoformat(),
        }
