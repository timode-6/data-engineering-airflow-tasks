from __future__ import annotations

from pathlib import Path

import pytest

from log_alerts.config import Settings


def test_defaults_work_from_the_project_folder():
    settings = Settings.from_env({})

    assert settings.incoming_dir == Path("data/incoming")
    assert settings.rules_file == Path("rules.toml")
    assert settings.smtp_host == ""
    assert settings.email_to == ("oncall@example.com",)


def test_environment_overrides_the_defaults():
    settings = Settings.from_env(
        {
            "INCOMING_DIR": "/data/incoming",
            "CHUNK_ROWS": "5000",
            "POLL_SECONDS": "0.5",
            "SMTP_HOST": "mailpit",
            "ALERT_EMAIL_TO": "oncall@example.com, dev@example.com",
            "LOG_LEVEL": "debug",
        }
    )

    assert settings.incoming_dir == Path("/data/incoming")
    assert (settings.chunk_rows, settings.poll_seconds) == (5000, 0.5)
    assert settings.smtp_host == "mailpit"
    assert settings.email_to == ("oncall@example.com", "dev@example.com")
    assert settings.log_level == "DEBUG"


def test_empty_variables_keep_the_defaults():
    assert Settings.from_env({"SMTP_PORT": "", "CHUNK_ROWS": "  "}) == Settings()


@pytest.mark.parametrize(
    ("env", "message"),
    [
        ({"CHUNK_ROWS": "many"}, "CHUNK_ROWS has an invalid value: 'many'"),
        ({"CHUNK_ROWS": "0"}, "CHUNK_ROWS must be at least 1"),
        ({"POLL_SECONDS": "-1"}, "POLL_SECONDS and REPORT_SECONDS"),
        ({"ALERT_EMAIL_TO": " , "}, "ALERT_EMAIL_TO has an invalid value"),
    ],
)
def test_invalid_values_name_the_variable(env, message):
    with pytest.raises(ValueError, match=message):
        Settings.from_env(env)
