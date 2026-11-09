from __future__ import annotations

import pytest

from bikes.config import AWS_VARIABLES, AwsSettings, ConfigError

COMPLETE = {
    "AWS_ACCESS_KEY_ID": "test",
    "AWS_SECRET_ACCESS_KEY": "test",
    "AWS_DEFAULT_REGION": "eu-north-1",
    "AWS_ENDPOINT_URL": "http://localhost:4566",
}


def test_settings_come_from_the_environment():
    settings = AwsSettings.from_env(COMPLETE)

    assert settings == AwsSettings(endpoint_url="http://localhost:4566", region="eu-north-1")


def test_every_missing_variable_is_named():
    partial = {**COMPLETE, "AWS_ENDPOINT_URL": "", "AWS_DEFAULT_REGION": ""}
    partial.pop("AWS_SECRET_ACCESS_KEY")

    with pytest.raises(
        ConfigError, match="AWS_SECRET_ACCESS_KEY, AWS_DEFAULT_REGION, AWS_ENDPOINT_URL"
    ):
        AwsSettings.from_env(partial)


def test_dot_env_in_the_current_folder_is_loaded(tmp_path, monkeypatch):
    for name in AWS_VARIABLES:  # set then delete, so teardown removes what .env adds
        monkeypatch.setenv(name, "placeholder")
        monkeypatch.delenv(name)
    lines = [f"{name}={value}" for name, value in COMPLETE.items()]
    (tmp_path / ".env").write_text("\n".join(lines) + "\n", encoding="utf-8")
    monkeypatch.chdir(tmp_path)

    assert AwsSettings.from_env().endpoint_url == "http://localhost:4566"


def test_the_real_environment_wins_over_dot_env(tmp_path, monkeypatch):
    for name, value in COMPLETE.items():
        monkeypatch.setenv(name, value)
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    (tmp_path / ".env").write_text("AWS_DEFAULT_REGION=eu-north-1\n", encoding="utf-8")
    monkeypatch.chdir(tmp_path)

    assert AwsSettings.from_env().region == "us-east-1"
