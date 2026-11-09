"""Settings from the environment, with .env loaded first"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass

from dotenv import find_dotenv, load_dotenv

AWS_VARIABLES = (
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_DEFAULT_REGION",
    "AWS_ENDPOINT_URL",
)


class ConfigError(ValueError):
    """A required setting is missing"""


def load_env() -> None:
    """Load the nearest .env; variables already set in the environment win"""
    load_dotenv(find_dotenv(usecwd=True))


@dataclass(frozen=True)
class AwsSettings:
    """Where LocalStack is, and the region everything lives in"""

    endpoint_url: str
    region: str

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> AwsSettings:
        """Read from env, or from os.environ after loading .env"""
        if env is None:
            load_env()
            env = os.environ
        missing = [name for name in AWS_VARIABLES if not env.get(name)]
        if missing:
            raise ConfigError(
                f"{', '.join(missing)} not set. Copy .env.example to .env and run from the "
                "project folder"
            )
        return cls(endpoint_url=env["AWS_ENDPOINT_URL"], region=env["AWS_DEFAULT_REGION"])
