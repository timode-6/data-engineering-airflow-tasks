"""boto3 clients and resources; the AWS_* variables say where they connect"""

from __future__ import annotations

from typing import Any

import boto3
from botocore.config import Config

# Path-style URLs (http://host/bucket/key) work with any endpoint, LocalStack too
S3_CONFIG = Config(s3={"addressing_style": "path"})


def client(service: str, **kwargs: Any) -> Any:
    """A boto3 client for service; S3 gets path-style addressing"""
    if service == "s3":
        kwargs.setdefault("config", S3_CONFIG)
    return boto3.client(service, **kwargs)


def resource(service: str, **kwargs: Any) -> Any:
    """A boto3 resource on a new session, since resources must not be shared between threads"""
    return boto3.session.Session().resource(service, **kwargs)
