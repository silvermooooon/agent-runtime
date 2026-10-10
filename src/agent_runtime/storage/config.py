"""Optional PostgreSQL / AWS settings. Never loads a .env file implicitly."""

import os
from dataclasses import dataclass, field
from urllib.parse import urlparse


@dataclass(frozen=True)
class DatabaseConfig:
    url: str = field(repr=False)
    pool_min_size: int = 1
    pool_max_size: int = 10
    connect_timeout_seconds: int = 10
    statement_timeout_seconds: int = 30

    def __post_init__(self):
        if not self.url:
            raise ValueError("AGENT_DB_URL is required")
        if not 0 <= self.pool_min_size <= self.pool_max_size or self.pool_max_size < 1:
            raise ValueError("Invalid DB pool sizes")
        if min(self.connect_timeout_seconds, self.statement_timeout_seconds) <= 0:
            raise ValueError("DB timeouts must be positive")

    @classmethod
    def from_env(cls, env=None):
        env = os.environ if env is None else env
        return cls(
            env.get("AGENT_DB_URL", ""),
            **{
                name: int(env.get("AGENT_DB_" + name.upper()) or default)
                for name, default in (
                    ("pool_min_size", 1),
                    ("pool_max_size", 10),
                    ("connect_timeout_seconds", 10),
                    ("statement_timeout_seconds", 30),
                )
            },
        )


@dataclass(frozen=True)
class ArchiveConfig:
    enabled: bool = False
    after_days: int = 30
    max_events: int = 10000
    target_bytes: int = 33554432
    s3_uri: str = ""
    region: str = ""
    kms_key_arn: str = ""

    def __post_init__(self):
        if self.after_days < 0 or min(self.max_events, self.target_bytes) <= 0:
            raise ValueError("Invalid archive interval or chunk limits")
        if self.enabled:
            uri = urlparse(self.s3_uri)
            if uri.scheme != "s3" or not uri.netloc:
                raise ValueError("AGENT_ARCHIVE_S3_URI must be s3://bucket/prefix")
            arn = self.kms_key_arn.split(":")
            if (
                len(arn) != 6
                or arn[0] != "arn"
                or arn[2] != "kms"
                or arn[3] != self.region
                or not arn[5].startswith("key/")
                or not self.region
            ):
                raise ValueError("A KMS key ARN in AWS_REGION is required")

    @classmethod
    def from_env(cls, env=None):
        env = os.environ if env is None else env
        enabled = env.get("AGENT_ARCHIVE_ENABLED", "false").lower()
        if enabled not in ("true", "false"):
            raise ValueError("AGENT_ARCHIVE_ENABLED must be true or false")
        return cls(
            enabled=enabled == "true",
            after_days=int(env.get("AGENT_ARCHIVE_AFTER_DAYS") or 30),
            max_events=int(env.get("AGENT_ARCHIVE_MAX_EVENTS") or 10000),
            target_bytes=int(env.get("AGENT_ARCHIVE_TARGET_BYTES") or 33554432),
            s3_uri=env.get("AGENT_ARCHIVE_S3_URI", ""),
            region=env.get("AWS_REGION", ""),
            kms_key_arn=env.get("AGENT_ARCHIVE_KMS_KEY_ARN", ""),
        )
