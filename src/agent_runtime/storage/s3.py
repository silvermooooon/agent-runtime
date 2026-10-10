"""Immutable compressed journals in AWS S3, always uploaded with SSE-KMS."""

import asyncio
import gzip
import hashlib
import json
from urllib.parse import quote, urlparse

from ..sessions.base import SessionError


def encode_records(records):
    raw = b"".join(
        (
            json.dumps(r, ensure_ascii=True, allow_nan=False, sort_keys=True, separators=(",", ":"))
            + "\n"
        ).encode()
        for r in records
    )
    return gzip.compress(raw, mtime=0)


class S3Archive:
    def __init__(self, config, *, client=None):
        self.config = config
        if client is None:
            import boto3

            client = boto3.client("s3", region_name=config.region)
        self.client = client

    async def put(self, tenant_id, session_id, records):
        if not self.config.enabled:
            raise ValueError("Archive uploads must be explicitly enabled")
        return await asyncio.to_thread(self._put, tenant_id, session_id, records)

    def _put(self, tenant_id, session_id, records):
        body = encode_records(records)
        digest = hashlib.sha256(body).hexdigest()
        base = urlparse(self.config.s3_uri)
        key = "/".join(
            filter(
                None,
                [
                    base.path.strip("/"),
                    quote(tenant_id, safe=""),
                    quote(session_id, safe=""),
                    f"{records[0]['seq']}-{records[-1]['seq']}-{digest}.jsonl.gz",
                ],
            )
        )
        try:
            result = self.client.put_object(
                Bucket=base.netloc,
                Key=key,
                Body=body,
                IfNoneMatch="*",
                ServerSideEncryption="aws:kms",
                SSEKMSKeyId=self.config.kms_key_arn,
                ContentType="application/gzip",
                Metadata={"sha256": digest},
            )
        except Exception as error:
            if (
                getattr(error, "response", {}).get("ResponseMetadata", {}).get("HTTPStatusCode")
                != 412
            ):
                raise
            result = self.client.head_object(Bucket=base.netloc, Key=key)
        if (
            result.get("ServerSideEncryption") != "aws:kms"
            or result.get("SSEKMSKeyId") != self.config.kms_key_arn
        ):
            raise SessionError("S3 object did not use the configured KMS key")
        manifest = dict(
            object_uri=f"s3://{base.netloc}/{key}",
            version_id=result.get("VersionId"),
            sha256=digest,
            format_version=1,
            start_seq=records[0]["seq"],
            end_seq=records[-1]["seq"],
            metadata={"kms_key_arn": self.config.kms_key_arn, "bytes": len(body)},
        )
        # Verify stored bytes before the caller may remove any database rows.
        self._get(manifest)
        return manifest

    async def get(self, manifest):
        return await asyncio.to_thread(self._get, manifest)

    def _get(self, manifest):
        if manifest["format_version"] != 1:
            raise SessionError("Unsupported archive format")
        uri = urlparse(manifest["object_uri"])
        args = {"Bucket": uri.netloc, "Key": uri.path.lstrip("/")}
        if manifest.get("version_id"):
            args["VersionId"] = manifest["version_id"]
        response = self.client.get_object(**args)
        try:
            body = response["Body"].read()
        finally:
            response["Body"].close()
        if hashlib.sha256(body).hexdigest() != manifest["sha256"]:
            raise SessionError("Archive checksum mismatch")
        try:
            records = [json.loads(line) for line in gzip.decompress(body).splitlines()]
        except (ValueError, OSError, EOFError) as error:
            raise SessionError("Invalid archive contents") from error
        expected = list(range(manifest["start_seq"], manifest["end_seq"] + 1))
        if [r["seq"] for r in records] != expected:
            raise SessionError("Archive event range mismatch")
        return records
