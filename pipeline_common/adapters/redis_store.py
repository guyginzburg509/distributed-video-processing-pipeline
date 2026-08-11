"""Redis adapters: frame blob store, job repository, dedup guard.

Redis is the right tool for everything stored here -- all of it is hot,
high-churn and TTL-scoped. Note what is *not* here: durable, queryable job
history. That belongs in a relational store, and the ``JobRepository`` port
exists so adding ``PostgresJobRepository`` later is additive (design 5.4).
"""

from __future__ import annotations

from typing import Any

import redis.asyncio as aioredis
from redis.exceptions import RedisError

from pipeline_common.messages import JobRecord, JobStatus
from pipeline_common.ports import FrameStoreError


def _client(url: str) -> aioredis.Redis:
    # decode_responses stays False: frame blobs are binary JPEG.
    return aioredis.from_url(url, decode_responses=False)


class RedisFrameStore:
    """Claim-check blob storage. Keys carry a TTL so a crashed pipeline leaves
    no garbage behind and there is no orphan-collection job to run."""

    def __init__(self, url: str, *, client: aioredis.Redis | None = None) -> None:
        self._redis = client or _client(url)

    async def put(self, key: str, data: bytes, ttl_sec: int) -> None:
        try:
            await self._redis.set(key, data, ex=ttl_sec)
        except RedisError as exc:
            raise FrameStoreError(f"failed to store frame {key!r}: {exc}") from exc

    async def get(self, key: str) -> bytes | None:
        try:
            value = await self._redis.get(key)
        except RedisError as exc:
            raise FrameStoreError(f"failed to read frame {key!r}: {exc}") from exc
        return bytes(value) if value is not None else None

    async def ping(self) -> bool:
        try:
            return bool(await self._redis.ping())
        except RedisError:
            return False

    async def close(self) -> None:
        await self._redis.aclose()


class RedisJobRepository:
    """Job state as a Redis hash.

    Counters use ``HINCRBY`` rather than read-modify-write so concurrent
    detectors cannot lose increments.
    """

    def __init__(self, url: str, *, client: aioredis.Redis | None = None, ttl_sec: int = 86_400):
        self._redis = client or _client(url)
        self._ttl = ttl_sec

    @staticmethod
    def _key(job_id: str) -> str:
        return f"job:{job_id}"

    @staticmethod
    def _resume_pointer(resume_key: str) -> str:
        return f"resume:{resume_key}"

    async def create(self, record: JobRecord) -> None:
        payload = {k: ("" if v is None else str(v)) for k, v in record.model_dump().items()}
        key = self._key(record.job_id)
        try:
            async with self._redis.pipeline(transaction=True) as pipe:
                await pipe.hset(key, mapping=payload)
                await pipe.expire(key, self._ttl)
                if record.resume_key:
                    # Pointer from "this video at this rate" -> latest job id,
                    # so a retry can find the checkpoint without scanning.
                    pointer = self._resume_pointer(record.resume_key)
                    await pipe.set(pointer, record.job_id, ex=self._ttl)
                await pipe.execute()
        except RedisError as exc:
            raise FrameStoreError(f"failed to create job {record.job_id!r}: {exc}") from exc

    async def get(self, job_id: str) -> JobRecord | None:
        try:
            raw: dict[bytes, bytes] = await self._redis.hgetall(self._key(job_id))
        except RedisError as exc:
            raise FrameStoreError(f"failed to read job {job_id!r}: {exc}") from exc
        if not raw:
            return None

        fields: dict[str, Any] = {k.decode(): v.decode() for k, v in raw.items()}
        if fields.get("error") == "":
            fields["error"] = None
        return JobRecord.model_validate(fields)

    async def find_by_resume_key(self, resume_key: str) -> JobRecord | None:
        if not resume_key:
            return None
        try:
            job_id = await self._redis.get(self._resume_pointer(resume_key))
        except RedisError as exc:
            raise FrameStoreError(f"failed to resolve resume key {resume_key!r}: {exc}") from exc
        if job_id is None:
            return None
        # The pointer outliving its record is normal (independent TTLs), and
        # simply means "no checkpoint to resume from".
        return await self.get(job_id.decode())

    async def set_status(self, job_id: str, status: JobStatus, error: str | None = None) -> None:
        mapping: dict[str, str] = {"status": status.value, "updated_at": _now()}
        if error is not None:
            mapping["error"] = error
        try:
            await self._redis.hset(self._key(job_id), mapping=mapping)
        except RedisError as exc:
            raise FrameStoreError(f"failed to update job {job_id!r}: {exc}") from exc

    async def checkpoint(
        self, job_id: str, *, frames_dispatched: int, frames_failed: int, last_source_index: int
    ) -> None:
        mapping = {
            "frames_dispatched": str(frames_dispatched),
            "frames_failed": str(frames_failed),
            "last_source_index": str(last_source_index),
            "updated_at": _now(),
        }
        try:
            await self._redis.hset(self._key(job_id), mapping=mapping)
        except RedisError as exc:
            raise FrameStoreError(f"failed to checkpoint job {job_id!r}: {exc}") from exc

    async def increment_processed(self, job_id: str, amount: int = 1) -> None:
        try:
            await self._redis.hincrby(self._key(job_id), "frames_processed", amount)
        except RedisError as exc:
            raise FrameStoreError(f"failed to increment job {job_id!r}: {exc}") from exc

    async def ping(self) -> bool:
        try:
            return bool(await self._redis.ping())
        except RedisError:
            return False

    async def close(self) -> None:
        await self._redis.aclose()


class RedisDedupGuard:
    """``SET NX EX`` -- exactly the primitive at-least-once delivery needs."""

    def __init__(self, url: str, *, client: aioredis.Redis | None = None) -> None:
        self._redis = client or _client(url)

    async def claim(self, key: str, ttl_sec: int) -> bool:
        try:
            return bool(await self._redis.set(key, b"1", ex=ttl_sec, nx=True))
        except RedisError:
            # Fail open: a duplicate detection is far cheaper than a dropped one.
            return True

    async def close(self) -> None:
        await self._redis.aclose()


def _now() -> str:
    import time

    return str(time.time())
