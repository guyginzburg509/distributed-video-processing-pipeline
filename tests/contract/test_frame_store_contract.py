"""One suite, every FrameStore implementation.

Tests are written against the *port*, never a concrete class. Offline the
``redis`` parameter skips and only the double runs, so the suite stays green
with no infrastructure. With `docker compose up`, both run and any divergence
between the double and production fails the build.

This is the guard against the classic failure where a suite is green because
the in-memory double is more forgiving than the real thing.
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import AsyncIterator

import pytest

from pipeline_common.adapters.memory import InMemoryFrameStore
from pipeline_common.adapters.redis_store import RedisFrameStore
from pipeline_common.ports import FrameStore
from tests.conftest import DEFAULT_REDIS_URL, redis_available


@pytest.fixture(params=["memory", "redis"])
async def frame_store(request: pytest.FixtureRequest) -> AsyncIterator[FrameStore]:
    if request.param == "redis":
        if not redis_available():
            pytest.skip("live Redis not available (start it with `docker compose up redis`)")
        store = RedisFrameStore(DEFAULT_REDIS_URL)
        try:
            yield store
        finally:
            await store.close()
    else:
        yield InMemoryFrameStore()


@pytest.fixture
def key() -> str:
    return f"test:frame:{uuid.uuid4().hex}"


class TestBlobRoundTrip:
    async def test_returns_exactly_the_bytes_written(
        self, frame_store: FrameStore, key: str, jpeg_bytes: bytes
    ) -> None:
        await frame_store.put(key, jpeg_bytes, 60)
        assert await frame_store.get(key) == jpeg_bytes

    async def test_survives_arbitrary_binary(self, frame_store: FrameStore, key: str) -> None:
        """Real JPEGs contain null bytes and invalid UTF-8; a double that keeps
        Python objects would hide an encoding bug here."""
        payload = bytes(range(256)) * 4
        await frame_store.put(key, payload, 60)
        result = await frame_store.get(key)
        assert result == payload
        assert isinstance(result, bytes)

    async def test_overwrite_replaces(self, frame_store: FrameStore, key: str) -> None:
        await frame_store.put(key, b"first", 60)
        await frame_store.put(key, b"second", 60)
        assert await frame_store.get(key) == b"second"


class TestAbsence:
    async def test_missing_key_is_none_not_an_error(
        self, frame_store: FrameStore, key: str
    ) -> None:
        """The detector distinguishes "gone" (dead-letter) from "store broken"
        (retry), so absence must not raise."""
        assert await frame_store.get(key) is None



class TestExpiry:
    async def test_blob_expires(self, frame_store: FrameStore, key: str) -> None:
        """The TTL is what bounds backlog memory and self-cleans a crashed run.
        It is also what produces the "blob gone -> DLQ" path in the detector."""
        await frame_store.put(key, b"ephemeral", 1)
        assert await frame_store.get(key) == b"ephemeral"
        await asyncio.sleep(1.6)
        assert await frame_store.get(key) is None


class TestHealth:
    async def test_ping_true_when_reachable(self, frame_store: FrameStore) -> None:
        assert await frame_store.ping() is True
