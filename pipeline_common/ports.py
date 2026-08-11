"""Ports: the abstractions both services depend on.

These are `typing.Protocol`s rather than ABCs -- structural typing means an
adapter never has to import or subclass anything from here, so the dependency
arrow points only one way (services -> ports, adapters -> nothing).

Every port below has at least two real implementations (a production adapter and
a faithful in-memory double used by the tests). Nothing here is speculative.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Protocol

from pipeline_common.messages import FrameRef, JobRecord, JobStatus


class FrameStoreError(RuntimeError):
    """The frame store is unreachable or rejected a write."""


class PublishError(RuntimeError):
    """The broker did not confirm a published frame."""


class FrameStore(Protocol):
    """Content-addressed blob storage for JPEG frames, with TTL."""

    async def put(self, key: str, data: bytes, ttl_sec: int) -> None: ...

    async def get(self, key: str) -> bytes | None:
        """Return the blob, or None if absent/expired -- absence is not an error."""
        ...

    async def ping(self) -> bool: ...

    async def close(self) -> None: ...


class FramePublisher(Protocol):
    """Publishes frame references and waits for broker acknowledgement."""

    async def publish(self, ref: FrameRef) -> None:
        """Publish and *await the publisher confirm*.

        Returning normally means the broker has durably accepted the message.
        This is what lets /analyze return 200 truthfully rather than
        fire-and-forget. Raises PublishError otherwise.
        """
        ...

    async def ping(self) -> bool: ...

    async def close(self) -> None: ...


class Delivery(Protocol):
    """One message handed to a consumer, still awaiting settlement."""

    @property
    def ref(self) -> FrameRef: ...

    @property
    def tag(self) -> int:
        """Broker delivery tag -- monotonically increasing per channel."""
        ...

    @property
    def redelivered(self) -> bool:
        """True if the broker has handed this message out before."""
        ...

    async def ack(self, *, multiple: bool = False) -> None:
        """Settle. With multiple=True, settles every unacked tag up to this one."""
        ...

    async def nack(self, *, requeue: bool) -> None:
        """Reject. requeue=False dead-letters the message."""
        ...


class FrameConsumer(Protocol):
    """Pulls frame references off the broker with bounded prefetch."""

    def deliveries(self) -> AsyncIterator[Delivery]: ...

    async def close(self) -> None: ...


class JobRepository(Protocol):
    """Job lifecycle state. See design 5.4 for why this is Redis today."""

    async def create(self, record: JobRecord) -> None: ...

    async def get(self, job_id: str) -> JobRecord | None: ...

    async def find_by_resume_key(self, resume_key: str) -> JobRecord | None:
        """Most recent job for this (video, rate), resumable or not."""
        ...

    async def set_status(
        self, job_id: str, status: JobStatus, error: str | None = None
    ) -> None: ...

    async def checkpoint(
        self,
        job_id: str,
        *,
        frames_dispatched: int,
        frames_failed: int,
        last_source_index: int,
    ) -> None: ...

    async def increment_processed(self, job_id: str, amount: int = 1) -> None: ...

    async def close(self) -> None: ...


class DedupGuard(Protocol):
    """At-least-once delivery means a frame can arrive twice; this makes the
    second arrival a no-op."""

    async def claim(self, key: str, ttl_sec: int) -> bool:
        """True if this caller won the claim (i.e. work should proceed)."""
        ...
