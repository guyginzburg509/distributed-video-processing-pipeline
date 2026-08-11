"""In-memory adapters.

These are *deliberately faithful*, not conveniences. Testing only against sloppy
doubles is how a suite goes green while production is broken, so each double
reproduces the behaviour that actually bites:

  * blobs are stored as ``bytes`` (not live objects), so serialization bugs surface;
  * TTL really expires, against an injectable clock, so the "blob gone -> DLQ"
    path is exercised without waiting an hour;
  * nack(requeue=True) really redelivers and flips ``redelivered``, so the
    ack-after-flush logic is exercised;
  * nack(requeue=False) really dead-letters.

tests/contract runs one shared suite against these *and* the real adapters, so
any divergence fails the build. See design 11.1.
"""

from __future__ import annotations

import asyncio
import itertools
import time
from collections import deque
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass, field

from pipeline_common.messages import FrameRef, JobRecord, JobStatus
from pipeline_common.ports import PublishError

Clock = Callable[[], float]


class InMemoryFrameStore:
    """Blob store with real TTL semantics against an injectable clock."""

    def __init__(self, clock: Clock = time.monotonic) -> None:
        self._clock = clock
        self._blobs: dict[str, tuple[bytes, float]] = {}
        self.closed = False

    async def put(self, key: str, data: bytes, ttl_sec: int) -> None:
        if not isinstance(data, bytes):  # pragma: no cover - guards misuse
            raise TypeError("frame store holds bytes, not objects")
        self._blobs[key] = (data, self._clock() + ttl_sec)

    async def get(self, key: str) -> bytes | None:
        entry = self._blobs.get(key)
        if entry is None:
            return None
        data, expires_at = entry
        if self._clock() >= expires_at:
            del self._blobs[key]
            return None
        return data

    async def ping(self) -> bool:
        return not self.closed

    async def close(self) -> None:
        self.closed = True

    def __len__(self) -> int:
        return len(self._blobs)


@dataclass
class _Envelope:
    ref: FrameRef
    tag: int
    redelivered: bool = False


class InMemoryDelivery:
    """One in-flight message. Settlement mutates the owning broker."""

    def __init__(self, envelope: _Envelope, broker: InMemoryBroker) -> None:
        self._env = envelope
        self._broker = broker

    @property
    def ref(self) -> FrameRef:
        return self._env.ref

    @property
    def tag(self) -> int:
        return self._env.tag

    @property
    def redelivered(self) -> bool:
        return self._env.redelivered

    async def ack(self, *, multiple: bool = False) -> None:
        self._broker._ack(self._env.tag, multiple=multiple)

    async def nack(self, *, requeue: bool) -> None:
        self._broker._nack(self._env.tag, requeue=requeue)


class InMemoryBroker:
    """FIFO work queue with delivery tags, redelivery and a dead-letter queue."""

    def __init__(self) -> None:
        self._ready: deque[_Envelope] = deque()
        self._unacked: dict[int, _Envelope] = {}
        self._tags = itertools.count(1)
        self._wakeup = asyncio.Event()
        self._closed = False
        self.dead_lettered: list[FrameRef] = []
        self.acked: list[int] = []

    # -- producer side ----------------------------------------------------
    def publish(self, ref: FrameRef) -> None:
        self._ready.append(_Envelope(ref=ref, tag=next(self._tags)))
        self._wakeup.set()

    # -- consumer side ----------------------------------------------------
    async def next_delivery(self) -> InMemoryDelivery | None:
        while True:
            if self._ready:
                return InMemoryDelivery(self._ready.popleft(), self)
            if self._closed:
                return None
            self._wakeup.clear()
            await self._wakeup.wait()

    def _ack(self, tag: int, *, multiple: bool) -> None:
        # Mirrors AMQP: multiple=True settles every still-unacked tag up to
        # this one. Already-settled tags are simply absent, so it is safe.
        targets = [t for t in self._unacked if t <= tag] if multiple else [tag]
        for t in targets:
            self._unacked.pop(t, None)
            self.acked.append(t)

    def _nack(self, tag: int, *, requeue: bool) -> None:
        env = self._unacked.pop(tag, None)
        if env is None:
            return
        if requeue:
            env.redelivered = True
            self._ready.append(env)
            self._wakeup.set()
        else:
            self.dead_lettered.append(env.ref)

    def track(self, delivery: InMemoryDelivery) -> None:
        self._unacked[delivery.tag] = delivery._env

    def close(self) -> None:
        self._closed = True
        self._wakeup.set()

    @property
    def unacked_count(self) -> int:
        return len(self._unacked)

    @property
    def depth(self) -> int:
        return len(self._ready)


class InMemoryFramePublisher:
    """Publisher whose `publish` returns only once the broker has the message --
    the in-memory analogue of a publisher confirm."""

    def __init__(self, broker: InMemoryBroker, fail_after: int | None = None) -> None:
        self._broker = broker
        self._fail_after = fail_after
        self.published: list[FrameRef] = []
        self.closed = False

    async def publish(self, ref: FrameRef) -> None:
        if self._fail_after is not None and len(self.published) >= self._fail_after:
            raise PublishError(f"simulated broker failure after {self._fail_after} frames")
        self._broker.publish(ref)
        self.published.append(ref)

    async def ping(self) -> bool:
        return not self.closed

    async def close(self) -> None:
        self.closed = True


class InMemoryFrameConsumer:
    def __init__(self, broker: InMemoryBroker) -> None:
        self._broker = broker

    async def deliveries(self) -> AsyncIterator[InMemoryDelivery]:
        while True:
            delivery = await self._broker.next_delivery()
            if delivery is None:
                return
            self._broker.track(delivery)
            yield delivery

    async def close(self) -> None:
        self._broker.close()


@dataclass
class InMemoryJobRepository:
    records: dict[str, JobRecord] = field(default_factory=dict)

    async def create(self, record: JobRecord) -> None:
        self.records[record.job_id] = record.model_copy()

    async def get(self, job_id: str) -> JobRecord | None:
        record = self.records.get(job_id)
        return record.model_copy() if record else None

    async def find_by_resume_key(self, resume_key: str) -> JobRecord | None:
        if not resume_key:
            return None
        # Last write wins, mirroring the Redis pointer key.
        for record in reversed(list(self.records.values())):
            if record.resume_key == resume_key:
                return record.model_copy()
        return None

    async def set_status(self, job_id: str, status: JobStatus, error: str | None = None) -> None:
        record = self.records.get(job_id)
        if record is None:
            return
        self.records[job_id] = record.model_copy(
            update={"status": status, "error": error, "updated_at": time.time()}
        )

    async def checkpoint(
        self, job_id: str, *, frames_dispatched: int, frames_failed: int, last_source_index: int
    ) -> None:
        record = self.records.get(job_id)
        if record is None:
            return
        self.records[job_id] = record.model_copy(
            update={
                "frames_dispatched": frames_dispatched,
                "frames_failed": frames_failed,
                "last_source_index": last_source_index,
                "updated_at": time.time(),
            }
        )

    async def increment_processed(self, job_id: str, amount: int = 1) -> None:
        record = self.records.get(job_id)
        if record is None:
            return
        self.records[job_id] = record.model_copy(
            update={"frames_processed": record.frames_processed + amount}
        )

    async def close(self) -> None:
        return None


class InMemoryDedupGuard:
    def __init__(self) -> None:
        self._claimed: set[str] = set()

    async def claim(self, key: str, ttl_sec: int) -> bool:
        if key in self._claimed:
            return False
        self._claimed.add(key)
        return True
