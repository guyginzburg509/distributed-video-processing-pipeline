"""Batching detection results, and acknowledging the frames they came from.

``send_results_next_service`` takes a ``List[RespObject]``, so batching is the
intended shape rather than an optimisation. Flush happens on whichever comes
first: batch size, age, or shutdown.

Two details carry most of the value here:

**The lock.** A size-triggered flush (from ``add``) and an age-triggered flush
(from the timer task) mutate the same buffer. Without ``asyncio.Lock`` they
interleave at an await point and produce double-flushes and lost results --
intermittent, and miserable to reproduce.

**Ack after flush, never on receipt.** The broker keeps a message until it is
acked. If we acked on arrival, a crash while results sat in an unflushed buffer
would lose them permanently and silently. Acking only after a successful flush
turns that same crash into a redelivery.
"""

from __future__ import annotations

import asyncio
import contextlib
import time
from collections.abc import Callable
from dataclasses import dataclass

from pipeline_common.logging import get_logger
from pipeline_common.ports import Delivery
from stream_detector.detector_response_handling import RespObject

log = get_logger(__name__)

Clock = Callable[[], float]
Sink = Callable[[list[RespObject]], None]


@dataclass(slots=True)
class _Pending:
    result: RespObject
    delivery: Delivery


class ResultBatcher:
    def __init__(
        self,
        *,
        max_size: int,
        max_latency_ms: int,
        sink: Sink,
        clock: Clock = time.monotonic,
    ) -> None:
        self._max_size = max_size
        self._max_latency = max_latency_ms / 1000.0
        self._sink = sink
        self._clock = clock

        self._lock = asyncio.Lock()
        self._pending: list[_Pending] = []
        self._oldest_at = 0.0
        self._timer: asyncio.Task[None] | None = None

        self.batches_flushed = 0
        self.results_flushed = 0

    async def start(self) -> None:
        if self._timer is None:
            self._timer = asyncio.create_task(self._age_watchdog(), name="batch-timer")

    async def add(self, result: RespObject, delivery: Delivery) -> None:
        async with self._lock:
            if not self._pending:
                self._oldest_at = self._clock()
            self._pending.append(_Pending(result, delivery))
            if len(self._pending) >= self._max_size:
                await self._flush_locked("size")

    async def flush(self) -> int:
        async with self._lock:
            return await self._flush_locked("explicit")

    async def stop(self) -> None:
        """Flush whatever is buffered before going away -- otherwise a clean
        SIGTERM would silently drop the tail of the stream."""
        if self._timer is not None:
            self._timer.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._timer
            self._timer = None
        await self.flush()

    @property
    def pending(self) -> int:
        return len(self._pending)

    # -- internals --------------------------------------------------------

    async def _age_watchdog(self) -> None:
        # Poll at half the deadline so the worst-case overshoot stays bounded.
        interval = max(self._max_latency / 2, 0.01)
        try:
            while True:
                await asyncio.sleep(interval)
                async with self._lock:
                    if self._pending and (self._clock() - self._oldest_at) >= self._max_latency:
                        await self._flush_locked("age")
        except asyncio.CancelledError:
            return

    async def _flush_locked(self, reason: str) -> int:
        """Caller must hold the lock."""
        if not self._pending:
            return 0

        batch, self._pending = self._pending, []

        try:
            # The real implementation would be a blocking network call, so it
            # is offloaded rather than run on the event loop.
            await asyncio.to_thread(self._sink, [item.result for item in batch])
        except Exception as exc:
            log.error("batch.flush_failed", reason=reason, size=len(batch), error=str(exc))
            # Never acked, so the broker still owns them: hand them straight
            # back rather than dropping results that never arrived.
            for item in batch:
                await item.delivery.nack(requeue=True)
            return 0

        # One round trip for the whole batch: multiple=True settles every
        # still-unacked tag up to the highest. Safe because this consumer
        # processes sequentially, so a batch's tags are contiguous and nothing
        # older is still in flight.
        await max(batch, key=lambda item: item.delivery.tag).delivery.ack(multiple=True)

        self.batches_flushed += 1
        self.results_flushed += len(batch)
        log.info("batch.flushed", reason=reason, size=len(batch))
        return len(batch)
