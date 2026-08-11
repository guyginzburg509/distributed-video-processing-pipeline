"""The detector's consume loop: settle every message, exactly once.

Messages are handled **sequentially**. That is deliberate: it keeps delivery
tags contiguous, which is what makes the batched ``ack(multiple=True)`` in
ResultBatcher provably safe. Parallelism comes from running more replicas --
`docker compose up --scale stream_detector=4` -- which is the lever that also
survives a machine dying. Prefetch still matters, because it keeps the local
buffer warm so there is no round-trip stall between frames.

The invariant: every delivery is acked, nacked, or handed to the batcher (which
will ack it later). Nothing is ever silently dropped.
"""

from __future__ import annotations

import asyncio
import time

from pipeline_common.logging import get_logger
from pipeline_common.ports import Delivery, FrameConsumer, JobRepository
from stream_detector.batching import ResultBatcher
from stream_detector.processing import (
    DuplicateFrameError,
    FrameProcessor,
    PermanentFrameError,
    StaleFrameError,
    TransientFrameError,
)

log = get_logger(__name__)


class DetectorWorker:
    def __init__(
        self,
        *,
        consumer: FrameConsumer,
        processor: FrameProcessor,
        batcher: ResultBatcher,
        jobs: JobRepository,
    ) -> None:
        self._consumer = consumer
        self._processor = processor
        self._batcher = batcher
        self._jobs = jobs

        self.processed = 0
        self.duplicates = 0
        self.dead_lettered = 0
        self.requeued = 0
        #: Frames shed for being past the latency budget. A rising rate is the
        #: signal that detector capacity is short -- worth alarming on.
        self.stale = 0
        self.max_age_seen_sec = 0.0

    async def run(self, stop: asyncio.Event | None = None) -> None:
        await self._batcher.start()
        try:
            async for delivery in self._consumer.deliveries():
                await self._handle(delivery)
                if stop is not None and stop.is_set():
                    break
        finally:
            # Flush the tail before exiting, or the last partial batch is lost.
            await self._batcher.stop()

    async def _handle(self, delivery: Delivery) -> None:
        ref = delivery.ref
        try:
            result = await self._processor.process(ref)

        except DuplicateFrameError:
            # Redelivery of work already done. Ack so the broker stops
            # resending; do not double-report it downstream.
            self.duplicates += 1
            log.info("frame.duplicate", job_id=ref.job_id, frame_id=ref.frame_id)
            await delivery.ack()
            return

        except StaleFrameError as exc:
            # Ack, not dead-letter: nothing is wrong with the frame, we simply
            # arrived too late for it to be useful. Dead-lettering would fill
            # the DLQ with noise during exactly the overload it signals.
            # The counter is the alarm -- a rising rate means under-capacity.
            self.stale += 1
            log.warning(
                "frame.shed_stale",
                job_id=ref.job_id,
                frame_id=ref.frame_id,
                age_sec=round(exc.age_sec, 3),
                budget_sec=exc.budget_sec,
            )
            await delivery.ack()
            return

        except PermanentFrameError as exc:
            self.dead_lettered += 1
            log.error(
                "frame.dead_lettered",
                job_id=ref.job_id,
                frame_id=ref.frame_id,
                reason=str(exc),
                redelivered=delivery.redelivered,
            )
            await delivery.nack(requeue=False)
            return

        except TransientFrameError as exc:
            # One retry: if it already came back once and still fails, the
            # "transient" diagnosis was wrong. Dead-letter rather than loop.
            if delivery.redelivered:
                self.dead_lettered += 1
                log.error(
                    "frame.dead_lettered_after_retry",
                    job_id=ref.job_id,
                    frame_id=ref.frame_id,
                    reason=str(exc),
                )
                await delivery.nack(requeue=False)
            else:
                self.requeued += 1
                log.warning("frame.requeued", job_id=ref.job_id, frame_id=ref.frame_id)
                await delivery.nack(requeue=True)
            return

        except Exception:
            self.dead_lettered += 1
            log.exception("frame.unexpected_error", job_id=ref.job_id, frame_id=ref.frame_id)
            await delivery.nack(requeue=False)
            return

        # Handed to the batcher, which owns the ack from here on.
        await self._batcher.add(result, delivery)
        self.processed += 1
        self.max_age_seen_sec = max(self.max_age_seen_sec, ref.age_sec(time.time()))
        await self._jobs.increment_processed(ref.job_id)
