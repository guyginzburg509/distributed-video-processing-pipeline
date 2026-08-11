"""The concurrency machinery: decode -> encode -> store -> publish -> confirm.

One responsibility: move every sampled frame from the video into the broker, and
report exactly what got confirmed. It knows nothing about job identity, resume
decisions or HTTP -- ``AnalysisService`` owns those.

Concurrency model
-----------------
One blocking decode thread feeds a **bounded** ``asyncio.Queue`` drained by a
fixed pool of K publisher tasks::

    decode thread  --(bounded queue)-->  K publisher tasks --> Redis + RabbitMQ

The queue bound is the backpressure knob. The decode thread hands items over
with ``run_coroutine_threadsafe(...).result()``, which *blocks the thread* while
the queue is full -- so a slow broker throttles decoding instead of letting
memory grow without limit.

A fixed worker pool is used rather than a semaphore plus a task per frame: it
bounds in-flight work just as tightly, with no task-creation churn and no
unbounded task list. See design 6.1.

Shutdown is by sentinel: once decoding ends, one ``_STOP`` per worker is queued.
Workers never raise -- on failure they record the error and switch to draining --
so those puts can never deadlock against a dead consumer.
"""

from __future__ import annotations

import asyncio
import threading
import time
from collections.abc import Callable, Coroutine
from dataclasses import dataclass, field
from typing import Any

from pipeline_common.logging import get_logger
from pipeline_common.messages import FrameRef, JobRecord
from pipeline_common.ports import FramePublisher, FrameStore, JobRepository
from pipeline_common.settings import AnalyzerSettings
from video_analyzer.domain.frame_sampler import FrameSampler
from video_analyzer.domain.identity import blob_key
from video_analyzer.domain.video_source import DecodeFailure, SampledFrame, VideoSource

log = get_logger(__name__)

#: Sentinel pushed once per worker to end the pipeline cleanly.
_STOP = None


@dataclass
class RunState:
    """Progress of one dispatch run.

    All mutation happens on the event-loop thread between awaits, so no lock is
    needed -- unlike ResultBatcher, where a timer task genuinely races.
    """

    #: Cumulative for the job -- seeded from the checkpoint on a resumed run,
    #: so `frames_dispatched` always means "of the whole video", not "this run".
    frames_confirmed: int = 0
    frames_failed: int = 0
    last_source_index: int = -1
    error: BaseException | None = None
    #: Dispatched by this invocation alone; 0 == the resume found nothing left.
    frames_confirmed_this_run: int = 0
    _since_checkpoint: int = field(default=0, repr=False)

    @classmethod
    def resuming(cls, prior: JobRecord) -> RunState:
        return cls(
            frames_confirmed=prior.frames_dispatched,
            frames_failed=prior.frames_failed,
            last_source_index=prior.last_source_index,
        )

    def record_confirmed(self, frame: SampledFrame) -> None:
        self.frames_confirmed += 1
        self.frames_confirmed_this_run += 1
        self.last_source_index = max(self.last_source_index, frame.source_index)
        self._since_checkpoint += 1

    def take_checkpoint_due(self, every: int) -> bool:
        if self._since_checkpoint >= every:
            self._since_checkpoint = 0
            return True
        return False

    def record_error(self, exc: BaseException) -> None:
        if self.error is None:  # keep the first, most diagnostic failure
            self.error = exc


class FramePipeline:
    """Runs one video's frames through store-and-publish, with backpressure."""

    def __init__(
        self,
        settings: AnalyzerSettings,
        *,
        store: FrameStore,
        publisher: FramePublisher,
        jobs: JobRepository,
    ) -> None:
        self._settings = settings
        self._store = store
        self._publisher = publisher
        self._jobs = jobs

    async def run(
        self,
        source: VideoSource,
        sampler: FrameSampler,
        *,
        job_id: str,
        video_id: str,
        cancelled: Callable[[], Coroutine[Any, Any, bool]] | None = None,
        prior: JobRecord | None = None,
    ) -> tuple[RunState, bool]:
        """Dispatch every sampled frame. Returns (state, was_aborted).

        Never raises for a dispatch failure -- the error is left on the returned
        state so the caller can persist it against the job before deciding what
        the client sees.
        """
        settings = self._settings
        queue: asyncio.Queue[SampledFrame | None] = asyncio.Queue(
            maxsize=settings.analyzer_queue_size
        )
        abort = threading.Event()
        state = RunState() if prior is None else RunState.resuming(prior)

        workers = [
            asyncio.create_task(
                self._publish_worker(queue, state, job_id, video_id, abort),
                name=f"publisher-{i}",
            )
            for i in range(settings.analyzer_publishers)
        ]
        watchdog = (
            asyncio.create_task(self._cancel_watchdog(cancelled, abort), name="disconnect-watchdog")
            if cancelled is not None
            else None
        )

        loop = asyncio.get_running_loop()
        try:
            await asyncio.to_thread(
                self._decode_into_queue, source, sampler, queue, abort, loop, state
            )
        finally:
            if watchdog is not None:
                watchdog.cancel()
            for _ in workers:
                await queue.put(_STOP)
            await asyncio.gather(*workers)

        return state, abort.is_set()

    # -- stage 1: decode (worker thread) ----------------------------------

    def _decode_into_queue(
        self,
        source: VideoSource,
        sampler: FrameSampler,
        queue: asyncio.Queue[SampledFrame | None],
        abort: threading.Event,
        loop: asyncio.AbstractEventLoop,
        state: RunState,
    ) -> None:
        """Runs on a worker thread. Blocking by design."""
        # On a resumed run the sampler already points past the checkpoint, so a
        # single seek skips the frames a previous run confirmed. It is 0 for a
        # fresh run, which makes this the same code path.
        for item in source.iter_sampled(
            sampler,
            jpeg_quality=self._settings.jpeg_quality,
            abort=abort,
            start_source_index=sampler.next_source_index,
        ):
            if isinstance(item, DecodeFailure):
                state.frames_failed += 1
                log.warning(
                    "frame.decode_failed",
                    source_index=item.source_index,
                    reason=item.reason,
                )
                continue
            # Blocks this thread while the queue is full: the backpressure.
            asyncio.run_coroutine_threadsafe(queue.put(item), loop).result()
            if abort.is_set():
                return

    # -- stage 2: store + publish (event loop) ----------------------------

    async def _publish_worker(
        self,
        queue: asyncio.Queue[SampledFrame | None],
        state: RunState,
        job_id: str,
        video_id: str,
        abort: threading.Event,
    ) -> None:
        while True:
            item = await queue.get()
            try:
                if item is _STOP:
                    return
                if state.error is not None:
                    continue  # draining so the decode thread can finish
                await self._dispatch(item, job_id, video_id)
                state.record_confirmed(item)
                if state.take_checkpoint_due(self._settings.checkpoint_every):
                    await self._jobs.checkpoint(
                        job_id,
                        frames_dispatched=state.frames_confirmed,
                        frames_failed=state.frames_failed,
                        last_source_index=state.last_source_index,
                    )
            except Exception as exc:
                log.error("frame.dispatch_failed", job_id=job_id, error=str(exc))
                state.record_error(exc)
                abort.set()
            finally:
                queue.task_done()

    async def _dispatch(self, frame: SampledFrame, job_id: str, video_id: str) -> None:
        key = blob_key(job_id, frame.source_index)
        # Blob first, reference second. The reverse order would let a detector
        # receive a reference to a blob that does not exist yet.
        await self._store.put(key, frame.jpeg, self._settings.frame_blob_ttl_sec)
        await self._publisher.publish(
            FrameRef(
                job_id=job_id,
                video_id=video_id,
                frame_id=frame.source_index,
                timestamp_sec=frame.timestamp_sec,
                # Stamped here rather than at decode time: this is the moment
                # the latency clock starts for the detector's freshness check.
                dispatched_at=time.time(),
                blob_key=key,
            )
        )

    @staticmethod
    async def _cancel_watchdog(
        cancelled: Callable[[], Coroutine[Any, Any, bool]], abort: threading.Event
    ) -> None:
        """Stop decoding if the client hangs up mid-request."""
        try:
            while not abort.is_set():
                if await cancelled():
                    log.info("job.client_disconnected")
                    abort.set()
                    return
                await asyncio.sleep(0.5)
        except asyncio.CancelledError:
            return
