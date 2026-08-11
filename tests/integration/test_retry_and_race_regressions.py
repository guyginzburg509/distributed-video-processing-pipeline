"""Regressions for two bugs found in the final review.

Both were silent: no test failed, nothing was logged, and the system looked
healthy while losing work.
"""

from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import cv2
import numpy as np
from fastapi.testclient import TestClient

from pipeline_common.adapters.memory import (
    InMemoryBroker,
    InMemoryDedupGuard,
    InMemoryFrameConsumer,
    InMemoryFramePublisher,
    InMemoryFrameStore,
    InMemoryJobRepository,
)
from pipeline_common.messages import FrameRef, JobRecord
from pipeline_common.ports import FrameStoreError
from pipeline_common.settings import AnalyzerSettings
from stream_detector.batching import ResultBatcher
from stream_detector.consumer import DetectorWorker
from stream_detector.detector import StreamFaceDetector
from stream_detector.detector_response_handling import RespObject
from stream_detector.processing import FrameProcessor
from tests.conftest import make_video
from video_analyzer.main import create_app


def jpeg() -> bytes:
    ok, buf = cv2.imencode(".jpg", np.full((32, 32, 3), 128, dtype=np.uint8))
    assert ok
    return bytes(buf.tobytes())


class FlakyStore(InMemoryFrameStore):
    """Fails the first N reads, then behaves. Models Redis blipping."""

    def __init__(self, fail_times: int) -> None:
        super().__init__()
        self._remaining = fail_times

    async def get(self, key: str) -> bytes | None:
        if self._remaining > 0:
            self._remaining -= 1
            raise FrameStoreError("simulated transient outage")
        return await super().get(key)


class TestDedupDoesNotSwallowRetries:
    """Bug: the dedup key was claimed *before* the work.

    A transient failure nacked and requeued the frame; the redelivery then found
    the key already claimed, called it a duplicate, acked it and dropped it. The
    guard meant to absorb redelivery was defeating the retry instead -- and the
    frame vanished with no error anywhere.
    """

    async def test_frame_survives_a_transient_failure_and_retry(self) -> None:
        broker = InMemoryBroker()
        store = FlakyStore(fail_times=1)  # first read fails, retry succeeds
        jobs = InMemoryJobRepository()
        batches: list[list[RespObject]] = []
        executor = ThreadPoolExecutor(max_workers=1)

        ref = FrameRef(
            job_id="job1",
            video_id="vid1",
            frame_id=7,
            timestamp_sec=3.5,
            blob_key="frame:job1:7",
        )
        await InMemoryFrameStore.put(store, ref.blob_key, jpeg(), 3600)
        await jobs.create(JobRecord(job_id="job1", video_id="vid1"))
        broker.publish(ref)

        worker = DetectorWorker(
            consumer=InMemoryFrameConsumer(broker),
            processor=FrameProcessor(
                detector=StreamFaceDetector(),  # type: ignore[no-untyped-call]
                store=store,
                dedup=InMemoryDedupGuard(),
                executor=executor,
                dedup_ttl_sec=3600,
            ),
            batcher=ResultBatcher(max_size=1, max_latency_ms=50, sink=batches.append),
            jobs=jobs,
        )

        async def drain() -> None:
            await worker.run()

        task = asyncio.create_task(drain())
        await asyncio.sleep(0.3)  # let the requeue + redelivery happen
        broker.close()
        await asyncio.wait_for(task, timeout=5)
        executor.shutdown(wait=True)

        results = [r for b in batches for r in b]
        assert [r.frame_id for r in results] == [7], "the retried frame was lost"
        assert worker.requeued == 1
        assert worker.duplicates == 0, "the retry was misread as a duplicate"
        assert broker.dead_lettered == []

    async def test_a_genuine_redelivery_is_still_deduplicated(self) -> None:
        """The guard must still do its actual job."""
        broker = InMemoryBroker()
        store = InMemoryFrameStore()
        jobs = InMemoryJobRepository()
        batches: list[list[RespObject]] = []
        executor = ThreadPoolExecutor(max_workers=1)

        ref = FrameRef(
            job_id="job1", video_id="vid1", frame_id=0, timestamp_sec=0.0,
            blob_key="frame:job1:0",
        )
        await store.put(ref.blob_key, jpeg(), 3600)
        await jobs.create(JobRecord(job_id="job1", video_id="vid1"))
        broker.publish(ref)
        broker.publish(ref)  # same frame twice

        worker = DetectorWorker(
            consumer=InMemoryFrameConsumer(broker),
            processor=FrameProcessor(
                detector=StreamFaceDetector(),  # type: ignore[no-untyped-call]
                store=store,
                dedup=InMemoryDedupGuard(),
                executor=executor,
                dedup_ttl_sec=3600,
            ),
            batcher=ResultBatcher(max_size=1, max_latency_ms=50, sink=batches.append),
            jobs=jobs,
        )
        broker.close()
        await worker.run()
        executor.shutdown(wait=True)

        assert len([r for b in batches for r in b]) == 1
        assert worker.duplicates == 1


class TestConcurrentDuplicateJobIsRefused:
    """Bug: the 409 guard checked and reserved with awaits in between.

    Two concurrent requests for the same (video, rate) both passed the check
    before either wrote its reservation, so both ran -- dispatching every frame
    twice, which is the exact outcome the guard exists to prevent.
    """

    async def test_only_one_of_two_simultaneous_duplicates_runs(
        self, video_root: Path
    ) -> None:
        make_video(video_root / "clip.mp4", fps=25.0, frames=200)
        broker = InMemoryBroker()
        publisher = InMemoryFramePublisher(broker)
        settings = AnalyzerSettings(video_root=video_root, analyzer_publishers=1)
        app = create_app(
            settings,
            store=InMemoryFrameStore(),
            publisher=publisher,
            jobs=InMemoryJobRepository(),
        )

        with TestClient(app, raise_server_exceptions=False):  # runs lifespan
            service = app.state.analysis_service
            path = (video_root / "clip.mp4").resolve()

            # Fire both through the service directly, on one event loop, so they
            # genuinely interleave at every await -- which is what a real server
            # handling two simultaneous requests does.
            results = await asyncio.gather(
                service.analyze(path, 2),
                service.analyze(path, 2),
                return_exceptions=True,
            )

        from video_analyzer.services.errors import JobAlreadyRunningError

        refused = [r for r in results if isinstance(r, JobAlreadyRunningError)]
        succeeded = [r for r in results if not isinstance(r, BaseException)]

        assert len(succeeded) == 1, "both duplicates ran; every frame dispatched twice"
        assert len(refused) == 1, "the duplicate should have been refused with 409"

        # And the decisive check: no frame was published twice.
        ids = [ref.frame_id for ref in publisher.published]
        assert len(ids) == len(set(ids)), "a frame was dispatched more than once"

    async def test_different_rates_still_run_together(self, video_root: Path) -> None:
        """The guard must be narrow: same video at a *different* rate is a
        different job and must not be blocked."""
        make_video(video_root / "clip.mp4", fps=25.0, frames=200)
        settings = AnalyzerSettings(video_root=video_root, analyzer_publishers=1)
        app = create_app(
            settings,
            store=InMemoryFrameStore(),
            publisher=InMemoryFramePublisher(InMemoryBroker()),
            jobs=InMemoryJobRepository(),
        )
        with TestClient(app, raise_server_exceptions=False):  # runs lifespan
            service = app.state.analysis_service
            path = (video_root / "clip.mp4").resolve()
            results = await asyncio.gather(
                service.analyze(path, 2),
                service.analyze(path, 4),
                return_exceptions=True,
            )

        assert all(not isinstance(r, BaseException) for r in results), results
        two, four = results
        assert not isinstance(two, BaseException) and not isinstance(four, BaseException)
        assert two.job_id != four.job_id
