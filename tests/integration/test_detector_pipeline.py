"""DetectorWorker end-to-end against in-memory adapters.

The invariant under test throughout: **every delivery is settled exactly once**
-- acked, dead-lettered, or requeued. Nothing is silently dropped, and nothing
is acked before its results have actually gone downstream.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

import cv2
import numpy as np
import pytest

from pipeline_common.adapters.memory import (
    InMemoryBroker,
    InMemoryDedupGuard,
    InMemoryFrameConsumer,
    InMemoryFrameStore,
    InMemoryJobRepository,
)
from pipeline_common.messages import FrameRef, JobRecord
from stream_detector.batching import ResultBatcher
from stream_detector.consumer import DetectorWorker
from stream_detector.detector import StreamFaceDetector
from stream_detector.detector_response_handling import RespObject
from stream_detector.processing import FrameProcessor


def encode(colour: int = 128) -> bytes:
    frame = np.full((32, 32, 3), colour, dtype=np.uint8)
    ok, buf = cv2.imencode(".jpg", frame)
    assert ok
    return bytes(buf.tobytes())


class Pipeline:
    def __init__(self, *, batch_size: int = 4, latency_ms: int = 50) -> None:
        self.broker = InMemoryBroker()
        self.store = InMemoryFrameStore()
        self.dedup = InMemoryDedupGuard()
        self.jobs = InMemoryJobRepository()
        self.batches: list[list[RespObject]] = []
        self.executor = ThreadPoolExecutor(max_workers=2)

        self.batcher = ResultBatcher(
            max_size=batch_size, max_latency_ms=latency_ms, sink=self.batches.append
        )
        self.worker = DetectorWorker(
            consumer=InMemoryFrameConsumer(self.broker),
            processor=FrameProcessor(
                detector=StreamFaceDetector(),  # type: ignore[no-untyped-call]
                store=self.store,
                dedup=self.dedup,
                executor=self.executor,
                dedup_ttl_sec=3600,
            ),
            batcher=self.batcher,
            jobs=self.jobs,
        )

    async def seed(self, frame_id: int, *, store_blob: bytes | None = encode()) -> FrameRef:
        ref = FrameRef(
            job_id="job1",
            video_id="vid1",
            frame_id=frame_id,
            timestamp_sec=frame_id / 2,
            blob_key=f"frame:job1:{frame_id}",
        )
        if store_blob is not None:
            await self.store.put(ref.blob_key, store_blob, 3600)
        self.broker.publish(ref)
        return ref

    async def drain(self) -> None:
        """Consume everything currently queued, then shut down cleanly."""
        self.broker.close()
        await self.worker.run()
        self.executor.shutdown(wait=True)

    @property
    def results(self) -> list[RespObject]:
        return [r for batch in self.batches for r in batch]


@pytest.fixture
async def pipeline() -> Pipeline:
    p = Pipeline()
    await p.jobs.create(JobRecord(job_id="job1", video_id="vid1"))
    return p


class TestHappyPath:
    async def test_every_frame_becomes_a_respobject(self, pipeline: Pipeline) -> None:
        for i in range(10):
            await pipeline.seed(i)
        await pipeline.drain()

        assert len(pipeline.results) == 10
        assert {r.frame_id for r in pipeline.results} == set(range(10))
        assert all(r.video_id == "vid1" for r in pipeline.results)
        assert all(len(r.faces) == 2 for r in pipeline.results)  # the mock returns 2

    async def test_results_are_delivered_in_batches_not_singly(self, pipeline: Pipeline) -> None:
        for i in range(10):
            await pipeline.seed(i)
        await pipeline.drain()

        assert len(pipeline.batches) < 10, "results should be batched"
        assert all(len(b) <= 4 for b in pipeline.batches)
        assert all(isinstance(b, list) for b in pipeline.batches)

    async def test_all_frames_acked_and_nothing_dead_lettered(self, pipeline: Pipeline) -> None:
        for i in range(10):
            await pipeline.seed(i)
        await pipeline.drain()

        assert len(pipeline.broker.acked) == 10
        assert pipeline.broker.unacked_count == 0
        assert pipeline.broker.dead_lettered == []

    async def test_job_progress_is_reported_back(self, pipeline: Pipeline) -> None:
        for i in range(6):
            await pipeline.seed(i)
        await pipeline.drain()

        record = await pipeline.jobs.get("job1")
        assert record is not None
        assert record.frames_processed == 6


class TestDeadLettering:
    async def test_missing_blob_is_dead_lettered_immediately(self, pipeline: Pipeline) -> None:
        """An expired blob can never succeed, so retrying would just burn the
        queue."""
        await pipeline.seed(0, store_blob=None)
        await pipeline.drain()

        assert pipeline.results == []
        assert [r.frame_id for r in pipeline.broker.dead_lettered] == [0]
        assert pipeline.worker.dead_lettered == 1

    async def test_corrupt_jpeg_is_dead_lettered(self, pipeline: Pipeline) -> None:
        await pipeline.seed(0, store_blob=b"definitely not a jpeg")
        await pipeline.drain()

        assert pipeline.results == []
        assert len(pipeline.broker.dead_lettered) == 1

    async def test_one_bad_frame_does_not_stop_the_stream(self, pipeline: Pipeline) -> None:
        await pipeline.seed(0)
        await pipeline.seed(1, store_blob=None)
        await pipeline.seed(2)
        await pipeline.drain()

        assert {r.frame_id for r in pipeline.results} == {0, 2}
        assert [r.frame_id for r in pipeline.broker.dead_lettered] == [1]


class TestDeduplication:
    async def test_redelivered_frame_is_not_reported_twice(self, pipeline: Pipeline) -> None:
        """At-least-once delivery means this happens; downstream must not see
        the same detection twice."""
        ref = await pipeline.seed(0)
        pipeline.broker.publish(ref)  # same frame delivered again
        await pipeline.drain()

        assert len(pipeline.results) == 1
        assert pipeline.worker.duplicates == 1

    async def test_duplicate_is_still_acked(self, pipeline: Pipeline) -> None:
        """Otherwise the broker would redeliver it forever."""
        ref = await pipeline.seed(0)
        pipeline.broker.publish(ref)
        await pipeline.drain()

        assert pipeline.broker.unacked_count == 0
        assert pipeline.broker.dead_lettered == []


class TestSettlementInvariant:
    @pytest.mark.parametrize("count", [1, 5, 17, 40])
    async def test_every_delivery_is_settled_exactly_once(
        self, pipeline: Pipeline, count: int
    ) -> None:
        for i in range(count):
            await pipeline.seed(i)
        await pipeline.drain()

        settled = len(set(pipeline.broker.acked)) + len(pipeline.broker.dead_lettered)
        assert settled == count
        assert pipeline.broker.unacked_count == 0

    async def test_tail_batch_is_flushed_on_shutdown(self, pipeline: Pipeline) -> None:
        """5 frames with batch size 4: the 5th must not be stranded."""
        for i in range(5):
            await pipeline.seed(i)
        await pipeline.drain()

        assert len(pipeline.results) == 5
        assert pipeline.broker.unacked_count == 0


class TestBackpressureShape:
    async def test_large_burst_is_fully_processed(self, pipeline: Pipeline) -> None:
        for i in range(200):
            await pipeline.seed(i)
        await pipeline.drain()

        assert len(pipeline.results) == 200
        assert len({r.frame_id for r in pipeline.results}) == 200
        assert pipeline.broker.unacked_count == 0
