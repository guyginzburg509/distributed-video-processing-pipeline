"""ResultBatcher: flush triggers, and the ack-after-flush invariant."""

from __future__ import annotations

import asyncio

import pytest

from pipeline_common.adapters.memory import InMemoryBroker, InMemoryDelivery
from pipeline_common.messages import FrameRef
from stream_detector.batching import ResultBatcher
from stream_detector.detector import BoundingBox
from stream_detector.detector_response_handling import RespObject


def make_ref(frame_id: int) -> FrameRef:
    return FrameRef(
        job_id="job1",
        video_id="vid1",
        frame_id=frame_id,
        timestamp_sec=frame_id / 2,
        blob_key=f"frame:job1:{frame_id}",
    )


def make_result(frame_id: int) -> RespObject:
    return RespObject(
        faces=[BoundingBox(x=0, y=0, w=10, h=10)], video_id="vid1", frame_id=frame_id
    )


class Harness:
    """A broker plus a recording sink."""

    def __init__(self) -> None:
        self.broker = InMemoryBroker()
        self.batches: list[list[RespObject]] = []
        self.fail_next = False

    def sink(self, results: list[RespObject]) -> None:
        if self.fail_next:
            self.fail_next = False
            raise RuntimeError("downstream unavailable")
        self.batches.append(list(results))

    def delivery(self, frame_id: int) -> InMemoryDelivery:
        self.broker.publish(make_ref(frame_id))
        env = self.broker._ready.popleft()
        delivery = InMemoryDelivery(env, self.broker)
        self.broker.track(delivery)
        return delivery


@pytest.fixture
def harness() -> Harness:
    return Harness()


class TestSizeFlush:
    async def test_flushes_exactly_at_max_size(self, harness: Harness) -> None:
        batcher = ResultBatcher(max_size=3, max_latency_ms=60_000, sink=harness.sink)
        for i in range(2):
            await batcher.add(make_result(i), harness.delivery(i))
        assert harness.batches == []  # not yet

        await batcher.add(make_result(2), harness.delivery(2))
        assert len(harness.batches) == 1
        assert len(harness.batches[0]) == 3

    async def test_sink_receives_a_list(self, harness: Harness) -> None:
        """send_results_next_service takes List[RespObject], not one at a time."""
        batcher = ResultBatcher(max_size=2, max_latency_ms=60_000, sink=harness.sink)
        for i in range(2):
            await batcher.add(make_result(i), harness.delivery(i))
        assert isinstance(harness.batches[0], list)
        assert all(isinstance(r, RespObject) for r in harness.batches[0])


class TestTimeFlush:
    async def test_flushes_on_age_even_when_under_size(self, harness: Harness) -> None:
        batcher = ResultBatcher(max_size=1000, max_latency_ms=50, sink=harness.sink)
        await batcher.start()
        try:
            await batcher.add(make_result(0), harness.delivery(0))
            await asyncio.sleep(0.3)
            assert len(harness.batches) == 1
            assert len(harness.batches[0]) == 1
        finally:
            await batcher.stop()

    async def test_idle_batcher_never_flushes_empty(self, harness: Harness) -> None:
        batcher = ResultBatcher(max_size=10, max_latency_ms=20, sink=harness.sink)
        await batcher.start()
        try:
            await asyncio.sleep(0.2)
            assert harness.batches == []
        finally:
            await batcher.stop()


class TestShutdownFlush:
    async def test_stop_flushes_the_tail(self, harness: Harness) -> None:
        """A clean SIGTERM must not drop a partial batch."""
        batcher = ResultBatcher(max_size=100, max_latency_ms=60_000, sink=harness.sink)
        await batcher.start()
        for i in range(3):
            await batcher.add(make_result(i), harness.delivery(i))
        assert harness.batches == []

        await batcher.stop()
        assert len(harness.batches) == 1
        assert len(harness.batches[0]) == 3


class TestAckDiscipline:
    async def test_nothing_is_acked_before_the_flush(self, harness: Harness) -> None:
        batcher = ResultBatcher(max_size=5, max_latency_ms=60_000, sink=harness.sink)
        for i in range(3):
            await batcher.add(make_result(i), harness.delivery(i))
        assert harness.broker.acked == []
        assert harness.broker.unacked_count == 3

    async def test_whole_batch_acked_after_flush(self, harness: Harness) -> None:
        batcher = ResultBatcher(max_size=3, max_latency_ms=60_000, sink=harness.sink)
        for i in range(3):
            await batcher.add(make_result(i), harness.delivery(i))
        assert sorted(harness.broker.acked) == [1, 2, 3]
        assert harness.broker.unacked_count == 0

    async def test_failed_flush_requeues_instead_of_acking(self, harness: Harness) -> None:
        """The results never reached the next service, so the frames must come
        back rather than being quietly acked away."""
        harness.fail_next = True
        batcher = ResultBatcher(max_size=2, max_latency_ms=60_000, sink=harness.sink)
        for i in range(2):
            await batcher.add(make_result(i), harness.delivery(i))

        assert harness.batches == []
        assert harness.broker.acked == []
        assert harness.broker.depth == 2
        assert all(env.redelivered for env in harness.broker._ready)

    async def test_recovers_after_a_failed_flush(self, harness: Harness) -> None:
        harness.fail_next = True
        batcher = ResultBatcher(max_size=2, max_latency_ms=60_000, sink=harness.sink)
        await batcher.add(make_result(0), harness.delivery(0))
        await batcher.add(make_result(1), harness.delivery(1))
        assert harness.batches == []

        await batcher.add(make_result(2), harness.delivery(2))
        await batcher.add(make_result(3), harness.delivery(3))
        assert len(harness.batches) == 1


class TestConcurrencySafety:
    async def test_size_and_age_flushes_do_not_race(self, harness: Harness) -> None:
        """The reason ResultBatcher holds an asyncio.Lock.

        Hammer `add` while a short-deadline timer fires underneath it. Without
        the lock the two flush paths interleave at an await and results are
        duplicated or dropped.
        """
        total = 200
        batcher = ResultBatcher(max_size=7, max_latency_ms=10, sink=harness.sink)
        await batcher.start()
        try:
            async def feed(start: int, count: int) -> None:
                for i in range(start, start + count):
                    await batcher.add(make_result(i), harness.delivery(i))
                    await asyncio.sleep(0)

            await asyncio.gather(feed(0, 100), feed(100, 100))
            await asyncio.sleep(0.1)
        finally:
            await batcher.stop()

        flushed = [r for batch in harness.batches for r in batch]
        assert len(flushed) == total, "results were lost or duplicated"
        assert len({r.frame_id for r in flushed}) == total
        assert batcher.pending == 0

    async def test_counters_match_what_the_sink_saw(self, harness: Harness) -> None:
        batcher = ResultBatcher(max_size=4, max_latency_ms=60_000, sink=harness.sink)
        for i in range(12):
            await batcher.add(make_result(i), harness.delivery(i))
        assert batcher.batches_flushed == 3
        assert batcher.results_flushed == 12
        assert len(harness.batches) == 3
