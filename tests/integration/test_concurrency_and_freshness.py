"""Behaviours the interviewer's answers made requirements.

* Q10 -- concurrent /analyze must be handled, and bounded.
* Q5 + Q7 -- a live deployment allows ~2 s end to end, and the failure policy
  is ours to design. In a real-time system a frame past that budget is worth
  little, so it is shed rather than processed.
"""

from __future__ import annotations

import time
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import cv2
import numpy as np
import pytest
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
from pipeline_common.settings import AnalyzerSettings
from stream_detector.batching import ResultBatcher
from stream_detector.consumer import DetectorWorker
from stream_detector.detector import StreamFaceDetector
from stream_detector.detector_response_handling import RespObject
from stream_detector.processing import FrameProcessor
from tests.conftest import make_video
from video_analyzer.main import create_app

# --------------------------------------------------------------------------
# Q10: concurrency
# --------------------------------------------------------------------------


@pytest.fixture
def client(video_root: Path) -> Iterator[TestClient]:
    make_video(video_root / "clip.mp4", fps=25.0, frames=400)
    make_video(video_root / "other.mp4", fps=25.0, frames=100)
    settings = AnalyzerSettings(
        video_root=video_root, analyzer_publishers=1, max_concurrent_jobs=2
    )
    app = create_app(
        settings,
        store=InMemoryFrameStore(),
        publisher=InMemoryFramePublisher(InMemoryBroker()),
        jobs=InMemoryJobRepository(),
    )
    with TestClient(app, raise_server_exceptions=False) as c:
        yield c


class TestConcurrentAnalyze:
    def test_different_videos_run_concurrently(self, client: TestClient) -> None:
        import threading

        results: list[int] = []
        lock = threading.Lock()

        def go(name: str) -> None:
            r = client.post("/analyze", json={"file_path": name, "fps": 2})
            with lock:
                results.append(r.status_code)

        threads = [
            threading.Thread(target=go, args=("clip.mp4",)),
            threading.Thread(target=go, args=("other.mp4",)),
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=60)

        assert sorted(results) == [200, 200], "distinct videos must not block each other"

    def test_sequential_repeats_are_fine(self, client: TestClient) -> None:
        """The duplicate guard must only reject *concurrent* duplicates."""
        assert client.post("/analyze", json={"file_path": "clip.mp4", "fps": 2}).status_code == 200
        assert client.post("/analyze", json={"file_path": "clip.mp4", "fps": 2}).status_code == 200

    def test_same_video_different_fps_is_a_different_job(self, client: TestClient) -> None:
        two = client.post("/analyze", json={"file_path": "clip.mp4", "fps": 2})
        four = client.post("/analyze", json={"file_path": "clip.mp4", "fps": 4})
        assert two.status_code == 200
        assert four.status_code == 200
        assert two.json()["job_id"] != four.json()["job_id"]


class TestAdmissionControl:
    async def test_duplicate_in_flight_job_is_rejected_with_409(self, video_root: Path) -> None:
        """Running the same video at the same rate twice at once would dispatch
        every frame twice."""
        make_video(video_root / "clip.mp4", fps=25.0, frames=400)
        settings = AnalyzerSettings(video_root=video_root, analyzer_publishers=1)
        app = create_app(
            settings,
            store=InMemoryFrameStore(),
            publisher=InMemoryFramePublisher(InMemoryBroker()),
            jobs=InMemoryJobRepository(),
        )
        with TestClient(app, raise_server_exceptions=False) as c:
            service = app.state.analysis_service
            # Simulate a run already in flight for this (video, rate).
            key = next(iter(_resume_keys(video_root / "clip.mp4", 2)))
            service._in_flight[key] = "existing-job"
            response = c.post("/analyze", json={"file_path": "clip.mp4", "fps": 2})

        assert response.status_code == 409
        assert response.json()["error"]["code"] == "job_already_running"

    async def test_at_the_ceiling_requests_are_refused_with_429(self, video_root: Path) -> None:
        make_video(video_root / "clip.mp4", fps=25.0, frames=50)
        settings = AnalyzerSettings(
            video_root=video_root, analyzer_publishers=1, max_concurrent_jobs=1
        )
        app = create_app(
            settings,
            store=InMemoryFrameStore(),
            publisher=InMemoryFramePublisher(InMemoryBroker()),
            jobs=InMemoryJobRepository(),
        )
        with TestClient(app, raise_server_exceptions=False) as c:
            service = app.state.analysis_service
            await service._slots.acquire()  # occupy the only slot
            try:
                response = c.post("/analyze", json={"file_path": "clip.mp4", "fps": 2})
            finally:
                service._slots.release()

        assert response.status_code == 429
        assert response.json()["error"]["code"] == "too_many_concurrent_jobs"
        assert response.headers.get("Retry-After") == "10"


def _resume_keys(path: Path, fps: int) -> list[str]:
    from video_analyzer.domain.identity import make_video_id, resume_key

    return [resume_key(make_video_id(path.resolve()), fps)]


# --------------------------------------------------------------------------
# Q5 + Q7: freshness
# --------------------------------------------------------------------------


def encode() -> bytes:
    ok, buf = cv2.imencode(".jpg", np.full((32, 32, 3), 128, dtype=np.uint8))
    assert ok
    return bytes(buf.tobytes())


class FreshnessHarness:
    def __init__(self, *, max_age: float, now: float) -> None:
        self.broker = InMemoryBroker()
        self.store = InMemoryFrameStore()
        self.jobs = InMemoryJobRepository()
        self.batches: list[list[RespObject]] = []
        self.executor = ThreadPoolExecutor(max_workers=1)
        self._now = now

        self.batcher = ResultBatcher(max_size=1, max_latency_ms=50, sink=self.batches.append)
        self.worker = DetectorWorker(
            consumer=InMemoryFrameConsumer(self.broker),
            processor=FrameProcessor(
                detector=StreamFaceDetector(),  # type: ignore[no-untyped-call]
                store=self.store,
                dedup=InMemoryDedupGuard(),
                executor=self.executor,
                dedup_ttl_sec=3600,
                max_frame_age_sec=max_age,
                clock=lambda: self._now,
            ),
            batcher=self.batcher,
            jobs=self.jobs,
        )

    async def seed(self, frame_id: int, *, dispatched_at: float) -> None:
        ref = FrameRef(
            job_id="job1",
            video_id="vid1",
            frame_id=frame_id,
            timestamp_sec=frame_id / 2,
            dispatched_at=dispatched_at,
            blob_key=f"frame:job1:{frame_id}",
        )
        await self.store.put(ref.blob_key, encode(), 3600)
        self.broker.publish(ref)

    async def drain(self) -> None:
        self.broker.close()
        await self.worker.run()
        self.executor.shutdown(wait=True)

    @property
    def results(self) -> list[RespObject]:
        return [r for b in self.batches for r in b]


class TestFreshness:
    async def test_frames_past_the_budget_are_shed(self) -> None:
        """A late answer has little value, and processing it starves the frame
        that is still current -- so a backlog would compound."""
        h = FreshnessHarness(max_age=2.0, now=1000.0)
        await h.seed(0, dispatched_at=999.5)  # 0.5s old -- fresh
        await h.seed(1, dispatched_at=995.0)  # 5.0s old -- stale
        await h.seed(2, dispatched_at=999.9)  # 0.1s old -- fresh
        await h.jobs.create(JobRecord(job_id="job1", video_id="vid1"))
        await h.drain()

        assert [r.frame_id for r in h.results] == [0, 2]
        assert h.worker.stale == 1

    async def test_shed_frames_are_acked_not_dead_lettered(self) -> None:
        """Nothing is wrong with the frame; we were simply late. Dead-lettering
        would flood the DLQ during exactly the overload it signals."""
        h = FreshnessHarness(max_age=1.0, now=1000.0)
        await h.seed(0, dispatched_at=900.0)
        await h.jobs.create(JobRecord(job_id="job1", video_id="vid1"))
        await h.drain()

        assert h.broker.dead_lettered == []
        assert h.broker.unacked_count == 0
        assert len(h.broker.acked) == 1

    async def test_disabled_by_default_nothing_is_shed(self) -> None:
        """Archive processing of a file: completeness beats latency, and every
        frame must be processed however old it is."""
        h = FreshnessHarness(max_age=0.0, now=1000.0)
        for i in range(3):
            await h.seed(i, dispatched_at=0.0)  # ancient
        await h.jobs.create(JobRecord(job_id="job1", video_id="vid1"))
        await h.drain()

        assert len(h.results) == 3
        assert h.worker.stale == 0

    async def test_unstamped_frames_are_never_shed(self) -> None:
        """dispatched_at=0 means "no stamp", not "epoch" -- an unstamped frame
        must not be mistaken for a 56-year-old one."""
        h = FreshnessHarness(max_age=2.0, now=1000.0)
        await h.seed(0, dispatched_at=0.0)
        await h.jobs.create(JobRecord(job_id="job1", video_id="vid1"))
        await h.drain()

        assert len(h.results) == 1
        assert h.worker.stale == 0

    async def test_observed_latency_is_measured(self) -> None:
        """The SLO has to be measured, not assumed -- the worker records the
        worst end-to-end age it actually saw."""
        now = time.time()
        h = FreshnessHarness(max_age=0.0, now=now)
        await h.seed(0, dispatched_at=now - 0.25)
        await h.jobs.create(JobRecord(job_id="job1", video_id="vid1"))
        await h.drain()

        assert h.worker.max_age_seen_sec == pytest.approx(0.25, abs=0.2)
