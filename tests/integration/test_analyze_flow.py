"""End-to-end /analyze against in-memory adapters -- no Docker, no stack.broker.

That this file passes with nothing running is the concrete proof that the
ports/adapters split is real rather than decorative.
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from pipeline_common.adapters.memory import (
    InMemoryBroker,
    InMemoryFramePublisher,
    InMemoryFrameStore,
    InMemoryJobRepository,
)
from pipeline_common.messages import JobStatus
from pipeline_common.settings import AnalyzerSettings
from tests.conftest import make_video
from video_analyzer.domain.frame_sampler import FrameSampler
from video_analyzer.main import create_app


@dataclass
class Stack:
    """The app plus the doubles behind it, so tests can assert on both ends."""

    client: TestClient
    store: InMemoryFrameStore
    publisher: InMemoryFramePublisher
    jobs: InMemoryJobRepository
    broker: InMemoryBroker


@pytest.fixture
def stack(video_root: Path) -> Iterator[Stack]:
    broker = InMemoryBroker()
    store = InMemoryFrameStore()
    publisher = InMemoryFramePublisher(broker)
    jobs = InMemoryJobRepository()
    settings = AnalyzerSettings(
        video_root=video_root,
        analyzer_queue_size=16,
        analyzer_publishers=3,
        checkpoint_every=10,
    )
    app = create_app(settings, store=store, publisher=publisher, jobs=jobs)
    with TestClient(app) as client:
        yield Stack(client, store, publisher, jobs, broker)


class TestHappyPath:
    def test_25fps_to_2fps_dispatches_the_exact_expected_frames(
        self, stack: Stack, sample_video: Path
    ) -> None:
        response = stack.client.post("/analyze", json={"file_path": "sample.mp4", "fps": 2})
        assert response.status_code == 200
        body = response.json()

        expected = [i for _, i in FrameSampler(25.0, 2).iter_slots(100)]
        assert [ref.frame_id for ref in stack.publisher.published] == expected
        assert body["frames_dispatched"] == len(expected)
        assert body["status"] == JobStatus.COMPLETED
        # One blob per dispatched frame -- the claim-check actually happened.
        assert len(stack.store) == len(expected)

    def test_25fps_to_4fps(self, stack: Stack, sample_video: Path) -> None:
        response = stack.client.post("/analyze", json={"file_path": "sample.mp4", "fps": 4})
        assert response.status_code == 200
        expected = [i for _, i in FrameSampler(25.0, 4).iter_slots(100)]
        assert [ref.frame_id for ref in stack.publisher.published] == expected

    def test_response_reports_measured_throughput(self, stack: Stack, sample_video: Path) -> None:
        body = stack.client.post("/analyze", json={"file_path": "sample.mp4", "fps": 2}).json()
        assert body["elapsed_sec"] > 0
        assert body["realtime_factor"] > 0
        assert body["video_duration_sec"] == pytest.approx(4.0, abs=0.1)

    def test_every_reference_points_at_a_real_blob(self, stack: Stack, sample_video: Path) -> None:
        """The claim-check contract: the blob must exist before the reference
        is published, or a fast detector would find nothing."""
        stack.client.post("/analyze", json={"file_path": "sample.mp4", "fps": 4})
        for ref in stack.publisher.published:
            blob = stack.store._blobs.get(ref.blob_key)
            assert blob is not None, f"dangling reference {ref.blob_key}"
            assert blob[0].startswith(b"\xff\xd8"), "blob is not a JPEG"

    def test_frame_id_is_the_source_index(self, stack: Stack, sample_video: Path) -> None:
        """The interviewer confirmed frame_id must be the *source* index, so the
        second kept frame at 25->2 fps is 13, not 12 and not 1."""
        stack.client.post("/analyze", json={"file_path": "sample.mp4", "fps": 2})
        refs = stack.publisher.published
        assert refs[1].frame_id == 13
        assert refs[1].timestamp_sec == pytest.approx(13 / 25, abs=0.02)
        assert refs[0].dispatched_at > 0, "the latency clock must be stamped"


class TestJobState:
    def test_job_record_is_queryable_after_the_200(self, stack: Stack, sample_video: Path) -> None:
        started = stack.client.post("/analyze", json={"file_path": "sample.mp4", "fps": 2})
        job_id = started.json()["job_id"]

        job = stack.client.get(f"/jobs/{job_id}")
        assert job.status_code == 200
        body = job.json()
        assert body["status"] == JobStatus.COMPLETED
        assert body["frames_dispatched"] == len(stack.publisher.published)
        assert body["last_source_index"] > 0

    def test_unknown_job_is_404(self, stack: Stack) -> None:
        assert stack.client.get("/jobs/does-not-exist").status_code == 404


class TestValidation:
    @pytest.mark.parametrize("fps", [0, 1, 3, 5, -2, 100])
    def test_rejects_fps_outside_2_or_4(self, stack: Stack, sample_video: Path, fps: int) -> None:
        response = stack.client.post("/analyze", json={"file_path": "sample.mp4", "fps": fps})
        assert response.status_code == 422

    @pytest.mark.parametrize("fps", ["2", None, True, [2], {"v": 2}, 2.5, 2.0000001])
    def test_rejects_wrongly_typed_or_lossy_fps(
        self, stack: Stack, sample_video: Path, fps: object
    ) -> None:
        """strict=True stops "2" and True being coerced into the literal 2, and
        a near-miss float like 2.5 is not silently rounded."""
        response = stack.client.post("/analyze", json={"file_path": "sample.mp4", "fps": fps})
        assert response.status_code == 422

    @pytest.mark.parametrize("fps", [2.0, 4.0])
    def test_accepts_integral_float_and_normalises_to_int(
        self, stack: Stack, sample_video: Path, fps: float
    ) -> None:
        """A deliberate concession: JSON has one number type, so 2.0 means 2.
        It must arrive downstream as a real int, not a float."""
        response = stack.client.post("/analyze", json={"file_path": "sample.mp4", "fps": fps})
        assert response.status_code == 200
        assert response.json()["target_fps"] == int(fps)

    def test_rejects_missing_fields(self, stack: Stack) -> None:
        assert stack.client.post("/analyze", json={"file_path": "sample.mp4"}).status_code == 422
        assert stack.client.post("/analyze", json={"fps": 2}).status_code == 422

    def test_rejects_unknown_fields(self, stack: Stack, sample_video: Path) -> None:
        response = stack.client.post(
            "/analyze", json={"file_path": "sample.mp4", "fps": 2, "quality": "high"}
        )
        assert response.status_code == 422

    def test_path_traversal_is_400(self, stack: Stack) -> None:
        response = stack.client.post("/analyze", json={"file_path": "../../etc/passwd", "fps": 2})
        assert response.status_code == 400
        assert response.json()["error"]["code"] == "invalid_file_path"

    def test_missing_video_is_404(self, stack: Stack) -> None:
        response = stack.client.post("/analyze", json={"file_path": "nope.mp4", "fps": 2})
        assert response.status_code == 404

    def test_non_video_file_is_415(self, stack: Stack, video_root: Path) -> None:
        (video_root / "notes.mp4").write_text("this is not a video")
        response = stack.client.post("/analyze", json={"file_path": "notes.mp4", "fps": 2})
        assert response.status_code == 415
        assert response.json()["error"]["code"] == "undecodable_video"

    def test_target_above_source_rate_is_422(self, stack: Stack, video_root: Path) -> None:
        """A 3 fps source cannot yield 4 fps."""
        make_video(video_root / "slow.mp4", fps=3.0, frames=30)
        response = stack.client.post("/analyze", json={"file_path": "slow.mp4", "fps": 4})
        assert response.status_code == 422
        assert response.json()["error"]["code"] == "invalid_frame_rate"

    def test_errors_use_the_uniform_envelope(self, stack: Stack) -> None:
        body = stack.client.post("/analyze", json={"file_path": "nope.mp4", "fps": 2}).json()
        assert set(body) == {"error"}
        assert {"code", "message"} <= set(body["error"])


class TestFailureHandling:
    def test_broker_failure_yields_502_not_200(
        self, video_root: Path, sample_video: Path
    ) -> None:
        """A partial dispatch must never be reported as success."""
        broker = InMemoryBroker()
        publisher = InMemoryFramePublisher(broker, fail_after=5)
        settings = AnalyzerSettings(video_root=video_root, analyzer_publishers=1)
        app = create_app(
            settings,
            store=InMemoryFrameStore(),
            publisher=publisher,
            jobs=InMemoryJobRepository(),
        )
        with TestClient(app, raise_server_exceptions=False) as client:
            response = client.post("/analyze", json={"file_path": "sample.mp4", "fps": 4})
        assert response.status_code == 502
        assert response.json()["error"]["code"] == "dispatch_incomplete"

    def test_unavailable_infrastructure_yields_503(
        self, video_root: Path, sample_video: Path
    ) -> None:
        store = InMemoryFrameStore()
        broker = InMemoryBroker()
        publisher = InMemoryFramePublisher(broker)
        settings = AnalyzerSettings(video_root=video_root)
        app = create_app(
            settings, store=store, publisher=publisher, jobs=InMemoryJobRepository()
        )
        with TestClient(app, raise_server_exceptions=False) as client:
            store.closed = True  # simulate Redis going away
            response = client.post("/analyze", json={"file_path": "sample.mp4", "fps": 2})
        assert response.status_code == 503
        assert response.headers.get("Retry-After") == "5"


class TestBackpressure:
    def test_small_queue_still_dispatches_every_frame(self, video_root: Path) -> None:
        """A queue far smaller than the frame count forces the decode thread to
        block repeatedly; nothing may be lost as a result."""
        make_video(video_root / "long.mp4", fps=25.0, frames=400)
        broker = InMemoryBroker()
        publisher = InMemoryFramePublisher(broker)
        settings = AnalyzerSettings(
            video_root=video_root, analyzer_queue_size=2, analyzer_publishers=1
        )
        app = create_app(
            settings,
            store=InMemoryFrameStore(),
            publisher=publisher,
            jobs=InMemoryJobRepository(),
        )
        with TestClient(app) as client:
            body = client.post("/analyze", json={"file_path": "long.mp4", "fps": 4}).json()

        expected = FrameSampler(25.0, 4).expected_frame_count(400)
        assert body["frames_dispatched"] == expected
        assert len(publisher.published) == expected

class TestHealth:
    def test_health_reports_dependencies(self, stack: Stack) -> None:
        body = stack.client.get("/health").json()
        assert body == {"status": "ok", "broker": True, "frame_store": True}

    def test_health_degrades_when_a_dependency_is_down(self, stack: Stack) -> None:
        stack.store.closed = True
        body = stack.client.get("/health").json()
        assert body["status"] == "degraded"
        assert body["frame_store"] is False
