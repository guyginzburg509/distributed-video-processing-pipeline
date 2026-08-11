"""Crash a job mid-dispatch, re-submit it, and prove nothing is lost or repeated.

The property that matters is the union: run 1's frames plus run 2's frames must
equal exactly the frames a single uninterrupted run would have produced -- no
gap at the seam, no frame dispatched twice.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from httpx import Response

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

SOURCE_FPS = 25.0
TOTAL_FRAMES = 300


@pytest.fixture
def video(video_root: Path) -> Path:
    return make_video(video_root / "clip.mp4", fps=SOURCE_FPS, frames=TOTAL_FRAMES)


def expected_indices(target_fps: int) -> list[int]:
    return [i for _, i in FrameSampler(SOURCE_FPS, target_fps).iter_slots(TOTAL_FRAMES)]


class Harness:
    """Two sequential runs sharing one job repository and frame store."""

    def __init__(self, video_root: Path) -> None:
        self.video_root = video_root
        self.jobs = InMemoryJobRepository()
        self.store = InMemoryFrameStore()
        self.published: list[int] = []

    def run(self, *, fps: int, fail_after: int | None = None) -> Response:
        broker = InMemoryBroker()
        publisher = InMemoryFramePublisher(broker, fail_after=fail_after)
        settings = AnalyzerSettings(
            video_root=self.video_root, analyzer_publishers=1, checkpoint_every=1
        )
        app = create_app(
            settings, store=self.store, publisher=publisher, jobs=self.jobs
        )
        with TestClient(app, raise_server_exceptions=False) as client:
            response = client.post("/analyze", json={"file_path": "clip.mp4", "fps": fps})
        self.published.extend(ref.frame_id for ref in publisher.published)
        return response


@pytest.fixture
def harness(video_root: Path, video: Path) -> Harness:
    return Harness(video_root)


class TestResumeAfterFailure:
    def test_second_run_completes_what_the_first_started(self, harness: Harness) -> None:
        first = harness.run(fps=2, fail_after=10)
        assert first.status_code == 502, "a partial dispatch must not report success"

        second = harness.run(fps=2)
        assert second.status_code == 200
        body = second.json()

        assert body["resumed_after_frame"] is not None, "the second run should have resumed"
        assert body["status"] == JobStatus.COMPLETED
        assert body["frames_dispatched"] == len(expected_indices(2))

    def test_union_of_both_runs_is_exactly_the_full_sequence(self, harness: Harness) -> None:
        """No gap at the seam, no duplicates across the seam."""
        harness.run(fps=2, fail_after=10)
        harness.run(fps=2)

        assert harness.published == expected_indices(2)
        assert len(harness.published) == len(set(harness.published)), "a frame was dispatched twice"

    def test_resumed_run_does_less_work_than_a_full_one(self, harness: Harness) -> None:
        harness.run(fps=2, fail_after=10)
        body = harness.run(fps=2).json()

        total = len(expected_indices(2))
        assert body["frames_dispatched_this_run"] < total
        assert body["frames_dispatched_this_run"] + 10 == total
        assert body["frames_dispatched"] == total

    def test_same_job_id_is_continued_not_duplicated(self, harness: Harness) -> None:
        first_job = harness.run(fps=2, fail_after=10).json()["error"]  # 502 envelope
        assert first_job["code"] == "dispatch_incomplete"

        second = harness.run(fps=2).json()
        assert len(harness.jobs.records) == 1, "resume must continue the job, not fork a new one"
        assert second["job_id"] in harness.jobs.records

    # All below the 48 frames a full 4 fps run produces, so each really interrupts.
    @pytest.mark.parametrize("fail_after", [1, 5, 20, 40])
    def test_resumes_correctly_from_any_interruption_point(
        self, video_root: Path, video: Path, fail_after: int
    ) -> None:
        harness = Harness(video_root)
        harness.run(fps=4, fail_after=fail_after)
        harness.run(fps=4)
        assert harness.published == expected_indices(4)


class TestFreshRunsAreUnaffected:
    def test_completed_job_is_not_resumed(self, harness: Harness) -> None:
        """Re-analysing a finished video is a legitimate request: new job, full work."""
        first = harness.run(fps=2).json()
        second = harness.run(fps=2).json()

        assert second["resumed_after_frame"] is None
        assert second["job_id"] != first["job_id"]
        assert second["frames_dispatched_this_run"] == len(expected_indices(2))

    def test_different_fps_is_a_different_job(self, harness: Harness) -> None:
        """The resume key includes the target rate, so 2 fps and 4 fps never
        contaminate each other's checkpoints."""
        two = harness.run(fps=2).json()
        four = harness.run(fps=4).json()

        assert four["job_id"] != two["job_id"]
        assert four["resumed_after_frame"] is None
        assert four["frames_dispatched"] == len(expected_indices(4))

    def test_first_run_is_never_marked_resumed(self, harness: Harness) -> None:
        assert harness.run(fps=2).json()["resumed_after_frame"] is None


class TestNothingLeftToDo:
    def test_resuming_a_cancelled_job_at_the_very_end_is_a_noop(
        self, video_root: Path, video: Path
    ) -> None:
        """If the checkpoint already covers the last frame, the resumed run
        dispatches nothing and still reports the job complete."""
        harness = Harness(video_root)
        harness.run(fps=2)

        # Force the completed job back into a resumable state, checkpoint intact.
        job_id = next(iter(harness.jobs.records))
        record = harness.jobs.records[job_id]
        harness.jobs.records[job_id] = record.model_copy(
            update={"status": JobStatus.CANCELLED}
        )

        body = harness.run(fps=2).json()
        assert body["resumed_after_frame"] == record.last_source_index
        assert body["frames_dispatched_this_run"] == 0
        assert body["frames_dispatched"] == len(expected_indices(2))
        assert body["status"] == JobStatus.COMPLETED
