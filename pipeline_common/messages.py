"""The wire contract between VideoAnalyzer and StreamDetector."""

from __future__ import annotations

from enum import StrEnum

from pydantic import BaseModel


class FrameRef(BaseModel):
    """A claim-check reference to one extracted frame.

    The JPEG bytes live in the frame store (Redis); only this ~200 byte
    reference travels through the broker. That keeps queue depth cheap, bounds
    backlog memory by the blob TTL rather than by the queue, and means the two
    services share no filesystem -- they can run on different machines.

    Every field here is read by something. Frame dimensions are deliberately
    absent: the JPEG already carries them, and duplicating data on the wire is
    just another thing that can disagree.
    """

    # frozen: a reference is a fact, not a mutable buffer.
    # extra="forbid": a stale producer sending a removed field must fail
    # loudly rather than have it silently ignored.
    model_config = {"frozen": True, "extra": "forbid"}

    job_id: str
    video_id: str

    #: Index of this frame in the *source* video (0, 13, 25, ... for 25->2 fps),
    #: which is what lands in RespObject.frame_id.
    frame_id: int
    #: Presentation time within the video -- "when was this face seen".
    timestamp_sec: float
    #: Wall-clock epoch seconds at which the analyzer dispatched this frame.
    #: The detector measures age against it to enforce the latency budget: in a
    #: real-time system a frame older than the budget is worthless, and
    #: processing it only puts you further behind.
    dispatched_at: float = 0.0

    blob_key: str

    def age_sec(self, now: float) -> float:
        """Seconds since dispatch. 0.0 when the analyzer did not stamp it."""
        return max(0.0, now - self.dispatched_at) if self.dispatched_at else 0.0

    def to_bytes(self) -> bytes:
        return self.model_dump_json().encode("utf-8")

    @classmethod
    def from_bytes(cls, raw: bytes) -> FrameRef:
        return cls.model_validate_json(raw)


class JobStatus(StrEnum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    PARTIAL = "PARTIAL"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class JobRecord(BaseModel):
    """Progress record for one /analyze invocation.

    Lives in Redis today; the JobRepository port exists so that swapping in a
    Postgres-backed system of record is a one-adapter change (see design 5.4).
    """

    job_id: str
    video_id: str
    #: Deterministic "this video at this rate" key, so a retry can find the
    #: checkpoint a previous crashed run left behind.
    resume_key: str = ""
    status: JobStatus = JobStatus.PENDING
    source_fps: float = 0.0
    target_fps: int = 0
    total_source_frames: int = 0
    frames_expected: int = 0
    frames_dispatched: int = 0
    frames_failed: int = 0
    frames_processed: int = 0
    #: Highest source index whose frame has been *confirmed* by the broker.
    #: Only confirmed frames advance it, so a checkpoint never claims progress
    #: the broker did not accept.
    last_source_index: int = -1
    started_at: float = 0.0
    updated_at: float = 0.0
    error: str | None = None
