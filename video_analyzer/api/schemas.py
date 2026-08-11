"""Request/response contracts for the HTTP API."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from pipeline_common.messages import JobStatus
from video_analyzer.services.analysis_service import AnalysisOutcome

#: The brief requires the rate to be strictly 2 or 4.
ALLOWED_FPS = (2, 4)


class AnalyzeRequest(BaseModel):
    """``strict=True`` matters here: without it pydantic would happily coerce
    ``2.0`` or ``"2"`` into the literal 2, quietly widening a contract the brief
    states as *strictly* 2 or 4. ``extra="forbid"`` catches typo'd field names
    instead of ignoring them."""

    model_config = ConfigDict(extra="forbid", strict=True)

    file_path: str = Field(
        min_length=1,
        max_length=4096,
        description="Video path, absolute or relative to VIDEO_ROOT.",
        examples=["G20_Summit.mp4"],
    )
    fps: Literal[2, 4] = Field(description="Target sampling rate. Strictly 2 or 4.")

    @field_validator("fps", mode="before")
    @classmethod
    def _accept_integral_float(cls, value: object) -> object:
        """JSON has a single number type, so some clients serialise 2 as ``2.0``.

        That is exactly equal to 2 and is accepted, normalised to a real ``int``
        so nothing downstream sees a float. Anything lossy or differently typed
        (``2.5``, ``"2"``, ``True``) falls through to strict Literal validation
        and is rejected.
        """
        if type(value) is float and value.is_integer():
            return int(value)
        return value


class AnalyzeResponse(BaseModel):
    job_id: str
    video_id: str
    status: JobStatus
    source_fps: float
    target_fps: int
    total_source_frames: int = Field(
        description=(
            "Frame count as reported by the container. Many MP4s store no frame "
            "count, in which case this is an estimate (duration x fps) and can be "
            "off by a frame or two. Never used as a decode bound."
        )
    )
    frames_expected: int = Field(
        description=(
            "Frames predicted from total_source_frames, so it inherits that "
            "estimate. May exceed frames_dispatched by a frame or two on such "
            "files; frames_dispatched is the authoritative count of what was "
            "actually decoded and confirmed."
        )
    )
    frames_dispatched: int = Field(
        description="Cumulative for the job, including frames confirmed by an earlier run."
    )
    frames_failed: int
    video_duration_sec: float
    elapsed_sec: float
    realtime_factor: float = Field(
        description="Video seconds processed per wall-clock second; >1 is faster than real time."
    )
    resumed_after_frame: int | None = Field(
        default=None,
        description=(
            "Set when this request continued a previous crashed or failed run: the "
            "last source frame index that run confirmed. Null for a fresh job."
        ),
    )
    frames_dispatched_this_run: int = Field(
        description="Frames dispatched by this request alone; differs on a resumed job."
    )

    @classmethod
    def from_outcome(cls, outcome: AnalysisOutcome) -> AnalyzeResponse:
        return cls(
            job_id=outcome.job_id,
            video_id=outcome.video_id,
            status=outcome.status,
            source_fps=round(outcome.source_fps, 4),
            target_fps=outcome.target_fps,
            total_source_frames=outcome.total_source_frames,
            frames_expected=outcome.frames_expected,
            frames_dispatched=outcome.frames_dispatched,
            frames_failed=outcome.frames_failed,
            video_duration_sec=round(outcome.video_duration_sec, 3),
            elapsed_sec=round(outcome.elapsed_sec, 3),
            realtime_factor=round(outcome.realtime_factor, 2),
            resumed_after_frame=outcome.resumed_after_frame,
            frames_dispatched_this_run=outcome.frames_dispatched_this_run,
        )


class JobResponse(BaseModel):
    """Progress of a job, including work done by detectors after the 200."""

    job_id: str
    video_id: str
    status: JobStatus
    source_fps: float
    target_fps: int
    frames_expected: int
    frames_dispatched: int
    frames_failed: int
    frames_processed: int
    last_source_index: int
    error: str | None = None


class ErrorDetail(BaseModel):
    code: str
    message: str
    details: dict[str, object] | None = None


class ErrorResponse(BaseModel):
    error: ErrorDetail


class HealthResponse(BaseModel):
    status: Literal["ok", "degraded"]
    broker: bool
    frame_store: bool
