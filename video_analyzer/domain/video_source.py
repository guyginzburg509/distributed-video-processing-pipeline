"""Decoding video into JPEG-encoded sampled frames.

The decode strategy is ``grab()`` for every frame, ``retrieve()`` only for the
frames the sampler keeps. ``grab()`` advances the decoder without converting to
BGR or allocating a numpy array, so the ~96% of frames we discard skip that
cost. Being honest about the size of the win: h264 is temporally compressed, so
every frame must still be decoded -- this is a solid constant factor, not an
order of magnitude. Per-frame ``CAP_PROP_POS_FRAMES`` seeking would be worse
(it snaps to keyframes and is unreliable across codecs).

Frames are streamed, never accumulated: all 3488 decoded frames of the sample
video would be ~9.4 GB in RAM.
"""

from __future__ import annotations

import threading
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

import cv2
import numpy as np
from pydantic import BaseModel

from video_analyzer.domain.frame_sampler import FrameSampler


class VideoOpenError(RuntimeError):
    """The file exists but no decoder could open it."""


class VideoMetadataError(RuntimeError):
    """The file opened but reports unusable metadata."""


class VideoMetadata(BaseModel):
    model_config = {"frozen": True}

    source_fps: float
    total_frames: int
    width: int
    height: int

    @property
    def duration_sec(self) -> float:
        return self.total_frames / self.source_fps if self.source_fps > 0 else 0.0


@dataclass(frozen=True, slots=True)
class SampledFrame:
    """One kept frame, already JPEG-encoded and ready to store."""

    slot: int
    source_index: int
    timestamp_sec: float
    jpeg: bytes
    width: int
    height: int


@dataclass(frozen=True, slots=True)
class DecodeFailure:
    """A frame the sampler wanted but that could not be decoded or encoded.

    Yielded rather than raised: one bad frame in a long video should not
    destroy the whole job (see AnalysisService's failure-ratio policy).
    """

    source_index: int
    reason: str


class VideoSource(Protocol):
    def metadata(self) -> VideoMetadata: ...

    def iter_sampled(
        self,
        sampler: FrameSampler,
        *,
        jpeg_quality: int,
        abort: threading.Event | None = None,
        start_source_index: int = 0,
    ) -> Iterator[SampledFrame | DecodeFailure]: ...

    def close(self) -> None: ...


class OpenCVVideoSource:
    """VideoSource backed by ``cv2.VideoCapture``.

    Blocking by design; the service runs it on a worker thread. OpenCV releases
    the GIL during decode and ``imencode``, so that thread genuinely overlaps
    with the event loop's I/O.
    """

    def __init__(self, path: Path) -> None:
        self._path = path
        self._capture = cv2.VideoCapture(str(path))
        if not self._capture.isOpened():
            self._capture.release()
            raise VideoOpenError(f"no decoder could open {path.name!r}")

        fps = float(self._capture.get(cv2.CAP_PROP_FPS))
        if not np.isfinite(fps) or fps <= 0.0:
            self._capture.release()
            raise VideoMetadataError(f"{path.name!r} reports an unusable frame rate ({fps!r})")

        # CAP_PROP_FRAME_COUNT is unreliable in several containers, so it is
        # used only for reporting and expectations -- never as a loop bound.
        raw_count = self._capture.get(cv2.CAP_PROP_FRAME_COUNT)
        total = int(raw_count) if np.isfinite(raw_count) and raw_count > 0 else 0

        self._metadata = VideoMetadata(
            source_fps=fps,
            total_frames=total,
            width=int(self._capture.get(cv2.CAP_PROP_FRAME_WIDTH)),
            height=int(self._capture.get(cv2.CAP_PROP_FRAME_HEIGHT)),
        )

    def metadata(self) -> VideoMetadata:
        return self._metadata

    def iter_sampled(
        self,
        sampler: FrameSampler,
        *,
        jpeg_quality: int,
        abort: threading.Event | None = None,
        start_source_index: int = 0,
    ) -> Iterator[SampledFrame | DecodeFailure]:
        encode_params = [int(cv2.IMWRITE_JPEG_QUALITY), int(jpeg_quality)]

        index = self._seek_to(start_source_index) if start_source_index > 0 else 0
        while True:
            if abort is not None and abort.is_set():
                return
            if not self._capture.grab():
                return  # end of stream, or truncated file -- both end the run

            slot = sampler.take(index)
            if slot is not None:
                yield self._retrieve(index, slot, encode_params)
            index += 1

    def _seek_to(self, target_index: int) -> int:
        """Seek for a checkpoint resume, returning the index we actually land on.

        A *single* seek is safe -- this is not the per-frame seeking that
        `frame_sampler` rejects. But it is not blindly trusted either: with
        temporally compressed codecs a backend may snap to a keyframe.

        * Landing *before* the target is harmless: the extra frames are
          ``grab()``-ed and skipped, since the sampler only fires on the exact
          index it is waiting for. Correct, just slightly slower.
        * Landing *after* the target would silently skip frames, so we give up
          on seeking entirely and rescan from zero. Slower, never wrong.
        """
        self._capture.set(cv2.CAP_PROP_POS_FRAMES, float(target_index))
        landed_raw = self._capture.get(cv2.CAP_PROP_POS_FRAMES)
        landed = int(landed_raw) if np.isfinite(landed_raw) and landed_raw >= 0 else 0

        if landed > target_index:
            self._capture.set(cv2.CAP_PROP_POS_FRAMES, 0.0)
            return 0
        return landed

    def _retrieve(
        self, index: int, slot: int, encode_params: list[int]
    ) -> SampledFrame | DecodeFailure:
        ok, frame = self._capture.retrieve()
        if not ok or frame is None or frame.size == 0:
            return DecodeFailure(index, "retrieve returned no image")

        # POS_MSEC is authoritative for variable-frame-rate sources, where
        # index/fps would drift; fall back to the CFR computation when the
        # backend does not supply it.
        pos_msec = float(self._capture.get(cv2.CAP_PROP_POS_MSEC))
        timestamp = (
            pos_msec / 1000.0
            if np.isfinite(pos_msec) and pos_msec > 0.0
            else index / self._metadata.source_fps
        )

        ok, buffer = cv2.imencode(".jpg", frame, encode_params)
        if not ok:
            return DecodeFailure(index, "JPEG encoding failed")

        height, width = frame.shape[:2]
        return SampledFrame(
            slot=slot,
            source_index=index,
            timestamp_sec=timestamp,
            jpeg=buffer.tobytes(),
            width=int(width),
            height=int(height),
        )

    def close(self) -> None:
        self._capture.release()

    def __enter__(self) -> OpenCVVideoSource:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


def open_video(path: Path) -> OpenCVVideoSource:
    """Factory used as the composition-root seam; tests substitute their own."""
    return OpenCVVideoSource(path)
