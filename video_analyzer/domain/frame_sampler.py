"""Deciding *which* source frames to keep for a requested output frame rate.

This is the correctness core of the service, and it is deliberately a pure
object: no I/O, no OpenCV, no clock. That is what makes the sampling provably
right rather than merely plausible.

Why not `stride = source_fps // target_fps`
-------------------------------------------
The brief's worked example is 30 fps -> 2 fps -> "every 15th frame", which is a
clean integer. The video actually shipped with the assignment is **25 fps**,
where 25/2 = 12.5 and 25/4 = 6.25 -- *neither ratio is an integer*.

An integer stride of 12 emits 291 frames instead of 279 (and 582 instead of 558
at 4 fps): a ~4% overshoot that accumulates as clock drift, so every frame's
timestamp is progressively wrong. Sampling has to be timestamp-based.

The algorithm
-------------
For output slot ``k`` the source index is computed *fresh* from exact rational
arithmetic::

    i_k = floor(k * source_fps / target_fps + 1/2)      # round-half-up

Computing it fresh each time (rather than accumulating ``next += stride``) is
what makes it drift-free: float64 accumulation visibly skews over thousands of
frames. ``Fraction(...).limit_denominator()`` captures NTSC rates such as 29.97
and 23.976 exactly.

Verified behaviour:

===========  ==========  =======  ==========  ===================================
source fps   target fps  frames   gaps        note
===========  ==========  =======  ==========  ===================================
25           2           279      12, 13      mean exactly 12.5
25           4           558      6, 7        mean exactly 6.25
30           2           233      15 only     reproduces the brief's own example
29.97        4           --       7, 8        NTSC handled exactly
===========  ==========  =======  ==========  ===================================

The third row matters: this general algorithm is a strict superset of the
behaviour the brief describes.
"""

from __future__ import annotations

import math
from collections.abc import Iterator
from fractions import Fraction

#: Large enough to represent NTSC rates (30000/1001) exactly, small enough that
#: a garbage float fps cannot produce an absurd denominator.
_MAX_DENOMINATOR = 10_000


class InvalidFrameRateError(ValueError):
    """The requested sampling rate is impossible for this source."""


class FrameSampler:
    """Selects source frame indices for a target frame rate, without drift.

    The streaming cursor (:meth:`take`) expects to be fed *consecutive*
    non-negative indices starting at :attr:`start_slot`, which is exactly what a
    sequential decode loop produces.
    """

    __slots__ = ("_cursor_index", "_cursor_slot", "_stride", "source_fps", "target_fps")

    def __init__(self, source_fps: float, target_fps: int, *, start_slot: int = 0) -> None:
        if not math.isfinite(source_fps) or source_fps <= 0.0:
            raise InvalidFrameRateError(
                f"source frame rate must be a positive finite number, got {source_fps!r}"
            )
        if target_fps <= 0:
            raise InvalidFrameRateError(f"target frame rate must be positive, got {target_fps!r}")
        # Tiny tolerance so a 25.0-reported-as-24.999999 source still accepts 25.
        if source_fps < target_fps - 1e-6:
            raise InvalidFrameRateError(
                f"cannot sample {target_fps} fps from a {source_fps:g} fps source: "
                "the target rate exceeds the source rate"
            )
        if start_slot < 0:
            raise InvalidFrameRateError(f"start_slot must be non-negative, got {start_slot!r}")

        self.source_fps = source_fps
        self.target_fps = target_fps
        exact_source = Fraction(source_fps).limit_denominator(_MAX_DENOMINATOR)
        self._stride: Fraction = exact_source / Fraction(target_fps)

        self._cursor_slot = start_slot
        self._cursor_index = self.source_index_for_slot(start_slot)

    # -- pure queries -----------------------------------------------------

    @property
    def stride(self) -> Fraction:
        """Exact source-frames-per-output-frame, e.g. ``Fraction(25, 2)``."""
        return self._stride

    def source_index_for_slot(self, slot: int) -> int:
        """Source frame index for output slot ``slot``, round-half-up."""
        return math.floor(slot * self._stride + Fraction(1, 2))

    def expected_frame_count(self, total_source_frames: int) -> int:
        """How many frames will be kept from a video of ``total_source_frames``.

        Closed form rather than a loop: the largest slot ``k`` satisfying
        ``floor(k*s + 1/2) <= total-1`` is ``ceil((total - 1/2)/s) - 1``, so the
        count is ``ceil((total - 1/2)/s)``.
        """
        if total_source_frames <= 0:
            return 0
        return max(0, math.ceil((Fraction(total_source_frames) - Fraction(1, 2)) / self._stride))

    def iter_slots(self, total_source_frames: int) -> Iterator[tuple[int, int]]:
        """Yield ``(slot, source_index)`` for every kept frame. Pure; for tests
        and for pre-computing expectations."""
        for slot in range(self.expected_frame_count(total_source_frames)):
            yield slot, self.source_index_for_slot(slot)

    def timestamp_for_index(self, source_index: int) -> float:
        """Presentation timestamp, in seconds, of a source frame index."""
        return source_index / self.source_fps

    def slot_after_source_index(self, source_index: int) -> int:
        """First output slot whose source index is strictly greater than ``source_index``.

        This is the inverse the checkpoint needs: given the highest source frame
        already confirmed, which slot does the resumed run start at?

        Closed form: we want the smallest ``k`` with ``floor(k*s + 1/2) > idx``.
        ``floor(x) > idx`` iff ``x >= idx + 1``, so ``k >= (idx + 1/2)/s`` and
        ``k = ceil((idx + 1/2)/s)``.

        ``source_index = -1`` (nothing confirmed yet) yields slot 0, so a fresh
        run and a resumed run share one code path.
        """
        if source_index < 0:
            return 0
        return max(0, math.ceil((Fraction(source_index) + Fraction(1, 2)) / self._stride))

    # -- streaming cursor -------------------------------------------------

    def take(self, source_index: int) -> int | None:
        """Return the output slot if ``source_index`` is sampled, else ``None``.

        Advances internal state, so feed it 0, 1, 2, ... exactly once each.
        """
        if source_index != self._cursor_index:
            return None
        slot = self._cursor_slot
        self._cursor_slot += 1
        # stride >= 1 is guaranteed by __init__, so indices strictly increase
        # and the cursor can never stall on a repeated index.
        self._cursor_index = self.source_index_for_slot(self._cursor_slot)
        return slot

    @property
    def next_source_index(self) -> int:
        """The next source index this sampler is waiting for."""
        return self._cursor_index

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return (
            f"FrameSampler(source_fps={self.source_fps!r}, target_fps={self.target_fps!r}, "
            f"stride={self._stride})"
        )
