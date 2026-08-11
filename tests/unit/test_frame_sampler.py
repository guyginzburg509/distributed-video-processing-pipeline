"""The sampling contract, pinned.

The headline cases are the ones the assignment actually exercises: the shipped
video is 25 fps, so both 25->2 (12.5) and 25->4 (6.25) are non-integer ratios.
"""

from __future__ import annotations

from fractions import Fraction
from itertools import pairwise

import pytest

from video_analyzer.domain.frame_sampler import FrameSampler, InvalidFrameRateError

# The real asset: videos/G20_Summit.mp4 -- 1280x720 h264, 25 fps, 3488 frames.
G20_FRAMES = 3488


class TestShippedVideo:
    """25 fps source: the case an integer stride gets wrong."""

    def test_25_to_2_yields_279_frames_not_291(self) -> None:
        sampler = FrameSampler(25.0, 2)
        assert sampler.expected_frame_count(G20_FRAMES) == 279
        # What a naive `int(25/2) == 12` stride would have produced:
        assert G20_FRAMES // 12 != 279

    def test_25_to_4_yields_558_frames_not_582(self) -> None:
        sampler = FrameSampler(25.0, 4)
        assert sampler.expected_frame_count(G20_FRAMES) == 558
        assert G20_FRAMES // 6 != 558

    @pytest.mark.parametrize(
        ("target", "expected_gaps"),
        [(2, {12, 13}), (4, {6, 7})],
    )
    def test_gaps_alternate_around_the_exact_ratio(
        self, target: int, expected_gaps: set[int]
    ) -> None:
        sampler = FrameSampler(25.0, target)
        indices = [idx for _, idx in sampler.iter_slots(G20_FRAMES)]
        gaps = {b - a for a, b in pairwise(indices)}
        assert gaps == expected_gaps

    @pytest.mark.parametrize("target", [2, 4])
    def test_mean_gap_equals_exact_stride_no_drift(self, target: int) -> None:
        """The whole point: the average spacing is *exactly* source/target."""
        sampler = FrameSampler(25.0, target)
        indices = [idx for _, idx in sampler.iter_slots(G20_FRAMES)]
        mean_gap = Fraction(indices[-1] - indices[0], len(indices) - 1)
        assert abs(mean_gap - Fraction(25, target)) < Fraction(1, 1000)

    def test_first_indices_25_to_2(self) -> None:
        sampler = FrameSampler(25.0, 2)
        assert [idx for _, idx in sampler.iter_slots(100)][:6] == [0, 13, 25, 38, 50, 63]

    def test_first_indices_25_to_4(self) -> None:
        sampler = FrameSampler(25.0, 4)
        assert [idx for _, idx in sampler.iter_slots(100)][:6] == [0, 6, 13, 19, 25, 31]


class TestBriefsWorkedExample:
    """The brief says 30 fps -> 2 fps -> "every 15th frame". We must match it."""

    def test_30_to_2_is_exactly_every_15th_frame(self) -> None:
        sampler = FrameSampler(30.0, 2)
        indices = [idx for _, idx in sampler.iter_slots(G20_FRAMES)]
        gaps = {b - a for a, b in pairwise(indices)}
        assert gaps == {15}, "the general algorithm must reproduce the brief's example exactly"
        assert indices[:5] == [0, 15, 30, 45, 60]

    def test_30_to_4_alternates_7_and_8(self) -> None:
        sampler = FrameSampler(30.0, 4)
        indices = [idx for _, idx in sampler.iter_slots(1000)]
        assert {b - a for a, b in pairwise(indices)} == {7, 8}


class TestNtscRates:
    """Fractional broadcast rates must not be rounded into drift."""

    @pytest.mark.parametrize(
        ("source", "target", "expected_stride"),
        [
            (29.97, 4, Fraction(2997, 400)),
            (23.976, 2, Fraction(2997, 250)),
            (59.94, 4, Fraction(2997, 200)),
        ],
    )
    def test_stride_is_exact_rational(
        self, source: float, target: int, expected_stride: Fraction
    ) -> None:
        assert FrameSampler(source, target).stride == expected_stride

    def test_23_976_to_2_matches_wall_clock(self) -> None:
        sampler = FrameSampler(23.976, 2)
        total = 2400  # ~100.1 seconds
        count = sampler.expected_frame_count(total)
        assert abs(count - (total / 23.976) * 2) < 1.0


class TestBoundaries:
    def test_source_equals_target_keeps_every_frame(self) -> None:
        sampler = FrameSampler(25.0, 25)
        assert [idx for _, idx in sampler.iter_slots(10)] == list(range(10))

    def test_zero_length_video_yields_nothing(self) -> None:
        assert FrameSampler(25.0, 2).expected_frame_count(0) == 0

    def test_negative_total_yields_nothing(self) -> None:
        assert FrameSampler(25.0, 2).expected_frame_count(-5) == 0

    def test_single_frame_video_keeps_that_frame(self) -> None:
        sampler = FrameSampler(25.0, 2)
        assert [idx for _, idx in sampler.iter_slots(1)] == [0]

    def test_video_shorter_than_one_output_period(self) -> None:
        """10 frames at 25 fps is 0.4 s; at 2 fps that is a single sample."""
        sampler = FrameSampler(25.0, 2)
        assert sampler.expected_frame_count(10) == 1

    def test_first_kept_frame_is_always_index_zero(self) -> None:
        for source, target in [(25.0, 2), (25.0, 4), (30.0, 2), (29.97, 4)]:
            assert FrameSampler(source, target).source_index_for_slot(0) == 0

    def test_indices_are_strictly_increasing(self) -> None:
        for source, target in [(25.0, 2), (25.0, 4), (30.0, 4), (29.97, 4), (25.0, 25)]:
            indices = [idx for _, idx in FrameSampler(source, target).iter_slots(500)]
            assert all(b > a for a, b in pairwise(indices))

    def test_all_indices_within_bounds(self) -> None:
        for total in (1, 7, 100, 3488):
            for _, idx in FrameSampler(25.0, 4).iter_slots(total):
                assert 0 <= idx < total


class TestValidation:
    @pytest.mark.parametrize("source_fps", [0.0, -1.0, -25.0, float("nan"), float("inf")])
    def test_rejects_unusable_source_rate(self, source_fps: float) -> None:
        with pytest.raises(InvalidFrameRateError):
            FrameSampler(source_fps, 2)

    @pytest.mark.parametrize("target_fps", [0, -2])
    def test_rejects_non_positive_target(self, target_fps: int) -> None:
        with pytest.raises(InvalidFrameRateError):
            FrameSampler(25.0, target_fps)

    def test_rejects_target_above_source(self) -> None:
        """Cannot synthesise 4 fps from a 3 fps source."""
        with pytest.raises(InvalidFrameRateError, match="exceeds the source rate"):
            FrameSampler(3.0, 4)

    def test_tolerates_float_noise_at_the_boundary(self) -> None:
        FrameSampler(3.9999999, 4)  # must not raise

    def test_rejects_negative_start_slot(self) -> None:
        with pytest.raises(InvalidFrameRateError):
            FrameSampler(25.0, 2, start_slot=-1)


class TestStreamingCursor:
    """`take()` is what the decode loop actually calls."""

    def test_cursor_agrees_with_pure_iteration(self) -> None:
        total = 500
        for source, target in [(25.0, 2), (25.0, 4), (30.0, 2), (29.97, 4)]:
            expected = list(FrameSampler(source, target).iter_slots(total))
            cursor = FrameSampler(source, target)
            actual = [
                (slot, i) for i in range(total) if (slot := cursor.take(i)) is not None
            ]
            assert actual == expected, f"cursor diverged from pure iteration at {source}->{target}"

    def test_slots_are_consecutive_from_zero(self) -> None:
        sampler = FrameSampler(25.0, 4)
        slots = [s for i in range(500) if (s := sampler.take(i)) is not None]
        assert slots == list(range(len(slots)))

    def test_skipped_indices_return_none(self) -> None:
        sampler = FrameSampler(25.0, 2)
        assert sampler.take(0) == 0
        for i in range(1, 13):
            assert sampler.take(i) is None
        assert sampler.take(13) == 1

    def test_next_source_index_is_exposed(self) -> None:
        sampler = FrameSampler(25.0, 2)
        assert sampler.next_source_index == 0
        sampler.take(0)
        assert sampler.next_source_index == 13


class TestResume:
    """Checkpoint recovery restarts mid-stream at an arbitrary slot."""

    def test_start_slot_resumes_the_same_sequence(self) -> None:
        full = [idx for _, idx in FrameSampler(25.0, 2).iter_slots(3488)]
        resumed = FrameSampler(25.0, 2, start_slot=100)
        assert resumed.next_source_index == full[100]

    def test_resumed_cursor_matches_the_tail_of_a_full_run(self) -> None:
        total = 1000
        full = list(FrameSampler(25.0, 4).iter_slots(total))
        resume_at = 40
        cursor = FrameSampler(25.0, 4, start_slot=resume_at)
        tail = [
            (slot, i)
            for i in range(cursor.next_source_index, total)
            if (slot := cursor.take(i)) is not None
        ]
        assert tail == full[resume_at:]


class TestTimestamps:
    def test_timestamp_matches_index_over_source_rate(self) -> None:
        sampler = FrameSampler(25.0, 2)
        assert sampler.timestamp_for_index(0) == 0.0
        assert sampler.timestamp_for_index(25) == pytest.approx(1.0)
        assert sampler.timestamp_for_index(3487) == pytest.approx(139.48)

    def test_spacing_is_within_the_theoretical_optimum(self) -> None:
        """Frames are discrete, so the best achievable jitter is +/- half a
        source-frame period. At 25 fps that is exactly 0.02 s; we must hit it."""
        sampler = FrameSampler(25.0, 2)
        stamps = [sampler.timestamp_for_index(idx) for _, idx in sampler.iter_slots(G20_FRAMES)]
        deltas = [b - a for a, b in pairwise(stamps)]
        optimum = 1.0 / (2 * 25.0)
        assert max(abs(d - 0.5) for d in deltas) <= optimum + 1e-9

    @pytest.mark.parametrize("target", [2, 4])
    def test_cumulative_drift_stays_below_one_frame(self, target: int) -> None:
        """The anti-drift guarantee, stated directly.

        An integer stride would fall steadily further behind wall time; here the
        error against the ideal k/target_fps schedule must never accumulate.
        """
        sampler = FrameSampler(25.0, target)
        worst = max(
            abs(sampler.timestamp_for_index(idx) - slot / target)
            for slot, idx in sampler.iter_slots(G20_FRAMES)
        )
        assert worst <= 1.0 / 25.0, f"drifted {worst:.4f}s from the ideal schedule"

    def test_integer_stride_would_drift_by_seconds(self) -> None:
        """Contrast: what the naive implementation costs over this video."""
        naive_stride = int(25.0 // 2)  # 12
        last_slot = 279 - 1
        naive_error = abs((naive_stride * last_slot) / 25.0 - last_slot / 2)
        assert naive_error > 5.0, "sanity: the naive approach really is badly wrong"
