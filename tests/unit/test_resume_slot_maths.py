"""`slot_after_source_index` is the inverse the checkpoint depends on.

Get it wrong by one and a resumed run either skips a frame or dispatches one
twice -- both silent. So it is pinned against the forward mapping directly.
"""

from __future__ import annotations

import pytest

from video_analyzer.domain.frame_sampler import FrameSampler

RATES = [(25.0, 2), (25.0, 4), (30.0, 2), (30.0, 4), (29.97, 4), (25.0, 25)]


class TestInverseAgreesWithForward:
    @pytest.mark.parametrize(("source", "target"), RATES)
    def test_round_trips_for_every_slot(self, source: float, target: int) -> None:
        sampler = FrameSampler(source, target)
        for slot, index in sampler.iter_slots(500):
            # The slot after the previous kept frame is this one.
            assert sampler.slot_after_source_index(index - 1) == slot
            # And the slot after this frame is strictly the next one.
            assert sampler.slot_after_source_index(index) == slot + 1

    @pytest.mark.parametrize(("source", "target"), RATES)
    def test_result_is_always_strictly_past_the_checkpoint(
        self, source: float, target: int
    ) -> None:
        sampler = FrameSampler(source, target)
        for index in range(0, 400, 7):
            slot = sampler.slot_after_source_index(index)
            assert sampler.source_index_for_slot(slot) > index
            if slot > 0:
                assert sampler.source_index_for_slot(slot - 1) <= index


class TestBoundaries:
    def test_nothing_confirmed_yet_starts_at_slot_zero(self) -> None:
        """-1 is the "no checkpoint" sentinel, so fresh and resumed runs share
        one code path."""
        assert FrameSampler(25.0, 2).slot_after_source_index(-1) == 0

    def test_negative_indices_clamp_to_zero(self) -> None:
        assert FrameSampler(25.0, 2).slot_after_source_index(-99) == 0

    def test_known_values_for_the_shipped_video(self) -> None:
        sampler = FrameSampler(25.0, 2)  # indices 0, 13, 25, 38, 50, ...
        assert sampler.slot_after_source_index(0) == 1
        assert sampler.slot_after_source_index(12) == 1
        assert sampler.slot_after_source_index(13) == 2
        assert sampler.slot_after_source_index(24) == 2
        assert sampler.slot_after_source_index(25) == 3

    def test_checkpoint_at_the_final_frame_leaves_nothing(self) -> None:
        sampler = FrameSampler(25.0, 2)
        last_index = sampler.source_index_for_slot(278)  # 3475, for 3488 frames
        next_slot = sampler.slot_after_source_index(last_index)
        assert next_slot == 279
        assert sampler.source_index_for_slot(next_slot) >= 3488


class TestResumedSamplerContinuesCleanly:
    @pytest.mark.parametrize(("source", "target"), RATES)
    @pytest.mark.parametrize("break_at", [1, 5, 20])
    def test_split_run_reproduces_an_uninterrupted_one(
        self, source: float, target: int, break_at: int
    ) -> None:
        """The end-to-end property, at the sampler level."""
        total = 400
        full = [i for _, i in FrameSampler(source, target).iter_slots(total)]
        if break_at >= len(full):
            pytest.skip("interruption point beyond the end of this sequence")

        first_half = full[:break_at]
        checkpoint = first_half[-1]

        resume_slot = FrameSampler(source, target).slot_after_source_index(checkpoint)
        resumed = FrameSampler(source, target, start_slot=resume_slot)
        second_half = [
            i
            for i in range(resumed.next_source_index, total)
            if resumed.take(i) is not None
        ]

        assert first_half + second_half == full
        assert len(set(first_half) & set(second_half)) == 0
