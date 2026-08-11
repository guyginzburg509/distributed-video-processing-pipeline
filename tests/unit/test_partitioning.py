"""Partition assignment: the basis of per-video ordering.

Two services in different processes must agree on where a video's frames go,
forever. That rules out ``hash()``, which Python salts per process.
"""

from __future__ import annotations

import subprocess
import sys
from collections import Counter

import pytest

from pipeline_common.adapters.rabbitmq import partition_for, partition_queue


class TestStability:
    def test_same_video_always_lands_on_one_partition(self) -> None:
        assert len({partition_for("g20-summit-af924b63", 4) for _ in range(100)}) == 1

    def test_stable_across_separate_processes(self) -> None:
        """The real requirement: the analyzer and the detector are different
        processes with different hash seeds. `hash()` would silently break this
        and only show up as out-of-order results in production."""
        code = (
            "from pipeline_common.adapters.rabbitmq import partition_for;"
            "print(partition_for('g20-summit-af924b63', 8))"
        )
        runs = {
            subprocess.run(
                [sys.executable, "-c", code],
                capture_output=True,
                text=True,
                check=True,
                env={"PYTHONHASHSEED": seed, "PATH": "", "SYSTEMROOT": ""},
            ).stdout.strip()
            for seed in ("0", "1", "12345")
        }
        assert len(runs) == 1, f"partition changed with hash seed: {runs}"

    def test_partition_is_always_in_range(self) -> None:
        for count in (1, 2, 4, 8, 64):
            for i in range(200):
                assert 0 <= partition_for(f"video-{i}", count) < count


class TestDistribution:
    def test_videos_spread_across_partitions(self) -> None:
        """Uneven spread would mean one detector doing all the work."""
        counts = Counter(partition_for(f"video-{i}", 4) for i in range(400))
        assert len(counts) == 4
        # Nothing should be starved or swamped; SHA-256 is comfortably better
        # than this bound, which is loose on purpose to avoid a flaky test.
        assert min(counts.values()) > 400 / 4 * 0.5
        assert max(counts.values()) < 400 / 4 * 1.5

    def test_single_partition_degenerates_to_one_queue(self) -> None:
        assert all(partition_for(f"v{i}", 1) == 0 for i in range(50))


class TestQueueNaming:
    @pytest.mark.parametrize("index", [0, 1, 7])
    def test_name_includes_the_index(self, index: int) -> None:
        assert partition_queue("frames.work", index) == f"frames.work.{index}"

    def test_distinct_partitions_get_distinct_queues(self) -> None:
        names = {partition_queue("frames.work", i) for i in range(4)}
        assert len(names) == 4
