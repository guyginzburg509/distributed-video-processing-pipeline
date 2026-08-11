"""Shared fixtures.

Tests never touch ``videos/G20_Summit.mp4`` (129 MB): they synthesise small
videos at whatever frame rate the case needs, which is both faster and lets us
cover rates the sample file does not have.
"""

from __future__ import annotations

import os
from pathlib import Path

import cv2
import numpy as np
import pytest

DEFAULT_REDIS_URL = os.environ.get("TEST_REDIS_URL", "redis://localhost:6379/15")
DEFAULT_RABBIT_URL = os.environ.get("TEST_RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")


def make_video(
    path: Path, *, fps: float, frames: int, size: tuple[int, int] = (160, 120)
) -> Path:
    """Write a small synthetic video. Each frame is visually distinct so a test
    can tell *which* frames were sampled, not merely how many."""
    width, height = size
    fourcc = cv2.VideoWriter_fourcc(*"mp4v")  # type: ignore[attr-defined]
    writer = cv2.VideoWriter(str(path), fourcc, fps, (width, height))
    if not writer.isOpened():  # pragma: no cover
        pytest.skip("no mp4v encoder available in this OpenCV build")
    try:
        for i in range(frames):
            frame = np.full((height, width, 3), i % 256, dtype=np.uint8)
            cv2.putText(
                frame, str(i), (5, height // 2), cv2.FONT_HERSHEY_SIMPLEX, 1, (255, 0, 0), 2
            )
            writer.write(frame)
    finally:
        writer.release()
    return path


@pytest.fixture
def video_root(tmp_path: Path) -> Path:
    root = tmp_path / "videos"
    root.mkdir()
    return root


@pytest.fixture
def sample_video(video_root: Path) -> Path:
    """25 fps / 100 frames -- the same awkward rate as the shipped asset, so
    both 2 fps (12.5) and 4 fps (6.25) are non-integer here too."""
    return make_video(video_root / "sample.mp4", fps=25.0, frames=100)


@pytest.fixture
def jpeg_bytes() -> bytes:
    frame = np.full((32, 32, 3), 128, dtype=np.uint8)
    ok, buffer = cv2.imencode(".jpg", frame)
    assert ok
    return bytes(buffer.tobytes())


def redis_available(url: str = DEFAULT_REDIS_URL) -> bool:
    try:
        import redis

        client = redis.from_url(url, socket_connect_timeout=1)  # type: ignore[no-untyped-call]
        client.ping()
        client.close()
        return True
    except Exception:
        return False


def rabbitmq_available(url: str = DEFAULT_RABBIT_URL) -> bool:
    import socket
    from urllib.parse import urlparse

    parsed = urlparse(url)
    try:
        with socket.create_connection((parsed.hostname or "localhost", parsed.port or 5672), 1):
            return True
    except OSError:
        return False


@pytest.fixture(scope="session")
def has_redis() -> bool:
    return redis_available()


@pytest.fixture(scope="session")
def has_rabbitmq() -> bool:
    return rabbitmq_available()
