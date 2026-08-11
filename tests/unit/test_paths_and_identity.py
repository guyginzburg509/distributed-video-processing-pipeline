"""Path containment is a security control, so it gets adversarial tests."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

from video_analyzer.domain.identity import make_video_id, slugify
from video_analyzer.domain.paths import (
    PathValidationError,
    VideoNotFoundError,
    resolve_video_path,
)


@pytest.fixture
def root(tmp_path: Path) -> Path:
    videos = tmp_path / "videos"
    videos.mkdir()
    (videos / "clip.mp4").write_bytes(b"data")
    (tmp_path / "secret.txt").write_bytes(b"top secret")
    return videos


class TestContainment:
    def test_accepts_plain_filename(self, root: Path) -> None:
        assert resolve_video_path("clip.mp4", root) == (root / "clip.mp4").resolve()

    def test_accepts_absolute_path_inside_root(self, root: Path) -> None:
        assert resolve_video_path(str(root / "clip.mp4"), root).name == "clip.mp4"

    @pytest.mark.parametrize(
        "attack",
        [
            "../secret.txt",
            "../../etc/passwd",
            "subdir/../../secret.txt",
            "./../secret.txt",
            "....//secret.txt",
        ],
    )
    def test_rejects_traversal(self, root: Path, attack: str) -> None:
        with pytest.raises((PathValidationError, VideoNotFoundError)):
            resolve_video_path(attack, root)

    def test_rejects_absolute_path_outside_root(self, root: Path) -> None:
        with pytest.raises(PathValidationError, match="outside the permitted"):
            resolve_video_path(str(root.parent / "secret.txt"), root)

    @pytest.mark.skipif(sys.platform == "win32", reason="symlinks need privileges on Windows")
    def test_rejects_symlink_escaping_root(self, root: Path) -> None:
        """A raw-string check would pass this; only post-resolution containment
        catches it."""
        (root / "escape.mp4").symlink_to(root.parent / "secret.txt")
        with pytest.raises(PathValidationError, match="outside the permitted"):
            resolve_video_path("escape.mp4", root)

    def test_error_never_leaks_the_resolved_path(self, root: Path) -> None:
        with pytest.raises(PathValidationError) as excinfo:
            resolve_video_path(str(root.parent / "secret.txt"), root)
        assert "secret.txt" not in str(excinfo.value)


class TestMalformedInput:
    @pytest.mark.parametrize("bad", ["", "   ", "\t\n"])
    def test_rejects_blank(self, root: Path, bad: str) -> None:
        with pytest.raises(PathValidationError, match="empty"):
            resolve_video_path(bad, root)

    def test_rejects_null_byte(self, root: Path) -> None:
        with pytest.raises(PathValidationError, match="null byte"):
            resolve_video_path("clip\x00.mp4", root)

    def test_rejects_absurdly_long_path(self, root: Path) -> None:
        with pytest.raises(PathValidationError, match="exceeds"):
            resolve_video_path("a" * 5000, root)


class TestExistence:
    def test_missing_file_raises_not_found(self, root: Path) -> None:
        with pytest.raises(VideoNotFoundError):
            resolve_video_path("nope.mp4", root)

    def test_directory_is_rejected(self, root: Path) -> None:
        (root / "adir").mkdir()
        with pytest.raises(PathValidationError, match="regular file"):
            resolve_video_path("adir", root)


class TestVideoId:
    def test_readable_slug_prefix(self, tmp_path: Path) -> None:
        assert make_video_id(tmp_path / "G20_Summit.mp4").startswith("g20-summit-")

    def test_distinguishes_names_that_slugify_identically(self, tmp_path: Path) -> None:
        """"a b.mp4" and "a-b.mp4" share a slug; without the digest suffix their
        results would be interleaved under one video_id."""
        assert make_video_id(tmp_path / "a b.mp4") != make_video_id(tmp_path / "a-b.mp4")

    def test_same_path_is_stable(self, tmp_path: Path) -> None:
        assert make_video_id(tmp_path / "x.mp4") == make_video_id(tmp_path / "x.mp4")

    def test_different_directories_differ(self) -> None:
        assert make_video_id(Path("/a/clip.mp4")) != make_video_id(Path("/b/clip.mp4"))

    @pytest.mark.parametrize(
        ("raw", "expected"),
        [
            ("G20 Summit", "g20-summit"),
            ("Ünïcodé Vidéo", "unicode-video"),
            ("__weird--name__", "weird-name"),
            ("!!!", ""),
        ],
    )
    def test_slugify(self, raw: str, expected: str) -> None:
        assert slugify(raw) == expected

    def test_unnameable_file_still_gets_an_id(self, tmp_path: Path) -> None:
        assert make_video_id(tmp_path / "!!!.mp4").startswith("video-")
