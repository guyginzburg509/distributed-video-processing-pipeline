"""Identifiers for videos, jobs and blobs.

The brief allows using the filename as the video id "for simplicity". A bare
filename slug is not quite safe though: ``"a b.mp4"`` and ``"a-b.mp4"`` slugify
to the same string, which would silently interleave results from two different
videos under one id. A short path digest suffix costs nothing and removes the
whole class of bug.
"""

from __future__ import annotations

import hashlib
import re
import unicodedata
import uuid
from pathlib import Path

_NON_SLUG = re.compile(r"[^a-z0-9]+")
_DIGEST_CHARS = 8


def slugify(text: str) -> str:
    """Lowercase ASCII slug. Unicode is normalised, not discarded blindly."""
    normalised = unicodedata.normalize("NFKD", text)
    ascii_only = normalised.encode("ascii", "ignore").decode("ascii")
    return _NON_SLUG.sub("-", ascii_only.lower()).strip("-")


def make_video_id(path: Path) -> str:
    """Stable id for a video file: readable slug + collision-proof suffix."""
    stem = slugify(path.stem) or "video"
    digest = hashlib.sha256(str(path).encode("utf-8")).hexdigest()[:_DIGEST_CHARS]
    return f"{stem}-{digest}"


def make_job_id() -> str:
    """Fresh id for one /analyze invocation."""
    return uuid.uuid4().hex


def resume_key(video_id: str, target_fps: int) -> str:
    """Deterministic key identifying "this video at this rate", so a retry can
    find the checkpoint left by a previous crashed run."""
    return hashlib.sha256(f"{video_id}|{target_fps}".encode()).hexdigest()[:16]


def blob_key(job_id: str, frame_id: int) -> str:
    """Frame blobs are namespaced by job, so concurrent runs never collide."""
    return f"frame:{job_id}:{frame_id}"


def dedup_key(job_id: str, frame_id: int) -> str:
    return f"processed:{job_id}:{frame_id}"
