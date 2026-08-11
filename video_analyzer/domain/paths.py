"""Resolving and containing the caller-supplied ``file_path``.

This is a **security control**, not merely input validation: ``/analyze`` takes
a filesystem path from an untrusted request body, so without containment the
endpoint is an arbitrary-file-read primitive (`{"file_path": "../../../etc/passwd"}`).

Containment is enforced *after* full resolution, so ``..`` traversal and
symlinks that point outside the root are both caught -- a check on the raw
string would miss the symlink case entirely.
"""

from __future__ import annotations

from pathlib import Path

#: Defence in depth against absurd inputs before touching the filesystem.
MAX_PATH_LENGTH = 4096


class PathValidationError(ValueError):
    """The supplied path is malformed or escapes the permitted root."""


class VideoNotFoundError(FileNotFoundError):
    """The path is legal but nothing is there."""


def resolve_video_path(raw_path: str, video_root: Path) -> Path:
    """Resolve ``raw_path`` and prove it lies inside ``video_root``.

    Relative paths are interpreted against the root, which is what makes
    ``{"file_path": "G20_Summit.mp4"}`` the natural request.

    Raises:
        PathValidationError: malformed, or resolves outside the root.
        VideoNotFoundError: resolves inside the root but does not exist.
    """
    if not raw_path or not raw_path.strip():
        raise PathValidationError("file_path must not be empty")
    if "\x00" in raw_path:
        # Path() would raise ValueError deeper in; reject explicitly.
        raise PathValidationError("file_path must not contain null bytes")
    if len(raw_path) > MAX_PATH_LENGTH:
        raise PathValidationError(f"file_path exceeds {MAX_PATH_LENGTH} characters")

    try:
        root = video_root.resolve()
        candidate = Path(raw_path)
        resolved = (candidate if candidate.is_absolute() else root / candidate).resolve()
    except (OSError, ValueError, RuntimeError) as exc:
        # RuntimeError covers symlink loops on some platforms.
        raise PathValidationError("file_path could not be resolved") from exc

    if not resolved.is_relative_to(root):
        # Deliberately vague: never echo a filesystem path back to the caller.
        raise PathValidationError("file_path resolves outside the permitted video directory")

    if not resolved.exists():
        raise VideoNotFoundError(str(candidate))
    if not resolved.is_file():
        raise PathValidationError("file_path does not point at a regular file")

    return resolved
