"""Service-level failures.

In their own module so ``analysis_service`` and ``frame_pipeline`` can both
raise them without importing each other. ``api/errors.py`` maps each to a status
code -- these types are the entire vocabulary of that mapping.
"""

from __future__ import annotations


class DispatchError(RuntimeError):
    """The broker did not confirm every frame.

    Carries the counts so the caller can report exactly how far it got, rather
    than an opaque failure. Maps to 502 -- never a 200, which would claim a
    delivery guarantee we did not obtain.
    """

    def __init__(self, message: str, *, frames_confirmed: int, frames_expected: int) -> None:
        super().__init__(message)
        self.frames_confirmed = frames_confirmed
        self.frames_expected = frames_expected


class TooManyFrameFailuresError(RuntimeError):
    """Undecodable frames exceeded the configured tolerance.

    One bad frame in a long video should not fail the job; a mostly-corrupt
    file should. The ratio is the dividing line.
    """


class InfrastructureUnavailableError(RuntimeError):
    """Redis or RabbitMQ is unreachable -- our fault, not the caller's (503)."""


class JobAlreadyRunningError(RuntimeError):
    """This exact (video, rate) is already being analyzed.

    Running it twice concurrently would dispatch every frame twice, so the
    duplicate request is refused (409) rather than silently doubling the work.
    Re-analysing after the first finishes is fine, and a *different* rate is a
    different job.
    """

    def __init__(self, job_id: str) -> None:
        super().__init__(f"job {job_id} is already analyzing this video at this rate")
        self.job_id = job_id


class TooManyConcurrentJobsError(RuntimeError):
    """At the concurrent-job ceiling.

    Each job holds a decode thread and a publisher pool, so admission is capped
    rather than letting load collapse the service. 429 with Retry-After: the
    request is valid, just not right now.
    """

    def __init__(self, limit: int) -> None:
        super().__init__(f"all {limit} analysis slots are busy")
        self.limit = limit
