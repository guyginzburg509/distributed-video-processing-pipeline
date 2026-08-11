"""Job lifecycle for one /analyze request.

Owns the decisions -- is the infrastructure up, is this a fresh job or a resumed
one, what status did it end in -- and delegates the actual frame movement to
``FramePipeline``. Splitting those apart keeps each readable: this module is
about *what a job is*, the pipeline is about *how frames flow*.

Why 200 OK is truthful
----------------------
``analyze()`` returns only after every frame has been *confirmed durable by the
broker* (publisher confirms, awaited inside ``FramePublisher.publish``). If any
confirm fails the caller gets a 502, never a 200. It deliberately does **not**
wait for detection -- that would undo the decoupling the brief asks for.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable, Coroutine
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from pipeline_common.logging import get_logger
from pipeline_common.messages import JobRecord, JobStatus
from pipeline_common.ports import FramePublisher, FrameStore, JobRepository
from pipeline_common.settings import AnalyzerSettings
from video_analyzer.domain.frame_sampler import FrameSampler
from video_analyzer.domain.identity import make_job_id, make_video_id, resume_key
from video_analyzer.domain.video_source import VideoSource, open_video
from video_analyzer.services.errors import (
    DispatchError,
    InfrastructureUnavailableError,
    JobAlreadyRunningError,
    TooManyConcurrentJobsError,
    TooManyFrameFailuresError,
)
from video_analyzer.services.frame_pipeline import FramePipeline, RunState

log = get_logger(__name__)

#: A job left RUNNING with no checkpoint update for this long is presumed to
#: belong to a crashed analyzer, and becomes resumable.
#:
#: This delay is a *safety property*, not laziness: a RUNNING record is
#: ambiguous -- the analyzer may have died, or may still be working (possibly on
#: another replica). Resuming a genuinely live job would double-dispatch every
#: remaining frame. Absent a distributed lock, the heartbeat gap is the guard.
#: A Redis lease (SET NX PX + renewal) would shrink this to seconds.
STALE_RUNNING_JOB_SEC = 120.0

#: Statuses a re-submission may continue rather than restart.
_RESUMABLE = frozenset({JobStatus.FAILED, JobStatus.PARTIAL, JobStatus.CANCELLED})

#: Placeholder held between reserving a (video, rate) slot and knowing the job id.
_RESERVED = ""


@dataclass(frozen=True, slots=True)
class AnalysisOutcome:
    job_id: str
    video_id: str
    source_fps: float
    target_fps: int
    total_source_frames: int
    frames_expected: int
    frames_dispatched: int
    frames_failed: int
    video_duration_sec: float
    elapsed_sec: float
    status: JobStatus
    #: Source index this run picked up after, or None for a fresh run.
    resumed_after_frame: int | None = None
    #: Frames dispatched by *this* run; differs from the cumulative
    #: ``frames_dispatched`` when a checkpoint was resumed.
    frames_dispatched_this_run: int = 0

    @property
    def realtime_factor(self) -> float:
        """Video seconds processed per wall-clock second. >1 beats real time."""
        return self.video_duration_sec / self.elapsed_sec if self.elapsed_sec > 0 else 0.0


class AnalysisService:
    def __init__(
        self,
        settings: AnalyzerSettings,
        *,
        store: FrameStore,
        publisher: FramePublisher,
        jobs: JobRepository,
        source_factory: Callable[[Path], VideoSource] = open_video,
    ) -> None:
        self._settings = settings
        self._store = store
        self._publisher = publisher
        self._jobs = jobs
        self._source_factory = source_factory
        self._pipeline = FramePipeline(
            settings, store=store, publisher=publisher, jobs=jobs
        )
        # Admission control. Each job costs a decode thread plus a publisher
        # pool, so concurrency is bounded rather than letting N requests spawn
        # N threads. Non-blocking: at the ceiling we refuse (429) instead of
        # queueing, because a request already waiting on a held-open HTTP
        # connection should not also wait in line.
        self._slots = asyncio.Semaphore(settings.max_concurrent_jobs)
        #: resume_key -> job_id for jobs in flight in *this* process.
        self._in_flight: dict[str, str] = {}

    async def analyze(
        self,
        path: Path,
        target_fps: int,
        *,
        cancelled: Callable[[], Coroutine[Any, Any, bool]] | None = None,
    ) -> AnalysisOutcome:
        if self._slots.locked():
            raise TooManyConcurrentJobsError(self._settings.max_concurrent_jobs)

        await self._slots.acquire()
        try:
            return await self._analyze(path, target_fps, cancelled=cancelled)
        finally:
            self._slots.release()

    async def _analyze(
        self,
        path: Path,
        target_fps: int,
        *,
        cancelled: Callable[[], Coroutine[Any, Any, bool]] | None,
    ) -> AnalysisOutcome:
        started = time.perf_counter()

        # Preflight: distinguish "our infrastructure is down" (503) from "your
        # request is wrong" (4xx) *before* spending time decoding a video we
        # could never dispatch.
        await self._require_infrastructure()

        source = await asyncio.to_thread(self._source_factory, path)
        try:
            metadata = source.metadata()
            video_id = make_video_id(path)
            rkey = resume_key(video_id, target_fps)

            # Raises InvalidFrameRateError for e.g. 4 fps from a 3 fps source.
            probe = FrameSampler(metadata.source_fps, target_fps)
            expected = probe.expected_frame_count(metadata.total_frames)

            # Two concurrent runs of the same video at the same rate would
            # dispatch every frame twice. A different rate is a different job,
            # and a different video is unaffected -- only the exact duplicate
            # is refused.
            #
            # Check and reserve with NO await between them. These statements run
            # to completion before another coroutine can interleave; put an await
            # in the middle and both requests pass the check before either
            # reserves -- which is exactly the case this guard exists to prevent.
            reserved = self._in_flight.get(rkey)
            if reserved is not None:
                raise JobAlreadyRunningError(reserved or "starting")
            self._in_flight[rkey] = _RESERVED

            try:
                resumed = await self._find_checkpoint(rkey)
                if resumed is not None:
                    job_id = resumed.job_id
                    start_slot = probe.slot_after_source_index(resumed.last_source_index)
                    await self._jobs.set_status(job_id, JobStatus.RUNNING)
                else:
                    job_id = make_job_id()
                    start_slot = 0
                    await self._jobs.create(
                        JobRecord(
                            job_id=job_id,
                            video_id=video_id,
                            resume_key=rkey,
                            status=JobStatus.RUNNING,
                            source_fps=metadata.source_fps,
                            target_fps=target_fps,
                            total_source_frames=metadata.total_frames,
                            frames_expected=expected,
                            started_at=time.time(),
                            updated_at=time.time(),
                        )
                    )
                self._in_flight[rkey] = job_id

                # One code path: start_slot is simply 0 for a fresh run.
                sampler = FrameSampler(metadata.source_fps, target_fps, start_slot=start_slot)

                log.info(
                    "job.started" if resumed is None else "job.resumed",
                    job_id=job_id,
                    video_id=video_id,
                    source_fps=metadata.source_fps,
                    target_fps=target_fps,
                    frames_expected=expected,
                    resumed_after_frame=resumed.last_source_index if resumed else None,
                    already_dispatched=resumed.frames_dispatched if resumed else 0,
                )

                state, aborted = await self._pipeline.run(
                    source,
                    sampler,
                    job_id=job_id,
                    video_id=video_id,
                    cancelled=cancelled,
                    prior=resumed,
                )
                if state.error is not None:
                    await self._record_failure(job_id, state, expected)

                elapsed = time.perf_counter() - started
                status = await self._finalise(job_id, state, aborted)
                outcome = AnalysisOutcome(
                    job_id=job_id,
                    video_id=video_id,
                    source_fps=metadata.source_fps,
                    target_fps=target_fps,
                    total_source_frames=metadata.total_frames,
                    frames_expected=expected,
                    frames_dispatched=state.frames_confirmed,
                    frames_failed=state.frames_failed,
                    video_duration_sec=metadata.duration_sec,
                    elapsed_sec=elapsed,
                    status=status,
                    resumed_after_frame=resumed.last_source_index if resumed else None,
                    frames_dispatched_this_run=state.frames_confirmed_this_run,
                )
                log.info(
                    "job.finished",
                    job_id=job_id,
                    status=status,
                    frames_dispatched=outcome.frames_dispatched,
                    frames_dispatched_this_run=outcome.frames_dispatched_this_run,
                    frames_failed=outcome.frames_failed,
                    elapsed_sec=round(elapsed, 3),
                    realtime_factor=round(outcome.realtime_factor, 2),
                )
                return outcome
            finally:
                self._in_flight.pop(rkey, None)
        finally:
            await asyncio.to_thread(source.close)

    # -- decisions --------------------------------------------------------

    async def _require_infrastructure(self) -> None:
        try:
            broker_ok = await self._publisher.ping()
            store_ok = await self._store.ping()
        except Exception as exc:
            raise InfrastructureUnavailableError(f"dependency check failed: {exc}") from exc

        unavailable = [
            name for name, ok in (("broker", broker_ok), ("frame store", store_ok)) if not ok
        ]
        if unavailable:
            raise InfrastructureUnavailableError(f"unavailable: {', '.join(unavailable)}")

    async def _find_checkpoint(self, rkey: str) -> JobRecord | None:
        """Return a job this request should continue, or None to start fresh.

        Resumable: a run that ended badly (FAILED / PARTIAL / CANCELLED), or one
        still marked RUNNING whose checkpoint has gone stale -- which is what a
        crashed analyzer leaves behind, and the case this exists for.

        Not resumable: a COMPLETED job (re-analysis is a legitimate request and
        gets a fresh id) or a RUNNING job that is still heartbeating.
        """
        previous = await self._jobs.find_by_resume_key(rkey)
        if previous is None or previous.last_source_index < 0:
            return None

        if previous.status in _RESUMABLE:
            return previous
        if (
            previous.status is JobStatus.RUNNING
            and time.time() - previous.updated_at > STALE_RUNNING_JOB_SEC
        ):
            log.warning(
                "job.stale_running_reclaimed",
                job_id=previous.job_id,
                idle_sec=round(time.time() - previous.updated_at, 1),
            )
            return previous
        return None

    # -- outcomes ---------------------------------------------------------

    async def _record_failure(self, job_id: str, state: RunState, expected: int) -> None:
        """Persist progress and mark FAILED *before* propagating.

        Otherwise the job stays RUNNING with a fresh heartbeat, and a retry
        would refuse to resume it -- the checkpoint would be useless exactly
        when it is needed.
        """
        await self._save_checkpoint(job_id, state)
        await self._jobs.set_status(job_id, JobStatus.FAILED, str(state.error))
        raise DispatchError(
            f"broker did not confirm all frames: {state.error}",
            frames_confirmed=state.frames_confirmed,
            frames_expected=expected,
        ) from state.error

    async def _finalise(self, job_id: str, state: RunState, aborted: bool) -> JobStatus:
        await self._save_checkpoint(job_id, state)

        attempted = state.frames_confirmed + state.frames_failed
        ratio = state.frames_failed / attempted if attempted else 0.0
        if ratio > self._settings.max_frame_failure_ratio:
            await self._jobs.set_status(
                job_id, JobStatus.FAILED, f"{state.frames_failed}/{attempted} frames undecodable"
            )
            raise TooManyFrameFailuresError(
                f"{state.frames_failed} of {attempted} frames could not be decoded "
                f"({ratio:.1%} > {self._settings.max_frame_failure_ratio:.1%})"
            )

        if aborted:
            status = JobStatus.CANCELLED
        elif state.frames_failed:
            status = JobStatus.PARTIAL
        else:
            status = JobStatus.COMPLETED
        await self._jobs.set_status(job_id, status)
        return status

    async def _save_checkpoint(self, job_id: str, state: RunState) -> None:
        await self._jobs.checkpoint(
            job_id,
            frames_dispatched=state.frames_confirmed,
            frames_failed=state.frames_failed,
            last_source_index=state.last_source_index,
        )
