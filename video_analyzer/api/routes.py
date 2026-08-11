"""HTTP routes.

Thin by design: parse, delegate, serialise. Every failure path is expressed by
raising a domain exception that ``errors.py`` maps to a status code, so there
are no try/except ladders here.
"""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, Request, status

from pipeline_common.ports import FramePublisher, FrameStore, JobRepository
from pipeline_common.settings import AnalyzerSettings
from video_analyzer.api.schemas import (
    AnalyzeRequest,
    AnalyzeResponse,
    ErrorResponse,
    HealthResponse,
    JobResponse,
)
from video_analyzer.dependencies import (
    get_analysis_service,
    get_jobs,
    get_publisher,
    get_settings,
    get_store,
)
from video_analyzer.domain.paths import resolve_video_path
from video_analyzer.services.analysis_service import AnalysisService

router = APIRouter()

SettingsDep = Annotated[AnalyzerSettings, Depends(get_settings)]
ServiceDep = Annotated[AnalysisService, Depends(get_analysis_service)]
JobsDep = Annotated[JobRepository, Depends(get_jobs)]
StoreDep = Annotated[FrameStore, Depends(get_store)]
PublisherDep = Annotated[FramePublisher, Depends(get_publisher)]

_ANALYZE_ERRORS: dict[int | str, dict[str, object]] = {
    400: {"model": ErrorResponse, "description": "Path malformed or escapes VIDEO_ROOT"},
    404: {"model": ErrorResponse, "description": "No such video"},
    415: {"model": ErrorResponse, "description": "File is not decodable video"},
    422: {"model": ErrorResponse, "description": "Invalid fps, or unusable video metadata"},
    409: {"model": ErrorResponse, "description": "Same video+fps already being analyzed"},
    429: {"model": ErrorResponse, "description": "All concurrent analysis slots busy"},
    502: {"model": ErrorResponse, "description": "Broker did not confirm every frame"},
    503: {"model": ErrorResponse, "description": "Redis or RabbitMQ unavailable"},
}


class JobNotFoundError(KeyError):
    """No job with that id."""


@router.post(
    "/analyze",
    response_model=AnalyzeResponse,
    status_code=status.HTTP_200_OK,
    responses=_ANALYZE_ERRORS,
    summary="Extract frames at 2 or 4 fps and dispatch them for detection",
    description=(
        "Returns 200 **only after every extracted frame has been confirmed durable by "
        "the broker**. The connection is held open for the duration (~2.2 s for the "
        "139 s sample video). It does not wait for detection to finish -- poll "
        "`GET /jobs/{job_id}` for that."
    ),
)
async def analyze(
    payload: AnalyzeRequest,
    request: Request,
    service: ServiceDep,
    settings: SettingsDep,
) -> AnalyzeResponse:
    # Containment happens before anything touches the file (security control).
    path = resolve_video_path(payload.file_path, settings.video_root)

    outcome = await service.analyze(path, payload.fps, cancelled=request.is_disconnected)
    return AnalyzeResponse.from_outcome(outcome)


@router.get(
    "/jobs/{job_id}",
    response_model=JobResponse,
    responses={404: {"model": ErrorResponse}},
    summary="Progress of a job, including detection work done after the 200",
)
async def get_job(job_id: str, jobs: JobsDep) -> JobResponse:
    record = await jobs.get(job_id)
    if record is None:
        raise JobNotFoundError(job_id)
    return JobResponse(
        job_id=record.job_id,
        video_id=record.video_id,
        status=record.status,
        source_fps=record.source_fps,
        target_fps=record.target_fps,
        frames_expected=record.frames_expected,
        frames_dispatched=record.frames_dispatched,
        frames_failed=record.frames_failed,
        frames_processed=record.frames_processed,
        last_source_index=record.last_source_index,
        error=record.error,
    )


@router.get("/health", response_model=HealthResponse, summary="Liveness and dependency probe")
async def health(store: StoreDep, publisher: PublisherDep) -> HealthResponse:
    async def _safe(probe: FrameStore | FramePublisher) -> bool:
        try:
            return await probe.ping()
        except Exception:
            return False

    broker_ok = await _safe(publisher)
    store_ok = await _safe(store)
    return HealthResponse(
        status="ok" if (broker_ok and store_ok) else "degraded",
        broker=broker_ok,
        frame_store=store_ok,
    )
