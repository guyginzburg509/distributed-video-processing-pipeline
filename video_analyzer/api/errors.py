"""Domain exception -> HTTP status mapping, in one place.

Routes stay free of try/except ladders, and the wire format is uniform:
``{"error": {"code", "message", "details"}}``.

Filesystem paths are never echoed back to the caller: ``/analyze`` takes a path
from an untrusted body, so a verbose error is an information-disclosure channel
(it would confirm which paths exist outside the permitted root).
"""

from __future__ import annotations

from fastapi import FastAPI, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from pipeline_common.logging import get_logger
from video_analyzer.api.routes import JobNotFoundError
from video_analyzer.domain.frame_sampler import InvalidFrameRateError
from video_analyzer.domain.paths import PathValidationError, VideoNotFoundError
from video_analyzer.domain.video_source import VideoMetadataError, VideoOpenError
from video_analyzer.services.errors import (
    DispatchError,
    InfrastructureUnavailableError,
    JobAlreadyRunningError,
    TooManyConcurrentJobsError,
    TooManyFrameFailuresError,
)

log = get_logger(__name__)


def _error(
    status_code: int, code: str, message: str, details: dict[str, object] | None = None
) -> JSONResponse:
    body: dict[str, object] = {"code": code, "message": message}
    if details:
        body["details"] = details
    return JSONResponse(status_code=status_code, content={"error": body})


def register_exception_handlers(app: FastAPI) -> None:
    @app.exception_handler(RequestValidationError)
    async def _validation(_: Request, exc: RequestValidationError) -> JSONResponse:
        return _error(
            status.HTTP_422_UNPROCESSABLE_ENTITY,
            "validation_error",
            "Request body failed validation.",
            {"violations": exc.errors()},
        )

    @app.exception_handler(PathValidationError)
    async def _bad_path(_: Request, exc: Exception) -> JSONResponse:
        return _error(status.HTTP_400_BAD_REQUEST, "invalid_file_path", str(exc))

    @app.exception_handler(VideoNotFoundError)
    async def _not_found(_: Request, exc: Exception) -> JSONResponse:
        return _error(
            status.HTTP_404_NOT_FOUND, "video_not_found", "No such video under the video root."
        )

    @app.exception_handler(JobNotFoundError)
    async def _job_not_found(_: Request, exc: Exception) -> JSONResponse:
        return _error(status.HTTP_404_NOT_FOUND, "job_not_found", "No job with that id.")

    @app.exception_handler(VideoOpenError)
    async def _unsupported(_: Request, exc: Exception) -> JSONResponse:
        return _error(
            status.HTTP_415_UNSUPPORTED_MEDIA_TYPE,
            "undecodable_video",
            "The file exists but could not be decoded as video.",
        )

    @app.exception_handler(VideoMetadataError)
    async def _bad_metadata(_: Request, exc: Exception) -> JSONResponse:
        return _error(status.HTTP_422_UNPROCESSABLE_ENTITY, "unusable_video_metadata", str(exc))

    @app.exception_handler(InvalidFrameRateError)
    async def _bad_rate(_: Request, exc: Exception) -> JSONResponse:
        return _error(status.HTTP_422_UNPROCESSABLE_ENTITY, "invalid_frame_rate", str(exc))

    @app.exception_handler(TooManyFrameFailuresError)
    async def _too_many_failures(_: Request, exc: Exception) -> JSONResponse:
        return _error(status.HTTP_422_UNPROCESSABLE_ENTITY, "video_mostly_undecodable", str(exc))

    @app.exception_handler(InfrastructureUnavailableError)
    async def _unavailable(_: Request, exc: Exception) -> JSONResponse:
        response = _error(
            status.HTTP_503_SERVICE_UNAVAILABLE,
            "infrastructure_unavailable",
            str(exc),
        )
        response.headers["Retry-After"] = "5"
        return response

    @app.exception_handler(JobAlreadyRunningError)
    async def _already_running(_: Request, exc: Exception) -> JSONResponse:
        assert isinstance(exc, JobAlreadyRunningError)
        return _error(
            status.HTTP_409_CONFLICT,
            "job_already_running",
            "This video is already being analyzed at this frame rate.",
            {"job_id": exc.job_id},
        )

    @app.exception_handler(TooManyConcurrentJobsError)
    async def _too_busy(_: Request, exc: Exception) -> JSONResponse:
        assert isinstance(exc, TooManyConcurrentJobsError)
        response = _error(
            status.HTTP_429_TOO_MANY_REQUESTS,
            "too_many_concurrent_jobs",
            str(exc),
            {"limit": exc.limit},
        )
        response.headers["Retry-After"] = "10"
        return response

    @app.exception_handler(DispatchError)
    async def _dispatch_failed(_: Request, exc: Exception) -> JSONResponse:
        assert isinstance(exc, DispatchError)
        # 502, never 200: the broker did not confirm every frame, so claiming
        # success would be a lie.
        return _error(
            status.HTTP_502_BAD_GATEWAY,
            "dispatch_incomplete",
            "The broker did not confirm every frame; the job is incomplete.",
            {
                "frames_confirmed": exc.frames_confirmed,
                "frames_expected": exc.frames_expected,
            },
        )

    @app.exception_handler(Exception)
    async def _unexpected(request: Request, exc: Exception) -> JSONResponse:
        log.exception("request.unhandled_error", path=str(request.url.path))
        # Stack traces go to the log, never to the client.
        return _error(
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            "internal_error",
            "An unexpected error occurred.",
        )
