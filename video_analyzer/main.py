"""VideoAnalyzer entrypoint and composition root.

This is the *only* module in the service that knows Redis and RabbitMQ exist.
Everything below it depends on the ports in ``pipeline_common.ports``, which is
what lets the whole test suite run with no infrastructure at all.
"""

from __future__ import annotations

import contextlib
from collections.abc import AsyncIterator

import uvicorn
from fastapi import FastAPI

from pipeline_common.adapters.rabbitmq import RabbitMQFramePublisher
from pipeline_common.adapters.redis_store import RedisFrameStore, RedisJobRepository
from pipeline_common.logging import configure_logging, get_logger
from pipeline_common.ports import FramePublisher, FrameStore, JobRepository
from pipeline_common.settings import AnalyzerSettings
from video_analyzer.api.errors import register_exception_handlers
from video_analyzer.api.routes import router
from video_analyzer.services.analysis_service import AnalysisService

log = get_logger(__name__)


def create_app(
    settings: AnalyzerSettings | None = None,
    *,
    store: FrameStore | None = None,
    publisher: FramePublisher | None = None,
    jobs: JobRepository | None = None,
) -> FastAPI:
    """Build the app.

    The optional adapter arguments are the seam the integration tests use to
    inject in-memory doubles; production passes none and gets the real ones.
    """
    settings = settings or AnalyzerSettings()
    injected = store is not None and publisher is not None and jobs is not None

    @contextlib.asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        configure_logging(level=settings.log_level, json_output=settings.log_json)

        app.state.settings = settings
        # `is not None`, never `or`: an injected adapter may legitimately be
        # falsy (an empty InMemoryFrameStore has __len__ == 0), and `or` would
        # silently swap it for a real Redis client.
        app.state.store = store if store is not None else RedisFrameStore(settings.redis_url)
        app.state.publisher = (
            publisher
            if publisher is not None
            else RabbitMQFramePublisher(
                settings.rabbitmq_url,
                queue_name=settings.work_queue,
                dlq_name=settings.dlq_queue,
                partitions=settings.frame_partitions,
            )
        )
        app.state.jobs = jobs if jobs is not None else RedisJobRepository(settings.redis_url)
        app.state.analysis_service = AnalysisService(
            settings,
            store=app.state.store,
            publisher=app.state.publisher,
            jobs=app.state.jobs,
        )

        log.info(
            "analyzer.started",
            video_root=str(settings.video_root),
            publishers=settings.analyzer_publishers,
            queue_size=settings.analyzer_queue_size,
            partitions=settings.frame_partitions,
            max_concurrent_jobs=settings.max_concurrent_jobs,
        )
        try:
            yield
        finally:
            # Only close what we created; injected doubles belong to the test.
            if not injected:
                for resource in (app.state.publisher, app.state.store, app.state.jobs):
                    with contextlib.suppress(Exception):
                        await resource.close()
            log.info("analyzer.stopped")

    app = FastAPI(
        title="VideoAnalyzer",
        version="1.0.0",
        summary="Extracts frames at a requested rate and dispatches them for face detection.",
        lifespan=lifespan,
    )
    app.include_router(router)
    register_exception_handlers(app)
    return app


app = create_app()


if __name__ == "__main__":  # pragma: no cover
    settings = AnalyzerSettings()
    uvicorn.run(
        "video_analyzer.main:app",
        host="0.0.0.0",
        port=8000,
        log_config=None,  # structlog owns logging
    )
