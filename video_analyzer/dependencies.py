"""FastAPI dependency accessors.

Everything is constructed once in the lifespan (the composition root in
``main.py``) and merely *read* here. Routes therefore depend on the port types,
never on Redis or aio-pika.
"""

from __future__ import annotations

from fastapi import Request

from pipeline_common.ports import FramePublisher, FrameStore, JobRepository
from pipeline_common.settings import AnalyzerSettings
from video_analyzer.services.analysis_service import AnalysisService


def get_settings(request: Request) -> AnalyzerSettings:
    settings: AnalyzerSettings = request.app.state.settings
    return settings


def get_store(request: Request) -> FrameStore:
    store: FrameStore = request.app.state.store
    return store


def get_publisher(request: Request) -> FramePublisher:
    publisher: FramePublisher = request.app.state.publisher
    return publisher


def get_jobs(request: Request) -> JobRepository:
    jobs: JobRepository = request.app.state.jobs
    return jobs


def get_analysis_service(request: Request) -> AnalysisService:
    service: AnalysisService = request.app.state.analysis_service
    return service
