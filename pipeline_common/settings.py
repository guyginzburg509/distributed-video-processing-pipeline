"""Configuration, validated at startup so misconfiguration fails fast and loudly."""

from __future__ import annotations

from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class CommonSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", extra="ignore", frozen=True
    )

    rabbitmq_url: str = "amqp://guest:guest@rabbitmq:5672/"
    redis_url: str = "redis://redis:6379/0"
    work_queue: str = "frames.work"
    dlq_queue: str = "frames.dlq"
    frame_blob_ttl_sec: int = Field(default=3600, gt=0)

    #: Videos are hashed to partitions and each partition has a single active
    #: consumer, which is what gives ordered delivery per video. This therefore
    #: caps how many videos can be processed in parallel -- raise it alongside
    #: detector replicas. Both services must agree on the value.
    frame_partitions: int = Field(default=4, gt=0, le=64)

    log_level: str = "INFO"
    log_json: bool = True


class AnalyzerSettings(CommonSettings):
    #: Only files resolving inside this directory may be analyzed.
    video_root: Path = Path("/data/videos")
    jpeg_quality: int = Field(default=85, ge=1, le=100)

    #: Bound on the decode -> publish queue. A full queue blocks the decode
    #: thread; that blocking *is* the backpressure mechanism.
    analyzer_queue_size: int = Field(default=256, gt=0)
    analyzer_publishers: int = Field(default=4, gt=0)

    checkpoint_every: int = Field(default=50, gt=0)
    max_frame_failure_ratio: float = Field(default=0.05, ge=0.0, le=1.0)

    #: Concurrent /analyze jobs allowed. Each holds a decode thread and a
    #: publisher pool, so this is a real resource bound, not a formality.
    max_concurrent_jobs: int = Field(default=4, gt=0)


class DetectorSettings(CommonSettings):
    #: Max unacked messages one detector holds -> consumer-side backpressure.
    detector_prefetch: int = Field(default=32, gt=0)

    batch_max_size: int = Field(default=32, gt=0)
    #: A quarter of the 2 s budget, leaving room for decode, detection and the
    #: downstream hop. Batching must never be the reason the SLO is missed.
    batch_max_latency_ms: int = Field(default=500, gt=0)
    detector_executor_workers: int = Field(default=4, gt=0)
    dedup_ttl_sec: int = Field(default=3600, gt=0)

    #: Drop frames older than this instead of processing them.
    #:
    #: In a real-time facial recognition system a late answer has little value,
    #: and processing a stale frame costs capacity that the *current* frame
    #: needs -- so a backlog compounds instead of draining. Shedding restores
    #: freshness. Disabled (0) for archive/batch processing, where completeness
    #: matters more than latency and nothing should ever be skipped.
    max_frame_age_sec: float = Field(default=0.0, ge=0.0)
