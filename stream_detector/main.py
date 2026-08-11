"""StreamDetector entrypoint and composition root.

Not a web server: it is a queue consumer. Liveness is probed by
``healthcheck.py`` rather than an HTTP endpoint, which keeps the image free of
a web framework it would otherwise never use.
"""

from __future__ import annotations

import asyncio
import contextlib
import signal
from concurrent.futures import ThreadPoolExecutor

from pipeline_common.adapters.rabbitmq import RabbitMQFrameConsumer
from pipeline_common.adapters.redis_store import (
    RedisDedupGuard,
    RedisFrameStore,
    RedisJobRepository,
)
from pipeline_common.logging import configure_logging, get_logger
from pipeline_common.settings import DetectorSettings
from stream_detector.batching import ResultBatcher
from stream_detector.consumer import DetectorWorker
from stream_detector.detector import StreamFaceDetector
from stream_detector.detector_response_handling import send_results_next_service
from stream_detector.processing import FrameProcessor

log = get_logger(__name__)


async def run(settings: DetectorSettings, stop: asyncio.Event) -> None:
    store = RedisFrameStore(settings.redis_url)
    dedup = RedisDedupGuard(settings.redis_url)
    jobs = RedisJobRepository(settings.redis_url)
    consumer = RabbitMQFrameConsumer(
        settings.rabbitmq_url,
        queue_name=settings.work_queue,
        dlq_name=settings.dlq_queue,
        prefetch=settings.detector_prefetch,
        partitions=settings.frame_partitions,
    )
    executor = ThreadPoolExecutor(
        max_workers=settings.detector_executor_workers, thread_name_prefix="detect"
    )

    batcher = ResultBatcher(
        max_size=settings.batch_max_size,
        max_latency_ms=settings.batch_max_latency_ms,
        # The provided placeholder stands in for the next service in the pipeline.
        sink=send_results_next_service,
    )
    worker = DetectorWorker(
        consumer=consumer,
        processor=FrameProcessor(
            detector=StreamFaceDetector(),  # type: ignore[no-untyped-call]
            store=store,
            dedup=dedup,
            executor=executor,
            dedup_ttl_sec=settings.dedup_ttl_sec,
            max_frame_age_sec=settings.max_frame_age_sec,
        ),
        batcher=batcher,
        jobs=jobs,
    )

    log.info(
        "detector.started",
        prefetch=settings.detector_prefetch,
        partitions=settings.frame_partitions,
        batch_max_size=settings.batch_max_size,
        batch_max_latency_ms=settings.batch_max_latency_ms,
        max_frame_age_sec=settings.max_frame_age_sec or "disabled",
    )

    runner = asyncio.create_task(worker.run(), name="detector-worker")
    stopper = asyncio.create_task(stop.wait(), name="shutdown-signal")
    try:
        await asyncio.wait({runner, stopper}, return_when=asyncio.FIRST_COMPLETED)
    finally:
        # Order matters: stop pulling new work, then let the worker's own
        # `finally` flush the batcher, then tear down connections.
        await consumer.close()
        runner.cancel()
        stopper.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await runner
        executor.shutdown(wait=True, cancel_futures=False)
        for resource in (store, dedup, jobs):
            with contextlib.suppress(Exception):
                await resource.close()
        log.info(
            "detector.stopped",
            processed=worker.processed,
            duplicates=worker.duplicates,
            dead_lettered=worker.dead_lettered,
            stale_shed=worker.stale,
            max_age_seen_sec=round(worker.max_age_seen_sec, 3),
            batches_flushed=batcher.batches_flushed,
        )


async def main() -> None:
    settings = DetectorSettings()
    configure_logging(level=settings.log_level, json_output=settings.log_json)

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        with contextlib.suppress(NotImplementedError):  # not available on Windows
            loop.add_signal_handler(sig, stop.set)

    await run(settings, stop)


if __name__ == "__main__":  # pragma: no cover
    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(main())
