"""Docker HEALTHCHECK for the detector.

The detector is a queue consumer, not a web service, so it has no HTTP endpoint
to probe. This script verifies it can still reach both dependencies and exits
0 (healthy) or 1 (unhealthy).
"""

from __future__ import annotations

import asyncio
import sys

from pipeline_common.adapters.rabbitmq import RabbitMQFramePublisher
from pipeline_common.adapters.redis_store import RedisFrameStore
from pipeline_common.settings import DetectorSettings


async def _probe() -> bool:
    settings = DetectorSettings()
    store = RedisFrameStore(settings.redis_url)
    broker = RabbitMQFramePublisher(
        settings.rabbitmq_url,
        queue_name=settings.work_queue,
        dlq_name=settings.dlq_queue,
        partitions=settings.frame_partitions,
    )
    try:
        return await store.ping() and await broker.ping()
    finally:
        await store.close()
        await broker.close()


def main() -> int:
    try:
        return 0 if asyncio.run(asyncio.wait_for(_probe(), timeout=5)) else 1
    except Exception as exc:
        print(f"healthcheck failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
