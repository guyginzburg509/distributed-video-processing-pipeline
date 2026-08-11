"""RabbitMQ adapters.

Two acknowledgement mechanisms live in this file, and they are unrelated
despite AMQP using the same ``basic.ack`` frame for both:

* **Publisher confirms** (broker -> analyzer). Enabled with
  ``publisher_confirms=True``; ``exchange.publish()`` then only returns once the
  broker has durably accepted the message. This is what makes the 200 truthful.
* **Consumer acks** (detector -> broker). Manual mode; until the detector acks,
  the broker keeps the message and redelivers it if the consumer dies. This is
  what makes the system crash-tolerant.

Topology: one durable work queue with a dead-letter route, so poison frames end
up in ``frames.dlq`` instead of looping forever.
"""

from __future__ import annotations

import asyncio
import hashlib
from collections.abc import AsyncIterator

import aio_pika
from aio_pika.abc import (
    AbstractChannel,
    AbstractIncomingMessage,
    AbstractQueue,
    AbstractRobustConnection,
)

from pipeline_common.logging import get_logger
from pipeline_common.messages import FrameRef
from pipeline_common.ports import PublishError

log = get_logger(__name__)


#: The DLQ holds ~200 byte references, not frame bytes, so it is cheap -- but it
#: must still be bounded. The failure that matters is not five dead frames, it is
#: a *stampede*: if blob TTL is ever short against a deep backlog, every frame
#: dead-letters at once. These caps make that self-limiting instead of unbounded.
DLQ_MESSAGE_TTL_MS = 24 * 60 * 60 * 1000  # evidence older than a day is noise
DLQ_MAX_LENGTH = 10_000


def partition_for(video_id: str, partitions: int) -> int:
    """Stable partition for a video.

    ``hash()`` is salted per process in Python, so it cannot be used here: two
    services must agree on the partition for the same video id, forever.
    """
    digest = hashlib.sha256(video_id.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big") % partitions


def partition_queue(base_name: str, partition: int) -> str:
    return f"{base_name}.{partition}"


async def declare_topology(
    channel: AbstractChannel, *, queue_name: str, dlq_name: str, partitions: int
) -> list[AbstractQueue]:
    """Declare the DLQ, then one work queue per partition.

    **Per-video ordering** is the reason partitions exist. A single queue with
    competing consumers delivers each message to exactly one consumer, but says
    nothing about the order in which they *finish* -- so results for one video
    interleave. Hashing ``video_id`` to a fixed partition, and allowing only one
    active consumer per partition, restores order within a video while still
    running different videos in parallel.

    ``x-single-active-consumer`` is what makes that safe without client-side
    coordination: every detector subscribes to every partition, RabbitMQ
    activates exactly one of them per queue, and promotes another automatically
    if the active one dies. Ordering and failover, with no leader election of
    our own.

    Both services declare identically, so whichever starts first wins and the
    other is a no-op -- no ordering dependency in compose.
    """
    await channel.declare_queue(
        dlq_name,
        durable=True,
        arguments={
            "x-message-ttl": DLQ_MESSAGE_TTL_MS,
            "x-max-length": DLQ_MAX_LENGTH,
            # drop-head keeps the *newest* failures: during a stampede the recent
            # ones describe the ongoing incident, the oldest are just history.
            "x-overflow": "drop-head",
        },
    )
    return [
        await channel.declare_queue(
            partition_queue(queue_name, index),
            durable=True,
            arguments={
                "x-dead-letter-exchange": "",
                "x-dead-letter-routing-key": dlq_name,
                "x-single-active-consumer": True,
            },
        )
        for index in range(partitions)
    ]


class RabbitMQFramePublisher:
    """Publishes frame references and waits for the broker's confirm.

    Routing is by ``video_id`` partition, so every frame of one video lands on
    one queue in dispatch order -- the precondition for ordered delivery.
    """

    def __init__(self, url: str, *, queue_name: str, dlq_name: str, partitions: int = 4) -> None:
        self._url = url
        self._queue_name = queue_name
        self._dlq_name = dlq_name
        self._partitions = partitions
        self._connection: AbstractRobustConnection | None = None
        self._channel: AbstractChannel | None = None

    async def _ensure_channel(self) -> AbstractChannel:
        if self._channel is not None and not self._channel.is_closed:
            return self._channel
        self._connection = await aio_pika.connect_robust(self._url)
        # publisher_confirms=True is the whole point: publish() awaits the ack.
        channel = await self._connection.channel(publisher_confirms=True)
        await declare_topology(
            channel,
            queue_name=self._queue_name,
            dlq_name=self._dlq_name,
            partitions=self._partitions,
        )
        self._channel = channel
        return channel

    async def publish(self, ref: FrameRef) -> None:
        try:
            channel = await self._ensure_channel()
            routing_key = partition_queue(
                self._queue_name, partition_for(ref.video_id, self._partitions)
            )
            await channel.default_exchange.publish(
                aio_pika.Message(
                    body=ref.to_bytes(),
                    content_type="application/json",
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                    message_id=f"{ref.job_id}:{ref.frame_id}",
                ),
                routing_key=routing_key,
            )
        except Exception as exc:
            raise PublishError(
                f"broker did not confirm frame {ref.frame_id} of job {ref.job_id}: {exc}"
            ) from exc

    async def ping(self) -> bool:
        try:
            channel = await self._ensure_channel()
            return not channel.is_closed
        except Exception:
            return False

    async def close(self) -> None:
        if self._connection is not None and not self._connection.is_closed:
            await self._connection.close()
        self._connection = None
        self._channel = None


class RabbitMQDelivery:
    """Adapts an aio-pika message to the ``Delivery`` port."""

    __slots__ = ("_message", "_ref")

    def __init__(self, message: AbstractIncomingMessage, ref: FrameRef) -> None:
        self._message = message
        self._ref = ref

    @property
    def ref(self) -> FrameRef:
        return self._ref

    @property
    def tag(self) -> int:
        return self._message.delivery_tag or 0

    @property
    def redelivered(self) -> bool:
        return bool(self._message.redelivered)

    async def ack(self, *, multiple: bool = False) -> None:
        await self._message.ack(multiple=multiple)

    async def nack(self, *, requeue: bool) -> None:
        # requeue=False routes to the dead-letter queue via the queue policy.
        await self._message.nack(requeue=requeue)


class RabbitMQFrameConsumer:
    """Consumes frame references from every partition, with bounded prefetch.

    ``prefetch_count`` caps how many unacked messages this consumer holds. That
    is the consumer-side backpressure: a slow detector simply stops being handed
    work rather than accumulating an unbounded backlog in memory.

    Every replica subscribes to *every* partition, but each queue is declared
    ``x-single-active-consumer``, so RabbitMQ activates only one of them per
    partition. A replica is therefore active on some partitions and a warm
    standby on the rest -- and when an active consumer dies, the broker promotes
    a standby with no coordination on our side.
    """

    def __init__(
        self, url: str, *, queue_name: str, dlq_name: str, prefetch: int, partitions: int = 4
    ) -> None:
        self._url = url
        self._queue_name = queue_name
        self._dlq_name = dlq_name
        self._prefetch = prefetch
        self._partitions = partitions
        self._connection: AbstractRobustConnection | None = None

    async def deliveries(self) -> AsyncIterator[RabbitMQDelivery]:
        self._connection = await aio_pika.connect_robust(self._url)
        channel = await self._connection.channel()
        await channel.set_qos(prefetch_count=self._prefetch)
        queues = await declare_topology(
            channel,
            queue_name=self._queue_name,
            dlq_name=self._dlq_name,
            partitions=self._partitions,
        )

        log.info(
            "consumer.started",
            queue=self._queue_name,
            partitions=self._partitions,
            prefetch=self._prefetch,
        )

        # One inbox fed by every partition. Ordering lives *within* a partition,
        # which is what per-video ordering needs; interleaving across partitions
        # is by design, since those are different videos.
        inbox: asyncio.Queue[RabbitMQDelivery | None] = asyncio.Queue(maxsize=self._prefetch)
        pumps = [
            asyncio.create_task(self._pump(queue, inbox), name=f"partition-{i}")
            for i, queue in enumerate(queues)
        ]
        try:
            while True:
                delivery = await inbox.get()
                if delivery is None:
                    return
                yield delivery
        finally:
            for pump in pumps:
                pump.cancel()
            await asyncio.gather(*pumps, return_exceptions=True)

    async def _pump(
        self, queue: AbstractQueue, inbox: asyncio.Queue[RabbitMQDelivery | None]
    ) -> None:
        """Forward one partition's messages into the shared inbox.

        Blocks on a full inbox, which is what keeps prefetch meaningful: a
        detector that is behind stops draining partitions rather than buffering.
        """
        async with queue.iterator() as messages:
            async for message in messages:
                try:
                    ref = FrameRef.from_bytes(message.body)
                except Exception:
                    log.error("message.malformed", delivery_tag=message.delivery_tag)
                    await message.nack(requeue=False)
                    continue
                await inbox.put(RabbitMQDelivery(message, ref))

    async def close(self) -> None:
        if self._connection is not None and not self._connection.is_closed:
            await self._connection.close()
        self._connection = None
