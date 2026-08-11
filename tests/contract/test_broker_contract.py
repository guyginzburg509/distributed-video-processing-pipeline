"""One suite, every publisher/consumer pair.

The behaviours pinned here are precisely the ones a naive double gets wrong:
redelivery after nack, dead-lettering, and the fact that an unacked message is
still owned by the broker.
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import AsyncIterator
from dataclasses import dataclass

import pytest

from pipeline_common.adapters.memory import (
    InMemoryBroker,
    InMemoryFrameConsumer,
    InMemoryFramePublisher,
)
from pipeline_common.adapters.rabbitmq import RabbitMQFrameConsumer, RabbitMQFramePublisher
from pipeline_common.messages import FrameRef
from pipeline_common.ports import Delivery, FrameConsumer, FramePublisher
from tests.conftest import DEFAULT_RABBIT_URL, rabbitmq_available

CONSUME_TIMEOUT = 10.0


@dataclass
class BrokerPair:
    publisher: FramePublisher
    consumer: FrameConsumer
    _iterator: AsyncIterator[Delivery] | None = None

    async def next_delivery(self, timeout: float = CONSUME_TIMEOUT) -> Delivery:
        if self._iterator is None:
            self._iterator = self.consumer.deliveries().__aiter__()
        return await asyncio.wait_for(self._iterator.__anext__(), timeout=timeout)

    async def expect_nothing(self, within: float = 0.7) -> None:
        with pytest.raises(asyncio.TimeoutError):
            await self.next_delivery(timeout=within)


def make_ref(frame_id: int = 0, job_id: str = "job1") -> FrameRef:
    return FrameRef(
        job_id=job_id,
        video_id="vid1",
        frame_id=frame_id,
        timestamp_sec=frame_id / 2,
        blob_key=f"frame:{job_id}:{frame_id}",
    )


@pytest.fixture(params=["memory", "rabbitmq"])
async def pair(request: pytest.FixtureRequest) -> AsyncIterator[BrokerPair]:
    if request.param == "rabbitmq":
        if not rabbitmq_available():
            pytest.skip("live RabbitMQ not available (`docker compose up rabbitmq`)")
        suffix = uuid.uuid4().hex[:8]
        work, dlq = f"test.work.{suffix}", f"test.dlq.{suffix}"
        publisher = RabbitMQFramePublisher(DEFAULT_RABBIT_URL, queue_name=work, dlq_name=dlq)
        consumer = RabbitMQFrameConsumer(
            DEFAULT_RABBIT_URL, queue_name=work, dlq_name=dlq, prefetch=8
        )
        pair = BrokerPair(publisher, consumer)
        try:
            yield pair
        finally:
            await consumer.close()
            await publisher.close()
    else:
        broker = InMemoryBroker()
        yield BrokerPair(InMemoryFramePublisher(broker), InMemoryFrameConsumer(broker))


class TestRoundTrip:
    async def test_published_reference_arrives_intact(self, pair: BrokerPair) -> None:
        """Serialisation is real here: a double holding live objects would not
        catch a broken FrameRef encoding."""
        sent = make_ref(137)
        await pair.publisher.publish(sent)

        delivery = await pair.next_delivery()
        assert delivery.ref == sent
        await delivery.ack()

    async def test_fifo_order_for_a_single_consumer(self, pair: BrokerPair) -> None:
        for i in range(5):
            await pair.publisher.publish(make_ref(i))
        seen = []
        for _ in range(5):
            delivery = await pair.next_delivery()
            seen.append(delivery.ref.frame_id)
            await delivery.ack()
        assert seen == [0, 1, 2, 3, 4]

    async def test_first_delivery_is_not_marked_redelivered(self, pair: BrokerPair) -> None:
        await pair.publisher.publish(make_ref())
        delivery = await pair.next_delivery()
        assert delivery.redelivered is False
        await delivery.ack()


class TestRedelivery:
    async def test_nack_with_requeue_redelivers(self, pair: BrokerPair) -> None:
        """The mechanism that makes a detector crash survivable."""
        await pair.publisher.publish(make_ref(7))

        first = await pair.next_delivery()
        await first.nack(requeue=True)

        second = await pair.next_delivery()
        assert second.ref.frame_id == 7
        assert second.redelivered is True
        await second.ack()

    async def test_acked_message_is_not_redelivered(self, pair: BrokerPair) -> None:
        await pair.publisher.publish(make_ref())
        delivery = await pair.next_delivery()
        await delivery.ack()
        await pair.expect_nothing()


class TestDeadLettering:
    async def test_nack_without_requeue_removes_from_the_work_queue(
        self, pair: BrokerPair
    ) -> None:
        """Poison frames must leave the queue instead of looping forever."""
        await pair.publisher.publish(make_ref(3))

        delivery = await pair.next_delivery()
        await delivery.nack(requeue=False)

        await pair.expect_nothing()


class TestBatchAck:
    async def test_multiple_true_settles_the_whole_run(self, pair: BrokerPair) -> None:
        """ResultBatcher acks a batch with one round trip; this is the
        behaviour it relies on."""
        for i in range(5):
            await pair.publisher.publish(make_ref(i))

        deliveries = [await pair.next_delivery() for _ in range(5)]
        await deliveries[-1].ack(multiple=True)

        await pair.expect_nothing()

    async def test_delivery_tags_increase_so_multiple_ack_is_meaningful(
        self, pair: BrokerPair
    ) -> None:
        """`ack(multiple=True)` settles everything *up to* a tag, so the tags a
        batch holds must be strictly increasing for that to be safe."""
        for i in range(3):
            await pair.publisher.publish(make_ref(i))
        deliveries = [await pair.next_delivery() for _ in range(3)]

        tags = [d.tag for d in deliveries]
        assert tags == sorted(tags) and len(set(tags)) == 3

        await deliveries[-1].ack(multiple=True)
        await pair.expect_nothing()


class TestPerVideoOrdering:
    """The interviewer's requirement: results must be in order *per video*.

    Cross-video interleaving is fine and expected -- those are different
    partitions, which is exactly what buys the parallelism.
    """

    async def test_frames_of_one_video_arrive_in_dispatch_order(
        self, pair: BrokerPair
    ) -> None:
        for i in range(12):
            await pair.publisher.publish(make_ref(i, job_id="jobA"))

        seen = []
        for _ in range(12):
            delivery = await pair.next_delivery()
            seen.append(delivery.ref.frame_id)
            await delivery.ack()

        assert seen == sorted(seen), "frames of one video arrived out of order"

    async def test_two_videos_each_keep_their_own_order(self, pair: BrokerPair) -> None:
        """Interleave the publishes; each video's own sequence must survive."""
        for i in range(8):
            await pair.publisher.publish(_ref_for("alpha", i))
            await pair.publisher.publish(_ref_for("beta", i))

        per_video: dict[str, list[int]] = {"alpha": [], "beta": []}
        for _ in range(16):
            delivery = await pair.next_delivery()
            per_video[delivery.ref.video_id].append(delivery.ref.frame_id)
            await delivery.ack()

        assert per_video["alpha"] == sorted(per_video["alpha"])
        assert per_video["beta"] == sorted(per_video["beta"])
        assert len(per_video["alpha"]) == len(per_video["beta"]) == 8


def _ref_for(video_id: str, frame_id: int) -> FrameRef:
    return FrameRef(
        job_id=f"job-{video_id}",
        video_id=video_id,
        frame_id=frame_id,
        timestamp_sec=frame_id / 2,
        blob_key=f"frame:{video_id}:{frame_id}",
    )


class TestHealth:
    async def test_publisher_ping(self, pair: BrokerPair) -> None:
        assert await pair.publisher.ping() is True
