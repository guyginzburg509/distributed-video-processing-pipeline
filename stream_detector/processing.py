"""Turning a frame reference into a RespObject.

Fetch the blob -> decode to ndarray -> run the provided detector -> build the
response object. The detector is called exactly as shipped.

Errors are classified, because the right response differs sharply:

* **Permanent** (blob expired, corrupt JPEG): retrying re-runs the same failing
  computation on the same bytes. Dead-letter immediately; a retry loop here
  would just burn the queue.
* **Transient** (Redis unreachable): the work is still valid, the dependency is
  not. Requeue so another attempt -- possibly on another replica -- can succeed.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from concurrent.futures import Executor
from typing import cast

import cv2
import numpy as np
import numpy.typing as npt

from pipeline_common.logging import get_logger
from pipeline_common.messages import FrameRef
from pipeline_common.ports import DedupGuard, FrameStore, FrameStoreError
from stream_detector.detector import BoundingBox, StreamFaceDetector
from stream_detector.detector_response_handling import RespObject

log = get_logger(__name__)


class PermanentFrameError(Exception):
    """This frame can never succeed; dead-letter it."""


class TransientFrameError(Exception):
    """A dependency failed; the frame itself is fine. Requeue."""


class DuplicateFrameError(Exception):
    """Already processed (at-least-once redelivery). Ack and move on."""


class StaleFrameError(Exception):
    """The frame aged past the latency budget before we reached it.

    Not a failure of the frame -- a signal that the pipeline is behind. Shed it
    and stay current; see FrameProcessor for why that is the right call in a
    real-time system.
    """

    def __init__(self, age_sec: float, budget_sec: float) -> None:
        super().__init__(f"frame is {age_sec:.2f}s old, past the {budget_sec:.2f}s budget")
        self.age_sec = age_sec
        self.budget_sec = budget_sec


class FrameProcessor:
    def __init__(
        self,
        *,
        detector: StreamFaceDetector,
        store: FrameStore,
        dedup: DedupGuard,
        executor: Executor,
        dedup_ttl_sec: int,
        max_frame_age_sec: float = 0.0,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._detector = detector
        self._store = store
        self._dedup = dedup
        self._executor = executor
        self._dedup_ttl = dedup_ttl_sec
        self._max_frame_age = max_frame_age_sec
        self._clock = clock

    async def process(self, ref: FrameRef) -> RespObject:
        # Freshness first, before anything expensive. In a real-time facial
        # recognition system a late answer is worth little, and spending
        # capacity on a stale frame is capacity the *current* frame does not
        # get -- so a backlog compounds instead of draining. Shedding is how the
        # pipeline catches up.
        #
        # Disabled by default (max_frame_age_sec = 0): when processing a file,
        # completeness matters more than latency and nothing should be skipped.
        # The deployment mode decides, not the code.
        if self._max_frame_age > 0:
            age = ref.age_sec(self._clock())
            if age > self._max_frame_age:
                raise StaleFrameError(age, self._max_frame_age)

        frame = await self._load_frame(ref)
        faces = await self._detect(frame)

        # Claim *after* the work, not before.
        #
        # Claiming first looks cheaper -- it would skip redundant decoding -- but
        # it silently loses frames: a transient failure nacks and requeues, the
        # redelivery finds the key already claimed, and the frame is discarded as
        # a "duplicate". The guard meant to protect against redelivery would
        # defeat the retry instead.
        #
        # The guard's actual job is "never report the same frame downstream
        # twice", so the claim belongs at the moment of reporting. The cost is a
        # rare duplicated detection when two consumers race -- and with one
        # active consumer per partition, that race is already rare.
        if not await self._dedup.claim(f"processed:{ref.job_id}:{ref.frame_id}", self._dedup_ttl):
            raise DuplicateFrameError(f"frame {ref.frame_id} of job {ref.job_id} already processed")

        return RespObject(faces=faces, video_id=ref.video_id, frame_id=ref.frame_id)

    async def _load_frame(self, ref: FrameRef) -> npt.NDArray[np.uint8]:
        try:
            payload = await self._store.get(ref.blob_key)
        except FrameStoreError as exc:
            raise TransientFrameError(f"frame store unavailable: {exc}") from exc

        if payload is None:
            # Expired or evicted. Not retryable -- and worth alerting on, since
            # it means the backlog outlived the blob TTL.
            raise PermanentFrameError(f"blob {ref.blob_key!r} is gone (expired or evicted)")

        frame = cv2.imdecode(np.frombuffer(payload, dtype=np.uint8), cv2.IMREAD_COLOR)
        if frame is None or frame.size == 0:
            raise PermanentFrameError(f"blob {ref.blob_key!r} is not a decodable image")
        # imdecode's declared dtype is broader than the IMREAD_COLOR reality.
        return cast(npt.NDArray[np.uint8], frame)

    async def _detect(self, frame: npt.NDArray[np.uint8]) -> list[BoundingBox]:
        """Run the provided mock off the event loop.

        Real inference (torch/CUDA) releases the GIL, so a thread pool is the
        right offload. For pure-Python CPU inference you would swap in a
        ProcessPoolExecutor and pay frame pickling -- but the primary scaling
        lever is replicas, not in-process parallelism.
        """
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self._executor, self._detector.detect_faces, frame)
