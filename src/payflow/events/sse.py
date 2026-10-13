"""Server-Sent Events (SSE) broadcaster for real-time streaming to the PayFlow dashboard."""

import asyncio
import json
import logging
import queue
import threading
import time
from typing import Any, AsyncGenerator

logger = logging.getLogger(__name__)


class SSEBroadcaster:
    """Thread-safe and async-compatible broadcaster for live event streaming to browser clients."""

    def __init__(self) -> None:
        self._queues: list[queue.Queue[str]] = []
        self._lock = threading.Lock()
        self._message_history: list[dict[str, Any]] = []

    def broadcast(self, event_type: str, data: dict[str, Any]) -> None:
        """Pushes an event payload to all connected SSE browser clients."""
        payload = {
            "type": event_type,
            "timestamp": time.time(),
            "data": data,
        }
        with self._lock:
            self._message_history.append(payload)
            if len(self._message_history) > 100:
                self._message_history.pop(0)

            msg_str = f"data: {json.dumps(payload)}\n\n"
            dead_queues = []
            for q in self._queues:
                try:
                    q.put_nowait(msg_str)
                except queue.Full:
                    dead_queues.append(q)

            for dead in dead_queues:
                if dead in self._queues:
                    self._queues.remove(dead)

    async def stream_events(self) -> AsyncGenerator[str, None]:
        """Async generator yielding Server-Sent Events to an HTTP client."""
        q: queue.Queue[str] = queue.Queue(maxsize=200)
        with self._lock:
            self._queues.append(q)
            # Send initial connected handshake with recent history
            recent_summary = {
                "type": "handshake",
                "timestamp": time.time(),
                "data": {
                    "status": "CONNECTED",
                    "channel": "payflow.kafka_redis_stream",
                    "buffer_size": len(self._message_history),
                },
            }
            q.put_nowait(f"data: {json.dumps(recent_summary)}\n\n")

        try:
            while True:
                # Poll non-blocking in threadpool to allow clean cancellation
                try:
                    msg = await asyncio.to_thread(q.get, timeout=1.0)
                    yield msg
                except (queue.Empty, TimeoutError):
                    # Keep-alive heartbeat ping every second
                    yield ": ping\n\n"
        finally:
            with self._lock:
                if q in self._queues:
                    self._queues.remove(q)

    def get_recent_events(self, limit: int = 50) -> list[dict[str, Any]]:
        """Returns the most recent broadcasted events."""
        with self._lock:
            return list(self._message_history[-limit:])


# Singleton broadcaster
sse_broadcaster = SSEBroadcaster()
