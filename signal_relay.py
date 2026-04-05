#!/usr/bin/env python3
"""Lightweight signal relay for broadcasting Tree News events to many followers.

Tree News publishes a JSON payload to the `/publish` endpoint (HTTP POST) and the
relay fans the signal out to all connected WebSocket clients at `/ws`.

Configuration is driven by environment variables and optional CLI arguments.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import signal
from typing import Any, Dict, Optional, Sequence

from aiohttp import web, WSMsgType

LOGGER = logging.getLogger("signal_relay")

DEFAULT_HOST = os.getenv("RELAY_BIND_HOST", "127.0.0.1")
DEFAULT_PORT = int(os.getenv("RELAY_PORT", "9100"))
DEFAULT_HEARTBEAT = int(os.getenv("RELAY_WS_HEARTBEAT", "25"))
DEFAULT_QUEUE_SIZE = int(os.getenv("RELAY_QUEUE_SIZE", "2048"))
PUBLISH_TOKEN = (
    os.getenv("RELAY_PUBLISH_TOKEN")
    or os.getenv("SIGNAL_RELAY_API_KEY")
    or os.getenv("RELAY_API_KEY")
)


class RelayHub:
    """Manages connected clients and delivers messages to them."""

    def __init__(self, queue_size: int = 2048) -> None:
        self.clients: set[web.WebSocketResponse] = set()
        self.queue: asyncio.Queue[Dict[str, Any]] = asyncio.Queue(maxsize=queue_size)
        self.stats = {
            "clients": 0,
            "enqueued": 0,
            "dropped": 0,
            "delivered": 0,
            "deliver_errors": 0,
        }
        self._lock = asyncio.Lock()

    async def add_client(self, client: web.WebSocketResponse) -> None:
        async with self._lock:
            self.clients.add(client)
            self.stats["clients"] = len(self.clients)
            LOGGER.info("Client connected (total=%s)", len(self.clients))

    async def remove_client(self, client: web.WebSocketResponse) -> None:
        async with self._lock:
            if client in self.clients:
                self.clients.remove(client)
                self.stats["clients"] = len(self.clients)
                LOGGER.info("Client disconnected (total=%s)", len(self.clients))

    async def enqueue(self, payload: Dict[str, Any]) -> bool:
        try:
            self.queue.put_nowait(payload)
            self.stats["enqueued"] += 1
            return True
        except asyncio.QueueFull:
            self.stats["dropped"] += 1
            LOGGER.warning("Relay queue full; dropping payload")
            return False

    async def broadcast_loop(self) -> None:
        while True:
            payload = await self.queue.get()
            message = json.dumps(payload)

            # Snapshot clients to avoid iteration issues if clients disconnect mid-send.
            clients: Sequence[web.WebSocketResponse] = tuple(self.clients)
            if not clients:
                continue

            results = await asyncio.gather(
                *(self._send(client, message) for client in clients),
                return_exceptions=True,
            )

            for client, result in zip(clients, results):
                if isinstance(result, Exception):
                    self.stats["deliver_errors"] += 1
                    LOGGER.debug("Client delivery failed: %s", result)
                    await self.remove_client(client)
                    try:
                        await client.close()
                    except Exception:
                        pass
                else:
                    self.stats["delivered"] += 1

    async def _send(self, client: web.WebSocketResponse, message: str) -> None:
        await client.send_str(message)

    async def close(self) -> None:
        async with self._lock:
            clients = list(self.clients)
            self.clients.clear()
            self.stats["clients"] = 0
        for client in clients:
            try:
                await client.close(code=1001, message="relay shutting down".encode())
            except Exception:
                pass


class RelayService:
    """HTTP/WebSocket surface for publishing and subscribing to signals."""

    def __init__(self, host: str, port: int, *, queue_size: int, token: Optional[str], heartbeat: int) -> None:
        self.host = host
        self.port = port
        self.token = token
        self.heartbeat = heartbeat
        self.hub = RelayHub(queue_size=queue_size)
        self.app = web.Application()
        self.app.add_routes(
            [
                web.get("/", self.index_handler),
                web.get("/health", self.health_handler),
                web.get("/metrics", self.metrics_handler),
                web.get("/ws", self.websocket_handler),
                web.post("/publish", self.publish_handler),
            ]
        )
        self.runner: Optional[web.AppRunner] = None
        self.site: Optional[web.TCPSite] = None
        self.broadcast_task: Optional[asyncio.Task] = None

    async def start(self) -> None:
        LOGGER.info("Starting relay on %s:%s", self.host, self.port)
        self.runner = web.AppRunner(self.app)
        await self.runner.setup()
        self.site = web.TCPSite(self.runner, self.host, self.port)
        await self.site.start()
        self.broadcast_task = asyncio.create_task(self.hub.broadcast_loop())

    async def stop(self) -> None:
        if self.broadcast_task:
            self.broadcast_task.cancel()
            try:
                await self.broadcast_task
            except asyncio.CancelledError:
                pass
            self.broadcast_task = None

        await self.hub.close()

        if self.site:
            await self.site.stop()
            self.site = None
        if self.runner:
            await self.runner.cleanup()
            self.runner = None

    # ----- Routes -----

    async def index_handler(self, request: web.Request) -> web.Response:
        return web.json_response(
            {
                "status": "ok",
                "message": "Signal relay running",
                "ws_endpoint": f"ws://{self.host}:{self.port}/ws",
            }
        )

    async def health_handler(self, request: web.Request) -> web.Response:
        return web.json_response({"status": "ok", "clients": self.hub.stats["clients"]})

    async def metrics_handler(self, request: web.Request) -> web.Response:
        return web.json_response(self.hub.stats)

    async def websocket_handler(self, request: web.Request) -> web.StreamResponse:
        ws = web.WebSocketResponse(heartbeat=self.heartbeat)
        await ws.prepare(request)
        await self.hub.add_client(ws)

        try:
            async for msg in ws:
                if msg.type == WSMsgType.ERROR:
                    LOGGER.debug("WebSocket error: %s", ws.exception())
                # Ignore any payload from clients
        finally:
            await self.hub.remove_client(ws)

        return ws

    async def publish_handler(self, request: web.Request) -> web.Response:
        if self.token and not self._authorised(request):
            return web.json_response({"error": "unauthorised"}, status=401)

        try:
            payload = await request.json()
        except Exception:
            return web.json_response({"error": "invalid_json"}, status=400)

        if isinstance(payload, dict):
            success = await self.hub.enqueue(payload)
            return web.json_response({"status": "queued" if success else "dropped"})

        if isinstance(payload, list):
            results = [await self.hub.enqueue(item) for item in payload if isinstance(item, dict)]
            queued = sum(1 for r in results if r)
            dropped = len(results) - queued
            return web.json_response({"queued": queued, "dropped": dropped})

        return web.json_response({"error": "unsupported_payload"}, status=400)

    def _authorised(self, request: web.Request) -> bool:
        header = request.headers.get("Authorization", "").strip()
        if header.lower().startswith("bearer "):
            token = header[7:].strip()
            return token == self.token

        query_token = request.query.get("token")
        if query_token:
            return query_token == self.token

        return False


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Signal relay fan-out server")
    parser.add_argument("--host", default=DEFAULT_HOST, help="Bind host (default: %(default)s)")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT, help="Listen port (default: %(default)s)")
    parser.add_argument("--queue-size", type=int, default=DEFAULT_QUEUE_SIZE, help="Max queued signals (default: %(default)s)")
    parser.add_argument("--heartbeat", type=int, default=DEFAULT_HEARTBEAT, help="WebSocket heartbeat seconds (default: %(default)s)")
    parser.add_argument("--token", default=PUBLISH_TOKEN, help="Bearer token for /publish (optional)")
    return parser.parse_args()


async def main_async(args: argparse.Namespace) -> None:
    logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s %(name)s: %(message)s")
    service = RelayService(
        host=args.host,
        port=args.port,
        queue_size=max(1, args.queue_size),
        token=args.token,
        heartbeat=max(5, args.heartbeat),
    )

    await service.start()
    LOGGER.info("Relay running on http://%s:%s", args.host, args.port)

    stop_event = asyncio.Event()

    def _stop() -> None:
        LOGGER.info("Shutdown requested")
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _stop)
        except NotImplementedError:
            # Windows (or restricted environments) degrade gracefully
            pass

    try:
        await stop_event.wait()
    finally:
        await service.stop()
        LOGGER.info("Relay shutdown complete")


def main() -> None:
    args = parse_args()
    try:
        asyncio.run(main_async(args))
    except KeyboardInterrupt:
        LOGGER.info("Interrupted by user")


if __name__ == "__main__":
    main()
