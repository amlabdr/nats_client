import asyncio
from nats.aio.client import Client as NATS
from nats.js.api import StreamConfig, RetentionPolicy
from nats.aio.errors import ErrTimeout


class NATSClient:
    def __init__(self, servers=["nats://127.0.0.1:4222"]):
        self.servers = servers
        self.nc = NATS()
        self.js = None  # JetStream context
        self.subscriptions = {}

    async def connect(self):
        if not self.nc.is_connected:
            await self.nc.connect(servers=self.servers)
            print("Connected to NATS!")
            self.js = self.nc.jetstream()

    async def close(self):
        if self.nc.is_connected:
            await self.nc.flush()
            await self.nc.close()
            print("Connection closed.")

    # ---------------------------
    # Core Publish / Subscribe
    # ---------------------------
    async def publish(self, subject: str, message: str):
        if not self.nc.is_connected:
            await self.connect()
        await self.nc.publish(subject, message.encode())

    async def subscribe(self, subject: str, callback):
        if not self.nc.is_connected:
            await self.connect()

        async def handler(msg):
            data = msg.data.decode()
            await callback(msg.subject, msg.reply, data)

        sid = await self.nc.subscribe(subject, cb=handler)
        self.subscriptions[sid] = subject
        return sid

    async def unsubscribe(self, sid):
        if sid in self.subscriptions:
            await self.nc.unsubscribe(sid)
            print(f"Unsubscribed from {self.subscriptions[sid]}")
            del self.subscriptions[sid]

    async def request(self, subject: str, message: str, timeout=1):
        if not self.nc.is_connected:
            await self.connect()
        try:
            msg = await self.nc.request(subject, message.encode(), timeout=timeout)
            return msg.data.decode()
        except ErrTimeout:
            return None

    # ---------------------------
    # JetStream Streaming
    # ---------------------------
    async def ensure_stream(self, stream: str, subject: str):
        """Create stream only once"""
        try:
            await self.js.stream_info(stream)
        except:
            await self.js.add_stream(
                StreamConfig(
                    name=stream,
                    subjects=[subject],
                    retention=RetentionPolicy.INTEREST,
                    max_age=1_000_000_000,
                )
            )
            print(f"Stream '{stream}' created for subject '{subject}'")

    async def stream_publish(self, stream: str, subject: str, data: bytes):
        """Publish large binary/JSON streaming content (no manual chunking)"""
        if not self.nc.is_connected:
            await self.connect()

        await self.ensure_stream(stream, subject)
        await self.js.publish(subject, data)

    async def stream_subscribe(self, stream: str, subject: str, callback):
        """Subscribe to JetStream stream with backpressure"""
        if not self.nc.is_connected:
            await self.connect()

        await self.ensure_stream(stream, subject)

        async def handler(msg):
            await callback(msg.data)
            await msg.ack()

        await self.js.subscribe(subject, cb=handler)
