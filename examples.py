import asyncio
from nats_client import NATSClient   # Your class above

# --------------------------------------------
# Core Publish Example
# --------------------------------------------
async def pub_example():
    client = NATSClient(["nats://localhost:4222"])
    await client.connect()

    await client.publish("demo.test", "Hello World!")
    print("Published simple message!")

    await client.close()


# --------------------------------------------
# Core Subscribe Example
# --------------------------------------------
async def sub_example():
    client = NATSClient(["nats://localhost:4222"])
    await client.connect()

    async def handler(subject, reply, data):
        print(f"[SUB RECEIVED] subject={subject} data={data}")

    await client.subscribe("demo.test", handler)

    print("Subscribed, waiting for messages...")
    await asyncio.sleep(30)  # Keep alive

    await client.close()


# --------------------------------------------
# JetStream Stream Subscriber
# --------------------------------------------
async def stream_sub():
    client = NATSClient(["nats://localhost:4222"])
    await client.connect()

    async def stream_handler(data):
        print("[STREAM DATA RECEIVED]", len(data), "bytes")
        print("Preview:", data[:50])

    await client.stream_subscribe(
        stream="MEASURE",
        subject="measurement.data",
        callback=stream_handler,
    )

    print("Listening for real-time stream...")
    await asyncio.sleep(60)


# --------------------------------------------
# JetStream Stream Publisher
# --------------------------------------------
async def stream_pub():
    client = NATSClient(["nats://localhost:4222"])
    await client.connect()

    for _ in range(10):
        payload = (str(_) * 5000).encode()
        await client.stream_publish(
            stream="MEASURE",
            subject="measurement.data",
            data=payload,
        )

    print("Stream message sent!")
    await client.close()


# ----------------------------------------------------
# RUN ANY TEST → comment/uncomment
# ----------------------------------------------------
if __name__ == "__main__":
    # ---- Core messaging tests ----
    # asyncio.run(sub_example())   # Run subscriber first
    # asyncio.run(pub_example())   # Then run publisher

    # ---- JetStream streaming tests ----
    #asyncio.run(stream_sub())    # Run subscriber first
    asyncio.run(stream_pub())    # Then run publisher
    pass
