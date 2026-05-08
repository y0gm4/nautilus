import asyncio
from pyclient import Nautilus
from pyclient._internal.engine import EnginePoolOptions


async def main() -> None:
    db = Nautilus(pool_options=EnginePoolOptions(max_connections=1))
    await db.connect()
    stream = db.user.stream_many(order_by={"id": "asc"}, chunk_size=1)
    seen = []
    try:
        async for row in stream:
            seen.append(row.name)
            if len(seen) == __BREAK_AFTER__:
                break
    finally:
        await stream.aclose()

    await asyncio.sleep(0.1)

    follow = await db.user.find_many(order_by={"id": "asc"}, take=5)
    tail = await db.user.find_many(order_by={"id": "asc"}, skip=__TAIL_SKIP__, take=1)

    print(f"count={len(seen)}")
    print(f"first={seen[0]}")
    print(f"tenth={seen[-1]}")
    print("follow=" + ",".join(row.name for row in follow))
    print(f"tail={tail[0].name}")
    print(f"partial_data={len(db._partial_data)}")
    print(f"stream_queues={len(db._stream_queues)}")

    await db.disconnect()


asyncio.run(main())
