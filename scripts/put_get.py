import asyncio
import aiohttp
import time
import sys
import random

ARGV = dict(enumerate(sys.argv))
NUM_OPERATION = int(ARGV.get(1, "1"))
PLAZA = "http://nsl-node1.d2:8080"


def to_timestamp(system_time):
    return (
        system_time["secs_since_epoch"]
        + system_time["nanos_since_epoch"] / 1000 / 1000 / 1000
    )


async def list_peer():
    async with aiohttp.ClientSession() as session:
        while True:
            async with session.get(f"{PLAZA}/run") as resp:
                run = await resp.json()
            if "Ready" in run:
                break
            await asyncio.sleep(1)
    peers = [participant["Peer"]["uri"] for participant in run["Ready"]["participants"]]
    wait = to_timestamp(run["Ready"]["assemble_time"]) - time.time() + 2
    if wait > 0:
        await asyncio.sleep(wait)
    return peers


async def put_get(peer):
    async with aiohttp.ClientSession() as session:
        async with session.post(f'{peer}/benchmark/put') as resp:
            put_id = await resp.json()
        while True:
            await asyncio.sleep(1)
            async with session.get(f'{peer}/benchmark/put/{put_id}') as resp:
                result = await resp.json()
                if result['put_end']:
                    break
        latency = to_timestamp(result['put_end']) - to_timestamp(result['put_start'])
        print(f'{peer},put,{latency}')

        await session.post(f'{peer}/benchmark/get/{put_id}')
        while True:
            await asyncio.sleep(1)
            async with session.get(f'{peer}/benchmark/put/{put_id}') as resp:
                result = await resp.json()
                if result['get_end']:
                    break
        latency = to_timestamp(result['get_end']) - to_timestamp(result['get_start'])
        print(f'{peer},get,{latency}')


async def main():
    peers = await list_peer()
    peer = random.choice(peers)
    await put_get(peer)


if __name__ == "__main__":
    asyncio.run(main())
