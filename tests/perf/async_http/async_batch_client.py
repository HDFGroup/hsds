# modified fetch function with semaphore
import random
import asyncio
from aiohttp import ClientSession
import config


async def fetch(url):
    async with ClientSession() as session:
        async with session.get(url) as response:
            delay = response.headers.get("DELAY")
            date = response.headers.get("DATE")
            print("{}:{} with delay {}".format(date, response.url, delay))
            return await response.read()


async def bound_fetch(sem, url):
    # getter function with semaphore
    async with sem:
        await fetch(url)


async def run(loop, r):
    url = config.get("server_url") + "{}"
    tasks = []
    # create instance of Semaphore
    batch_size = int(config.get("batch_size"))
    sem = asyncio.Semaphore(batch_size)
    for i in range(r):
        # pass Semaphore to every GET request
        task = asyncio.ensure_future(bound_fetch(sem, url.format(i)))
        tasks.append(task)

    responses = await asyncio.gather(*tasks)
    # await responses
    print("Got {} responses".format(len(responses)))


number = int(config.get("run_count"))
print("run_count: {}".format(number))
loop = asyncio.get_event_loop()

future = asyncio.ensure_future(run(loop, number))
loop.run_until_complete(future)
