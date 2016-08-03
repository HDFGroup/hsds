import asyncio
from aiohttp import ClientSession
import config

async def fetch(url):
    async with ClientSession() as session:
        async with session.get(url) as res:
            await res.read()
            delay = res.headers.get("DELAY")
            d = res.headers.get("DATE")    
            retval = "{}:{} delay {}".format(d, res.url, delay)
            return retval
              

async def run(loop,  r):
    url = config.get("server_url") + "{}"
    tasks = []
    for i in range(r):
        task = asyncio.ensure_future(fetch(url.format(i)))
        tasks.append(task)

    responses = await asyncio.gather(*tasks)
    # you now have all response bodies in this variable
    print_responses(responses)
     

def print_responses(results):
    print("num results: {}".format(len(results)))
    for res in results:
        print(res)
        #delay = res.headers.get("DELAY")
        #d = res.headers.get("DATE")
        #print("{}:{} delay {}".format(d, res.url, delay))
    

loop = asyncio.get_event_loop()
run_count = int(config.get("run_count"))
print("run_count:{}".format(run_count))
print("making: {} calls".format(run_count))
future = asyncio.ensure_future(run(loop, run_count))
loop.run_until_complete(future)
