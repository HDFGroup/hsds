import asyncio
from datetime import datetime
from aiohttp import web
import random

# set seed to ensure async and sync client get same distribution of delay values
# and tests are fair
random.seed(1)


async def hello(request):
    n = datetime.now().isoformat()
    delay = random.randint(0, 3)
    await asyncio.sleep(delay)
    headers = {"content_type": "text/html", "delay": str(delay)}
    # opening file is not async here, so it may block, to improve
    # efficiency of this you can consider using asyncio Executors
    # that will delegate file operation to separate thread or process
    # and improve performance
    # https://docs.python.org/3/library/asyncio-eventloop.html#executor
    # https://pymotw.com/3/asyncio/executors.html
    with open("html_text.html", "rb") as html_body:
        print("{}: {} delay: {}".format(n, request.path, delay))
        response = web.Response(body=html_body.read(), headers=headers)
    return response


app = web.Application()
app.router.add_route("GET", "/{name}", hello)
web.run_app(app)
