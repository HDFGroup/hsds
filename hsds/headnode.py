#
# Head node of hsds cluster
# 
import asyncio
import textwrap
import uuid
import json
import time

from aiohttp.web import Application, Response, StreamResponse, run_app

import config
from timeUtil import unixTimeToUTC, elapsedTime

state = {}

async def intro(request):
    url = "127.0.0.1:{}".format(config.get("head_port"))
    txt = textwrap.dedent("""\
        Type {url}/hello/John  {url}/simple or {url}/change_body
        in browser url bar
    """).format(url=url)
    binary = txt.encode('utf8')
    resp = StreamResponse()
    resp.content_length = len(binary)
    await resp.prepare(request)
    resp.write(binary)
    return resp


async def simple(request):
    return Response(text="Simple answer")


async def change_body(request):
    resp = Response()
    resp.body = b"Body changed"
    return resp


async def hello(request):
    resp = StreamResponse()
    name = request.match_info.get('name', 'Anonymous')
    answer = ('Hello, ' + name).encode('utf8')
    resp.content_length = len(answer)
    await resp.prepare(request)
    resp.write(answer)
    await resp.write_eof()
    return resp

async def info(request):
    global state
    resp = StreamResponse()
    resp.headers['Content-Type'] = 'application/json'
    answer = {}
    # copy relevant entries from state dictionary to response
    answer['id'] = state['id']
    answer['start_time'] = unixTimeToUTC(state['start_time'])
    answer['up_time'] = elapsedTime(state['start_time'])
    answer['cluster_state'] = state['cluster_state']     
    answer['target_sn_count'] = state['target_sn_count']  
    answer['active_sn_count'] = state['active_sn_count']
    answer['target_dn_count'] = state['target_dn_count']  
    answer['active_dn_count'] = state['active_dn_count']  

    answer = json.dumps(answer)
    answer = answer.encode('utf8')
    resp.content_length = len(answer)
    await resp.prepare(request)
    resp.write(answer)
    await resp.write_eof()
    return resp

async def register(request):
    global state
    resp = StreamResponse()
    resp.headers['Content-Type'] = 'application/json'
    answer = {}
    answer['rank'] = 42
    answer = json.dumps(answer)
    answer = answer.encode('utf8')
    resp.content_length = len(answer)
    await resp.prepare(request)
    resp.write(answer)
    await resp.write_eof()
    return resp



async def init(loop):
    global state
    state['id'] = str(uuid.uuid1())
    state['cluster_state'] = "INITIALIZING"
    state['start_time'] = int(time.time())  # seconds after epoch
    target_sn_count = int(config.get("target_sn_count"))
    state['target_sn_count'] = target_sn_count
    state['active_sn_count'] = 0
    state['sn'] = [None] * target_sn_count
    target_dn_count = int(config.get("target_dn_count"))
    state['dn'] = [None] * target_dn_count
    state['target_dn_count'] = target_sn_count
    state['active_dn_count'] = 0

    app = Application(loop=loop)
    app.router.add_get('/', intro)
    app.router.add_get('/simple', simple)
    app.router.add_get('/change_body', change_body)
    app.router.add_get('/hello/{name}', hello)
    app.router.add_get('/hello', hello)
    app.router.add_get('/info', info)
    app.router.add_post('/register', register)
    return app

loop = asyncio.get_event_loop()
app = loop.run_until_complete(init(loop))
run_app(app, port=config.get("head_port"))
