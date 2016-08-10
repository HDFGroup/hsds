#
# Head node of hsds cluster
# 
import asyncio
import uuid
import json
import time
import logging
import logging.handlers
import sys

from aiohttp.web import Application, Response, StreamResponse, run_app
from aiohttp import log, ClientSession, TCPConnector 
from aiohttp.errors import HttpBadRequest, ClientOSError
 

import config
from timeUtil import unixTimeToUTC, elapsedTime

def isOK(http_response):
    if http_response < 300:
        return True
    return False
 
async def http_get(app, url):
    print("http_get:", url)
    client = app['client']
    rsp_json = None
    try:
        async with client.get(url) as rsp:
            print("head response status:", rsp.status)
            rsp_json = await rsp.json()
            print("got response: ", rsp_json)
    except ClientOSError:
        print("unable to connect with", url)
    return rsp_json

async def http_post(app, url, data):
    print("http_post:", url)
    print("post body:", data)
    client = app['client']
    rsp_json = None
    client = app['client']
    
    async with client.post(url, data=json.dumps(data)) as rsp:
        print("head response status:", rsp.status)
        if isOK(rsp.status):  
            rsp_json = await rsp.json()
            print("got response: ", rsp_json)
    return rsp_json


async def register(app):
    print("register...")
    req_reg = app["head_url"] + "/register"
    print("req:", req_reg)
    body = {"id": app["id"], "port": app["node_port"], "node_type": app["node_type"]}
    try:
        rsp_json = await http_post(app, req_reg, body)
        if rsp_json is not None:
            app["node_number"] = rsp_json["node_number"]
            app["node_state"] = "READY"
    except OSError:
        print("failed to register")


async def healthCheck(app):
    while True:
        
        if app["node_state"] == "INITIALIZING":
            print("register")
            await register(app)
        else:
            print("health check")
            # check in with the head node and make sure we are still active
            req_node = "{}/nodestate/{}/{}".format(app["head_url"], app["node_type"], app["node_number"])
            print("node check url:", req_node)
            rsp_json = await http_get(app, req_node)
            if rsp_json is None or rsp_json["host"] is None or rsp_json["id"] != app["id"]:
                print("reregister node")
                await register(app)
        sleep_secs = config.get("node_sleep_time")
        await asyncio.sleep(sleep_secs)
 
async def info(request):
    print("info") 
    app = request.app
    resp = StreamResponse()
    resp.headers['Content-Type'] = 'application/json'
    answer = {}
    # copy relevant entries from state dictionary to response
    answer['id'] = request.app['id']
    answer['node_type'] = request.app['node_type']
    answer['start_time'] = unixTimeToUTC(app['start_time'])
    answer['up_time'] = elapsedTime(app['start_time'])
    answer['node_state'] = app['node_state']     
     
    answer = json.dumps(answer)
    answer = answer.encode('utf8')
    resp.content_length = len(answer)
    await resp.prepare(request)
    resp.write(answer)
    await resp.write_eof()
    return resp


async def init(loop):
    
    app = Application(loop=loop)

    # set a bunch of global state 
    app["id"] = str(uuid.uuid1())
    app["node_state"] = "INITIALIZING"
    app["node_type"] = "dn"  # data node
    app["node_port"] = config.get("dn_port")
    app["start_time"] = int(time.time())  # seconds after epoch

    app["head_url"] = "http://{}:{}".format(config.get("head_host"), config.get("head_port"))
    

    #initLogger('aiohtp.server')
    #log = initLogger('head_node')
    #log.info("log init")

    app.router.add_get('/', info)
    app.router.add_get('/info', info)
      
    return app

#
# Main
#

if __name__ == '__main__':

    loop = asyncio.get_event_loop()

    # create a client Session here so that all client requests 
    #   will share the same connection pool
    max_tcp_connections = int(config.get("max_tcp_connections"))
    client = ClientSession(loop=loop, connector=TCPConnector(limit=max_tcp_connections))

    #create the app object
    app = loop.run_until_complete(init(loop))
    app['client'] = client

    # run background task
    asyncio.ensure_future(healthCheck(app), loop=loop)
   
    # run the app
    run_app(app, port=config.get("dn_port"))
