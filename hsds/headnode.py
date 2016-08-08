#
# Head node of hsds cluster
# 
import asyncio

import textwrap
import uuid
import json
import time
import logging
import logging.handlers
import sys

from aiohttp.web import Application, Response, StreamResponse, run_app
from aiohttp import log 
from aiohttp.errors import HttpBadRequest

import config
from timeUtil import unixTimeToUTC, elapsedTime
 

async def healthCheck():
    while True:
        #print("health check")
        await  asyncio.sleep(5)

async def info(request):
    print("info") 
    app = request.app
    resp = StreamResponse()
    resp.headers['Content-Type'] = 'application/json'
    answer = {}
    # copy relevant entries from state dictionary to response
    answer['id'] = request.app['id']
    answer['start_time'] = unixTimeToUTC(app['start_time'])
    answer['up_time'] = elapsedTime(app['start_time'])
    answer['cluster_state'] = app['cluster_state']     
    answer['target_sn_count'] = getTargetNodeCount(app, "sn") 
    answer['active_sn_count'] = getActiveNodeCount(app, "dn")
    answer['target_dn_count'] = getTargetNodeCount(app, "sn") 
    answer['active_dn_count'] = getActiveNodeCount(app, "sn")

    answer = json.dumps(answer)
    answer = answer.encode('utf8')
    resp.content_length = len(answer)
    await resp.prepare(request)
    resp.write(answer)
    await resp.write_eof()
    return resp

async def register(request):
    print("register")   
    
    app = request.app

    body = await request.json()
    print(body)
    if 'id' not in body:
        print("missing id")
        raise HttpBadRequest(message="missing key 'id'")
    if 'port' not in body:
        raise HttpBadRequest(message="missing key 'port'")
    if 'node_type' not in body:
        raise HttpBadRequest(message="missing key 'node_type'")
    if body['node_type'] not in ('sn', 'dn'):
        raise HttpBadRequest(message="invalid node_type")
    
    peername = request.transport.get_extra_info('peername')
    if peername is None:
        raise HttpBadRequest(message="Can not determine caller IP")
    host, req_port = peername

    nodes = None
    ret_node = None  

    node_ids = app['node_ids']
    if body['id'] in node_ids:
        # already registered?  
        ret_node = node_ids[body['id']] 
    else:
        nodes = app['nodes']
        for node in nodes:
            if node['host'] is None and node['node_type'] == body['node_type']:
                # found a free node
                node['host'] = host
                node['port'] = body["port"]
                node['id'] =   body["id"]
                ret_node = node
                node_ids[body["id"]] = ret_node
                break
 

    if getInactiveNodeCount(app) == 0:
        # all the nodes have checked in
        print("setting cluster state to ready")
        app['cluster_state'] = "READY"
         
    resp = StreamResponse()
    resp.headers['Content-Type'] = 'application/json'
    answer = {}
    if ret_node is not None:
        answer["node_number"] = ret_node["node_number"]
    else:
        # all nodes allocated, let caller know it's in the reserve pool
        answer["node_number"] = -1
     
    answer = json.dumps(answer)
    answer = answer.encode('utf8')
    resp.content_length = len(answer)
    await resp.prepare(request)
    resp.write(answer)
    await resp.write_eof()
    return resp

async def nodestate(request):
    print("nodestat") 
    node_type = request.match_info.get('nodetype', '*')
    print("node_type:", node_type)
    if node_type not in ("sn", "dn", "*"):
        print("bad nodetype")
        raise HttpBadRequest(message="Invalid nodetype")
        
    app = request.app
    resp = StreamResponse()
    resp.headers['Content-Type'] = 'application/json'
    nodes = []
    for node in app["nodes"]:
        if node["node_type"] == node_type or node_type == "*":
            nodes.append(node)

    answer = {"nodes": nodes }
    
    answer = json.dumps(answer)
    answer = answer.encode('utf8')
    resp.content_length = len(answer)
    await resp.prepare(request)
    resp.write(answer)
    await resp.write_eof()
    return resp

def getTargetNodeCount(app, node_type):
    count = None
    if node_type == "dn":
        count = app['target_dn_count']
    elif node_type == "sn":
        count = app['target_sn_count']
    return count

def getActiveNodeCount(app, node_type):
    count = 0
    for node in app['nodes']:
        if node["node_type"] == node_type and node["host"] is not None:
            count += 1
    return count

def getInactiveNodeCount(app):
    count = 0
    for node in app['nodes']:
        if node['host'] is None:
            count += 1
    return count


async def init(loop):
    
    app = Application(loop=loop)

    # set a bunch of global state 
    app["id"] = str(uuid.uuid1())
    app["cluster_state"] = "INITIALIZING"
    app["start_time"] = int(time.time())  # seconds after epoch
     
    app["target_sn_count"] = int(config.get("target_sn_count"))
    app["target_dn_count"] = int(config.get("target_dn_count"))
    
    nodes = []
    for node_type in ("dn", "sn"):
        target_count = getTargetNodeCount(app, node_type)
        for i in range(target_count):
            node = {"node_number": i,
                "node_type": node_type,
                "host": None,
                "port": None }
            nodes.append(node)
    app["nodes"] = nodes
    app["node_ids"] = {}  # dictionary to look up node by id


    #initLogger('aiohtp.server')
    #log = initLogger('head_node')
    #log.info("log init")

    app.router.add_get('/', info)
    app.router.add_get('/nodestate', nodestate)
    app.router.add_get('/nodestate/{nodetype}', nodestate)
    app.router.add_get('/info', info)
    app.router.add_post('/register', register)
    
    return app


loop = asyncio.get_event_loop()
app = loop.run_until_complete(init(loop))
 
print("is coroutine:", asyncio.iscoroutine(healthCheck()))
asyncio.ensure_future(healthCheck(), loop=loop)

print("port: ", config.get("head_port"))
#handler = app.make_handler(access_log=log.access_logger, logger=log.server_logger)
run_app(app, port=config.get("head_port"))
