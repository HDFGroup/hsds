#
# common node methods of hsds cluster
# 
import asyncio
import uuid
import json
import time
import sys

from aiohttp.web import Application, Response, StreamResponse, run_app
from aiohttp import log, ClientSession, TCPConnector 
from aiohttp.errors import HttpBadRequest, ClientOSError
import aiobotocore
 

import config
from timeUtil import unixTimeToUTC, elapsedTime
from hsdsUtil import http_get, isOK, http_post


async def register(app):
    """ register node with headnode
    OK to call idempotetently (e.g. if the headnode seems to have forgetten us)"""

    print("register...")
    req_reg = app["head_url"] + "/register"
    print("req:", req_reg)
    body = {"id": app["id"], "port": app["node_port"], "node_type": app["node_type"]}
    try:
        rsp_json = await http_post(app, req_reg, body)
        print("register response:", rsp_json)
        if rsp_json is not None:
            app["node_number"] = rsp_json["node_number"]
            app["node_count"] = rsp_json["node_count"]
            app["node_state"] = "READY"
    except OSError:
        print("failed to register")


async def healthCheck(app):
    """ Periodic method that either registers with headnode (if state in INITIALIZING) or 
    calls headnode to verify vitals about this node (otherwise)"""
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
            if rsp_json is None or "host" not in rsp_json or rsp_json["host"] is None or rsp_json["id"] != app["id"]:
                print("reregister node")
                await register(app)
        sleep_secs = config.get("node_sleep_time")
        await asyncio.sleep(sleep_secs)
 
async def info(request):
    """HTTP Method to retun node state to caller"""
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
    answer['node_number'] = app['node_number']
    answer['node_count'] = app['node_count']
        
     
    answer = json.dumps(answer)
    answer = answer.encode('utf8')
    resp.content_length = len(answer)
    await resp.prepare(request)
    resp.write(answer)
    await resp.write_eof()
    return resp


def baseInit(loop, node_type):
    """Intitialize application and return app object"""
    app = Application(loop=loop)

    # set a bunch of global state 
    app["id"] = str(uuid.uuid1())
    app["node_state"] = "INITIALIZING"
    app["node_type"] = node_type
    app["node_port"] = config.get(node_type + "_port")
    app["node_number"] = -1
    app["node_count"] = -1
    app["start_time"] = int(time.time())  # seconds after epoch
    app["bucket_name"] = config.get("bucket_name")
    app["head_url"] = "http://{}:{}".format(config.get("head_host"), config.get("head_port"))

    # create a client Session here so that all client requests 
    #   will share the same connection pool
    max_tcp_connections = int(config.get("max_tcp_connections"))
    client = ClientSession(loop=loop, connector=TCPConnector(limit=max_tcp_connections))

    # get connection to S3
    # app["bucket_name"] = config.get("bucket_name")
    aws_region = config.get("aws_region")
    aws_secret_access_key = config.get("aws_secret_access_key")
    print("aws_secret_access_key:", aws_secret_access_key)
    aws_access_key_id = config.get("aws_access_key_id")
    print("aws_access_key:_id", aws_access_key_id)

    session = aiobotocore.get_session(loop=loop)
    aws_client = session.create_client('s3', region_name=aws_region,
                                   aws_secret_access_key=aws_secret_access_key,
                                   aws_access_key_id=aws_access_key_id)
    app['client'] = client
    app['s3'] = aws_client

    
    #initLogger('aiohtp.server')
    #log = initLogger('head_node')
    #log.info("log init")

    app.router.add_get('/', info)
    app.router.add_get('/info', info)
      
    return app
 