##############################################################################
# Copyright by The HDF Group.                                                #
# All rights reserved.                                                       #
#                                                                            #
# This file is part of HSDS (HDF5 Scalable Data Service), Libraries and      #
# Utilities.  The full HSDS copyright notice, including                      #
# terms governing use, modification, and redistribution, is contained in     #
# the file COPYING, which can be found at the root of the source code        #
# distribution tree.  If you do not have access to this file, you may        #
# request a copy from help@hdfgroup.org.                                     #
##############################################################################
#
# Head node of hsds cluster
# 
import asyncio
import json
import time
import sys

from aiohttp.web import Application, StreamResponse, run_app
from aiohttp import  ClientSession, TCPConnector
from aiohttp.errors import HttpBadRequest
import aiobotocore

import config
from util.timeUtil import unixTimeToUTC, elapsedTime
from util.httpUtil import http_get_json, jsonResponse
from util.s3Util import  getS3JSONObj, putS3JSONObj, isS3Obj
from util.idUtil import  createNodeId, getHeadNodeS3Key
import hsds_logger as log

 
async def healthCheck(app):
    """ Periodic method that pings each active node and verifies it is still healthy.  
    If node doesn't respond, free up the node slot (the node can re-register if it comes back)'"""

    app["last_health_check"] = int(time.time())

    # update/initialize root object before starting node updates
    headnode_key = getHeadNodeS3Key()
    log.info("headnode S3 key".format(headnode_key))
    headnode_obj_found = await isS3Obj(app, headnode_key)
    
    if not headnode_obj_found:
        # first time hsds has run with this bucket name?
        log.warn("need to create headnode obj")
        head_state = {  }
        head_state["created"] = int(time.time())
        head_state["id"] = app["id"]
        head_state["last_health_check"] = app["last_health_check"]
        log.info("write head_state to S3: {}".format(head_state))
        await putS3JSONObj(app, headnode_key, head_state)

    nodes =  app["nodes"]
    while True:
        # sleep for a bit
        sleep_secs = config.get("head_sleep_time")
        await  asyncio.sleep(sleep_secs)

        now = int(time.time())
        log.info("health check {}".format(unixTimeToUTC(now)))
        
        head_state = await getS3JSONObj(app, headnode_key)
        log.info("head_state: {}".format(head_state))
        log.info("elapsed time since last health check: {}".format(elapsedTime(head_state["last_health_check"])))
        if head_state['id'] != app['id']:
            log.warn("mis-match bucket head id: {}".format(head_state["id"]))
            if now - head_state["last_health_check"] < sleep_secs * 4:
                log.warn("other headnode may be active")
                continue  # skip node checks and loop around again
            else:
                log.warn("other headnode is not active, making this headnode leader")
                head_state['id'] = app['id']
        else:
            log.info("head_state id matches S3 Object")

        head_state["last_health_check"] = now
        log.info("write head_state to S3: {}".format(head_state))
        await putS3JSONObj(app, headnode_key, head_state)
         
        log.info("putS3JSONObj complete")
        
        for node in nodes:         
            if node["host"] is None:
                continue
            url = "http://{}:{}/info".format(node["host"], node["port"])
            log.info("health check for: ".format(url))
            try:
                rsp_json = await http_get_json(app, url)
                log.info("get health check response: {}".format(rsp_json))
                if rsp_json['id'] != node['id']:
                    log.warn("unexpected node_id (expecting: {})".format(node['id']))
                    node['host'] = None
                    node['id'] = None
                    app["cluster_state"] = "INITIALIZING"
                if rsp_json['node_number'] != node['node_number']:
                    log.warn("unexpected node_number (expecting: {})".format(node['node_number']))
                    node['host'] = None
                    node['id'] = None
                    app["cluster_state"] = "INITIALIZING"
                # mark the last time we got a response from this node
                node["healthcheck"] = unixTimeToUTC(int(time.time()))
            except OSError as ose:
                log.warn("OSError: {}".format(str(ose)))
                # node has gone away?
                log.warn("removing {}:{} from active list".format(node["host"], node["port"]))
                node["host"] = None
                if app["cluster_state"] == "READY":
                    # go back to INITIALIZING state until another node is registered
                    log.warn("Setting cluster_state from READY to INITIALIZING")
                    app["cluster_state"] = "INITIALIZING"
        

async def info(request):
    """HTTP Method to return node state to caller"""
    log.request(request) 
    app = request.app
    resp = StreamResponse()
    resp.headers['Content-Type'] = 'application/json'
    answer = {}
    # copy relevant entries from state dictionary to response
    answer['id'] = request.app['id']
    answer['start_time'] = unixTimeToUTC(app['start_time'])
    answer['last_health_check'] = unixTimeToUTC(app['last_health_check'])
    answer['up_time'] = elapsedTime(app['start_time'])
    answer['cluster_state'] = app['cluster_state']  
    answer['bucket_name'] = app['bucket_name']   
    answer['target_sn_count'] = getTargetNodeCount(app, "sn") 
    answer['active_sn_count'] = getActiveNodeCount(app, "sn")
    answer['target_dn_count'] = getTargetNodeCount(app, "dn") 
    answer['active_dn_count'] = getActiveNodeCount(app, "dn")

    resp = await jsonResponse(request, answer)
    log.response(request, resp=resp)
    return resp

async def register(request):
    """ HTTP method for nodes to register with head node"""
    log.request(request)   
    app = request.app
    text = await request.text()
    # body = await request.json()
    body = json.loads(text)
    log.info("body: {}".format(body))
    if 'id' not in body:
        msg = "Missing 'id'"
        log.response(request, code=400, message=msg)
        raise HttpBadRequest(message=msg)
    if 'port' not in body:
        msg = "missing key 'port'"
        log.response(request, code=400, message=msg)
        raise HttpBadRequest(message=msg)
    if 'node_type' not in body:
        raise HttpBadRequest(message="missing key 'node_type'")
    if body['node_type'] not in ('sn', 'dn'):
        msg="invalid node_type"
        log.response(request, code=400, message=msg)
        raise HttpBadRequest(message=msg)
    
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
                log.info("got free node: {}".format(node))
                node['host'] = host
                node['port'] = body["port"]
                node['id'] =   body["id"]
                node["connected"] = unixTimeToUTC(int(time.time()))
                ret_node = node
                node_ids[body["id"]] = ret_node
                break
 

    if getInactiveNodeCount(app) == 0:
        # all the nodes have checked in
        log.info("setting cluster state to ready")
        app['cluster_state'] = "READY"
         
    resp = StreamResponse()
    resp.headers['Content-Type'] = 'application/json'
    answer = {}
    if ret_node is not None:
        answer["node_number"] = ret_node["node_number"]
    else:
        # all nodes allocated, let caller know it's in the reserve pool
        answer["node_number"] = -1
    if body["node_type"] == "sn":
        answer["node_count"] = app["target_sn_count"]
    else:
        answer["node_count"] = app["target_dn_count"]
        
    resp = await jsonResponse(request, answer)
    log.response(request, resp=resp)
    return resp

async def nodestate(request):
    """HTTP method to return information about registed nodes"""
    log.request(request) 
    node_type = request.match_info.get('nodetype', '*')
    node_number = '*'
    if node_type is not '*':
        node_number = request.match_info.get('nodenumber', '*')
        
    log.info("nodestate/{}/{}".format(node_type, node_number))
    if node_type not in ("sn", "dn", "*"):
        msg="invalid node_type"
        log.response(request, code=400, message=msg)
        raise HttpBadRequest(message=msg)
        
    app = request.app
    resp = StreamResponse()
    resp.headers['Content-Type'] = 'application/json'
    
    if node_number == '*':
        nodes = []
        for node in app["nodes"]:
            if node["node_type"] == node_type or node_type == "*":
                nodes.append(node)
        answer = {"nodes": nodes }
    else:
         answer = {}
         for node in app["nodes"]:
            if node["node_type"] == node_type and str(node["node_number"]) == node_number:
                answer = node
                break
    answer["cluster_state"] = app["cluster_state"]  
    resp = await jsonResponse(request, answer)
    log.response(request, resp=resp)
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
    """Intitialize application and return app object"""
    app = Application(loop=loop)

    # set a bunch of global state 
    app["id"] = createNodeId("head")
    app["cluster_state"] = "INITIALIZING"
    app["start_time"] = int(time.time())  # seconds after epoch 
    app["target_sn_count"] = int(config.get("target_sn_count"))
    app["target_dn_count"] = int(config.get("target_dn_count"))
    app["bucket_name"] = config.get("bucket_name")
    
    nodes = []
    for node_type in ("dn", "sn"):
        target_count = getTargetNodeCount(app, node_type)
        for i in range(target_count):
            node = {"node_number": i,
                "node_type": node_type,
                "host": None,
                "port": None}
            nodes.append(node)
    app["nodes"] = nodes
    app["node_ids"] = {}  # dictionary to look up node by id
    app.router.add_get('/', info)
    app.router.add_get('/nodestate', nodestate)
    app.router.add_get('/nodestate/{nodetype}', nodestate)
    app.router.add_get('/nodestate/{nodetype}/{nodenumber}', nodestate)
    app.router.add_get('/info', info)
    app.router.add_post('/register', register)
    
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


    # get connection to S3
    # app["bucket_name"] = config.get("bucket_name")
    aws_region = config.get("aws_region")
    aws_secret_access_key = config.get("aws_secret_access_key")
    if not aws_secret_access_key or aws_secret_access_key == 'xxx':
        msg="Invalid aws secret access key"
        log.error(msg)
        sys.exit(msg)
    aws_access_key_id = config.get("aws_access_key_id")
    if not aws_access_key_id or aws_access_key_id == 'xxx':
        msg="Invalid aws access key"
        log.error(msg)
        sys.exit(msg)

    session = aiobotocore.get_session(loop=loop)
    aws_client = session.create_client('s3', region_name=aws_region,
                                   aws_secret_access_key=aws_secret_access_key,
                                   aws_access_key_id=aws_access_key_id)
    

    app = loop.run_until_complete(init(loop))
    app['client'] = client
    app['s3'] = aws_client
    asyncio.ensure_future(healthCheck(app), loop=loop)
    head_port = config.get("head_port")
    log.info("Starting service on port: {}".format(head_port))
    run_app(app, port=head_port)
