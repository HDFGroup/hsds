#
# service node of hsds cluster
# 
import asyncio
import json
import time
import sys
 

from aiohttp.web import Application, Response, StreamResponse, run_app
from aiohttp import ClientSession, TCPConnector, HttpProcessingError 
from aiohttp.errors import HttpBadRequest, ClientError
 

import config
from timeUtil import unixTimeToUTC, elapsedTime
from hsdsUtil import http_get, isOK, http_post, http_get_json, jsonResponse, getS3Partition, validateUuid, isValidUuid
from authUtil import getUserFromRequest
from domainUtil import getParentDomain, getDomainFromRequest
from basenode import register, healthCheck, info, baseInit
import hsds_logger as log


def getDataNodeUrl(app, obj_id):
    """ Return host/port for datanode for given obj_id.
    Throw exception if service is not ready"""
    dn_urls = app["dn_urls"]
    node_number = app["node_number"]
    if app["node_state"] != "READY" or node_number not in dn_urls:
        print("Node_state:", app["node_state"])
        print("node_number:", node_number)
        msg="Service not ready"
        log.warn(msg)
        raise HttpProcessingError(message=msg, code=503)
    dn_number = getS3Partition(obj_id, app['node_count'])
      
    url = dn_urls[node_number]
    log.info("got dn url: {}".format(url))
    return url

async def getDomain(request):
    """HTTP method to return JSON for given domain"""
    log.request(request)
    app = request.app
    user = getUserFromRequest(request)
    if user:
        log.info("user: {}".format(user))
    print("query_string:", request.query_string)
    if 'myquery' in request.GET:
        print("myquery:", request.GET['myquery'])
    
    domain = getDomainFromRequest(request)
    if isValidUuid(domain):
        # valid uuid's are not valid domains'
        msg = "Invalid host value: {}".format(domain)
        log.warn(msg)
        raise HttpBadRequest(message=msg)
    
    domain_json = { }
    req = getDataNodeUrl(app, domain)
    req += "/domains/" + domain 
    print("datanode uri", req)
    try:
        domain_json = await http_get_json(app, req)
    except ClientError as ce:
        msg="Error getting domain state -- " + str(ce)
        log.warn(msg)
        raise HttpProcessingError(message=msg, code=503)
    resp = await jsonResponse(request, domain_json)
    log.response(request, resp=resp)
    return resp



async def getGroup(request):
    """HTTP method to return JSON for group"""
    log.request(request)
    app = request.app 

    group_id = request.match_info.get('id')
    if not group_id:
        msg = "Missing group id"
        log.warn(msg)
        raise HttpBadRequest(message=msg)
    if not isValidUuid(group_id) or not group_id.startswith("g-"):
        msg = "Invalid group id: {}".format(group_id)
        log.warn(msg)
        raise HttpBadRequest(message=msg)

    req = getDataNodeUrl(app, group_id)
    req += "/groups/" + group_id
    group_json = {} 
    try:
        group_json = await http_get_json(app, req)
    except ClientError as ce:
        msg="Error getting group state -- " + str(ce)
        log.warn(msg)
        raise HttpProcessingError(message=msg, code=503)
 
    resp = await jsonResponse(request, group_json)
    log.response(request, resp=resp)
    return resp

async def init(loop):
    """Intitialize application and return app object"""
    app = baseInit(loop, 'sn')

    #
    # call app.router.add_get() here to add node-specific routes
    #
    app.router.add_route('GET', '/', getDomain)
    app.router.add_route('GET', '/groups/{id}', getGroup)
    #app.router.add_route('POST', '/groups', createGroup)
      
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
    run_app(app, port=config.get("sn_port"))
