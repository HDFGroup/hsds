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
from hsdsUtil import http_get, isOK, http_post, http_put, http_get_json, jsonResponse, getS3Partition, validateUuid, isValidUuid, getDataNodeUrl
from authUtil import getUserFromRequest, authValidate, aclCheck
from domainUtil import getParentDomain, getDomainFromRequest, getDomainJson, isValidDomain
from basenode import register, healthCheck, info, baseInit
import hsds_logger as log


async def GET_Domain(request):
    """HTTP method to return JSON for given domain"""
    log.request(request)
    app = request.app
    await authValidate(request)
     
    #print("query_string:", request.query_string)
    #if 'myquery' in request.GET:
    #    print("myquery:", request.GET['myquery'])
    
    domain = getDomainFromRequest(request)
    if not isValidDomain(domain):
        msg = "Invalid host value: {}".format(domain)
        log.warn(msg)
        raise HttpBadRequest(message=msg)
    
    domain_json = await getDomainJson(app, domain)
    
    resp = await jsonResponse(request, domain_json)
    log.response(request, resp=resp)
    return resp

async def PUT_Domain(request):
    """HTTP method to create a new domain"""
    log.request(request)
    app = request.app
    # use getUserFromRequest rather than authValidate here becuase the domain does not
    # yet exist
    # await authValidate(request)
    req_user = getUserFromRequest(request) # throws exception if user/password is not valid
    log.info("PUT domain request from: {}".format(req_user))
    print("getdomain from req") 
    domain = getDomainFromRequest(request)
    print("got domain", domain)
    if not isValidDomain(domain):
        msg = "Invalid host value: {}".format(domain)
        log.warn(msg)
        raise HttpBadRequest(message=msg)

    parent_domain = getParentDomain(domain)
    if parent_domain is None:
        msg = "creation of top-level domains is not supported"
        log.warn(msg)
        raise HttpBadRequest(message=msg)
    print("parent_domain:", parent_domain)
    parent_json = None
    try:
        print("get parent domain", parent_domain)
        parent_json = await getDomainJson(app, parent_domain)
    except HttpProcessingError as hpe:
        print("error getting parent domain: {}".format(hpe.code))
        msg = "Parent domain not found"
        log.warn(msg)
        raise HttpProcessingError(code=404, message=msg)

    print("got parent json:", parent_json)
    if "acls" not in parent_json:
        log.warn("acls not found in domain: {}".format(parent_domain))
        raise HttpProcessingError(code=404, message="Forbidden")
    aclCheck(parent_json["acls"], "create", req_user)  # throws exception is not allowed
    
    domain_json = { }

    # construct dn request to create new domain
    req = getDataNodeUrl(app, domain)
    req += "/domains/" + domain 
    body = { "owner": req_user }
    body["acls"] = parent_json["acls"]  # copy parent acls to new domain

    try:
        domain_json = await http_put(app, req, body)
    except HttpProcessingError as ce:
        msg="Error creating domain state -- " + str(ce)
        log.warn(msg)
        raise ce

    # domain creation successful     
    resp = await jsonResponse(request, domain_json)
    log.response(request, resp=resp)
    return resp



async def GET_Group(request):
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
    app.router.add_route('GET', '/', GET_Domain)
    app.router.add_route('PUT', '/', PUT_Domain)
    app.router.add_route('GET', '/groups/{id}', GET_Group)
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
    app['domain_cache'] = {}

    # run background task
    asyncio.ensure_future(healthCheck(app), loop=loop)
   
    # run the app
    run_app(app, port=config.get("sn_port"))
