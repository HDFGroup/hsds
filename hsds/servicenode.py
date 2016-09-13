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
from util.timeUtil import unixTimeToUTC, elapsedTime
from util.httpUtil import http_get, isOK, http_post, http_put, http_get_json, jsonResponse
from util.idUtil import  getObjPartition, validateUuid, isValidUuid, getDataNodeUrl
from util.authUtil import getUserPasswordFromRequest, aclCheck, validateUserPassword
from util.domainUtil import getParentDomain, getDomainFromRequest, isValidDomain
from basenode import register, healthCheck, info, baseInit
import hsds_logger as log

async def getDomainJson(app, domain):
    """ Return domain JSON from cache or fetch from DN if not found
    """
    log.info("getDomainJson({})".format(domain))
    domain_cache = app["domain_cache"]
    #domain = getDomainFromRequest(request)

    if domain in domain_cache:
        log.info("returning domain_cache value")
        return domain_cache[domain]

    domain_json = { }
    req = getDataNodeUrl(app, domain)
    req += "/domains/" + domain 
    log.info("sending dn req: {}".format(req))
    try:
        domain_json = await http_get_json(app, req)
    except ClientError as ce:
        msg="Error getting domain state -- " + str(ce)
        log.warn(msg)
        raise HttpProcessingError(message=msg, code=503)
    if 'owner' not in domain_json:
        log.warn("No owner key found in domain")
        raise HttpProcessingError("Unexpected error", code=500)

    if 'acls' not in domain_json:
        log.warn("No acls key found in domain")
        raise HttpProcessingError("Unexpected error", code=500)

    domain_cache[domain] = domain_json  # add to cache
    return domain_json

async def GET_Domain(request):
    """HTTP method to return JSON for given domain"""
    log.request(request)
    app = request.app
    (username, pswd) = getUserPasswordFromRequest(request)
    validateUserPassword(username, pswd)
    domain = getDomainFromRequest(request)
    if not isValidDomain(domain):
        msg = "Invalid host value: {}".format(domain)
        log.warn(msg)
        raise HttpBadRequest(message=msg)
    
    domain_json = await getDomainJson(app, domain)
    # validate that the requesting user has permission to read this domain
    aclCheck(domain_json, "read", username)

    resp = await jsonResponse(request, domain_json)
    log.response(request, resp=resp)
    return resp

async def PUT_Domain(request):
    """HTTP method to create a new domain"""
    log.request(request)
    app = request.app
    # yet exist
    username, pswd = getUserPasswordFromRequest(request) # throws exception if user/password is not valid
    validateUserPassword(username, pswd)
    log.info("PUT domain request from: {}".format(username))
    domain = getDomainFromRequest(request)
    if not isValidDomain(domain):
        msg = "Invalid host value: {}".format(domain)
        log.warn(msg)
        raise HttpBadRequest(message=msg)

    parent_domain = getParentDomain(domain)
    if parent_domain is None:
        msg = "creation of top-level domains is not supported"
        log.warn(msg)
        raise HttpBadRequest(message=msg)
    log.info("parent_domain: {}".format(parent_domain))
    parent_json = None
    try:
        log.info("get parent domain {}".format(parent_domain))
        parent_json = await getDomainJson(app, parent_domain)
    except HttpProcessingError as hpe:
        msg = "Parent domain not found"
        log.warn(msg)
        raise HttpProcessingError(code=404, message=msg)

    aclCheck(parent_json, "create", username)  # throws exception if not allowed
    
    domain_json = { }

    # construct dn request to create new domain
    req = getDataNodeUrl(app, domain)
    req += "/domains/" + domain 
    body = { "owner": username }
    body["acls"] = parent_json["acls"]  # copy parent acls to new domain

    try:
        domain_json = await http_put(app, req, body)
    except HttpProcessingError as ce:
        msg="Error creating domain state -- " + str(ce)
        log.warn(msg)
        raise ce

    # domain creation successful     
    resp = await jsonResponse(request, domain_json, status=201)
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

    username, pswd = getUserPasswordFromRequest(request)
    validateUserPassword(username, pswd)
    
    domain = getDomainFromRequest(request)
    if not isValidDomain(domain):
        msg = "Invalid host value: {}".format(domain)
        log.warn(msg)
        raise HttpBadRequest(message=msg)
    
    domain_json = await getDomainJson(app, domain)
    aclCheck(domain_json, "read", username)  # throws exception if not allowed

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
