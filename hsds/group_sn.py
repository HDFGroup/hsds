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
 
import json
from aiohttp.errors import HttpBadRequest, HttpProcessingError
 
from util.httpUtil import http_post, http_delete, jsonResponse
from util.idUtil import   isValidUuid, getDataNodeUrl, createObjId
from util.authUtil import getUserPasswordFromRequest, aclCheck, validateUserPassword
from util.domainUtil import  getDomainFromRequest, isValidDomain
from servicenode_lib import getDomainJson, getObjectJson, validateAction
import hsds_logger as log



async def GET_Group(request):
    """HTTP method to return JSON for group"""
    log.request(request)
    app = request.app 

    group_id = request.match_info.get('id')
    if not group_id:
        msg = "Missing group id"
        log.warn(msg)
        raise HttpBadRequest(message=msg)
    if not isValidUuid(group_id, "Group"):
        msg = "Invalid group id: {}".format(group_id)
        log.warn(msg)
        raise HttpBadRequest(message=msg)

    username, pswd = getUserPasswordFromRequest(request)
    if username is None and app['allow_noauth']:
        username = "default"
    else:
        validateUserPassword(username, pswd)

    domain = getDomainFromRequest(request)
    if not isValidDomain(domain):
        msg = "Invalid host value: {}".format(domain)
        log.warn(msg)
        raise HttpBadRequest(message=msg)
    
    # get authoritative state for group from DN (even if it's in the meta_cache).
    group_json = await getObjectJson(app, group_id, refresh=True)  

    await validateAction(app, domain, group_id, username, "read")

    resp = await jsonResponse(request, group_json)
    log.response(request, resp=resp)
    return resp

async def POST_Group(request):
    """HTTP method to return JSON for group"""
    log.request(request)
    app = request.app

    username, pswd = getUserPasswordFromRequest(request)
    # write actions need auth
    validateUserPassword(username, pswd)

    domain = getDomainFromRequest(request)
    if not isValidDomain(domain):
        msg = "Invalid host value: {}".format(domain)
        log.warn(msg)
        raise HttpBadRequest(message=msg)
    
    domain_json = await getDomainJson(app, domain)

    aclCheck(domain_json, "create", username)  # throws exception if not allowed

    if "root" not in domain_json:
        msg = "Expected root key for domain: {}".format(domain)
        log.warn(msg)
        raise HttpBadRequest(message=msg)

    root_id = domain_json["root"]
    group_id = createObjId("group") 
    log.info("new  group id: {}".format(group_id))
    group_json = {"id": group_id, "root": root_id, "domain": domain }
    log.info("create group, body: " + json.dumps(group_json))
    req = getDataNodeUrl(app, group_id) + "/groups"
    
    group_json = await http_post(app, req, group_json)
    
    # domain creation successful     
    resp = await jsonResponse(request, group_json, status=201)
    log.response(request, resp=resp)
    return resp

async def DELETE_Group(request):
    """HTTP method to delete a group resource"""
    log.request(request)
    app = request.app 
    meta_cache = app['meta_cache']

    group_id = request.match_info.get('id')
    if not group_id:
        msg = "Missing group id"
        log.warn(msg)
        raise HttpBadRequest(message=msg)
    if not isValidUuid(group_id, "Group"):
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
    
    # get domain JSON
    domain_json = await getDomainJson(app, domain)
    if "root" not in domain_json:
        log.error("Expected root key for domain: {}".format(domain))
        raise HttpBadRequest(message="Unexpected Error")

    # TBD - verify that the obj_id belongs to the given domain
    await validateAction(app, domain, group_id, username, "delete")

    if group_id == domain_json["root"]:
        msg = "Forbidden - deletion of root group is not allowed - delete domain first"
        log.warn(msg)
        raise HttpProcessingError(code=403, message=msg)

    req = getDataNodeUrl(app, group_id)
    req += "/groups/" + group_id
 
    rsp_json = await http_delete(app, req)

    if group_id in meta_cache:
        del meta_cache[group_id]  # remove from cache
 
    resp = await jsonResponse(request, rsp_json)
    log.response(request, resp=resp)
    return resp

 