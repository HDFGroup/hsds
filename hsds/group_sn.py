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
 

from aiohttp import HttpProcessingError 
from aiohttp.errors import HttpBadRequest, ClientError
 
from util.httpUtil import  http_get_json, jsonResponse
from util.idUtil import   isValidUuid, getDataNodeUrl
from util.authUtil import getUserPasswordFromRequest, aclCheck, validateUserPassword
from util.domainUtil import  getDomainFromRequest, isValidDomain
from servicenode_lib import getDomainJson
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
    if not isValidUuid(group_id) or not group_id.startswith("g-"):
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
 