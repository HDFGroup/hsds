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
# data node of hsds cluster
# 
import time

from aiohttp.errors import HttpBadRequest, HttpProcessingError
 
from util.idUtil import isValidUuid 
from util.httpUtil import jsonResponse
from util.domainUtil import validateDomain
from datanode_lib import get_obj_id, check_metadata_obj, get_metadata_obj, save_metadata_obj, delete_metadata_obj
import hsds_logger as log
    

async def GET_Group(request):
    """HTTP GET method to return JSON for /groups/
    """
    log.request(request)
    app = request.app
    group_id = get_obj_id(request)
    log.info("GET group: {}".format(group_id))
    
    if not isValidUuid(group_id, obj_class="group"):
        log.error( "Unexpected group_id: {}".format(group_id))
        raise HttpProcessingError(code=500, message="Unexpected Error")
    
    group_json = await get_metadata_obj(app, group_id)

    resp_json = { } 
    resp_json["id"] = group_json["id"]
    resp_json["root"] = group_json["root"]
    resp_json["created"] = group_json["created"]
    resp_json["lastModified"] = group_json["lastModified"]
    resp_json["linkCount"] = len(group_json["links"])
    resp_json["attributeCount"] = len(group_json["attributes"])
    resp_json["domain"] = group_json["domain"]
     
    resp = await jsonResponse(request, resp_json)
    log.response(request, resp=resp)
    return resp

async def POST_Group(request):
    """ Handler for POST /groups"""
    log.request(request)
    app = request.app

    if not request.has_body:
        msg = "POST_Group with no body"
        log.warn(msg)
        raise HttpBadRequest(message=msg)

    body = await request.json()

    group_id = get_obj_id(request, body=body)
    log.info("POST group: {}".format(group_id))
    if not isValidUuid(group_id, obj_class="group"):
        log.error( "Unexpected group_id: {}".format(group_id))
        raise HttpProcessingError(code=500, message="Unexpected Error")

    try:
        # verify the id doesn't already exist
        await check_metadata_obj(app, group_id)
        log.error( "Post with existing group_id: {}".format(group_id))
        raise HttpProcessingError(code=500, message="Unexpected Error")
    except HttpProcessingError:
        pass  # expected
    
    root_id = None
    domain = None
    
    if "root" not in body:
        msg = "POST_Group with no root"
        log.error(msg)
        raise HttpProcessingError(code=500, message="Unexpected Error")
    root_id = body["root"]
    
    if not isValidUuid(root_id, obj_class="group"):
        msg = "Invalid root_id: " + root_id
        log.error(msg)
        raise HttpProcessingError(code=500, message="Unexpected Error")
     
    if "domain" not in body:
        log.error("POST_Group with no domain")
        raise HttpProcessingError(code=500, message="Unexpected Error")
    domain = body["domain"]
    try:
        validateDomain(domain)
    except ValueError:
        msg = "Invalid domain: " + domain
        log.error(msg)
        raise HttpBadRequest(message=msg)

    # ok - all set, create group obj
    now = time.time()
    
    group_json = {"id": group_id, "root": root_id, "created": now, "lastModified": now, "domain": domain, 
        "links": {}, "attributes": {} }

    save_metadata_obj(app, group_id, group_json)
     
    # formulate response 
    resp_json = {} 
    resp_json["id"] = group_id 
    resp_json["root"] = root_id
    resp_json["created"] = group_json["created"]
    resp_json["lastModified"] = group_json["lastModified"]
    resp_json["linkCount"] = 0  
    resp_json["attributeCount"] = 0

    resp = await jsonResponse(request, resp_json, status=201)
    log.response(request, resp=resp)
    return resp


async def DELETE_Group(request):
    """HTTP DELETE method for /groups/
    """
    log.request(request)
    app = request.app
    group_id = get_obj_id(request)
    log.info("DELETE group: {}".format(group_id))

    if not isValidUuid(group_id, obj_class="group"):
        log.error( "Unexpected group_id: {}".format(group_id))
        raise HttpProcessingError(code=500, message="Unexpected Error")

    # verify the id exist
    await check_metadata_obj(app, group_id)
        
    log.debug("deleting group: {}".format(group_id))

    notify=True
    if "Notify" in request.GET and not request.GET["Notify"]:
        notify=False
    await delete_metadata_obj(app, group_id, notify=notify)

    resp_json = {  } 
      
    resp = await jsonResponse(request, resp_json)
    log.response(request, resp=resp)
    return resp
   
