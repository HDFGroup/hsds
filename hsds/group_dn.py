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

from aiohttp import HttpProcessingError  
from aiohttp.errors import HttpBadRequest
 
from util.idUtil import validateInPartition, getS3Key, isValidUuid, validateUuid
from util.httpUtil import jsonResponse
from util.s3Util import  isS3Obj, deleteS3Obj 
from util.domainUtil import   validateDomain
from datanode_lib import get_metadata_obj
import hsds_logger as log
    

async def GET_Group(request):
    """HTTP GET method to return JSON for /groups/
    """
    log.request(request)
    app = request.app
    group_id = request.match_info.get('id')
    
    if not isValidUuid(group_id, obj_class="group"):
        log.error( "Unexpected group_id: {}".format(group_id))
        raise HttpProcessingError(code=500, message="Unexpected Error")

    validateInPartition(app, group_id)
    
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

    data = await request.json()
    
    root_id = None
    group_id = None
    domain = None
    
    if "root" not in data:
        msg = "POST_Group with no root"
        log.error(msg)
        raise HttpProcessingError(code=500, message="Unexpected Error")
    root_id = data["root"]
    try:
        validateUuid(root_id, "group")
    except ValueError:
        msg = "Invalid root_id: " + root_id
        log.error(msg)
        raise HttpProcessingError(code=500, message="Unexpected Error")
    if "id" not in data:
        msg = "POST_Group with no id"
        log.error(msg)
        raise HttpProcessingError(code=500, message="Unexpected Error")
    group_id = data["id"]
    try:
        validateUuid(group_id, "group")
    except ValueError:
        msg = "Invalid group_id: " + group_id
        log.error(msg)
        raise HttpProcessingError(code=500, message="Unexpected Error")
    if "domain" not in data:
        log.error("POST_Group with no domain")
        raise HttpProcessingError(code=500, message="Unexpected Error")
    domain = data["domain"]
    try:
        validateDomain(domain)
    except ValueError:
        msg = "Invalid domain: " + domain
        log.error(msg)
        raise HttpBadRequest(message=msg)

    validateInPartition(app, group_id)
    
    meta_cache = app['meta_cache'] 
    s3_key = getS3Key(group_id)
    obj_exists = False
    if group_id in meta_cache:
        obj_exists = True
    else:
        obj_exists = await isS3Obj(app, s3_key)
    if obj_exists:
        # duplicate uuid?
        msg = "Conflict: resource exists: " + group_id
        log.error(msg)
        raise HttpProcessingError(code=409, message=msg)

    # ok - all set, create group obj
    now = time.time()
    
    group_json = {"id": group_id, "root": root_id, "created": now, "lastModified": now, "domain": domain, 
        "links": {}, "attributes": {} }
     
    # await putS3JSONObj(app, s3_key, group_json)  # write to S3
    dirty_ids = app['dirty_ids']
    dirty_ids[group_id] = now  # mark to flush to S3

    # save the object to cache
    meta_cache[group_id] = group_json

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
    group_id = request.match_info.get('id')

    if not isValidUuid(group_id, obj_class="group"):
        log.error( "Unexpected group_id: {}".format(group_id))
        raise HttpProcessingError(code=500, message="Unexpected Error")

    validateInPartition(app, group_id)

    meta_cache = app['meta_cache'] 
    deleted_ids = app['deleted_ids']
    dirty_ids = app['dirty_ids']
    deleted_ids.add(group_id)
    
    s3_key = getS3Key(group_id)
    obj_exists = False
    if group_id in meta_cache:
        obj_exists = True
    else:
        obj_exists = await isS3Obj(app, s3_key)
    if not obj_exists:
        # duplicate uuid?
        msg = "{} not found".format(group_id)
        log.response(request, code=404, message=msg)
        raise HttpProcessingError(code=404, message=msg)
    
    await deleteS3Obj(app, s3_key)
     
    if group_id in meta_cache:
        del meta_cache[group_id]
    if group_id in dirty_ids:
        del dirty_ids[group_id]  # TBD - possible race condition?

    resp_json = {  } 
      
    resp = await jsonResponse(request, resp_json)
    log.response(request, resp=resp)
    return resp
   
