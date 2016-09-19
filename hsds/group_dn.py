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
from botocore.exceptions import ClientError
 
 
from util.idUtil import getObjPartition, getS3Key, validateUuid
from util.httpUtil import jsonResponse
from util.s3Util import getS3JSONObj, putS3JSONObj, isS3Obj, deleteS3Obj 
from util.domainUtil import   validateDomain
import hsds_logger as log
    

async def GET_Group(request):
    """HTTP GET method to return JSON for /groups/
    """
    log.request(request)
    app = request.app
    group_id = request.match_info.get('id')
    validateUuid(group_id, "group")
    
    if getObjPartition(group_id, app['node_count']) != app['node_number']:
        # The request shouldn't have come to this node'
        raise HttpBadRequest(message="wrong node for 'id':{}".format(group_id))

    deleted_ids = app['deleted_ids']
    if group_id in deleted_ids:
        msg = "{} has been deleted".format(group_id)
        log.response(request, code=510, message=msg)
        raise HttpProcessingError(code=510, message=msg)

    meta_cache = app['meta_cache'] 
    group_json = None 
    if group_id in meta_cache:
        log.info("{} found in meta cache".format(group_id))
        group_json = meta_cache[group_id]
    else:
        try:
            s3_key = getS3Key(group_id)
            log.info("getS3JSONObj({})".format(s3_key))
            group_json = await getS3JSONObj(app, s3_key)
        except ClientError as ce:
            # key does not exist?
            is_s3obj = await isS3Obj(app, s3_key)
            if is_s3obj:
                msg = "Error getting s3 obj: " + str(ce)
                log.response(request, code=500, message=msg)
                raise HttpProcessingError(code=500, message=msg)
            # not a S3 Key
            msg = "{} not found".format(group_id)
            log.response(request, code=404, message=msg)
            raise HttpProcessingError(code=404, message=msg)
        meta_cache[group_id] = group_json  # add to cache

    resp_json = { } 
    resp_json["id"] = group_json["id"]
    resp_json["root"] = group_json["root"]
    resp_json["created"] = group_json["created"]
    resp_json["lastModified"] = group_json["lastModified"]
    resp_json["linkCount"] = 0  # TBD
    resp_json["attributeCount"] = 0 # TBD
    if "domain" in group_json:
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
    #body = await request.read()
    #data = json.loads(body)
    
    root_id = None
    group_id = None
    domain = None
    
    if "root" not in data:
        msg = "POST_Group with no root"
        log.error(msg)
        raise HttpBadRequest(message=msg)
    root_id = data["root"]
    try:
        validateUuid(root_id, "group")
    except ValueError:
        msg = "Invalid root_id: " + root_id
        log.error(msg)
        raise HttpBadRequest(message=msg)
    if "id" not in data:
        msg = "POST_Group with no id"
        log.error(msg)
        raise HttpBadRequest(message=msg)
    group_id = data["id"]
    try:
        validateUuid(group_id, "group")
    except ValueError:
        msg = "Invalid group_id: " + group_id
        log.error(msg)
        raise HttpBadRequest(message=msg)
    if "domain" in data:
        domain = data["domain"]
        try:
            validateDomain(domain)
        except ValueError:
            msg = "Invalid domain: " + domain
            log.error(msg)
            raise HttpBadRequest(message=msg)

    if getObjPartition(group_id, app['node_count']) != app['node_number']:
        # The request shouldn't have come to this node'
        raise HttpBadRequest(message="wrong node for 'id':{}".format(group_id))

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
    now = int(time.time())
    
    group_json = {"id": group_id, "root": root_id, "created": now, "lastModified": now, "links": [], "attributes": [] }
    if domain is not None:
        group_json["domain"] = domain

    await putS3JSONObj(app, s3_key, group_json)  # write to S3

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
    validateUuid(group_id, "group")
    
    if getObjPartition(group_id, app['node_count']) != app['node_number']:
        # The request shouldn't have come to this node'
        raise HttpBadRequest(message="wrong node for 'id':{}".format(group_id))

    meta_cache = app['meta_cache'] 
    deleted_ids = app['deleted_ids']
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
    
    try:
        log.info("deleteS3Obj({})".format(s3_key))
        await deleteS3Obj(app, s3_key)
    except ClientError as ce:
        # key does not exist? 
        msg = "Error deleting s3 obj: " + str(ce)
        log.response(request, code=500, message=msg)
        raise HttpProcessingError(code=500, message=msg)

    if group_id in meta_cache:
        del meta_cache[group_id]

    resp_json = {  } 
      
    resp = await jsonResponse(request, resp_json)
    log.response(request, resp=resp)
    return resp
   