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
    

async def GET_Datatype(request):
    """HTTP GET method to return JSON for /groups/
    """
    log.request(request)
    app = request.app
    ctype_id = request.match_info.get('id')
    
    if not isValidUuid(ctype_id, obj_class="type"):
        log.error( "Unexpected type_id: {}".format(ctype_id))
        raise HttpProcessingError(code=500, message="Unexpected Error")

    validateInPartition(app, ctype_id)
    
    ctype_json = await get_metadata_obj(app, ctype_id)

    resp_json = { } 
    resp_json["id"] = ctype_json["id"]
    resp_json["root"] = ctype_json["root"]
    resp_json["created"] = ctype_json["created"]
    resp_json["lastModified"] = ctype_json["lastModified"]
    resp_json["type"] = ctype_json["type"]
    resp_json["attributeCount"] = len(ctype_json["attributes"])
    resp_json["domain"] = ctype_json["domain"]
     
    resp = await jsonResponse(request, resp_json)
    log.response(request, resp=resp)
    return resp

async def POST_Datatype(request):
    """ Handler for POST /datatypes"""
    log.info("Post_Datatype")
    log.request(request)
    app = request.app

    if not request.has_body:
        msg = "POST_Datatype with no body"
        log.error(msg)
        raise HttpBadRequest(message=msg)

    data = await request.json()
    #body = await request.read()
    #data = json.loads(body)
    
    root_id = None
    ctype_id = None
    domain = None
    
    if "root" not in data:
        msg = "POST_Datatype with no root"
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
        msg = "POST_Dataset with no id"
        log.error(msg)
        raise HttpProcessingError(code=500, message="Unexpected Error")
    if "type" not in data:
        msg = "POST_Datatype with no type"
        log.error(msg)
        raise HttpProcessingError(code=500, message="Unexpected Error")
    type_json = data["type"]
    ctype_id = data["id"]
    try:
        validateUuid(ctype_id, "type")
    except ValueError:
        msg = "Invalid type_id: " + ctype_id
        log.error(msg)
        raise HttpProcessingError(code=500, message="Unexpected Error")
    if "domain" not in data:
        msg = "POST_Datatype with no domain key"
        log.error(msg)
        raise HttpProcessingError(code=500, message="Unexpected Error")

    domain = data["domain"]
    try:
        validateDomain(domain)
    except ValueError:
        msg = "Invalid domain: " + domain
        log.error(msg)
        raise HttpProcessingError(code=500, message="Unexpected Error")

    validateInPartition(app, ctype_id)
    
    meta_cache = app['meta_cache'] 
    s3_key = getS3Key(ctype_id)
    obj_exists = False
    if ctype_id in meta_cache:
        obj_exists = True
    else:
        obj_exists = await isS3Obj(app, s3_key)
    if obj_exists:
        # duplicate uuid?
        msg = "Conflict: resource exists: " + ctype_id
        log.error(msg)
        raise HttpProcessingError(code=409, message=msg)

    # ok - all set, create committed type obj
    now = time.time()

    log.info("POST_datatype, typejson: {}".format(type_json))
    
    ctype_json = {"id": ctype_id, "root": root_id, "created": now, "lastModified": now, "type": type_json, "attributes": {} }
    if domain is not None:
        ctype_json["domain"] = domain

    # await putS3JSONObj(app, s3_key, group_json)  # write to S3
    dirty_ids = app['dirty_ids']
    dirty_ids[ctype_id] = now  # mark to flush to S3

    # save the object to cache
    meta_cache[ctype_id] = ctype_json

    resp_json = {} 
    resp_json["id"] = ctype_id 
    resp_json["root"] = root_id
    resp_json["created"] = ctype_json["created"]
    resp_json["lastModified"] = ctype_json["lastModified"]
    resp_json["attributeCount"] = 0

    resp = await jsonResponse(request, resp_json, status=201)
    log.response(request, resp=resp)
    return resp


async def DELETE_Datatype(request):
    """HTTP DELETE method for datatype
    """
    log.request(request)
    app = request.app
    ctype_id = request.match_info.get('id')

    if not isValidUuid(ctype_id, obj_class="type"):
        log.error( "Unexpected datatype_id: {}".format(ctype_id))
        raise HttpProcessingError(code=500, message="Unexpected Error")

    validateInPartition(app, ctype_id)

    meta_cache = app['meta_cache'] 
    deleted_ids = app['deleted_ids']
    dirty_ids = app['dirty_ids']
    deleted_ids.add(ctype_id)
    
    s3_key = getS3Key(ctype_id)
    obj_exists = False
    if ctype_id in meta_cache:
        obj_exists = True
    else:
        obj_exists = await isS3Obj(app, s3_key)
    if not obj_exists:
        # duplicate uuid?
        msg = "{} not found".format(ctype_id)
        log.response(request, code=404, message=msg)
        raise HttpProcessingError(code=404, message=msg)
    
    await deleteS3Obj(app, s3_key)
     
    if ctype_id in meta_cache:
        del meta_cache[ctype_id]
    if ctype_id in dirty_ids:
        del dirty_ids[ctype_id]  # TBD - possible race condition?

    resp_json = {  } 
      
    resp = await jsonResponse(request, resp_json)
    log.response(request, resp=resp)
    return resp
   
