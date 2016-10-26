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
from datanode_lib import get_metadata_obj, save_metadata_obj
import hsds_logger as log
    

async def GET_Dataset(request):
    """HTTP GET method to return JSON for /groups/
    """
    log.request(request)
    app = request.app
    dset_id = request.match_info.get('id')
    
    if not isValidUuid(dset_id, obj_class="dataset"):
        log.error( "Unexpected type_id: {}".format(dset_id))
        raise HttpProcessingError(code=500, message="Unexpected Error")

    validateInPartition(app, dset_id)
    
    dset_json = await get_metadata_obj(app, dset_id)

    resp_json = { } 
    resp_json["id"] = dset_json["id"]
    resp_json["root"] = dset_json["root"]
    resp_json["created"] = dset_json["created"]
    resp_json["lastModified"] = dset_json["lastModified"]
    resp_json["type"] = dset_json["type"]
    resp_json["shape"] = dset_json["shape"]
    resp_json["attributeCount"] = len(dset_json["attributes"])
    if "domain" in dset_json:
        resp_json["domain"] = dset_json["domain"]
     
    resp = await jsonResponse(request, resp_json)
    log.response(request, resp=resp)
    return resp

async def POST_Dataset(request):
    """ Handler for POST /datasets"""
    log.info("Post_Dataset")
    log.request(request)
    app = request.app

    if not request.has_body:
        msg = "POST_Dataset with no body"
        log.error(msg)
        raise HttpBadRequest(message=msg)

    data = await request.json()
       
    if "root" not in data:
        msg = "POST_Dataset with no root"
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
    
    type_json = data["type"]
    dset_id = data["id"]
    try:
        validateUuid(dset_id, "dataset")
    except ValueError:
        msg = "Invalid type_id: " + dset_id
        log.error(msg)
        raise HttpProcessingError(code=500, message="Unexpected Error")
    
    if "type" not in data:
        msg = "POST_Dataset with no type"
        log.error(msg)
        raise HttpProcessingError(code=500, message="Unexpected Error")
    type_json = data["type"]
    if "shape" not in data:
        msg = "POST_Dataset with no shape"
        log.error(msg)
        raise HttpProcessingError(code=500, message="Unexpected Error")
    shape_json = data["shape"]
    if "domain" in data:
        domain = data["domain"]
        try:
            validateDomain(domain)
        except ValueError:
            msg = "Invalid domain: " + domain
            log.error(msg)
            raise HttpProcessingError(code=500, message="Unexpected Error")

    validateInPartition(app, dset_id)
    
    meta_cache = app['meta_cache'] 
    s3_key = getS3Key(dset_id)
    obj_exists = False
    if dset_id in meta_cache:
        obj_exists = True
    else:
        obj_exists = await isS3Obj(app, s3_key)
    if obj_exists:
        # duplicate uuid?
        msg = "Conflict: resource exists: " + dset_id
        log.error(msg)
        raise HttpProcessingError(code=409, message=msg)

    # ok - all set, create committed type obj
    now = int(time.time())

    log.info("POST_dataset typejson: {}, shapejson: {}".format(type_json, shape_json))
    
    dset_json = {"id": dset_id, "root": root_id, "created": now, "lastModified": now, "type": type_json, "shape": shape_json, "attributes": {} }
    if domain is not None:
        dset_json["domain"] = domain

    # await putS3JSONObj(app, s3_key, group_json)  # write to S3
    dirty_ids = app['dirty_ids']
    dirty_ids[dset_id] = now  # mark to flush to S3

    # save the object to cache
    meta_cache[dset_id] = dset_json

    resp_json = {} 
    resp_json["id"] = dset_id 
    resp_json["root"] = root_id
    resp_json["created"] = dset_json["created"]
    resp_json["type"] = type_json
    resp_json["shape"] = shape_json
    resp_json["lastModified"] = dset_json["lastModified"]
    resp_json["attributeCount"] = 0

    resp = await jsonResponse(request, resp_json, status=201)
    log.response(request, resp=resp)
    return resp


async def DELETE_Dataset(request):
    """HTTP DELETE method for dataset
    """
    log.request(request)
    app = request.app
    dset_id = request.match_info.get('id')

    if not isValidUuid(dset_id, obj_class="dataset"):
        log.error( "Unexpected dataset id: {}".format(dset_id))
        raise HttpProcessingError(code=500, message="Unexpected Error")

    validateInPartition(app, dset_id)

    meta_cache = app['meta_cache'] 
    deleted_ids = app['deleted_ids']
    dirty_ids = app['dirty_ids']
    deleted_ids.add(dset_id)
    
    s3_key = getS3Key(dset_id)
    obj_exists = False
    if dset_id in meta_cache:
        obj_exists = True
    else:
        obj_exists = await isS3Obj(app, s3_key)
    if not obj_exists:
        # duplicate uuid?
        msg = "{} not found".format(dset_id)
        log.response(request, code=404, message=msg)
        raise HttpProcessingError(code=404, message=msg)
    
    await deleteS3Obj(app, s3_key)
     
    if dset_id in meta_cache:
        del meta_cache[dset_id]
    if dset_id in dirty_ids:
        del dirty_ids[dset_id]  # TBD - possible race condition?

    resp_json = {  } 
      
    resp = await jsonResponse(request, resp_json)
    log.response(request, resp=resp)
    return resp

async def PUT_DatasetShape(request):
    """HTTP method to update dataset's shape"""
    log.request(request)
    app = request.app
    dset_id = request.match_info.get('id')
    
    if not isValidUuid(dset_id, obj_class="dataset"):
        log.error( "Unexpected type_id: {}".format(dset_id))
        raise HttpProcessingError(code=500, message="Unexpected Error")

    validateInPartition(app, dset_id)
    
    dset_json = await get_metadata_obj(app, dset_id)

    data = await request.json()
     
    shape_update = data["shape"]
     
    log.info("shape_update: {}".format(shape_update))

    
    shape_orig = dset_json["shape"]
    log.info("shape_orig: {}".format(shape_orig))

    # verify that the extend request is still valid
    # e.g. another client has already extended the shape since the SN
    # verified it
    dims = shape_orig["dims"]
      
    for i in range(len(dims)):
        if shape_update[i] < dims[i]:
            msg = "Dataspace can not be made smaller"
            log.warn(msg)
            raise HttpBadRequest(message=msg)

    # Update the shape!
    for i in range(len(dims)):    
        dims[i] = shape_update[i]
         
    # write back to S3, save to metadata cache
    await save_metadata_obj(app, dset_json)
 
    resp_json = { } 

    resp = await jsonResponse(request, resp_json, status=201)
    log.response(request, resp=resp)
    return resp
   