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
 
from util.idUtil import isValidUuid, validateUuid
from util.httpUtil import jsonResponse
from datanode_lib import get_obj_id, check_metadata_obj, get_metadata_obj, save_metadata_obj, delete_metadata_obj
import hsds_logger as log
    

async def GET_Dataset(request):
    """HTTP GET method to return JSON for /groups/
    """
    log.request(request)
    app = request.app
    dset_id = get_obj_id(request)
    
    if not isValidUuid(dset_id, obj_class="dataset"):
        log.error( "Unexpected type_id: {}".format(dset_id))
        raise HttpProcessingError(code=500, message="Unexpected Error")
    
    dset_json = await get_metadata_obj(app, dset_id)

    resp_json = { } 
    resp_json["id"] = dset_json["id"]
    resp_json["root"] = dset_json["root"]
    resp_json["created"] = dset_json["created"]
    resp_json["lastModified"] = dset_json["lastModified"]
    resp_json["type"] = dset_json["type"]
    resp_json["shape"] = dset_json["shape"]
    resp_json["attributeCount"] = len(dset_json["attributes"])
    if "creationProperties" in dset_json:
        resp_json["creationProperties"] = dset_json["creationProperties"]
    if "layout" in dset_json:
        resp_json["layout"] = dset_json["layout"]
     
    resp = await jsonResponse(request, resp_json)
    log.response(request, resp=resp)
    return resp

async def POST_Dataset(request):
    """ Handler for POST /datasets"""
    log.request(request)
    app = request.app

    if not request.has_body:
        msg = "POST_Dataset with no body"
        log.error(msg)
        raise HttpBadRequest(message=msg)

    body = await request.json()
    log.info("POST_Dataset, body: {}".format(body))

    dset_id = get_obj_id(request, body=body)
    if not isValidUuid(dset_id, obj_class="dataset"):
        log.error( "Unexpected dataset_id: {}".format(dset_id))
        raise HttpProcessingError(code=500, message="Unexpected Error")

    try:
        # verify the id doesn't already exist
        await check_metadata_obj(app, dset_id)
        log.error( "Post with existing dset_id: {}".format(dset_id))
        raise HttpProcessingError(code=500, message="Unexpected Error")
    except HttpProcessingError:
        pass  # expected
       
    if "root" not in body:
        msg = "POST_Dataset with no root"
        log.error(msg)
        raise HttpProcessingError(code=500, message="Unexpected Error")
    root_id = body["root"]
    try:
        validateUuid(root_id, "group")
    except ValueError:
        msg = "Invalid root_id: " + root_id
        log.error(msg)
        raise HttpProcessingError(code=500, message="Unexpected Error")
    
    if "type" not in body:
        msg = "POST_Dataset with no type"
        log.error(msg)
        raise HttpProcessingError(code=500, message="Unexpected Error")
    type_json = body["type"]
    if "shape" not in body:
        msg = "POST_Dataset with no shape"
        log.error(msg)
        raise HttpProcessingError(code=500, message="Unexpected Error")
    shape_json = body["shape"]
     
    layout = None
    if "layout" in body:       
        layout = body["layout"]  # client specified chunk layout
    
    # ok - all set, create committed type obj
    now = int(time.time())

    log.debug("POST_dataset typejson: {}, shapejson: {}".format(type_json, shape_json))
    
    dset_json = {"id": dset_id, "root": root_id, "created": now, "lastModified": now, "type": type_json, "shape": shape_json, "attributes": {} }
    if "creationProperties" in body:
        dset_json["creationProperties"] = body["creationProperties"]
    if layout is not None:
        dset_json["layout"] = layout

    await save_metadata_obj(app, dset_id, dset_json, notify=True)
     
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
    log.info("DELETE dataset: {}".format(dset_id))

    if not isValidUuid(dset_id, obj_class="dataset"):
        log.error( "Unexpected dataset id: {}".format(dset_id))
        raise HttpProcessingError(code=500, message="Unexpected Error")

    # verify the id  exist
    await check_metadata_obj(app, dset_id) 

    log.debug("deleting dataset: {}".format(dset_id))

    notify=True
    if "Notify" in request.GET and not request.GET["Notify"]:
        notify=False
    await delete_metadata_obj(app, dset_id, notify=notify)

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

    body = await request.json()

    log.info("PUT datasetshape: {}, body: {}".format(dset_id, body))

    dset_json = await get_metadata_obj(app, dset_id)

    shape_update = body["shape"]
     
    log.debug("shape_update: {}".format(shape_update))

    shape_orig = dset_json["shape"]
    log.debug("shape_orig: {}".format(shape_orig))

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
    await save_metadata_obj(app, dset_id, dset_json)
 
    resp_json = { } 

    resp = await jsonResponse(request, resp_json, status=201)
    log.response(request, resp=resp)
    return resp
   