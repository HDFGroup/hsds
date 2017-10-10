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
# attribute handling routines
# 
import time
from bisect import bisect_left

from aiohttp.errors import HttpBadRequest, HttpProcessingError
 
from util.httpUtil import jsonResponse
from util.attrUtil import validateAttributeName
from datanode_lib import get_obj_id, get_metadata_obj, save_metadata_obj
import hsds_logger as log

def index(a, x):
    """ Locate the leftmost value exactly equal to x
    """
    i = bisect_left(a, x)
    if i != len(a) and a[i] == x:
        return i
    return -1
    
async def GET_Attributes(request):
    """ Return JSON for attribute collection
    """
    log.request(request)
    app = request.app

    obj_id = get_obj_id(request)  
     
    include_data = False
    if "IncludeData" in request.GET and request.GET["IncludeData"]:
        include_data = True

    limit = None
    if "Limit" in request.GET:
        try:
            limit = int(request.GET["Limit"])
            log.info("GET_Links - using Limit: {}".format(limit))
        except ValueError:
            msg = "Bad Request: Expected int type for limit"
            log.error(msg)  # should be validated by SN
            raise HttpProcessingError(code=500, message="Unexpected Error")

    marker = None
    if "Marker" in request.GET:
        marker = request.GET["Marker"]
        log.info("GET_Links - using Marker: {}".format(marker))
     
    obj_json = await get_metadata_obj(app, obj_id)
    
    log.debug("GET attributes obj_id: {} got json".format(obj_id))
    if "attributes" not in obj_json:
        msg = "unexpected data for obj id: {}".format(obj_id)
        msg.error(msg)
        raise HttpProcessingError(code=500, message=msg)

    # return a list of attributes based on sorted dictionary keys
    attr_dict = obj_json["attributes"]
    attr_names = list(attr_dict.keys())
    attr_names.sort()  # sort by key 
    # TBD: provide an option to sort by create date

    start_index = 0
    if marker is not None:
        start_index = index(attr_names, marker) + 1
        if start_index == 0:
            # marker not found, return 404
            msg = "attribute marker: {}, not found".format(marker)
            log.warn(msg)
            raise HttpProcessingError(code=404, message=msg)

    end_index = len(attr_names) 
    if limit is not None and (end_index - start_index) > limit:
        end_index = start_index + limit
    
    attr_list = []
    for i in range(start_index, end_index):
        attr_name = attr_names[i]
        src_attr = attr_dict[attr_name]
        des_attr = {}
        des_attr["created"] = src_attr["created"]
        des_attr["type"] = src_attr["type"]
        des_attr["shape"] = src_attr["shape"]
        des_attr["name"] = attr_name
        if include_data:
            des_attr["value"] = src_attr["value"]
        attr_list.append(des_attr)

    resp_json = {"attributes": attr_list} 
    resp = await jsonResponse(request, resp_json)
    log.response(request, resp=resp)
    return resp    

async def GET_Attribute(request):
    """HTTP GET method to return JSON for /(obj)/<id>/attributes/<name>
    """
    log.request(request)
    app = request.app

    obj_id = get_obj_id(request)  

    attr_name = request.match_info.get('name')
    validateAttributeName(attr_name)
        
    obj_json = await get_metadata_obj(app, obj_id)
    log.info("GET attribute obj_id: {} name: {}".format(obj_id, attr_name))

    if "attributes" not in obj_json:
        log.error("unexpected obj data for id: {}".format(obj_id))
        raise HttpProcessingError(code=500, message="Unexpected Error")

    attributes = obj_json["attributes"]
    if attr_name not in attributes:
        msg = "Attribute  {} not with id: {}".format(attr_name, obj_id)
        log.warn(msg)
        raise HttpProcessingError(code=404, message=msg)

    attr_json = attributes[attr_name]
     
    resp = await jsonResponse(request, attr_json)
    log.response(request, resp=resp)
    return resp

async def PUT_Attribute(request):
    """ Handler for PUT /(obj)/<id>/attributes/<name>
    """
    log.request(request)
    app = request.app
    obj_id = get_obj_id(request) 

    attr_name = request.match_info.get('name')
    log.info("PUT attribute {} in {}".format(attr_name, obj_id))
    validateAttributeName(attr_name)
        
    if not request.has_body:
        log.error( "PUT_Attribute with no body")
        raise HttpBadRequest(message="Unexpected error")

    body = await request.json() 
    
    datatype = None
    shape = None
    value = None

    if "type" not in body:
        log.error("PUT attribute with no type in body")
        raise HttpProcessingError(code=500, message="Unexpected Error")

    datatype = body["type"]

    if "shape" not in body:
        log.error("PUT attribute with no shape in body")
        raise HttpProcessingError(code=500, message="Unexpected Error")
    shape = body["shape"]

    if "value" in body:
        #if shape["class"] != "H5S_NULL":
        #    log.error("non-null PUT attribute with no value in body")
        #    raise HttpProcessingError(code=500, message="Unexpected Error")
        value = body["value"]

  
    obj_json = await get_metadata_obj(app, obj_id)
    log.debug("PUT attribute obj_id: {} got json".format(obj_id))

    if "attributes" not in obj_json:
        log.error("unexpected obj data for id: {}".format(obj_id))
        raise HttpProcessingError(code=500, message="Unexpected Error")

    attributes = obj_json["attributes"]
    if attr_name in attributes:
        # Attribute already exists, return a 409
        log.warn("Attempt to overwrite attribute: {} in obj_id:".format(attr_name, obj_id))
        raise HttpProcessingError(code=409, message="Attribute with name: {} already exists".format(attr_name))

    # ok - all set, create attribute obj
    now = time.time()
    
    attr_json = {"type": datatype, "shape": shape, "value": value, "created": now }
    attributes[attr_name] = attr_json
     
    # write back to S3, save to metadata cache
    save_metadata_obj(app, obj_id, obj_json)
 
    resp_json = { } 

    resp = await jsonResponse(request, resp_json, status=201)
    log.response(request, resp=resp)
    return resp


async def DELETE_Attribute(request):
    """HTTP DELETE method for /(obj)/<id>/attributes/<name>
    """
    log.request(request)
    app = request.app
    
    obj_id = get_obj_id(request) 

    attr_name = request.match_info.get('name')
    log.info("DELETE attribute {} in {}".format(attr_name, obj_id))
    validateAttributeName(attr_name)

    obj_json = await get_metadata_obj(app, obj_id)
    
    log.debug("DELETE attribute obj_id: {} got json".format(obj_id))
    if "attributes" not in obj_json:
        msg = "unexpected data for obj id: {}".format(obj_id)
        msg.error(msg)
        raise HttpProcessingError(code=500, message=msg)

    # return a list of attributes based on sorted dictionary keys
    attributes = obj_json["attributes"]

    if attr_name not in attributes:
        msg = "Attribute  {} not found in id: {}".format(attr_name, obj_id)
        log.warn(msg)
        raise HttpProcessingError(code=404, message=msg)

    del attributes[attr_name] 

    save_metadata_obj(app, obj_id, obj_json)

    resp_json = { } 
    resp = await jsonResponse(request, resp_json)
    log.response(request, resp=resp)
    return resp    

 
     
