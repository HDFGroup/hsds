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
# handles dataset requests
# 
 
import json
from aiohttp.errors import HttpBadRequest 
 
from util.httpUtil import http_post, http_put, http_delete, jsonResponse
from util.idUtil import   isValidUuid, getDataNodeUrl, createObjId
from util.dsetUtil import  getNumElements
from util.chunkUtil import guess_chunk
from util.authUtil import getUserPasswordFromRequest, aclCheck, validateUserPassword
from util.domainUtil import  getDomainFromRequest, isValidDomain
from util.hdf5dtype import validateTypeItem, getBaseTypeJson, getItemSize
from servicenode_lib import getDomainJson, getObjectJson, validateAction
import config
import hsds_logger as log

"""
Use chunk layout given in the creationPropertiesList (if defined and layout is valid).
Return chunk_layout_json
"""
def validateChunkLayout(shape_json, item_size, body):
    layout = None
    if "creationProperties" in body:
        creationProps = body["creationProperties"]
        if "layout" in creationProps:
            layout = creationProps["layout"]
    
    #
    # if layout is not provided, return None
    #
    if not layout:
        return None

    if item_size == 'H5T_VARIABLE':
        item_size = 128  # just take a guess at the item size (used for chunk validation)
    #
    # validate provided layout
    #
    if "class" not in layout:
        msg = "class key not found in layout for creation property list"
        log.warn(msg)
        raise HttpBadRequest(message=msg)
    if layout["class"] not in ('H5D_CHUNKED', 'H5D_CONTIGUOUS', 'H5D_COMPACT'):
        msg = "Unknown dataset layout class: {}".format(layout["class"])
        log.warn(msg)
        raise HttpBadRequest(message=msg)
    if layout["class"] != 'H5D_CHUNKED':
        return None # nothing else to validate

    if "dims" not in layout:
        msg = "dims key not found in layout for creation property list"
        log.warn(msg)
        raise HttpBadRequest(message=msg)
    if shape_json["class"] != 'H5S_SIMPLE':
        msg = "Bad Request: chunked layout not valid with shape class: {}".format(shape_json["class"])
        log.warn(msg)
        raise HttpBadRequest(message=msg)
    space_dims = shape_json["dims"]
    chunk_dims = layout["dims"]
    max_dims = None
    if "maxdims" in shape_json:
        max_dims = shape_json["maxdims"]
    if isinstance(chunk_dims, int):
        chunk_dims = [chunk_dims,] # promote to array
    if len(chunk_dims) != len(space_dims):
        msg = "Layout rank does not match shape rank"
        log.warn(msg)
        raise HttpBadRequest(message=msg)
    for i in range(len(chunk_dims)):
        dim_extent = space_dims[i]
        chunk_extent = chunk_dims[i]
        if not isinstance(chunk_extent, int):
            msg = "Layout dims must be integer or integer array"
            log.warn(msg)
            raise HttpBadRequest(message=msg)
        if chunk_extent <= 0:
            msg = "Invalid layout value"
            log.warn(msg)
            raise HttpBadRequest(message=msg)
        if max_dims is None:
            if chunk_extent > dim_extent:
                msg = "Invalid layout value"
                log.warn(msg)
                raise HttpBadRequest(message=msg)
        elif max_dims[i] != 'H5S_UNLIMITED':
            if chunk_extent > max_dims[i]:
                msg = "Invalid layout value for extensible dimension"
                log.warn(msg)
                raise HttpBadRequest(message=msg)
        else:
            pass # allow any positive value for unlimited dimensions
     
    #
    # Verify the requested chunk size is within valid range.
    # If not, ignore client input
    #
    if chunk_dims is not None:      
        chunk_size = getNumElements(chunk_dims) * item_size
        min_chunk_size = config.get("min_chunk_size")
        max_chunk_size = config.get("max_chunk_size")
        if chunk_size < min_chunk_size:
            log.warn("requested chunk size of {} less than {}, ignoring".format(chunk_size, min_chunk_size))
            chunk_dims = None
        elif chunk_size > max_chunk_size:
            log.warn("requested chunk size of {} less than {}, ignoring".format(chunk_size, max_chunk_size))
            chunk_dims = None
        else:
            log.info("Using client requested chunk layout: {}".format(chunk_dims))  
    return chunk_dims


async def GET_Dataset(request):
    """HTTP method to return JSON for dataset's type"""
    log.request(request)
    app = request.app 

    dset_id = request.match_info.get('id')
    if not dset_id:
        msg = "Missing dataset id"
        log.warn(msg)
        raise HttpBadRequest(message=msg)
    if not isValidUuid(dset_id, "Dataset"):
        msg = "Invalid dataset id: {}".format(dset_id)
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
    
    # get authoritative state for dataset from DN (even if it's in the meta_cache).
    dset_json = await getObjectJson(app, dset_id, refresh=True)  

    log.info("got dset_json: {}".format(dset_json))
    await validateAction(app, domain, dset_id, username, "read")

    resp_json = {}
    resp_json["id"] = dset_json["id"]
    resp_json["shape"] = dset_json["shape"]
    resp_json["type"] = dset_json["type"]
    if "creationProperties" in dset_json:
        resp_json["creationProperties"] = dset_json["creationProperties"]
    if "layout" in dset_json:
        resp_json["layout"] = dset_json["layout"]
    resp_json["attributeCount"] = dset_json["attributeCount"]
    resp_json["created"] = dset_json["created"]
    resp_json["lastModified"] = dset_json["lastModified"]
    resp_json["hrefs"] = []  # TBD

    resp = await jsonResponse(request, resp_json)
    log.response(request, resp=resp)
    return resp

async def GET_DatasetType(request):
    """HTTP method to return JSON for dataset's type"""
    log.request(request)
    app = request.app 

    dset_id = request.match_info.get('id')
    if not dset_id:
        msg = "Missing dataset id"
        log.warn(msg)
        raise HttpBadRequest(message=msg)
    if not isValidUuid(dset_id, "Dataset"):
        msg = "Invalid dataset id: {}".format(dset_id)
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
    dset_json = await getObjectJson(app, dset_id, refresh=True)  

    await validateAction(app, domain, dset_id, username, "read")

    resp_json = {}
    resp_json["type"] = dset_json["type"]
    resp_json["hrefs"] = []  # TBD

    resp = await jsonResponse(request, resp_json)
    log.response(request, resp=resp)
    return resp

async def GET_DatasetShape(request):
    """HTTP method to return JSON for dataset's shape"""
    log.request(request)
    app = request.app 

    dset_id = request.match_info.get('id')
    if not dset_id:
        msg = "Missing dataset id"
        log.warn(msg)
        raise HttpBadRequest(message=msg)
    if not isValidUuid(dset_id, "Dataset"):
        msg = "Invalid dataset id: {}".format(dset_id)
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
    
    # get authoritative state for dataset from DN (even if it's in the meta_cache).
    dset_json = await getObjectJson(app, dset_id, refresh=True)  

    await validateAction(app, domain, dset_id, username, "read")

    resp_json = {}
    resp_json["shape"] = dset_json["shape"]
    resp_json["hrefs"] = []  # TBD
    resp_json["created"] = dset_json["created"]
    resp_json["lastModified"] = dset_json["lastModified"]

    resp = await jsonResponse(request, resp_json)
    log.response(request, resp=resp)
    return resp

async def PUT_DatasetShape(request):
    """HTTP method to update dataset's shape"""
    log.request(request)
    app = request.app 

    dset_id = request.match_info.get('id')
    if not dset_id:
        msg = "Missing dataset id"
        log.warn(msg)
        raise HttpBadRequest(message=msg)
    if not isValidUuid(dset_id, "Dataset"):
        msg = "Invalid dataset id: {}".format(dset_id)
        log.warn(msg)
        raise HttpBadRequest(message=msg)

    username, pswd = getUserPasswordFromRequest(request)
    validateUserPassword(username, pswd)

    # validate request
    if not request.has_body:
        msg = "PUT shape with no body"
        log.warn(msg)
        raise HttpBadRequest(message=msg)

    data = await request.json()
    if "shape" not in data:
        msg = "PUT shape has no shape key in body"
        log.warn(msg)
        raise HttpBadRequest(message=msg)   
    shape_update = data["shape"]
    if isinstance(shape_update, int):
        # convert to a list
        shape_update = [shape_update,]
    log.info("shape_update: {}".format(shape_update))

    domain = getDomainFromRequest(request)
    if not isValidDomain(domain):
        msg = "Invalid host value: {}".format(domain)
        log.warn(msg)
        raise HttpBadRequest(message=msg)
    
    # get authoritative state for dataset from DN (even if it's in the meta_cache).
    dset_json = await getObjectJson(app, dset_id, refresh=True)  
    shape_orig = dset_json["shape"]
    log.info("shape_orig: {}".format(shape_orig))

    # verify that the extend request is valid
    if shape_orig["class"] != "H5S_SIMPLE":
        msg = "Unable to extend shape of datasets who are not H5S_SIMPLE"
        log.warn(msg)
        raise HttpBadRequest(message=msg)
    if "maxdims" not in shape_orig:
        msg = "Dataset is not extensible"
        log.warn(msg)
        raise HttpBadRequest(message=msg)
    dims = shape_orig["dims"]
    maxdims = shape_orig["maxdims"]
    if len(shape_update) != len(maxdims):
        msg = "Extent of update shape request does not match dataset sahpe"
        log.warn(msg)
        raise HttpBadRequest(message=msg)
    for i in range(len(dims)):
        if shape_update[i] < dims[i]:
            msg = "Dataspace can not be made smaller"
            log.warn(msg)
            raise HttpBadRequest(message=msg)
        if maxdims[i] != 'H5S_UNLIMITED' and shape_update[i] > maxdims[i]:
            msg = "Database can not be extended past max extent"
            log.warn(msg)
            raise HttpBadRequest(message=msg)  
    
    # verify the user has permission to update shape
    await validateAction(app, domain, dset_id, username, "update")

    # send request onto DN
    req = getDataNodeUrl(app, dset_id) + "/datasets/" + dset_id + "/shape"
    
    data = {"shape": shape_update}
    await http_put(app, req, data=data)
    
    # return resp 
    json_resp = { "hrefs": []}
    resp = await jsonResponse(request, json_resp, status=201)
    log.response(request, resp=resp)
    return resp
 

async def POST_Dataset(request):
    """HTTP method to create a new dataset object"""
    log.request(request)
    app = request.app

    username, pswd = getUserPasswordFromRequest(request)
    # write actions need auth
    validateUserPassword(username, pswd)

    if not request.has_body:
        msg = "POST Datasets with no body"
        log.warn(msg)
        raise HttpBadRequest(message=msg)

    body = await request.json()

    #
    # validate type input
    #
    if "type" not in body:
        msg = "POST Dataset has no type key in body"
        log.warn(msg)
        raise HttpBadRequest(message=msg)   

    datatype = body["type"]
    if isinstance(datatype, str):
        try:
            # convert predefined type string (e.g. "H5T_STD_I32LE") to 
            # corresponding json representation
            datatype = getBaseTypeJson(datatype)
            log.info("got datatype: {}".format(datatype))
        except TypeError:
            # TBD: Handle the case where the string is a committed type reference
            msg = "POST Dataset with invalid predefined type"
            log.warn(msg)
            raise HttpBadRequest(message=msg) 

    validateTypeItem(datatype)
    item_size = getItemSize(datatype)
    

    #
    # Validate shape input
    #
    dims = None
    shape_json = {}

    if "shape" not in body:
        shape_json["class"] = "H5S_SCALAR"
    else:
        shape = body["shape"]
        if isinstance(shape, int):
            shape_json["class"] = "H5S_SIMPLE"
            dims = [shape]
            shape_json["dims"] = dims
        elif isinstance(shape, str):
            # only valid string value is H5S_NULL
            if shape != "H5S_NULL":
                msg = "POST Datset with invalid shape value"
                log.warn(msg)
                raise HttpBadRequest(message=msg)
            shape_json["class"] = "H5S_NULL"
        elif isinstance(shape, list):
            shape_json["class"] = "H5S_SIMPLE"
            dims = shape
            shape_json["dims"] = dims
        else:
            msg = "Bad Request: shape is invalid"
            log.warn(msg)
            raise HttpBadRequest(message=msg)
                
    if dims is not None:
        for i  in range(len(dims)):
            extent = dims[i]
            if not isinstance(extent, int):
                msg = "Invalid shape type"
                log.warn(msg)
                raise HttpBadRequest(message=msg)
            if extent < 0:
                msg = "shape dimension is negative"
                log.warn(msg)
                raise HttpBadRequest(message=msg)                

    maxdims = None
    if "maxdims" in body:
        if dims is None:
            msg = "Maxdims cannot be supplied if space is NULL"
            log.warn(msg)
            raise HttpBadRequest(message=msg)

        maxdims = body["maxdims"]
        if isinstance(maxdims, int):
            dim1 = maxdims
            maxdims = [dim1]
        elif isinstance(maxdims, list):
            pass  # can use as is
        else:
            msg = "Bad Request: maxdims is invalid"
            log.warn(msg)
            raise HttpBadRequest(message=msg)
        if len(dims) != len(maxdims):
            msg = "Maxdims rank doesn't match Shape"
            log.warn(msg)
            raise HttpBadRequest(message=msg)

    if maxdims is not None:
        for extent in maxdims:
            if not isinstance(extent, int):
                msg = "Invalid maxdims type"
                log.warn(msg)
                raise HttpBadRequest(message=msg)
            if extent < 0:
                msg = "maxdims dimension is negative"
                log.warn(msg)
                raise HttpBadRequest(message=msg)
        if len(maxdims) != len(dims):
                msg = "Bad Request: maxdims array length must equal shape array length"
                log.warn(msg)
                raise HttpBadRequest(message=msg)
        shape_json["maxdims"] = []
        for i in range(len(dims)):
            maxextent = maxdims[i]
            if maxextent == 0:
                # unlimited dimension
                shape_json["maxdims"].append("H5S_UNLIMITED")
            elif maxextent < dims[i]:
                msg = "Bad Request: maxdims extent can't be smaller than shape extent"
                log.warn(msg)
                raise HttpBadRequest(message=msg)
            else:
                shape_json["maxdims"].append(maxextent) 

    layout = validateChunkLayout(shape_json, item_size, body) 
    if layout is None:
        layout = guess_chunk(shape_json, item_size) 
        log.info("autochunk layout: {}".format(layout))
    else:
        log.info("client layout: {}".format(layout))

    
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
    dset_id = createObjId("datasets") 
    log.info("new  dataset id: {}".format(dset_id))

    dataset_json = {"id": dset_id, "root": root_id, "domain": domain, "type": datatype, "shape": shape_json }

    if "creationProperties" in body:
        # TBD - validate creationProperties
        dataset_json["creationProperties"] = body["creationProperties"]
    if layout is not None:
        dataset_json["layout"] = layout
    
    log.info("create dataset: " + json.dumps(dataset_json))
    req = getDataNodeUrl(app, dset_id) + "/datasets"
    
    post_json = await http_post(app, req, data=dataset_json)
    
    # dataset creation successful     
    resp = await jsonResponse(request, post_json, status=201)
    log.response(request, resp=resp)
    return resp

async def DELETE_Dataset(request):
    """HTTP method to delete a dataset resource"""
    log.request(request)
    app = request.app 
    meta_cache = app['meta_cache']

    dset_id = request.match_info.get('id')
    if not dset_id:
        msg = "Missing dataset id"
        log.warn(msg)
        raise HttpBadRequest(message=msg)
    if not isValidUuid(dset_id, "Dataset"):
        msg = "Invalid dataset id: {}".format(dset_id)
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
    await validateAction(app, domain, dset_id, username, "delete")

    req = getDataNodeUrl(app, dset_id) + "/datasets/" + dset_id
 
    rsp_json = await http_delete(app, req)

    if dset_id in meta_cache:
        del meta_cache[dset_id]  # remove from cache
 
    resp = await jsonResponse(request, rsp_json)
    log.response(request, resp=resp)
    return resp

 