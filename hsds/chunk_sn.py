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
# value operations 
# handles dataset /value requests
# 
import asyncio
from asyncio import CancelledError
import json
import base64 
import numpy as np
from aiohttp.errors import HttpBadRequest, HttpProcessingError, ClientError
from aiohttp.web import StreamResponse
from util.httpUtil import  jsonResponse, getHref, getAcceptType, get_http_client  
from util.idUtil import   isValidUuid, getDataNodeUrl
from util.domainUtil import  getDomainFromRequest, isValidDomain
from util.hdf5dtype import getItemSize, createDataType
from util.dsetUtil import getSliceQueryParam, setSliceQueryParam, getFillValue, isExtensible 
from util.dsetUtil import getSelectionShape, getNumElements, getDsetDims, getDsetMaxDims, getChunkLayout, getDeflateLevel
from util.chunkUtil import getNumChunks, getChunkIds, getChunkId
from util.chunkUtil import getChunkCoverage, getDataCoverage
from util.arrayUtil import bytesArrayToList, toTuple 
from util.authUtil import getUserPasswordFromRequest, validateUserPassword
from servicenode_lib import getObjectJson, validateAction
import config
import hsds_logger as log


"""
Write data to given chunk_id.  Pass in type, dims, and selection area.
"""
async def write_chunk_hyperslab(app, chunk_id, dset_json, slices, deflate_level, arr):
    """ write the chunk selection to the DN
    chunk_id: id of chunk to write to
    chunk_sel: chunk-relative selection to write to
    np_arr: numpy array of data to be written
    """
    log.info("write_chunk_hyperslab, chunk_id:{}, slices:{}".format(chunk_id, slices))
    if deflate_level is not None:
        log.info("deflate_level: {}".format(deflate_level))
    if "layout" not in dset_json:
        log.error("No layout found in dset_json: {}".format(dset_json))
        raise HttpProcessingError(message="Unexpected error", code=500)
     
    if "type" not in dset_json:
        log.error("No type found in dset_json: {}".format(dset_json))
        raise HttpProcessingError(message="Unexpected error", code=500)
    #type_json = dset_json["type"]
    #dims = getDsetDims(dset_json)
    layout = getChunkLayout(dset_json)

    chunk_sel = getChunkCoverage(chunk_id, slices, layout)
    log.debug("chunk_sel: {}".format(chunk_sel))
    data_sel = getDataCoverage(chunk_id, slices, layout)
    log.debug("data_sel: {}".format(data_sel))
    log.debug("arr.shape: {}".format(arr.shape))
    arr_chunk = arr[data_sel]
    req = getDataNodeUrl(app, chunk_id)
    req += "/chunks/" + chunk_id  

    log.debug("PUT chunk req: " + req)
    client = get_http_client(app)
    data = arr_chunk.tobytes()  # TBD - this makes a copy, use np_arr.data to get memoryview and avoid copy
    # pass itemsize, type, dimensions, and selection as query params
    params = {}
    params["dset"] = json.dumps(dset_json)
    setSliceQueryParam(params, chunk_sel)   

    try:
        async with client.put(req, data=data, params=params) as rsp:
            log.debug("req: {} status: {}".format(req, rsp.status))
            if rsp.status != 201:
                msg = "request error for {}: {}".format(req, str(rsp))
                log.warn(msg)
                raise HttpProcessingError(message=msg, code=rsp.status)
            else:
                log.debug("http_put({}) <201> Updated".format(req))
    except ClientError as ce:
        log.error("Error for http_post({}): {} ".format(req, str(ce)))
        raise HttpProcessingError(message="Unexpected error", code=500)
    except CancelledError as cle:
        log.error("CancelledError for http_post({}): {}".format(req, str(cle)))
        raise HttpProcessingError(message="Unexpected error", code=500)


"""
Read data from given chunk_id.  Pass in type, dims, and selection area.
""" 
async def read_chunk_hyperslab(app, chunk_id, dset_json, slices, np_arr):
    """ read the chunk selection from the DN
    chunk_id: id of chunk to write to
    chunk_sel: chunk-relative selection to read from
    np_arr: numpy array to store read bytes
    """
    msg = "read_chunk_hyperslab, chunk_id:{}, slices: {}".format(chunk_id, slices)
    log.info(msg)

    req = getDataNodeUrl(app, chunk_id)
    req += "/chunks/" + chunk_id 
    log.debug("GET chunk req: " + req)
    client = get_http_client(app)

    if "type" not in dset_json:
        log.error("No type found in dset_json: {}".format(dset_json))
        raise HttpProcessingError(message="Unexpected error", code=500)
    
    layout = getChunkLayout(dset_json)
    chunk_sel = getChunkCoverage(chunk_id, slices, layout)
    data_sel = getDataCoverage(chunk_id, slices, layout)
    
    # pass dset json and selection as query params
    params = {}
    params["dset"] = json.dumps(dset_json)
     
    fill_value = getFillValue(dset_json) 
     
    chunk_shape = getSelectionShape(chunk_sel)
    log.debug("chunk_shape: {}".format(chunk_shape))
    setSliceQueryParam(params, chunk_sel)  
    dt = np_arr.dtype
 
    chunk_arr = None
    try:
        async with client.get(req, params=params) as rsp:
            log.debug("http_get {} status: <{}>".format(req, rsp.status))
            if rsp.status == 200:
                data = await rsp.read()  # read response as bytes
                chunk_arr = np.fromstring(data, dtype=dt) 
                npoints_read = getNumElements(chunk_arr.shape)
                npoints_expected = getNumElements(chunk_shape)
                if npoints_read != npoints_expected:
                    msg = "Expected {} points, but got: {}".format(npoints_expected, npoints_read)
                chunk_arr = chunk_arr.reshape(chunk_shape)
            elif rsp.status == 404:
                # no data, return zero array
                if fill_value:
                    chunk_arr = np.empty(chunk_shape, dtype=dt, order='C')
                    chunk_arr[...] = fill_value
                else:
                    chunk_arr = np.zeros(chunk_shape, dtype=dt, order='C')
            else:
                msg = "request to {} failed with code: {}".format(req, rsp.status)
                log.warn(msg)
                raise HttpProcessingError(message=msg, code=rsp.status)
            
    except ClientError as ce:
        log.error("Error for http_get({}): {} ".format(req, str(ce)))
        raise HttpProcessingError(message="Unexpected error", code=500)
    except CancelledError as cle:
        log.error("CancelledError for http_get({}): {}".format(req, str(cle)))
        raise HttpProcessingError(message="Unexpected error", code=500)
    
    log.info("chunk_arr shape: {}".format(chunk_arr.shape))
    log.info("data_sel: {}".format(data_sel))

    np_arr[data_sel] = chunk_arr

"""
Read point selection
--
app: application object
chunk_id: id of chunk to write to
dset_json: dset JSON
point_list: array of points to read
point_index: index of arr element to update for a given point
arr: numpy array to store read bytes
"""
async def read_point_sel(app, chunk_id, dset_json, point_list, point_index, np_arr):
    
    #msg = "read_point_sel, chunk_id:{}, points: {}, index: {}".format(chunk_id, point_list, point_index)
    msg = "read_ooint_sel, chunk_id: {}".format(chunk_id)
    log.info(msg)

    req = getDataNodeUrl(app, chunk_id)
    req += "/chunks/" + chunk_id 
    log.debug("POST chunk req: " + req)
    client = get_http_client(app)
    point_dt = np.dtype('u8')  # use unsigned long for point index

    if "type" not in dset_json:
        log.error("No type found in dset_json: {}".format(dset_json))
        raise HttpProcessingError(message="Unexpected error", code=500)
     
    num_points = len(point_list)
    np_arr_points = np.asarray(point_list, dtype=point_dt)
    post_data = np_arr_points.tobytes()
    

    # pass dset_json as query params
    params = {}
    params["dset"] = json.dumps(dset_json)
     
    fill_value = getFillValue(dset_json)
     
    np_arr_rsp = None
    dt = np_arr.dtype
    try:
        async with client.post(req, params=params, data=post_data) as rsp:
            log.debug("http_post {} status: <{}>".format(req, rsp.status))
            if rsp.status == 200:
                rsp_data = await rsp.read()  # read response as bytes         
                np_arr_rsp = np.fromstring(rsp_data, dtype=dt) 
                npoints_read = len(np_arr_rsp)
                if npoints_read != num_points:
                    msg = "Expected {} points, but got: {}".format(num_points, npoints_read)
                    log.error(msg)
                    raise HttpProcessingError(message=msg, code=500)
            elif rsp.status == 404:
                # no data, return zero array
                if fill_value:
                    np_arr_rsp = np.empty((num_points,), dtype=dt)
                    np_arr_rsp[...] = fill_value
                else:
                    np_arr_rsp = np.zeros((num_points,), dtype=dt)
            else:
                msg = "request to {} failed with code: {}".format(req, rsp.status)
                log.warn(msg)
                raise HttpProcessingError(message=msg, code=rsp.status)
            
    except ClientError as ce:
        log.error("Error for http_get({}): {} ".format(req, str(ce)))
        raise HttpProcessingError(message="Unexpected error", code=500)
    except CancelledError as cle:
        log.error("CancelledError for http_get({}): {}".format(req, str(cle)))
        raise HttpProcessingError(message="Unexpected error", code=500)
    
    log.info("got {} points response".format(num_points))

    # Fill in the return array based on passed in index values
    for i in range(num_points):
        index = point_index[i]
        np_arr[index] = np_arr_rsp[i]

"""
Query for a given chunk_id.  Pass in type, dims, selection area, and query.
""" 
async def read_chunk_query(app, chunk_id, dset_json, slices, query, limit, rsp_dict):
    """ read the chunk selection from the DN
    chunk_id: id of chunk to write to
    chunk_sel: chunk-relative selection to read from
    np_arr: numpy array to store read bytes
    """
    msg = "read_chunk_query, chunk_id:{}, slices: {}, query: {}".format(chunk_id, slices, query)
    log.info(msg)

    req = getDataNodeUrl(app, chunk_id)
    req += "/chunks/" + chunk_id 
    log.debug("GET chunk req: " + req)
    client = get_http_client(app)
  
    layout = getChunkLayout(dset_json)
    chunk_sel = getChunkCoverage(chunk_id, slices, layout)
    
    # pass dset json and selection as query params
    params = {}
    params["dset"] = json.dumps(dset_json)
    params["query"] = query
    if limit > 0:
        params["Limit"] = limit
          
    chunk_shape = getSelectionShape(chunk_sel)
    log.debug("chunk_shape: {}".format(chunk_shape))
    setSliceQueryParam(params, chunk_sel)  
    dn_rsp = None
    try:
        async with client.get(req, params=params) as rsp:
            log.debug("http_get {} status: <{}>".format(req, rsp.status))
            if rsp.status == 200:
                dn_rsp = await rsp.json()  # read response as json
                log.debug("got query data: {}".format(dn_rsp))
            elif rsp.status == 404:
                # no data, don't return any results
                dn_rsp = {"index": [], "value": []}
            else:
                msg = "request to {} failed with code: {}".format(req, rsp.status)
                log.warn(msg)
                raise HttpProcessingError(message=msg, code=rsp.status)
            
    except ClientError as ce:
        log.error("Error for http_get({}): {} ".format(req, str(ce)))
        raise HttpProcessingError(message="Unexpected error", code=500)
    except CancelledError as cle:
        log.error("CancelledError for http_get({}): {}".format(req, str(cle)))
        raise HttpProcessingError(message="Unexpected error", code=500)
    
    rsp_dict[chunk_id] = dn_rsp


"""
 Handler for PUT /<dset_uuid>/value request
"""
async def PUT_Value(request):
    log.request(request)
    app = request.app 
    loop = app["loop"]
    body = None
    json_data = None

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
    validateUserPassword(app, username, pswd)

    domain = getDomainFromRequest(request)
    if not isValidDomain(domain):
        msg = "Invalid host value: {}".format(domain)
        log.warn(msg)
        raise HttpBadRequest(message=msg)
    
    request_type = "json"
    if "Content-Type" in request.headers:
        # client should use "application/octet-stream" for binary transfer
        content_type = request.headers["Content-Type"]
        if content_type not in ("application/json", "application/octet-stream"):
            msg = "Unknown content_type: {}".format(content_type)
            log.warn(msg)
            raise HttpBadRequest(message=msg)
        if content_type == "application/octet-stream":
            log.debug("PUT value - request_type is binary")
            request_type = "binary"
        else:
            log.debug("PUT value - request type is json")

    if not request.has_body:
        msg = "PUT Value with no body"
        log.warn(msg)
        raise HttpBadRequest(message=msg)
    if request_type == "json":
        body = await request.json()
    
    # get  state for dataset from DN.
    dset_json = await getObjectJson(app, dset_id, refresh=False)  

    layout = None 
    datashape = dset_json["shape"]
    if datashape["class"] == 'H5S_NULL':
        msg = "Null space datasets can not be used as target for PUT value"
        log.warn(msg)
        raise HttpBadRequest(message=msg)
    dims = getDsetDims(dset_json)
    maxdims = getDsetMaxDims(dset_json)
    rank = len(dims)
    layout = getChunkLayout(dset_json)
    deflate_level = getDeflateLevel(dset_json)
     
    type_json = dset_json["type"]
    item_size = getItemSize(type_json)

    if item_size == 'H5T_VARIABLE':
        # keep this check until we have variable length supported
        msg = "variable length data types not yet supported"
        log.warn(msg)
        raise HttpProcessingError(code=501, message="Variable length data not yet supported")
      

    if item_size == 'H5T_VARIABLE' and request_type != "json":
        msg = "Only JSON is supported for variable length data types"
        log.warn(msg)
        raise HttpBadRequest(message=msg)
    dset_dtype = createDataType(type_json)  # np datatype
    
    await validateAction(app, domain, dset_id, username, "update")
 
    binary_data = None
    # refetch the dims if the dataset is extensible 
    if isExtensible(dims, maxdims):
        dset_json = await getObjectJson(app, dset_id, refresh=True)
        dims = getDsetDims(dset_json) 
    slices = []  # selection for write 
    
    # Get query parameter for selection
    for dim in range(rank):
        body_json = None
        if request_type == "json":
            body_json = body
        # if they selection region is invalid here, it's really invalid
        dim_slice = getSliceQueryParam(request, dim, dims[dim], body=body_json)
        slices.append(dim_slice)   
    slices = tuple(slices)  
    log.debug("PUT Value selection: {}".format(slices))   
     
    log.debug("item size: {}".format(item_size))

    np_shape = getSelectionShape(slices)
                 
    log.debug("selection shape:" + str(np_shape))

    npoints = getNumElements(np_shape)
    log.debug("selection num points: " + str(npoints))
    if npoints <= 0:
        msg = "Selection is empty"
        log.warn(msg)
        raise HttpBadRequest(message=msg)

    if request_type == "json":
        if "value" in body:
            json_data = body["value"]
        elif "value_base64" in body:
            base64_data = body["value_base64"]
            base64_data = base64_data.encode("ascii")
            binary_data = base64.b64decode(base64_data)
        else:
            msg = "PUT value has no value or value_base64 key in body"
            log.warn(msg)
            raise HttpBadRequest(message=msg)   
    else:
        # read binary data
        binary_data = await request.read()
        if len(binary_data) != request.content_length:
            msg = "Read {} bytes, expecting: {}".format(len(binary_data), request.content_length)
            log.error(msg)
            raise HttpProcessingError(code=500, message="Unexpected Error")

    arr = None  # np array to hold request data
    if binary_data:
        if npoints*item_size != len(binary_data):
            msg = "Expected: " + str(npoints*item_size) + " bytes, but got: " + str(len(binary_data))
            log.warn(msg)
            raise HttpBadRequest(message=msg)
        arr = np.fromstring(binary_data, dtype=dset_dtype)
        arr = arr.reshape(np_shape)  # conform to selection shape
                
    else:
        # data is json

        # need some special conversion for compound types --
        # each element must be a tuple, but the JSON decoder
        # gives us a list instead.
        if len(dset_dtype) > 1 and type(json_data) in (list, tuple):
            np_shape_rank = len(np_shape)
            #log.info("np_shape_rank: {}".format(np_shape_rank))
            converted_data = []
            if npoints == 1:
                converted_data.append(toTuple(0, json_data))
            else:  
                converted_data = toTuple(np_shape_rank, json_data)
            json_data = converted_data

        arr = np.array(json_data, dtype=dset_dtype)
        # raise an exception of the array shape doesn't match the selection shape
        # allow if the array is a scalar and the selection shape is one element,
        # numpy is ok with this
        if arr.size != npoints:
            msg = "Input data doesn't match selection number of elements"
            msg += " Expected {}, but received: {}".format(npoints, arr.size)
            log.warn(msg)
            raise HttpBadRequest(message=msg)
        if arr.shape != np_shape:
            arr = arr.reshape(np_shape)  # reshape to match selection

        np_index = 0
        for dim in range(len(arr.shape)):
            data_extent = arr.shape[dim]
            selection_extent = 1
            if np_index < len(np_shape):
                selection_extent = np_shape[np_index]
            if selection_extent == data_extent:
                np_index += 1
                continue  # good
            if data_extent == 1:
                continue  # skip singleton selection
            if selection_extent == 1:
                np_index += 1
                continue  # skip singleton selection
                 
            # selection/data mismatch!
            msg = "data shape doesn't match selection shape"
            msg += "--data shape: " + str(arr.shape)
            msg += "--selection shape: " + str(np_shape)         
            log.warn(msg)
            raise HttpBadRequest(message=msg)

    #log.info("got np array: {}".format(arr))
    num_chunks = getNumChunks(slices, layout)
    log.debug("num_chunks: {}".format(num_chunks))
    if num_chunks > config.get("max_chunks_per_request"):
        msg = "PUT value request too large"
        log.warn(msg)
        raise HttpProcessingError(code=413, message=msg)

    chunk_ids = getChunkIds(dset_id, slices, layout)
    log.debug("chunk_ids: {}".format(chunk_ids))

    tasks = []
    for chunk_id in chunk_ids:
        task = asyncio.ensure_future(write_chunk_hyperslab(app, chunk_id, dset_json, slices, deflate_level, arr))
        tasks.append(task)
    await asyncio.gather(*tasks, loop=loop)

    resp_json = {}

    resp = await jsonResponse(request, resp_json)
    log.response(request, resp=resp)
    return resp

"""
 Convience function to set up hrefs for GET
"""
def get_hrefs(request, dset_json):
    hrefs = []
    dset_id = dset_json["id"]
    dset_uri = '/datasets/'+dset_id
    hrefs.append({'rel': 'self', 'href': getHref(request, dset_uri + '/value')})
    root_uri = '/groups/' + dset_json["root"]    
    hrefs.append({'rel': 'root', 'href': getHref(request, root_uri)})
    hrefs.append({'rel': 'home', 'href': getHref(request, '/')})
    hrefs.append({'rel': 'owner', 'href': getHref(request, dset_uri)})
    return hrefs

"""
 Handler for GET /<dset_uuid>/value request
"""
async def GET_Value(request):
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
        validateUserPassword(app, username, pswd)
     
    domain = getDomainFromRequest(request)
    if not isValidDomain(domain):
        msg = "Invalid host value: {}".format(domain)
        log.warn(msg)
        raise HttpBadRequest(message=msg)  
   
    # get state for dataset from DN.
    dset_json = await getObjectJson(app, dset_id)  
    log.debug("got dset_json: {}".format(dset_json))

    type_json = dset_json["type"]
    item_size = getItemSize(type_json)

    if item_size == 'H5T_VARIABLE':
        # keep this check until we have variable length supported
        msg = "variable length data types not yet supported"
        log.warn(msg)
        raise HttpProcessingError(code=501, message="Variable length data not yet supported")

    dims = getDsetDims(dset_json)  # throws 400 for HS_NULL dsets
    maxdims = getDsetMaxDims(dset_json)
    rank = len(dims)
    layout = getChunkLayout(dset_json)
    
    await validateAction(app, domain, dset_id, username, "read")

    # refetch the dims if the dataset is extensible and requestor hasn't provided 
    # an explicit region
    if isExtensible(dims, maxdims) and "select" not in request.GET:
        dset_json = await getObjectJson(app, dset_id, refresh=True)
        dims = getDsetDims(dset_json)  

    slices = None  # selection for read 
     
    # Get query parameter for selection
    if isExtensible:
        slices = []
        try:
            for dim in range(rank):
                dim_slice = getSliceQueryParam(request, dim, dims[dim])
                slices.append(dim_slice)   
        except HttpBadRequest:
            # exception might be due to us having stale version of dims, refresh
            dset_json = await getObjectJson(app, dset_id, refresh=True)
            dims = getDsetDims(dset_json) 
            slices = None  # retry below
            
    if slices is None:
        slices = []
        for dim in range(rank):
            dim_slice = getSliceQueryParam(request, dim, dims[dim])
            slices.append(dim_slice)  
            
    slices = tuple(slices)  
    log.debug("GET Value selection: {}".format(slices))   
     
    np_shape = getSelectionShape(slices)                
    log.debug("selection shape:" + str(np_shape))

    npoints = getNumElements(np_shape)
    log.debug("selection num points: " + str(npoints))

    num_chunks = getNumChunks(slices, layout)
    log.debug("num_chunks: {}".format(num_chunks))
    if num_chunks > config.get("max_chunks_per_request"):
        msg = "PUT value request too large"
        log.warn(msg)
        raise HttpProcessingError(code=413, message=msg)
    chunk_ids = getChunkIds(dset_id, slices, layout)

    if "query" in request.GET:
        if rank > 1:
            msg = "Query string is not supported for multidimensional arrays"
            log.warn(msg)
            raise HttpBadRequest(message=msg)

        resp = await doQueryRead(request, chunk_ids, dset_json, slices)
    else:
        log.debug("chunk_ids: {}".format(chunk_ids))
        resp = await doHyperSlabRead(request, chunk_ids, dset_json, slices)
    log.response(request, resp=resp)
    return resp

async def doQueryRead(request, chunk_ids, dset_json,  slices):
    query = request.GET["query"]
    log.info("Query request: {}".format(query))
    
    app = request.app 
    loop = app["loop"]

    type_json = dset_json["type"]
    item_size = getItemSize(type_json)
    query = request.GET["query"]
    log.debug("item size: {}".format(item_size))
    
    limit = 0
    if "Limit" in request.GET:
        try:
            limit = int(request.GET["Limit"])
        except ValueError:
            msg = "Invalid Limit query param"
            log.warn(msg)
            raise HttpBadRequest(message=msg)

    tasks = []
    node_count = app['node_count']
    log.debug("node_count:  {}".format(node_count))
    chunk_index = 0
    resp_index = [] 
    resp_value = []
    num_chunks = len(chunk_ids)

    while chunk_index < num_chunks:
        next_chunks = []
        for i in range(node_count):
            next_chunks.append(chunk_ids[chunk_index])
            chunk_index += 1
            if chunk_index >= num_chunks:
                break
        log.debug("next chunk ids: {}".format(next_chunks))
        # run query on DN nodes
        dn_rsp = {} # dictionary keyed by chunk_id
        for chunk_id in next_chunks:
            task = asyncio.ensure_future(read_chunk_query(app, chunk_id, dset_json, slices, query, limit, dn_rsp))
            tasks.append(task)
        await asyncio.gather(*tasks, loop=loop)
    
        for chunk_id in next_chunks:
            chunk_rsp = dn_rsp[chunk_id]
            resp_index.extend(chunk_rsp["index"])
            resp_value.extend(chunk_rsp["value"])
        # trim response if we're over limit
        if limit > 0 and len(resp_index) > limit:
            resp_index = resp_index[0:limit]
            resp_value = resp_index[0:limit]
            break  # don't need any more DN queries
    resp_json = { "index": resp_index, "value": resp_value}
    resp_json["hrefs"] = get_hrefs(request, dset_json)
    resp = await jsonResponse(request, resp_json)
    return resp

async def doHyperSlabRead(request, chunk_ids, dset_json, slices):
    app = request.app 
    loop = app["loop"]

    accept_type = getAcceptType(request)
    response_type = accept_type    # will adjust later if binary not possible

    type_json = dset_json["type"]
    item_size = getItemSize(type_json)
    log.debug("item size: {}".format(item_size))
    dset_dtype = createDataType(type_json)  # np datatype
    if item_size == 'H5T_VARIABLE' and accept_type != "json":
        msg = "Client requested binary, but only JSON is supported for variable length data types"
        log.info(msg)
        response_type = "json"

    # create array to hold response data
    np_shape = getSelectionShape(slices)   
    log.debug("selection shape: {}".format(np_shape))
    arr = np.zeros(np_shape, dtype=dset_dtype, order='C')
    tasks = []
    for chunk_id in chunk_ids:
        task = asyncio.ensure_future(read_chunk_hyperslab(app, chunk_id, dset_json, slices, arr))
        tasks.append(task)
    await asyncio.gather(*tasks, loop=loop)

    log.debug("arr shape: {}".format(arr.shape))

    if response_type == "binary":
        output_data = arr.tobytes()
        log.debug("GET Value - returning {} bytes binary data".format(len(output_data)))
     
        # write response
        resp = StreamResponse(status=200)
        resp.headers['Content-Type'] = "application/octet-stream"
        resp.content_length = len(output_data)
        await resp.prepare(request)
        resp.write(output_data)
        await resp.write_eof()
    else:
        log.debug("GET Value - returning JSON data")
        resp_json = {}
        data = arr.tolist()
        json_data = bytesArrayToList(data)
        datashape = dset_json["shape"]
        if datashape["class"] == 'H5S_SCALAR':
            # convert array response to value
            resp_json["value"] = json_data[0]
        else:
            resp_json["value"] = json_data  
        resp_json["hrefs"] = get_hrefs(request, dset_json)
 
        resp = await jsonResponse(request, resp_json)
    return resp


"""
 Handler for POST /<dset_uuid>/value request - point selection
"""
async def POST_Value(request):
    log.request(request)
    
    app = request.app 
    loop = app["loop"]
    body = None

    dset_id = request.match_info.get('id')
    if not dset_id:
        msg = "Missing dataset id"
        log.warn(msg)
        raise HttpBadRequest(message=msg)
    if not isValidUuid(dset_id, "Dataset"):
        msg = "Invalid dataset id: {}".format(dset_id)
        log.warn(msg)
        raise HttpBadRequest(message=msg)

    log.info("POST_VALUE, id: {}".format(dset_id))

    username, pswd = getUserPasswordFromRequest(request)
    if username is None and app['allow_noauth']:
        username = "default"
    else:
        validateUserPassword(app, username, pswd)
     
    domain = getDomainFromRequest(request)
    if not isValidDomain(domain):
        msg = "Invalid host value: {}".format(domain)
        log.warn(msg)
        raise HttpBadRequest(message=msg)


    accept_type = getAcceptType(request)
    response_type = accept_type # will adjust later if binary not possible

    request_type = "json"
    if "Content-Type" in request.headers:
        # client should use "application/octet-stream" for binary transfer
        content_type = request.headers["Content-Type"]
        if content_type not in ("application/json", "application/octet-stream"):
            msg = "Unknown content_type: {}".format(content_type)
            log.warn(msg)
            raise HttpBadRequest(message=msg)
        if content_type == "application/octet-stream":
            log.debug("POST value - request_type is binary")
            request_type = "binary"
        else:
            log.debug("POST value - request type is json")

    if not request.has_body:
        msg = "PPST Value with no body"
        log.warn(msg)
        raise HttpBadRequest(message=msg)
    
   
    # get  state for dataset from DN.
    dset_json = await getObjectJson(app, dset_id)  

    datashape = dset_json["shape"]
    if datashape["class"] == 'H5S_NULL':
        msg = "POST value not supported for datasets with NULL shape"
        log.warn(msg)
        raise HttpBadRequest(message=msg)
    if datashape["class"] == 'H5S_SCALAR':
        msg = "POST value not supported for datasets with SCALAR shape"
        log.warn(msg)
        raise HttpBadRequest(message=msg)
    dims = getDsetDims(dset_json)
    rank = len(dims)
    
    layout = getChunkLayout(dset_json)
     
    type_json = dset_json["type"]
    item_size = getItemSize(type_json)
    log.debug("item size: {}".format(item_size))
    dset_dtype = createDataType(type_json)  # np datatype

    log.debug("got dset_json: {}".format(dset_json))
    await validateAction(app, domain, dset_id, username, "read")

    # read body data
    num_points = None
    arr_points = None  # numpy array to hold request points
    point_dt = np.dtype('u8')  # use unsigned long for point index
    if request_type == "json":
        body = await request.json()
        if "points" not in body:
            msg = "Expected points key in request body"
            log.warn(msg)
            raise HttpBadRequest(message=msg)
        points = body["points"]
        if not isinstance(points, list):
            msg = "POST Value expected list of points"
            log.warn(msg)
            raise HttpBadRequest(message=msg) 
        num_points = len(points)

    else:
        # read binary data
        binary_data = await request.read()
        if len(binary_data) != request.content_length:
            msg = "Read {} bytes, expecting: {}".format(len(binary_data), request.content_length)
            log.error(msg)
            raise HttpProcessingError(code=500, message="Unexpected Error")
        if request.content_length % point_dt.itemsize != 0:
            msg = "Content length: {} not divisible by element size: {}".format(request.content_length, item_size)
            log.warn(msg)
            raise HttpBadRequest(message=msg)
        num_points = request.content_length // point_dt.itemsize
        arr_points = np.fromstring(binary_data, dtype=point_dt)
        log.debug("got arr_points: {}".format(arr_points))
        if rank > 1:
            if num_points % rank != 0:
                msg = "Number of points is not consistent with dataset rank"
                log.warn(msg)
                raise HttpBadRequest(message=msg)
            num_points //= rank
            arr_points = arr_points.reshape((num_points, rank))  # conform to point index shape
        points = arr_points.tolist()  # convert to Python list
    
    log.debug("points: {}".format(points))        
    log.debug("num_points: {}".format(num_points))
    
    # TBD - return 413 if too many points requested

    chunk_dict = {}  # chunk ids to list of points in chunk

    for pt_indx in range(num_points):
        point = points[pt_indx]
        log.debug("checking point: {}".format(point))
        log.debug("point type: {}".format(type(point)))
        if rank == 1:
            if point < 0 or point >= dims[0]:
                msg = "PUT Value point: {} is not within the bounds of the dataset"
                msg = msg.format(point)
                log.warn(msg)
                raise HttpBadRequest(message=msg) 
        else:
            if len(point) != rank:
                msg = "PUT Value point value did not match dataset rank"
                log.warn(msg)
                raise HttpBadRequest(message=msg) 
            for i in range(rank):
                if point[i] < 0 or point[i] >= dims[i]:
                    msg = "PUT Value point: {} is not within the bounds of the dataset"
                    msg = msg.format(point)
                    log.warn(msg)
                    raise HttpBadRequest(message=msg) 
        chunk_id = getChunkId(dset_id, point, layout)
        log.debug("chunk_id: {}".format(chunk_id))
        if chunk_id not in chunk_dict:
            point_list = [point,]
            point_index =[pt_indx]
            chunk_dict[chunk_id] = {"points": point_list, "indices": point_index}
        else:
            item = chunk_dict[chunk_id]
            point_list = item["points"]
            point_list.append(point)
            point_index = item["indices"]
            point_index.append(pt_indx)

    num_chunks = len(chunk_dict)
    log.debug("num_chunks: {}".format(num_chunks))
    if num_chunks > config.get("max_chunks_per_request"):
        msg = "POST value request too large"
        log.warn(msg)
        raise HttpProcessingError(code=413, message=msg)

    
    # create array to hold response data
    # TBD: initialize to fill value if not 0
    arr_rsp = np.zeros((num_points,), dtype=dset_dtype)
    tasks = []
    for chunk_id in chunk_dict.keys():
        item = chunk_dict[chunk_id]
        point_list = item["points"]
        point_index = item["indices"]
        task = asyncio.ensure_future(read_point_sel(app, chunk_id, dset_json, 
            point_list, point_index, arr_rsp))
        tasks.append(task)
    await asyncio.gather(*tasks, loop=loop)

    log.debug("arr shape: {}".format(arr_rsp.shape))

    if response_type == "binary":
        output_data = arr_rsp.tobytes()
        log.debug("POST Value - returning {} bytes binary data".format(len(output_data)))
     
        # write response
        resp = StreamResponse(status=200)
        resp.headers['Content-Type'] = "application/octet-stream"
        resp.content_length = len(output_data)
        await resp.prepare(request)
        resp.write(output_data)
        await resp.write_eof()
    else:
        log.debug("POST Value - returning JSON data")
        rsp_json = {}
        data = arr_rsp.tolist()
        log.debug("got rsp data: {}".format(data))
        json_data = bytesArrayToList(data)
        rsp_json["value"] = json_data  
        resp = await jsonResponse(request, rsp_json)
    log.response(request, resp=resp)
    return resp




