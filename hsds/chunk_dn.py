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
# handles regauests to read/write chunk data
# 
#
import json
import time
import numpy as np
from aiohttp.errors import HttpBadRequest 
from aiohttp import HttpProcessingError 
from aiohttp.web import StreamResponse
from util.httpUtil import  jsonResponse
from util.idUtil import getS3Key, validateInPartition, isValidUuid
from util.s3Util import  isS3Obj, getS3Bytes   
from util.hdf5dtype import createDataType
from util.dsetUtil import  getSelectionShape, getSliceQueryParam

import hsds_logger as log

"""
Update the requested chunk/selection
"""
async def PUT_Chunk(request):
    log.request(request)
    app = request.app 
    #loop = app["loop"]

    chunk_id = request.match_info.get('id')
    if not chunk_id:
        msg = "Missing chunk id"
        log.error(msg)
        raise HttpBadRequest(message=msg)
    if not isValidUuid(chunk_id, "Chunk"):
        msg = "Invalid chunk id: {}".format(chunk_id)
        log.warn(msg)
        raise HttpBadRequest(message=msg)

    content_type = "application/octet-stream"
    if "Content-Type" in request.headers:
        # client should use "application/octet-stream" for binary transfer
        content_type = request.headers["Content-Type"]
    if content_type not in ("application/json", "application/octet-stream"):
        msg = "Unexpected content_type: {}".format(content_type)
        log.error(msg)
        raise HttpBadRequest(message=msg)

    validateInPartition(app, chunk_id)
    log.info("request params: {}".format(list(request.GET.keys())))
    dims = []
    selection = []
    if "itemsize" not in request.GET:
        msg = "Missing itemsize in PUT request"
        log.error(msg)
        raise HttpBadRequest(message=msg)
    
    itemsize = int(request.GET["itemsize"])
    log.info("itemsize: {}".format(itemsize))
    
    if "type" not in request.GET:
        msg = "Missing type in PUT request"
        log.error(msg)
        raise HttpBadRequest(message=msg)
    type_json = json.loads(request.GET["type"])
    log.info("type: {}".format(type_json))

    # get chunk extents from query params
    if "dim" not in request.GET:
        msg = "Missing dim in PUT chunk request"
        log.error(msg)
        raise HttpBadRequest(message=msg)

    dim_param = request.GET["dim"]
    if not dim_param.startswith('[') or not dim_param.endswith(']'):
        msg = "Invalid dim query param"
        log.error(msg)
        raise HttpBadRequest(message=msg)
    dim_param = dim_param[1:-1]  # strip off brackets
    dim_params = dim_param.split(',')
    for field in dim_params:
        dim = int(field)
        dims.append(dim)
    log.info("got dims: {}".format(dims))
    rank = len(dims)  

    # get chunk selection from query params
    selection = []
    for i in range(rank):
        dim_slice = getSliceQueryParam(request, i, dims[i])
        selection.append(dim_slice)   
    selection = tuple(selection)  
    log.info("got selection: {}".format(selection))

    dt = createDataType(type_json)
    log.info("dtype: {}".format(dt))

    rank = len(dims)
    if rank == 0:
        msg = "No dimension passed to PUT chunk request"
        log.error(msg)
        raise HttpBadRequest(message=msg)
    if len(selection) != rank:
        msg = "Selection rank does not match shape rank"
        log.error(msg)
        raise HttpBadRequest(message=msg)
    for i in range(rank):
        s = selection[i]
        log.info("selection[{}]: {}".format(i, s))

    input_shape = getSelectionShape(selection)
    num_elements = 1
    for extent in input_shape:
        num_elements *= extent
        
    # check that the content_length is what we expect
    log.info("expect content_length: {}".format(num_elements*itemsize))
    log.info("actual content_length: {}".format(request.content_length))

    if (num_elements * itemsize) != request.content_length:
        msg = "Excpected content_length of: {}, but got: {}".format(num_elements*itemsize, request.content_length)
        log.error(msg)
        raise HttpBadRequest(message=msg)

    # create a numpy array for incoming data
    input_bytes = await request.read()
    if len(input_bytes) != request.content_length:
        msg = "Read {} bytes, expecting: {}".format(len(input_bytes), request.content_length)
        log.error(msg)
        raise HttpProcessingError(code=500, message="Unexpected Error")
        
    input_arr = np.fromstring(input_bytes, dtype=dt)

    chunk_arr = None 
    data_cache = app['data_cache'] 
    s3_key = getS3Key(chunk_id)
    log.info("PUT_Chunks s3_key: {}".format(s3_key))
    if chunk_id in data_cache:
        chunk_arr = data_cache[chunk_id]
    else:
        obj_exists = await isS3Obj(app, s3_key)
        # TBD - potential race condition?
        if obj_exists:
            log.info("Reading chunk from S3")
            chunk_bytes = await getS3Bytes(app, s3_key)
            chunk_arr = np.fromstring(chunk_bytes, dtype=dt)
        else:
            log.info("Initializing chunk")
            chunk_arr = np.zeros(dims, dtype=dt)
        data_cache[chunk_id] = chunk_arr

    # update chunk array
    chunk_arr[selection] = input_arr

    # async write to S3   
    dirty_ids = app["dirty_ids"]
    now = int(time.time())
    dirty_ids[chunk_id] = now
    
    # chunk update successful     
    resp = await jsonResponse(request, {}, status=201)
    log.response(request, resp=resp)
    return resp


"""
Return data from requested chunk and selection
"""
async def GET_Chunk(request):
    log.request(request)
    app = request.app 
    #loop = app["loop"]

    chunk_id = request.match_info.get('id')
    if not chunk_id:
        msg = "Missing chunk id"
        log.error(msg)
        raise HttpBadRequest(message=msg)
    if not isValidUuid(chunk_id, "Chunk"):
        msg = "Invalid chunk id: {}".format(chunk_id)
        log.warn(msg)
        raise HttpBadRequest(message=msg)

    validateInPartition(app, chunk_id)
    log.info("request params: {}".format(list(request.GET.keys())))
    dims = []
    selection = []
    if "itemsize" not in request.GET:
        msg = "Missing itemsize in GET request"
        log.error(msg)
        raise HttpBadRequest(message=msg)
    
    itemsize = int(request.GET["itemsize"])
    log.info("itemsize: {}".format(itemsize))
    
    if "type" not in request.GET:
        msg = "Missing type in GET request"
        log.error(msg)
        raise HttpBadRequest(message=msg)
    type_json = json.loads(request.GET["type"])
    log.info("type: {}".format(type_json))

    # get chunk extents from query params
    if "dim" not in request.GET:
        msg = "Missing dim in GET chunk request"
        log.error(msg)
        raise HttpBadRequest(message=msg)

    dim_param = request.GET["dim"]
    if not dim_param.startswith('[') or not dim_param.endswith(']'):
        msg = "Invalid dim query param"
        log.error(msg)
        raise HttpBadRequest(message=msg)
    dim_param = dim_param[1:-1]  # strip off brackets
    dim_params = dim_param.split(',')
    for field in dim_params:
        dim = int(field)
        dims.append(dim)
    log.info("got dims: {}".format(dims))
    rank = len(dims)  

    # get chunk selection from query params
    if "select" in request.GET:
        log.info("select: {}".format(request.GET["select"]))
    selection = []
    for i in range(rank):
        dim_slice = getSliceQueryParam(request, i, dims[i])
        selection.append(dim_slice)   
    selection = tuple(selection)  
    log.info("got selection: {}".format(selection))

    dt = createDataType(type_json)
    log.info("dtype: {}".format(dt))

    rank = len(dims)
    if rank == 0:
        msg = "No dimension passed to GET chunk request"
        log.error(msg)
        raise HttpBadRequest(message=msg)
    if len(selection) != rank:
        msg = "Selection rank does not match shape rank"
        log.error(msg)
        raise HttpBadRequest(message=msg)
    for i in range(rank):
        s = selection[i]
        log.info("selection[{}]: {}".format(i, s))

    input_shape = getSelectionShape(selection)
    num_elements = 1
    for extent in input_shape:
        num_elements *= extent

    chunk_arr = None 
    data_cache = app['data_cache'] 
    
    if chunk_id in data_cache:
        chunk_arr = data_cache[chunk_id]
    else:
        s3_key = getS3Key(chunk_id)
        log.info("GET_Chunks s3_key: {}".format(s3_key))
        # check to see if there's a chunk object
        # TBD - potential race condition?
        obj_exists = await isS3Obj(app, s3_key)
        if not obj_exists:
            # return a 404
            msg = "Chunk {} does not exist".format(chunk_id)
            log.warn(msg)
            raise HttpProcessingError(code=404, message="Not found")
        log.info("Reading chunk {} from S3".format(s3_key))
        chunk_bytes = await getS3Bytes(app, s3_key)
        chunk_arr = np.fromstring(chunk_bytes, dtype=dt)
        data_cache[chunk_id] = chunk_arr

    # get requested data
    output_arr = chunk_arr[selection]
    output_data = output_arr.tobytes()
     
    # write response
    resp = StreamResponse(status=200)
    resp.headers['Content-Type'] = "application/octet-stream"
    resp.content_length = len(output_data)
    await resp.prepare(request)
    resp.write(output_data)
    await resp.write_eof()
    return resp
 
 




