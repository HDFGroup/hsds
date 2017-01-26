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
from util.dsetUtil import  getSelectionShape, getSliceQueryParam, getFillValue, getChunkLayout
from util.chunkUtil import getChunkIndex, getChunkCoordinate, getChunkRelativePoint

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

    if not request.has_body:
        msg = "PUT Value with no body"
        log.warn(msg)
        raise HttpBadRequest(message=msg)

    content_type = "application/octet-stream"
    if "Content-Type" in request.headers:
        # client should use "application/octet-stream" for binary transfer
        content_type = request.headers["Content-Type"]
    if content_type != "application/octet-stream":
        msg = "Unexpected content_type: {}".format(content_type)
        log.error(msg)
        raise HttpBadRequest(message=msg)

    validateInPartition(app, chunk_id)
    log.info("request params: {}".format(list(request.GET.keys())))
    if "dset" not in request.GET:
        msg = "Missing dset in GET request"
        log.error(msg)
        raise HttpBadRequest(message=msg)
    dset_json = json.loads(request.GET["dset"])
    log.info("dset_json: {}".format(dset_json))
    dims = getChunkLayout(dset_json)
     
    rank = len(dims)  
   
    fill_value = getFillValue(dset_json)
     
    # get chunk selection from query params
    selection = []
    for i in range(rank):
        dim_slice = getSliceQueryParam(request, i, dims[i])
        selection.append(dim_slice)   
    selection = tuple(selection)  
    log.info("got selection: {}".format(selection))

    type_json = dset_json["type"]
    dt = createDataType(type_json)
    log.info("dtype: {}".format(dt))
    itemsize = dt.itemsize

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
    log.info("input_shape: {}".format(input_shape))
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
    input_bytes = await request.read()  # TBD - will it cause problems when failures are raised before reading data?
    if len(input_bytes) != request.content_length:
        msg = "Read {} bytes, expecting: {}".format(len(input_bytes), request.content_length)
        log.error(msg)
        raise HttpProcessingError(code=500, message="Unexpected Error")
        
    input_arr = np.fromstring(input_bytes, dtype=dt)
    #log.info("input arr: {}".format(input_arr))
    input_arr = input_arr.reshape(input_shape)

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
            chunk_arr = chunk_arr.reshape(dims)
        else:
            log.info("Initializing chunk")
            if fill_value:
                # need to convert list to tuples for numpy broadcast
                if isinstance(fill_value, list):
                    fill_value = tuple(fill_value)
                chunk_arr = np.empty(dims, dtype=dt, order='C')
                chunk_arr[...] = fill_value
            else:
                chunk_arr = np.zeros(dims, dtype=dt, order='C')
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
    if "dset" not in request.GET:
        msg = "Missing dset in GET request"
        log.error(msg)
        raise HttpBadRequest(message=msg)
    dset_json = json.loads(request.GET["dset"])
    
    log.info("dset_json: {}".format(dset_json)) 
    type_json = dset_json["type"]
     
    dims = getChunkLayout(dset_json)
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
        log.info("chunk size: {}".format(chunk_arr.size))
        log.info("chunk_arr before reshape: {}".format(chunk_arr))
        chunk_arr = chunk_arr.reshape(dims)
        log.info("got chunk array: {}".format(chunk_arr))
        data_cache[chunk_id] = chunk_arr  # store in cache

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

"""
Return data from requested chunk and point selection
"""
async def POST_Chunk(request):
    log.request(request)
    app = request.app 
    #loop = app["loop"]

    chunk_id = request.match_info.get('id')
    if not chunk_id:
        msg = "Missing chunk id"
        log.error(msg)
        raise HttpBadRequest(message=msg)
    log.info("chunk_id: {}".format(chunk_id))
    chunk_index = getChunkIndex(chunk_id)
    log.info("chunk_index: {}".format(chunk_index))
    
    if not isValidUuid(chunk_id, "Chunk"):
        msg = "Invalid chunk id: {}".format(chunk_id)
        log.warn(msg)
        raise HttpBadRequest(message=msg)

    validateInPartition(app, chunk_id)
    log.info("request params: {}".format(list(request.GET.keys())))
    if "dset" not in request.GET:
        msg = "Missing dset in GET request"
        log.error(msg)
        raise HttpBadRequest(message=msg)
    dset_json = json.loads(request.GET["dset"])
    log.info("dset_json: {}".format(dset_json))
    chunk_layout = getChunkLayout(dset_json)
    chunk_coord = getChunkCoordinate(chunk_id, chunk_layout)
    log.info("chunk_coord: {}".format(chunk_coord))
    
    
    if not request.has_body:
        msg = "POST Value with no body"
        log.warn(msg)
        raise HttpBadRequest(message=msg)

    content_type = "application/octet-stream"
    if "Content-Type" in request.headers:
        # client should use "application/octet-stream" for binary transfer
        content_type = request.headers["Content-Type"]
    if content_type != "application/octet-stream":
        msg = "Unexpected content_type: {}".format(content_type)
        log.error(msg)
        raise HttpBadRequest(message=msg)
    
    dims = getChunkLayout(dset_json)
    log.info("got dims: {}".format(dims))
    rank = len(dims)  

    type_json = dset_json["type"]
    dt = createDataType(type_json)
    log.info("dtype: {}".format(dt))

    rank = len(dims)
    if rank == 0:
        msg = "No dimension passed to POST chunk request"
        log.error(msg)
        raise HttpBadRequest(message=msg)

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
        chunk_arr = chunk_arr.reshape(dims)
        data_cache[chunk_id] = chunk_arr  # store in cache
    # create a numpy array for incoming points
    input_bytes = await request.read()  # TBD - will it cause problems when failures are raised before reading data?
    if len(input_bytes) != request.content_length:
        msg = "Read {} bytes, expecting: {}".format(len(input_bytes), request.content_length)
        log.error(msg)
        raise HttpProcessingError(code=500, message="Unexpected Error")
    point_dt = np.dtype('u8')  # use unsigned long for point index    
    point_arr = np.fromstring(input_bytes, dtype=point_dt)  # read points as unsigned longs
    if len(point_arr) % rank != 0:
        msg = "Unexpected size of point array"
        log.warn(msg)
        raise HttpBadRequest(message=msg)
    num_points = len(point_arr) // rank
    log.info("got {} points".format(num_points))

    point_arr = point_arr.reshape((num_points, rank))
    log.info("reshaped point array: {}".format(point_arr))
    
    output_arr = np.zeros((num_points,), dtype=dt)
    
    for i in range(num_points):
        point = point_arr[i,:]
        log.info("point: {}".format(point))
        tr_point = getChunkRelativePoint(chunk_coord, point)
        log.info("tr_point: {}".format(tr_point))
        val = chunk_arr[tuple(tr_point)]
        log.info("chunk val: {}".format(val))
        output_arr[i] = val
        log.info("processing point: {}".format(point))
     
    # write response
    resp = StreamResponse(status=200)
    resp.headers['Content-Type'] = "application/octet-stream"
    output_data = output_arr.tobytes()
    resp.content_length = len(output_data)
    await resp.prepare(request)
    resp.write(output_data)
    await resp.write_eof()
    return resp
 
 




