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
import numpy as np
from aiohttp.web_exceptions import HTTPBadRequest, HTTPInternalServerError, HTTPNotFound
from aiohttp.web import json_response, StreamResponse

from .util.httpUtil import  request_read
from .util.arrayUtil import bytesToArray, arrayToBytes
from .util.idUtil import getS3Key, validateInPartition, isValidUuid
from .util.storUtil import  isStorObj, deleteStorObj
from .util.hdf5dtype import createDataType
from .util.dsetUtil import  getSliceQueryParam, getChunkLayout, getSelectionShape
from .util.chunkUtil import getChunkIndex, getDatasetId, chunkQuery
from .util.chunkUtil import chunkWriteSelection, chunkReadSelection
from .util.chunkUtil import chunkWritePoints, chunkReadPoints
from .datanode_lib import get_metadata_obj, get_chunk, save_chunk

from . import hsds_logger as log

"""
Update the requested chunk/selection
"""
async def PUT_Chunk(request):
    log.request(request)
    app = request.app
    params = request.rel_url.query
    query = None
    query_update = None
    limit = 0
    bucket = None
    input_arr = None

    if "query" in params:
        query = params["query"]
        log.info(f"PUT_Chunk query: {query}")
    if "Limit" in params:
        limit = int(params["Limit"])
    chunk_id = request.match_info.get('id')
    if not chunk_id:
        msg = "Missing chunk id"
        log.error(msg)
        raise HTTPBadRequest(reason=msg)
    if not isValidUuid(chunk_id, "Chunk"):
        msg = f"Invalid chunk id: {chunk_id}"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    if not request.has_body:
        msg = "PUT Value with no body"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    if "bucket" in params:
        bucket = params["bucket"]
        log.debug(f"PUT_Chunk using bucket: {bucket}")
    else:
        bucket = None

    if query:
        expected_content_type = "text/plain; charset=utf-8"
        chunk_init = False  # don't initalize new chunks on query update
    else:
        expected_content_type = "application/octet-stream"
        chunk_init = True
    if "Content-Type" in request.headers:
        # client should use "application/octet-stream" for binary transfer
        content_type = request.headers["Content-Type"]
        if content_type != expected_content_type:
            msg = f"Unexpected content_type: {content_type}"
            log.error(msg)
            raise HTTPBadRequest(reason=msg)

    validateInPartition(app, chunk_id)
    if "dset" in params:
        msg = "Unexpected param dset in GET request"
        log.error(msg)
        raise HTTPBadRequest(reason=msg)

    log.debug(f"PUT_Chunk - id: {chunk_id}")

    dset_id = getDatasetId(chunk_id)

    dset_json = await get_metadata_obj(app, dset_id, bucket=bucket)

    log.debug(f"dset_json: {dset_json}")

    # TBD - does this work with linked datasets?
    dims = getChunkLayout(dset_json)
    log.debug(f"got dims: {dims}")
    rank = len(dims)

    type_json = dset_json["type"]
    dt = createDataType(type_json)
    log.debug(f"dtype: {dt}")
    itemsize = 'H5T_VARIABLE'
    if "size" in type_json:
        itemsize = type_json["size"]

    # get chunk selection from query params
    selection = []
    for i in range(rank):
        dim_slice = getSliceQueryParam(request, i, dims[i])
        selection.append(dim_slice)
    selection = tuple(selection)
    log.debug(f"got selection: {selection}")

    mshape = getSelectionShape(selection)
    log.debug(f"mshape: {mshape}")
    num_elements = 1
    for extent in mshape:
        num_elements *= extent

    chunk_arr = await get_chunk(app, chunk_id, dset_json, bucket=bucket, chunk_init=chunk_init)
    is_dirty = False
    if chunk_arr is None:
        if chunk_init:
            log.error("failed to create numpy array")
            raise HTTPInternalServerError()
        else:
            log.warn(f"chunk {chunk_id} not found")
            raise HTTPNotFound()

    if query:
        if not dt.fields:
            log.error("expected compound dtype for PUT query")
            raise HTTPInternalServerError()
        if rank != 1:
            log.error("expected one-dimensional array for PUT query")
            raise HTTPInternalServerError()
        query_update = await request.json()
        log.debug(f"query_update: {query_update}")
        # TBD - send back binary response to SN node
        try:
            resp = chunkQuery(chunk_id=chunk_id, chunk_layout=dims, chunk_arr=chunk_arr, slices=selection,
                query=query, query_update=query_update, limit=limit, return_json=True)
        except TypeError as te:
            log.warn(f"chunkQuery - TypeError: {te}")
            raise HTTPBadRequest()
        except ValueError as ve:
            log.warn(f"chunkQuery - ValueError: {ve}")
            raise HTTPBadRequest()
        if query_update and resp is not None:
            is_dirty = True


    else:
        # regular chunk update

        # check that the content_length is what we expect
        if itemsize != 'H5T_VARIABLE':
            log.debug(f"expect content_length: {num_elements*itemsize}")
        log.debug(f"actual content_length: {request.content_length}")

        if itemsize != 'H5T_VARIABLE' and (num_elements * itemsize) != request.content_length:
            msg = f"Expected content_length of: {num_elements*itemsize}, but got: {request.content_length}"
            log.error(msg)
            raise HTTPBadRequest(reason=msg)

        # create a numpy array for incoming data
        input_bytes = await request_read(request)  # TBD - will it cause problems when failures are raised before reading data?
        if len(input_bytes) != request.content_length:
            msg = f"Read {len(input_bytes)} bytes, expecting: {request.content_length}"
            log.error(msg)
            raise HTTPInternalServerError()

        input_arr = bytesToArray(input_bytes, dt, mshape)

        is_dirty = chunkWriteSelection(chunk_arr=chunk_arr, slices=selection, data=input_arr)

        # chunk update successful
        resp = {}
    if is_dirty:
        save_chunk(app, chunk_id, dset_json, bucket=bucket)
        status_code = 201
    else:
        status_code = 200

    resp = json_response(resp, status=status_code)
    log.response(request, resp=resp)
    return resp


"""
Return data from requested chunk and selection
"""
async def GET_Chunk(request):
    log.request(request)
    app = request.app
    params = request.rel_url.query

    chunk_id = request.match_info.get('id')
    if not chunk_id:
        msg = "Missing chunk id"
        log.error(msg)
        raise HTTPBadRequest(reason=msg)
    if not isValidUuid(chunk_id, "Chunk"):
        msg = f"Invalid chunk id: {chunk_id}"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    validateInPartition(app, chunk_id)
    log.debug(f"request params: {params.keys()}")

    bucket = None
    s3path = None
    s3offset = None
    s3size = None
    query = None
    limit = 0
    if "s3path" in params:
        s3path = params["s3path"]
        log.debug(f"GET_Chunk - using s3path: {s3path}")
    elif "bucket" in params:
        bucket = params["bucket"]
    if "s3offset" in params:
        try:
            s3offset = int(params["s3offset"])
        except ValueError:
            log.error(f"invalid s3offset params: {params['s3offset']}")
            raise HTTPBadRequest()
    if "s3size" in params:
        try:
            s3size = int(params["s3size"])
        except ValueError:
            log.error(f"invalid s3size params: {params['s3sieze']}")
            raise HTTPBadRequest()

    if "query" in params:
        query = params["query"]
    if "Limit" in params:
        limit = int(params["Limit"])

    dset_id = getDatasetId(chunk_id)

    dset_json = await get_metadata_obj(app, dset_id, bucket=bucket)
    dims = getChunkLayout(dset_json)
    log.debug(f"got dims: {dims}")
    rank = len(dims)

    log.debug(f"dset_json: {dset_json}")

    # get chunk selection from query params
    selection = []
    for i in range(rank):
        dim_slice = getSliceQueryParam(request, i, dims[i])
        selection.append(dim_slice)
    selection = tuple(selection)
    log.debug(f"got selection: {selection}")

    chunk_arr = await get_chunk(app, chunk_id, dset_json, bucket=bucket, s3path=s3path, s3offset=s3offset, s3size=s3size, chunk_init=False)
    if chunk_arr is None:
        msg = f"chunk {chunk_id} not found"
        log.warn(msg)
        raise HTTPNotFound()

    if query:
        # run given query
        try:
            read_resp = chunkQuery(chunk_id=chunk_id, chunk_layout=dims, chunk_arr=chunk_arr, slices=selection,
                query=query, limit=limit, return_json=True)
        except TypeError as te:
            log.warn(f"chunkQuery - TypeError: {te}")
            raise HTTPBadRequest()
        except ValueError as ve:
            log.warn(f"chunkQuery - ValueError: {ve}")
            raise HTTPBadRequest()
    else:
        # read selected data from chunk
        output_arr = chunkReadSelection(chunk_arr, slices=selection)
        read_resp = arrayToBytes(output_arr)

    # write response
    if isinstance(read_resp, bytes):

        try:
            resp = StreamResponse()
            resp.headers['Content-Type'] = "application/octet-stream"
            resp.content_length = len(read_resp)
            await resp.prepare(request)
            await resp.write(read_resp)
        except Exception as e:
            log.error(f"Exception during binary data write: {e}")
            raise HTTPInternalServerError()
        finally:
            await resp.write_eof()
    else:
        # JSON response
        resp = json_response(read_resp)

    return resp

"""
Return data from requested chunk and point selection
"""
async def POST_Chunk(request):
    log.request(request)
    app = request.app
    params = request.rel_url.query

    put_points = False
    num_points = 0
    if "count" not in params:
        log.warn("expected count param")
        raise HTTPBadRequest()
    if "count" in params:
        num_points = int(params["count"])

    if "action" in params and params["action"] == "put":
        log.info(f"POST Chunk put points - num_points: {num_points}")
        put_points = True
    else:
        log.info(f"POST Chunk get points - num_points: {num_points}")

    s3path = None
    s3offset = 0
    s3size = 0
    if "s3path" in params:
        if put_points:
            log.error("s3path can not be used with put points POST request")
            raise HTTPBadRequest()
        s3path = params["s3path"]
        log.debug(f"GET_Chunk - using s3path: {s3path}")
        bucket = None
    elif "bucket" in params:
        bucket = params["bucket"]
    else:
        bucket = None
    if "s3offset" in params:
        try:
            s3offset = int(params["s3offset"])
        except ValueError:
            log.error(f"invalid s3offset params: {params['s3offset']}")
            raise HTTPBadRequest()
    if "s3size" in params:
        try:
            s3size = int(params["s3size"])
        except ValueError:
            log.error(f"invalid s3size params: {params['s3sieze']}")
            raise HTTPBadRequest()

    chunk_id = request.match_info.get('id')
    if not chunk_id:
        msg = "Missing chunk id"
        log.error(msg)
        raise HTTPBadRequest(reason=msg)
    log.info(f"POST chunk_id: {chunk_id}")
    chunk_index = getChunkIndex(chunk_id)
    log.debug(f"chunk_index: {chunk_index}")

    if not isValidUuid(chunk_id, "Chunk"):
        msg = f"Invalid chunk id: {chunk_id}"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    validateInPartition(app, chunk_id)
    log.debug(f"request params: {list(params.keys())}")
    if "dset" in params:
        msg = "Unexpected dset in POST request"
        log.error(msg)
        raise HTTPBadRequest(reason=msg)

    if not request.has_body:
        msg = "POST Value with no body"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    content_type = "application/octet-stream"
    if "Content-Type" in request.headers:
        # client should use "application/octet-stream" for binary transfer
        content_type = request.headers["Content-Type"]
    if content_type != "application/octet-stream":
        msg = f"Unexpected content_type: {content_type}"
        log.error(msg)
        raise HTTPBadRequest(reason=msg)

    dset_id = getDatasetId(chunk_id)

    dset_json = await get_metadata_obj(app, dset_id, bucket=bucket)
    dims = getChunkLayout(dset_json)
    rank = len(dims)

    type_json = dset_json["type"]
    dset_dtype = createDataType(type_json)

    # create a numpy array for incoming points
    input_bytes = await request_read(request)
    if len(input_bytes) != request.content_length:
        msg = f"Read {len(input_bytes)} bytes, expecting: {request.content_length}"
        log.error(msg)
        raise HTTPInternalServerError()

    if rank == 1:
        coord_type_str = "uint64"
    else:
        coord_type_str = f"({rank},)uint64"

    if put_points:
        # create a numpy array with the following type:
        #       (coord1, coord2, ...) | dset_dtype
        point_dt = np.dtype([("coord", np.dtype(coord_type_str)), ("value", dset_dtype)])
        point_shape = (num_points,)
        chunk_init = True
    else:
        point_dt = np.dtype('uint64')
        point_shape = (num_points, rank)
        chunk_init = False

    point_arr = bytesToArray(input_bytes, point_dt, point_shape)

    chunk_arr = await get_chunk(app, chunk_id, dset_json, bucket=bucket, s3path=s3path, s3offset=s3offset, s3size=s3size, chunk_init=chunk_init)
    if chunk_arr is None:
        log.warn(f"chunk {chunk_id} not found")
        raise HTTPNotFound()

    if put_points:
        # writing point data
        try:
            chunkWritePoints(chunk_id=chunk_id, chunk_layout=dims, chunk_arr=chunk_arr, point_arr=point_arr)
        except ValueError as ve:
            log.warn(f"got value error from chunkWritePoints: {ve}")
            raise HTTPBadRequest()
         # write empty response
        resp = json_response({})

        save_chunk(app, chunk_id, dset_json, bucket=bucket) # lazily write chunk to storage
    else:
        # read points
        try:
            output_arr = chunkReadPoints(chunk_id=chunk_id, chunk_layout=dims, chunk_arr=chunk_arr, point_arr=point_arr)
        except ValueError as ve:
            log.warn(f"got value error from chunkReadPoints: {ve}")
            raise HTTPBadRequest()
        output_data = arrayToBytes(output_arr)
        # write response
        try:
            resp = StreamResponse()
            resp.headers['Content-Type'] = "application/octet-stream"
            resp.content_length = len(output_data)
            await resp.prepare(request)
            await resp.write(output_data)
        except Exception as e:
            log.error(f"Exception during binary data write: {e}")
            raise HTTPInternalServerError()
        finally:
            await resp.write_eof()

    return resp

async def DELETE_Chunk(request):
    """HTTP DELETE method for /chunks/
    Note: clients (i.e. SN nodes) don't directly delete chunks.  This method should
    only be called by the AN node.
    """
    log.request(request)
    app = request.app
    params = request.rel_url.query
    chunk_id = request.match_info.get('id')
    if not chunk_id:
        msg = "Missing chunk id"
        log.error(msg)
        raise HTTPBadRequest(reason=msg)
    log.info(f"DELETE chunk: {chunk_id}")

    if not isValidUuid(chunk_id, "Chunk"):
        msg = f"Invalid chunk id: {chunk_id}"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    if "bucket" in params:
        bucket = params["bucket"]
    else:
        bucket = None

    validateInPartition(app, chunk_id)

    chunk_cache = app['chunk_cache']
    s3key = getS3Key(chunk_id)
    log.debug(f"DELETE_Chunk s3_key: {s3key}")

    if chunk_id in chunk_cache:
        del chunk_cache[chunk_id]

    filter_map = app["filter_map"]
    dset_id = getDatasetId(chunk_id)
    if dset_id in filter_map:
        # The only reason chunks are ever deleted is if the dataset is being deleted,
        # so it should be safe to remove this entry now
        log.info(f"Removing filter_map entry for {dset_id}")
        del filter_map[dset_id]

    if await isStorObj(app, s3key, bucket=bucket):
        await deleteStorObj(app, s3key, bucket=bucket)
    else:
        log.info(f"delete_metadata_obj - key {s3key} not found (never written)?")

    resp_json = {  }
    resp = json_response(resp_json)
    log.response(request, resp=resp)
    return resp
