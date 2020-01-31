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
from aiohttp.web_exceptions import HTTPBadRequest, HTTPInternalServerError
from aiohttp.web import json_response, StreamResponse

from util.httpUtil import  request_read
from util.arrayUtil import bytesToArray
from util.idUtil import getS3Key, validateInPartition, isValidUuid
from util.storUtil import  isStorObj, deleteStorObj
from util.hdf5dtype import createDataType
from util.dsetUtil import  getSliceQueryParam, getChunkLayout, getSelectionShape
from util.chunkUtil import getChunkIndex, getDatasetId
from datanode_lib import get_metadata_obj
from chunk_lib import chunk_read_selection, chunk_write_selection, chunk_write_points, chunk_read_points


import hsds_logger as log

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
    else:
        expected_content_type = "application/octet-stream"
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
    log.debug(f"got selection: {selection}")

    mshape = getSelectionShape(selection)
    log.debug(f"mshape: {mshape}")
    num_elements = 1
    for extent in mshape:
        num_elements *= extent

    if query:
        if not dt.fields:
            log.error("expected compound dtype for PUT query")
            raise HTTPInternalServerError()
        if rank != 1:
            log.error("expected one-dimensional array for PUT query")
            raise HTTPInternalServerError()
        query_update = await request.json()
        log.debug(f"query_update: {query_update}")
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

    write_resp = await chunk_write_selection(app, chunk_id=chunk_id, selection=selection, dset_json=dset_json,
        bucket=bucket, query=query, query_update=query_update, limit=limit, input_arr=input_arr)

    # chunk update successful
    resp = json_response(write_resp)
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
    log.debug(f"got selection: {selection}")

    # read selected data from chunk
    read_resp = await chunk_read_selection(app, chunk_id=chunk_id,
        dset_json=dset_json, bucket=bucket,
        selection=selection, query=query, limit=limit,
        s3path=s3path, s3offset=s3offset, s3size=s3size)

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

    if "action" in params and params["action"] == "put":
        log.info(f"POST Chunk put points")
        put_points = True
    else:
        log.info("POST Chunk get points")

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

    # create a numpy array for incoming points
    input_bytes = await request_read(request)
    if len(input_bytes) != request.content_length:
        msg = f"Read {len(input_bytes)} bytes, expecting: {request.content_length}"
        log.error(msg)
        raise HTTPInternalServerError()

    if put_points:
        # writing point data
        await chunk_write_points(app, chunk_id=chunk_id, dset_json=dset_json, bucket=bucket, input_bytes=input_bytes)
        # write empty response
        resp = json_response({})
    else:
        # read points
        output_data = await chunk_read_points(app, chunk_id=chunk_id, dset_json=dset_json, bucket=bucket, 
            input_bytes=input_bytes, s3path=s3path, s3offset=s3offset, s3size=s3size)
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

    deflate_map = app["deflate_map"]
    shuffle_map = app["shuffle_map"]
    dset_id = getDatasetId(chunk_id)
    if dset_id in deflate_map:
        # The only reason chunks are ever deleted is if the dataset is being deleted,
        # so it should be safe to remove this entry now
        log.info(f"Removing deflate_map entry for {dset_id}")
        del deflate_map[dset_id]
    if dset_id in shuffle_map:
        log.info(f"Removing shuffle_map entry for {dset_id}")
        del shuffle_map[dset_id]

    if await isStorObj(app, s3key, bucket=bucket):
        await deleteStorObj(app, s3key, bucket=bucket)
    else:
        log.info(f"delete_metadata_obj - key {s3key} not found (never written)?")

    resp_json = {  }
    resp = json_response(resp_json)
    log.response(request, resp=resp)
    return resp
