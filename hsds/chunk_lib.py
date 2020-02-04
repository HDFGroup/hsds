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
import asyncio
import time
import numpy as np
from aiohttp.web_exceptions import HTTPBadRequest, HTTPNotFound, HTTPInternalServerError, HTTPServiceUnavailable

from util.arrayUtil import bytesArrayToList, arrayToBytes, bytesToArray
from util.idUtil import isValidUuid, getS3Key
from util.hdf5dtype import createDataType, getItemSize
from util.storUtil import  isStorObj, getStorBytes
from util.dsetUtil import getEvalStr, getChunkLayout, getSelectionShape
from util.dsetUtil import getFillValue, getDeflateLevel, isShuffle
from util.chunkUtil import getDatasetId, getChunkCoordinate, getChunkRelativePoint



import hsds_logger as log



"""
Return data from requested chunk and selection
"""
async def chunk_read_selection(app, chunk_id=None, selection=None, dset_json=None,
    bucket=None, query=None, limit=0, s3path=None, s3offset=None, s3size=None):
    log.info(f"chunk_read_selection - chunk_id: {chunk_id}")
    if not chunk_id:
        msg = "Missing chunk id"
        log.error(msg)
        raise HTTPBadRequest(reason=msg)
    if not isValidUuid(chunk_id, "Chunk"):
        msg = f"Invalid chunk id: {chunk_id}"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    log.debug(f"dset_json: {dset_json}")
    type_json = dset_json["type"]

    dims = getChunkLayout(dset_json)
    log.debug(f"got dims: {dims}")
    rank = len(dims)
    if rank == 0:
        msg = "No dimension passed to GET chunk request"
        log.error(msg)
        raise HTTPBadRequest(reason=msg)

    log.debug(f"got selection: {selection}")
    selection = tuple(selection)

    if len(selection) != rank:
        msg = "Selection rank does not match shape rank"
        log.error(msg)
        raise HTTPBadRequest(reason=msg)

    for i in range(rank):
        s = selection[i]
        log.debug(f"selection[{i}]: {s}")

    dt = createDataType(type_json)
    log.debug(f"dtype: {dt}")

    if query:
        # do query selection
        log.info(f"query: {query}")
        if rank != 1:
            msg = "Query selection only supported for one dimensional arrays"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)

    chunk_arr = await getChunk(app, chunk_id, dset_json, bucket=bucket, s3path=s3path, s3offset=s3offset, s3size=s3size)

    if chunk_arr is None:
        # return a 404
        msg = f"Chunk {chunk_id} does not exist"
        log.info(msg)
        raise HTTPNotFound()

    resp = None

    if query:
        limit = 0
        values = []
        indices = []
        field_names = []
        if dt.fields:
            field_names = list(dt.fields.keys())

        x = chunk_arr[selection]
        log.debug(f"x: {x}")
        eval_str = getEvalStr(query, "x", field_names)
        log.debug(f"eval_str: {eval_str}")
        where_result = np.where(eval(eval_str))
        log.debug(f"where_result: {where_result}")
        where_result_index = where_result[0]
        log.debug(f"whare_result index: {where_result_index}")
        log.debug(f"boolean selection: {x[where_result_index]}")
        s = selection[0]
        count = 0
        for index in where_result_index:
            log.debug(f"index: {index}")
            value = x[index].tolist()
            log.debug(f"value: {value}")
            json_val = bytesArrayToList(value)
            log.debug(f"json_value: {json_val}")
            json_index = index.tolist() * s.step + s.start  # adjust for selection
            indices.append(json_index)
            values.append(json_val)
            count += 1
            if limit > 0 and count >= limit:
                log.info("got limit items")
                break

        resp = {}
        resp["index"] = indices
        resp["value"] = values
        log.info(f"query_result retiurning: {len(indices)} rows")
        log.debug(f"query_result: {resp}")
    else:
        # get requested data
        output_arr = chunk_arr[selection]
        resp = arrayToBytes(output_arr)

    return resp


"""
Write data for requested chunk and selection
"""
async def chunk_write_selection(app, chunk_id=None, selection=None, dset_json=None,
    bucket=None, input_arr=None):
    log.info(f"chunk_write_selection - chunk_id: {chunk_id}")
    log.debug(f"dset_json: {dset_json}")
    dims = getChunkLayout(dset_json)

    rank = len(dims)

    type_json = dset_json["type"]

    dt = createDataType(type_json)
    log.debug(f"dtype: {dt}")
    log.debug(f"selection: {selection}")
    selection = tuple(selection)

    if rank == 0:
        msg = "chunk_write_selection - No dimension passed to PUT chunk request"
        log.error(msg)
        raise HTTPBadRequest(reason=msg)
    if len(selection) != rank:
        msg = "Selection rank does not match shape rank"
        log.error(msg)
        raise HTTPBadRequest(reason=msg)
    for i in range(rank):
        s = selection[i]
        log.debug(f"selection[{i}]: {s}")

    mshape = getSelectionShape(selection)
    log.debug(f"mshape: {mshape}")
    num_elements = 1
    for extent in mshape:
        num_elements *= extent

    resp = {}
    limit = 0
    chunk_init=True

    # TBD: Skip read if the input shape is the entire chunk?
    chunk_arr = await getChunk(app, chunk_id, dset_json, chunk_init=chunk_init, bucket=bucket)
    is_dirty = False

    # update chunk array
    try:
        chunk_arr[selection] = input_arr
    except ValueError as ve:
        log.error(f"Numpy Value updating array: {ve}")
        raise HTTPInternalServerError()

    chunk_cache = app["chunk_cache"]
    chunk_cache.setDirty(chunk_id)
    log.info(f"PUT_Chunk dirty cache count: {chunk_cache.dirtyCount}")

    # async write to S3
    dirty_ids = app["dirty_ids"]
    now = int(time.time())
    dirty_ids[chunk_id] = (now, bucket)
    # write empty response
    return {}


"""
Run query on chunk and selection
"""
async def chunk_query(app, chunk_id=None, selection=None, dset_json=None,
    bucket=None, query=None, query_update=None, limit=0):
    log.info(f"chunk_query - chunk_id: {chunk_id}")
    log.debug(f"dset_json: {dset_json}")
    dims = getChunkLayout(dset_json)

    rank = len(dims)

    type_json = dset_json["type"]

    dt = createDataType(type_json)
    log.debug(f"dtype: {dt}")
    log.debug(f"selection: {selection}")
    selection = tuple(selection)

    if rank != 1:
        msg = "Query operations only supported on one-dimensional datasets"
        log.error(msg)
        raise HTTPBadRequest(reason=msg)
    if len(selection) != rank:
        msg = "Selection rank does not match shape rank"
        log.error(msg)
        raise HTTPBadRequest(reason=msg)

    mshape = getSelectionShape(selection)
    log.debug(f"mshape: {mshape}")
    num_elements = 1
    for extent in mshape:
        num_elements *= extent

    resp = {}
    limit = 0

    # TBD: Skip read if the input shape is the entire chunk?
    chunk_arr = await getChunk(app, chunk_id, dset_json, chunk_init=False, bucket=bucket)
    if not chunk_arr:
        log.info(f"chunk_query - chunk {chunk_id} not found")
        raise HTTPNotFound()

    is_dirty = False

    values = []
    indices = []
    # do query selection
    field_names = list(dt.fields.keys())

    if query_update:
        replace_mask = [None,] * len(field_names)
        for i in range(len(field_names)):
            field_name = field_names[i]
            if field_name in query_update:
                replace_mask[i] = query_update[field_name]
            log.debug(f"replace_mask: {replace_mask}")
            if replace_mask == [None,] * len(field_names):
                log.warn(f"no fields found in query_update")
                raise HTTPBadRequest()
    else:
        replace_mask = None

    x = chunk_arr[selection]
    log.debug(f"put_query - x: {x}")
    eval_str = getEvalStr(query, "x", field_names)
    log.debug(f"put_query - eval_str: {eval_str}")
    where_result = np.where(eval(eval_str))
    log.debug(f"put_query - where_result: {where_result}")
    where_result_index = where_result[0]
    log.debug(f"put_query - whare_result index: {where_result_index}")
    log.debug(f"put_query - boolean selection: {x[where_result_index]}")
    s = selection[0]
    count = 0
    for index in where_result_index:
        log.debug(f"put_query - index: {index}")
        value = x[index].copy()
        log.debug(f"put_query - original value: {value}")
        for i in range(len(field_names)):
            if replace_mask[i] is not None:
                value[i] = replace_mask[i]
        log.debug(f"put_query - modified value: {value}")
        try:
            chunk_arr[index] = value
        except ValueError as ve:
            log.error(f"Numpy Value updating array: {ve}")
            raise HTTPInternalServerError()

        json_val = bytesArrayToList(value)
        log.debug(f"put_query - json_value: {json_val}")
        json_index = index.tolist() * s.step + s.start  # adjust for selection
        indices.append(json_index)
        values.append(json_val)
        count += 1
        is_dirty = True
        if limit > 0 and count >= limit:
            log.info("put_query - got limit items")
            break

    query_result = {}
    query_result["index"] = indices
    query_result["value"] = values
    log.info(f"query_result returning: {len(indices)} rows")
    log.debug(f"query_result: {query_result}")
    resp = query_result

    if is_dirty:
        chunk_cache = app["chunk_cache"]
        chunk_cache.setDirty(chunk_id)
        log.info(f"PUT_Chunk dirty cache count: {chunk_cache.dirtyCount}")

        # async write to S3
        dirty_ids = app["dirty_ids"]
        now = int(time.time())
        dirty_ids[chunk_id] = (now, bucket)
        # write empty response
        resp = {}
    return resp


"""
Write points to given chunk
"""
async def chunk_write_points(app, chunk_id=None, dset_json=None, bucket=None, input_bytes=None):
    # writing point data
    dims = getChunkLayout(dset_json)

    log.debug(f"got dims: {dims}")
    rank = len(dims)
    type_json = dset_json["type"]
    dset_dtype = createDataType(type_json)
    log.debug(f"dtype: {dset_dtype}")

    # get chunk from cache/s3.  If not found init a new chunk if this is a write request
    chunk_arr = await getChunk(app, chunk_id, dset_json, bucket=bucket, chunk_init=True)

    if chunk_arr is None:
        log.error("no array returned for put_points")
        raise HTTPInternalServerError()

    log.debug(f"chunk_arr.shape: {chunk_arr.shape}")

    # create a numpy array with the following type:
    #       (coord1, coord2, ...) | dset_dtype
    if rank == 1:
        coord_type_str = "uint64"
    else:
        coord_type_str = f"({rank},)uint64"
    comp_dtype = np.dtype([("coord", np.dtype(coord_type_str)), ("value", dset_dtype)])
    point_arr = np.fromstring(input_bytes, dtype=comp_dtype)

    num_points = len(point_arr)

    for i in range(num_points):
        elem = point_arr[i]
        log.debug(f"non-relative coordinate: {elem}")
        if rank == 1:
            coord = int(elem[0])
            coord = coord % dims[0] # adjust to chunk relative
        else:
            coord = elem[0] # index to update
            for dim in range(rank):
                # adjust to chunk relative
                coord[dim] = int(coord[dim]) % dims[dim]
            coord = tuple(coord)  # need to convert to a tuple
        log.debug(f"relative coordinate: {coord}")

        val = elem[1]   # value
        try:
            chunk_arr[coord] = val # update the point
        except IndexError:
            msg = "Out of bounds point index for POST Chunk"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)

    chunk_cache = app["chunk_cache"]
    chunk_cache.setDirty(chunk_id)

    # async write to S3
    dirty_ids = app["dirty_ids"]
    now = int(time.time())
    dirty_ids[chunk_id] = (now, bucket)
    log.info(f"set {chunk_id} to dirty")

"""
Read points from given chunk
"""
async def chunk_read_points(app, chunk_id=None, dset_json=None, bucket=None,
    input_bytes=None, s3path=None, s3offset=None, s3size=None):
    log.info(f"chunk_read_points - chunk_id: {chunk_id}")
    if not chunk_id:
        msg = "Missing chunk id"
        log.error(msg)
        raise HTTPBadRequest(reason=msg)
    if not isValidUuid(chunk_id, "Chunk"):
        msg = f"Invalid chunk id: {chunk_id}"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    log.debug(f"dset_json: {dset_json}")
    type_json = dset_json["type"]

    dims = getChunkLayout(dset_json)
    log.debug(f"got dims: {dims}")
    chunk_coord = getChunkCoordinate(chunk_id, dims)
    log.debug(f"chunk_coord: {chunk_coord}")
    rank = len(dims)
    if rank == 0:
        msg = "No dimension passed to chunk read points"
        log.error(msg)
        raise HTTPBadRequest(reason=msg)

    dset_dtype = createDataType(type_json)
    log.debug(f"dtype: {dset_dtype}")

    chunk_arr = await getChunk(app, chunk_id, dset_json, bucket=bucket, s3path=s3path, s3offset=s3offset, s3size=s3size)

    if chunk_arr is None:
        # return a 404
        msg = f"Chunk {chunk_id} does not exist"
        log.info(msg)
        raise HTTPNotFound()

    # reading point data
    point_dt = np.dtype('uint64')  # use unsigned long for point index
    point_arr = np.fromstring(input_bytes, dtype=point_dt)  # read points as unsigned longs
    if len(point_arr) % rank != 0:
        msg = "Unexpected size of point array"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    num_points = len(point_arr) // rank
    log.debug(f"got {num_points} points")

    point_arr = point_arr.reshape((num_points, rank))
    output_arr = np.zeros((num_points,), dtype=dset_dtype)

    for i in range(num_points):
        point = point_arr[i,:]
        tr_point = getChunkRelativePoint(chunk_coord, point)
        val = chunk_arr[tuple(tr_point)]
        output_arr[i] = val
    resp = arrayToBytes(output_arr)
    return resp


"""
Run query on chunk and selection
"""
async def chunk_query(app, chunk_id=None, selection=None, dset_json=None,
    bucket=None, query=None, query_update=None, limit=0):
    log.info(f"chunk_query - chunk_id: {chunk_id}")
    log.debug(f"dset_json: {dset_json}")
    dims = getChunkLayout(dset_json)

    rank = len(dims)

    type_json = dset_json["type"]

    dt = createDataType(type_json)
    log.debug(f"dtype: {dt}")
    log.debug(f"selection: {selection}")
    selection = tuple(selection)

    if rank != 1:
        msg = "Query operations only supported on one-dimensional datasets"
        log.error(msg)
        raise HTTPBadRequest(reason=msg)
    if len(selection) != rank:
        msg = "Selection rank does not match shape rank"
        log.error(msg)
        raise HTTPBadRequest(reason=msg)

    mshape = getSelectionShape(selection)
    log.debug(f"mshape: {mshape}")
    num_elements = 1
    for extent in mshape:
        num_elements *= extent

    resp = {}
    limit = 0

    # TBD: Skip read if the input shape is the entire chunk?
    chunk_arr = await getChunk(app, chunk_id, dset_json, chunk_init=False, bucket=bucket)
    if not chunk_arr:
        log.info(f"chunk_query - chunk {chunk_id} not found")
        raise HTTPNotFound()

    is_dirty = False

    values = []
    indices = []
    # do query selection
    field_names = list(dt.fields.keys())

    if query_update:
        replace_mask = [None,] * len(field_names)
        for i in range(len(field_names)):
            field_name = field_names[i]
            if field_name in query_update:
                replace_mask[i] = query_update[field_name]
            log.debug(f"replace_mask: {replace_mask}")
            if replace_mask == [None,] * len(field_names):
                log.warn(f"no fields found in query_update")
                raise HTTPBadRequest()
    else:
        replace_mask = None

    x = chunk_arr[selection]
    log.debug(f"put_query - x: {x}")
    eval_str = getEvalStr(query, "x", field_names)
    log.debug(f"put_query - eval_str: {eval_str}")
    where_result = np.where(eval(eval_str))
    log.debug(f"put_query - where_result: {where_result}")
    where_result_index = where_result[0]
    log.debug(f"put_query - whare_result index: {where_result_index}")
    log.debug(f"put_query - boolean selection: {x[where_result_index]}")
    s = selection[0]
    count = 0
    for index in where_result_index:
        log.debug(f"put_query - index: {index}")
        value = x[index].copy()
        log.debug(f"put_query - original value: {value}")
        for i in range(len(field_names)):
            if replace_mask[i] is not None:
                value[i] = replace_mask[i]
        log.debug(f"put_query - modified value: {value}")
        try:
            chunk_arr[index] = value
        except ValueError as ve:
            log.error(f"Numpy Value updating array: {ve}")
            raise HTTPInternalServerError()

        json_val = bytesArrayToList(value)
        log.debug(f"put_query - json_value: {json_val}")
        json_index = index.tolist() * s.step + s.start  # adjust for selection
        indices.append(json_index)
        values.append(json_val)
        count += 1
        is_dirty = True
        if limit > 0 and count >= limit:
            log.info("put_query - got limit items")
            break

    query_result = {}
    query_result["index"] = indices
    query_result["value"] = values
    log.info(f"query_result returning: {len(indices)} rows")
    log.debug(f"query_result: {query_result}")
    resp = query_result

    if is_dirty:
        chunk_cache = app["chunk_cache"]
        chunk_cache.setDirty(chunk_id)
        log.info(f"PUT_Chunk dirty cache count: {chunk_cache.dirtyCount}")

        # async write to S3
        dirty_ids = app["dirty_ids"]
        now = int(time.time())
        dirty_ids[chunk_id] = (now, bucket)
        # write empty response
        resp = {}
    return resp
