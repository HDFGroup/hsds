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
Get the deflate level for given dset
"""
def getDeflate(app, dset_id, dset_json):
    deflate_level = getDeflateLevel(dset_json)
    log.debug(f"got deflate_level: {deflate_level}")
    if deflate_level is not None:
        deflate_map = app['deflate_map']
        if dset_id not in deflate_map:
            # save the deflate level so the lazy chunk writer can access it
            deflate_map[dset_id] = deflate_level
            log.debug(f"update deflate_map {dset_id}: {deflate_level}")
    return deflate_level


"""
Get the shuffle item size for given dset (if shuffle filter is used)
"""
def getShuffle(app, dset_id, dset_json):
    shuffle_size = 0
    if isShuffle(dset_json):
        type_json = dset_json["type"]
        item_size = getItemSize(type_json)
        if item_size == 'H5T_VARIABLE':
            log.warn(f"shuffle filter can't be used on variable datatype for datasset: {dset_id}")
        else:
            shuffle_size = item_size
            log.debug(f"got shuffle_size: {shuffle_size}")
    else:
        log.debug("isShuffle is false")

    if shuffle_size > 1:
        shuffle_map = app['shuffle_map']
        if dset_id not in shuffle_map:
            # save the shuffle size so the lazy chunk writer can access it
            shuffle_map[dset_id] = shuffle_size
            log.debug(f"update shuffle_map {dset_id}: {shuffle_size}")
    return shuffle_size

"""
Utility method for GET_Chunk, PUT_Chunk, and POST_CHunk
Get a numpy array for the chunk (possibly initizaling a new chunk if requested)
"""
async def getChunk(app, chunk_id, dset_json, bucket=None, s3path=None, s3offset=0, s3size=0, chunk_init=False):
    # if the chunk cache has too many dirty items, wait till items get flushed to S3
    MAX_WAIT_TIME = 10.0  # TBD - make this a config
    chunk_cache = app['chunk_cache']
    if chunk_init and s3offset > 0:
        log.error(f"unable to initiale chunk {chunk_id} for reference layouts ")
        raise  HTTPInternalServerError()

    log.debug(f"getChunk cache utilization: {chunk_cache.cacheUtilizationPercent} per, dirty_count: {chunk_cache.dirtyCount}, mem_dirty: {chunk_cache.memDirty}")

    chunk_arr = None
    dset_id = getDatasetId(chunk_id)
    dims = getChunkLayout(dset_json)
    type_json = dset_json["type"]
    dt = createDataType(type_json)
    # note - officially we should follow the order in which the filters are defined in the filter_list,
    # but since we currently have just deflate and shuffle we will always apply deflate then shuffle on read,
    # and shuffle then deflate on write
    # also note - get deflate and shuffle will update the deflate and shuffle map so that the s3sync will do the right thing
    deflate_level = getDeflate(app, dset_id, dset_json)
    shuffle = getShuffle(app, dset_id, dset_json)
    s3key = None

    if s3path:
        if not s3path.startswith("s3://"):
            # TBD - verify these at dataset creation time?
            log.error(f"unexpected s3path for getChunk: {s3path}")
            raise  HTTPInternalServerError()
        path = s3path[5:]
        index = path.find('/')   # split bucket and key
        if index < 1:
            log.error(f"s3path is invalid: {s3path}")
            raise HTTPInternalServerError()
        bucket = path[:index]
        s3key = path[(index+1):]
        log.debug(f"Using s3path bucket: {bucket} and  s3key: {s3key}")
    else:
        s3key = getS3Key(chunk_id)
        log.debug(f"getChunk chunkid: {chunk_id} bucket: {bucket}")
    if chunk_id in chunk_cache:
        chunk_arr = chunk_cache[chunk_id]
    else:
        if s3path and s3size == 0:
            obj_exists = False
        else:
            obj_exists = await isStorObj(app, s3key, bucket=bucket)
        # TBD - potential race condition?
        if obj_exists:
            pending_s3_read = app["pending_s3_read"]

            if chunk_id in pending_s3_read:
                # already a read in progress, wait for it to complete
                read_start_time = pending_s3_read[chunk_id]
                log.info(f"s3 read request for {chunk_id} was requested at: {read_start_time}")
                while time.time() - read_start_time < 2.0:
                    log.debug("waiting for pending s3 read, sleeping")
                    await asyncio.sleep(1)  # sleep for sub-second?
                    if chunk_id in chunk_cache:
                        log.info(f"Chunk {chunk_id} has arrived!")
                        chunk_arr = chunk_cache[chunk_id]
                        break
                if chunk_arr is None:
                    log.warn(f"s3 read for chunk {chunk_id} timed-out, initiaiting a new read")

            if chunk_arr is None:
                if chunk_id not in pending_s3_read:
                    pending_s3_read[chunk_id] = time.time()
                log.debug(f"Reading chunk {s3key} from S3")

                chunk_bytes = await getStorBytes(app, s3key, shuffle=shuffle, deflate_level=deflate_level, offset=s3offset, length=s3size, bucket=bucket)
                if chunk_id in pending_s3_read:
                    # read complete - remove from pending map
                    elapsed_time = time.time() - pending_s3_read[chunk_id]
                    log.info(f"s3 read for {s3key} took {elapsed_time}")
                    del pending_s3_read[chunk_id]
                else:
                    log.warn(f"expected to find {chunk_id} in pending_s3_read map")
                chunk_arr = bytesToArray(chunk_bytes, dt, dims)

            log.debug(f"chunk size: {chunk_arr.size}")

        elif chunk_init:
            log.debug(f"Initializing chunk {chunk_id}")
            fill_value = getFillValue(dset_json)
            if fill_value:
                # need to convert list to tuples for numpy broadcast
                if isinstance(fill_value, list):
                    fill_value = tuple(fill_value)
                chunk_arr = np.empty(dims, dtype=dt, order='C')
                chunk_arr[...] = fill_value
            else:
                chunk_arr = np.zeros(dims, dtype=dt, order='C')
        else:
            log.debug(f"Chunk {chunk_id} not found")

        if chunk_arr is not None:
            # check that there's room in the cache before adding it
            if chunk_cache.memTarget - chunk_cache.memDirty < chunk_arr.size:
                # no room in the cache, wait till space is freed by the s3sync task
                wait_start = time.time()
                while chunk_cache.memTarget - chunk_cache.memDirty < chunk_arr.size:
                    log.warn(f"getChunk, cache utilization: {chunk_cache.cacheUtilizationPercent}, sleeping till items are flushed")
                    if time.time() - wait_start > MAX_WAIT_TIME:
                        log.error(f"unable to save updated chunk {chunk_id} to cache returning 503 error")
                        raise HTTPServiceUnavailable()
                    await asyncio.sleep(1)

            chunk_cache[chunk_id] = chunk_arr  # store in cache
    return chunk_arr


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
    bucket=None, query=None, query_update=None, limit=0, input_arr=None):
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
        msg = "No dimension passed to PUT chunk request"
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
    if query:
        values = []
        indices = []
        if chunk_arr is not None:
            # do query selection
            field_names = list(dt.fields.keys())
            replace_mask = [None,] * len(field_names)
            for i in range(len(field_names)):
                field_name = field_names[i]
                if field_name in query_update:
                    replace_mask[i] = query_update[field_name]
            log.debug(f"replace_mask: {replace_mask}")
            if replace_mask == [None,] * len(field_names):
                log.warn(f"no fields found in query_update")
                raise HTTPBadRequest()

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
    else:
        # update chunk array
        try:
            chunk_arr[selection] = input_arr
        except ValueError as ve:
            log.error(f"Numpy Value updating array: {ve}")
            raise HTTPInternalServerError()
        is_dirty = True
        resp = {}

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
