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
from aiohttp.web_exceptions import HTTPBadRequest, HTTPNotFound, HTTPRequestEntityTooLarge, HTTPConflict, HTTPInternalServerError, HTTPServiceUnavailable
from aiohttp.client_exceptions import ClientError
from aiohttp.web import StreamResponse

from util.httpUtil import  getHref, getAcceptType, get_http_client, http_put, request_read, jsonResponse
from util.idUtil import   isValidUuid, getDataNodeUrl
from util.domainUtil import  getDomainFromRequest, isValidDomain
from util.hdf5dtype import getItemSize, createDataType
from util.dsetUtil import getSliceQueryParam, setSliceQueryParam, getFillValue, isExtensible 
from util.dsetUtil import getSelectionShape, getDsetMaxDims, getChunkLayout, getDeflateLevel
from util.chunkUtil import getNumChunks, getChunkIds, getChunkId, getChunkIndex, getChunkSuffix
from util.chunkUtil import getChunkCoverage, getDataCoverage
from util.arrayUtil import bytesArrayToList, jsonToArray, getShapeDims, getNumElements, arrayToBytes, bytesToArray
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
        raise HTTPInternalServerError()
     
    if "type" not in dset_json:
        log.error("No type found in dset_json: {}".format(dset_json))
        raise HTTPInternalServerError()
    
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
    data = arrayToBytes(arr_chunk)  
    # pass itemsize, type, dimensions, and selection as query params
    params = {}
    params["dset"] = json.dumps(dset_json)
    setSliceQueryParam(params, chunk_sel)   

    try:
        async with client.put(req, data=data, params=params) as rsp:
            log.debug(f"req: {req} status: {rsp.status}")
            if rsp.status == 201:
                log.debug(f"http_put({req}) <201> Updated")
            elif rsp.status == 503:
                log.warn(f"DN node too busy to handle request: {req}")
                raise HTTPServiceUnavailable()
            else:
                log.error(f"request error status: {rsp.status} for {req}: {str(rsp)}")
                raise HTTPInternalServerError()
                
    except ClientError as ce:
        log.error("Error for http_post({}): {} ".format(req, str(ce)))
        raise HTTPInternalServerError()
    except CancelledError as cle:
        log.warn("CancelledError for http_post({}): {}".format(req, str(cle)))


"""
Read data from given chunk_id.  Pass in type, dims, and selection area.
""" 
async def read_chunk_hyperslab(app, chunk_id, dset_json, slices, np_arr, chunk_map=None):
    """ read the chunk selection from the DN
    chunk_id: id of chunk to write to
    chunk_sel: chunk-relative selection to read from
    np_arr: numpy array to store read bytes
    chunk_offset: location of chunk with the s3 object
    chunk_offset: size of chunk within the s3 object (or 0 if the entire object)
    """
    msg = "read_chunk_hyperslab, chunk_id:{}, slices: {}".format(chunk_id, slices)
    log.info(msg)
    if chunk_map and chunk_id not in chunk_map:
        log.warn(f"expected to find {chunk_id} in chunk_map")

    req = getDataNodeUrl(app, chunk_id)
    req += "/chunks/" + chunk_id 
    log.debug("GET chunk req: " + req)
    client = get_http_client(app)

    if "type" not in dset_json:
        log.error("No type found in dset_json: {}".format(dset_json))
        raise HTTPInternalServerError()
    
    layout = getChunkLayout(dset_json)
    chunk_sel = getChunkCoverage(chunk_id, slices, layout)
    data_sel = getDataCoverage(chunk_id, slices, layout)
    
    # pass dset json and selection as query params
    params = {}
    params["dset"] = json.dumps(dset_json)
     
    fill_value = getFillValue(dset_json) 
     
    chunk_shape = getSelectionShape(chunk_sel)
    log.debug("chunk_shape: {}".format(chunk_shape))
    dt = np_arr.dtype

    def defaultChunk():
        # no data, return zero array
        if fill_value:
            chunk_arr = np.empty(chunk_shape, dtype=dt, order='C')
            chunk_arr[...] = fill_value
        else:
            chunk_arr = np.zeros(chunk_shape, dtype=dt, order='C')
        return chunk_arr

    chunk_arr = None
    setSliceQueryParam(params, chunk_sel)  
    if chunk_map:
        if chunk_id not in chunk_map:
            log.debug(f"{chunk_id} not found in chunk_map, returning default arr")
            chunk_arr = defaultChunk()
        else:
            chunk_info = chunk_map[chunk_id]
            params["s3path"] = chunk_info["s3path"]
            params["s3offset"] = chunk_info["s3offset"]
            params["s3size"] = chunk_info["s3size"]
  
    if chunk_arr is None:
        try:
            async with client.get(req, params=params) as rsp:
                log.debug("http_get {} status: <{}>".format(req, rsp.status))
                if rsp.status == 200:
                    data = await rsp.read()  # read response as bytes
                    chunk_arr = bytesToArray(data, dt, chunk_shape)  
                    npoints_read = getNumElements(chunk_arr.shape)
                    npoints_expected = getNumElements(chunk_shape)
                    if npoints_read != npoints_expected:
                        log.error("Expected {} points, but got: {}".format(npoints_expected, npoints_read))
                        raise HTTPInternalServerError()
                    chunk_arr = chunk_arr.reshape(chunk_shape)
                elif rsp.status == 404:
                    if "s3path" in params:
                        s3path = params["s3path"]
                        # external HDF5 file, should exist 
                        log.warn(f"s3path: {s3path} for S3 range get found")
                        raise HTTPNotFound()
                    # no data, return zero array
                    chunk_arr = defaultChunk()
                else:
                    msg = "request to {} failed with code: {}".format(req, rsp.status)
                    log.error(msg)
                    raise HTTPInternalServerError()
            
        except ClientError as ce:
            log.error("Error for http_get({}): {} ".format(req, str(ce)))
            raise HTTPInternalServerError()
        except CancelledError as cle:
            log.warn("CancelledError for http_get({}): {}".format(req, str(cle)))
            return
    
    log.info("chunk_arr shape: {}".format(chunk_arr.shape))
    log.info("data_sel: {}".format(data_sel))

    np_arr[data_sel] = chunk_arr

"""
Read point selection
--
app: application object
chunk_id: id of chunk to read from
dset_json: dset JSON
point_list: array of points to read
point_index: index of arr element to update for a given point
arr: numpy array to store read bytes
"""
async def read_point_sel(app, chunk_id, dset_json, point_list, point_index, np_arr, chunk_map=None):
    
    msg = "read_point_sel, chunk_id: {}".format(chunk_id)
    log.info(msg)

    req = getDataNodeUrl(app, chunk_id)
    req += "/chunks/" + chunk_id 
    log.debug("POST chunk req: " + req)
    client = get_http_client(app)
    point_dt = np.dtype('u8')  # use unsigned long for point index

    if "type" not in dset_json:
        log.error("No type found in dset_json: {}".format(dset_json))
        raise HTTPInternalServerError()
     
    num_points = len(point_list)
    np_arr_points = np.asarray(point_list, dtype=point_dt)
    post_data = np_arr_points.tobytes()
    

    # pass dset_json as query params
    params = {}
    params["dset"] = json.dumps(dset_json)
    params["action"] = "get"
     
    fill_value = getFillValue(dset_json)
     
    np_arr_rsp = None
    dt = np_arr.dtype

    def defaultArray():
        # no data, return zero array
        if fill_value:
            arr = np.empty((num_points,), dtype=dt)
            arr[...] = fill_value
        else:
            arr = np.zeros((num_points,), dtype=dt)
        return arr

    np_arr_rsp = None
    if chunk_map:
        if chunk_id not in chunk_map:
            log.debug(f"{chunk_id} not found in chunk_map, returning default arr")
            np_arr_rsp = defaultArray()
        else:
            chunk_info = chunk_map[chunk_id]
            params["s3path"] = chunk_info["s3path"]
            params["s3offset"] = chunk_info["s3offset"]
            params["s3size"] = chunk_info["s3size"]

    if np_arr_rsp is None:

        try:
            async with client.post(req, params=params, data=post_data) as rsp:
                log.debug("http_post {} status: <{}>".format(req, rsp.status))
                if rsp.status == 200:
                    rsp_data = await rsp.read()  # read response as bytes  
                    # TBD - Does not support VLEN response data       
                    np_arr_rsp = np.fromstring(rsp_data, dtype=dt) 
                    npoints_read = len(np_arr_rsp)
                    if npoints_read != num_points:
                        msg = "Expected {} points, but got: {}".format(num_points, npoints_read)
                        log.error(msg)
                        raise HTTPInternalServerError()
                elif rsp.status == 404:
                    if "s3path" in params:
                        s3path = params["s3path"]
                        # external HDF5 file, should exist 
                        log.warn(f"s3path: {s3path} for S3 range get found")
                        raise HTTPNotFound()
                    # no data, return zero array
                    np_arr_rsp = defaultArray()
                else:
                    msg = "request to {} failed with code: {}".format(req, rsp.status)
                    log.error(msg)
                    raise HTTPInternalServerError()
            
        except ClientError as ce:
            log.error("Error for http_get({}): {} ".format(req, str(ce)))
            raise HTTPInternalServerError()
        except CancelledError as cle:
            log.warn("CancelledError for http_get({}): {}".format(req, str(cle)))
            return
    
    log.info("got {} points response".format(num_points))
    log.info(f"np_arr {np_arr}")
    log.info(f"np_arr_rsp: {np_arr_rsp}")

    # Fill in the return array based on passed in index values
    for i in range(num_points):
        index = point_index[i]
        np_arr[index] = np_arr_rsp[i]

"""
Write point selection
--
app: application object
chunk_id: id of chunk to write to
dset_json: dset JSON
point_list: array of points to write
point_data: index of arr element to update for a given point
"""
async def write_point_sel(app, chunk_id, dset_json, point_list, point_data):
    
    msg = "write_point_sel, chunk_id:{}, points: {}, data: {}".format(chunk_id, point_list, point_data)
    #msg = "write_point_sel, chunk_id: {}".format(chunk_id)
    log.info(msg)
    if "type" not in dset_json:
        log.error("No type found in dset_json: {}".format(dset_json))
        raise HTTPInternalServerError()

    datashape = dset_json["shape"]
    dims = getShapeDims(datashape)
    rank = len(dims)
    type_json = dset_json["type"]
    dset_dtype = createDataType(type_json)  # np datatype

    req = getDataNodeUrl(app, chunk_id)
    req += "/chunks/" + chunk_id 
    log.debug("POST chunk req: " + req)
    client = get_http_client(app)
       
    num_points = len(point_list)

    #create a numpy array with point_data
    data_arr = jsonToArray((num_points,), dset_dtype, point_data)

    # create a numpy array with the following type:
    #   (coord1, coord2, ...) | dset_dtype
    if rank == 1:
        coord_type_str = "uint64"
    else:
        coord_type_str = "({},)uint64".format(rank)
    comp_type = np.dtype([("coord", np.dtype(coord_type_str)), ("value", dset_dtype)])
    np_arr = np.zeros((num_points,),dtype=comp_type)
    
    # Zip together coordinate and point_data to one numpy array
    for i in range(num_points):
        if rank == 1:
            elem = (point_list[i], data_arr[i])
        else:
            elem = (tuple(point_list[i]), data_arr[i])
        np_arr[i] = elem

    # TBD - support VLEN data
    post_data = np_arr.tobytes()
    
    # pass dset_json as query params
    params = {}
    params["dset"] = json.dumps(dset_json)
    params["action"] = "put"
    params["count"] = num_points
       
    try:
        async with client.post(req, params=params, data=post_data) as rsp:
            log.debug("http_post {} status: <{}>".format(req, rsp.status))
            if rsp.status == 200:
                log.info("req: {} OK".format(req))
            else:
                msg = "request to {} failed with code: {}".format(req, rsp.status)
                log.error(msg)
                raise HTTPInternalServerError()
            
    except ClientError as ce:
        log.error("Error for http_get({}): {} ".format(req, str(ce)))
        raise HTTPInternalServerError()
    except CancelledError as cle:
        log.warn("CancelledError for http_get({}): {}".format(req, str(cle)))


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
            elif rsp.status == 400:
                log.warn(f"request {req} failed withj code {rsp.status}")
                raise HTTPBadRequest()
            else:
                log.error(f"request {req} failed with code: {rsp.status}")
                raise HTTPInternalServerError()
            
    except ClientError as ce:
        log.error("Error for http_get({}): {} ".format(req, str(ce)))
        raise HTTPInternalServerError()
    except CancelledError as cle:
        log.warn("CancelledError for http_get({}): {}".format(req, str(cle)))
        return
    
    rsp_dict[chunk_id] = dn_rsp

"""
Return list of elements from a dataset
"""
async def getPointData(app, dset_id, dset_json, points):
    loop = app["loop"]
    num_points = len(points)
    log.info(f"getPointData for {num_points} points")
    log.debug(f"dset_json: {dset_json}")
    log.debug(f"points: {points}")
    chunk_dict = {}  # chunk ids to list of points in chunk
    datashape = dset_json["shape"]
    if datashape["class"] in ('H5S_NULL', 'H5S_SCALAR'):
        log.error("H5S_NULL, H5S_SCALAR shape classes can not be used with point selection")
        raise HTTPInternalServerError()    
    dims = getShapeDims(datashape)
    rank = len(dims)
    type_json = dset_json["type"]
    dset_dtype = createDataType(type_json)  # np datatype
    layout = dset_json["layout"]
    chunk_dims = layout["dims"]  # TBD: What if this is not defined?
    log.debug(f"chunk_dims: {chunk_dims}")

    for pt_indx in range(num_points):
        point = points[pt_indx]
        log.debug(f"point: {point}")
        if rank == 1:
            if point < 0 or point >= dims[0]:
                msg = "POST Value point: {} is not within the bounds of the dataset"
                msg = msg.format(point)
                log.warn(msg)
                raise HTTPBadRequest(reason=msg) 
        else:
            if len(point) != rank:
                msg = "POST Value point value did not match dataset rank"
                log.warn(msg)
                raise HTTPBadRequest(reason=msg) 
            for i in range(rank):
                if point[i] < 0 or point[i] >= dims[i]:
                    msg = "POST Value point: {} is not within the bounds of the dataset"
                    msg = msg.format(point)
                    log.warn(msg)
                    raise HTTPBadRequest(reason=msg) 
        chunk_id = getChunkId(dset_id, point, chunk_dims)
        log.debug(f"got chunk_id: {chunk_id}")
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
    log.debug("getPointData - num_chunks: {}".format(num_chunks))
    max_chunks = config.get("max_chunks_per_request")
    if num_chunks > max_chunks:
        log.warn(f"Point selection request too large, num_chunks: {num_chunks} max_chunks: {max_chunks}")
        raise HTTPRequestEntityTooLarge(num_chunks, max_chunks)

    # Get information about where chunks are located
    #   Will be None except for H5D_CHUNKED_REF_INDIRECT type
    chunk_map = await getChunkInfoMap(app, dset_id, dset_json, list(chunk_dict))
    log.debug(f"chunkinfo_map: {chunk_map}")
 
    # create array to hold response data
    # TBD: initialize to fill value if not 0
    arr_rsp = np.zeros((num_points,), dtype=dset_dtype)
    tasks = []
    for chunk_id in chunk_dict.keys():
        item = chunk_dict[chunk_id]
        point_list = item["points"]
        point_index = item["indices"]
        task = asyncio.ensure_future(read_point_sel(app, chunk_id, dset_json, 
            point_list, point_index, arr_rsp, chunk_map=chunk_map))
        tasks.append(task)
    await asyncio.gather(*tasks, loop=loop)

    log.debug("arr shape: {}".format(arr_rsp.shape))
    return arr_rsp


"""
Get info for chunk locations (for reference layouts)
"""
async def getChunkInfoMap(app, dset_id, dset_json, chunk_ids):
    layout = dset_json["layout"]
    if layout["class"] not in  ('H5D_CONTIGUOUS_REF', 'H5D_CHUNKED_REF', 'H5D_CHUNKED_REF_INDIRECT'):
        log.debug(f"skip getChunkInfoMap for layout class: { layout['class'] }")
        return None

    datashape = dset_json["shape"]
    datatype = dset_json["type"]
    if datashape["class"] == 'H5S_NULL':
        log.error("H5S_NULL shape class used with reference chunk layout")
        raise HTTPInternalServerError()    
    dims = getShapeDims(datashape)
    rank = len(dims)
    log.debug(f"getChunkInfoMap for dset: {dset_id} rank: {rank} num chunk_ids: {len(chunk_ids)}")
    chunkinfo_map = {}

    if layout["class"] == 'H5D_CONTIGUOUS_REF':
        s3path = layout["file_uri"]
        s3size = layout["size"]
        chunk_dims = layout["dims"]
        item_size = getItemSize(datatype)
        chunk_size = item_size
        for dim in chunk_dims:
            chunk_size *= dim
        log.debug(f"using chunk_size: {chunk_size}")

        for chunk_id in chunk_ids:    
            chunk_index = getChunkIndex(chunk_id)
            if len(chunk_index) != rank:
                log.error("Unexpected chunk_index")
                raise HTTPInternalServerError()
            extent = item_size
            for i in range(rank):
                index = chunk_index[i]
                s3offset = layout["offset"] + extent * chunk_dims[i] * index 
                extent *= dims[i]
            log.debug("setting chunk_info_map to s3offset: {s3offset} s3size: {s3size} for chunk_id: {chunk_id}")
            if s3offset > layout["offset"] + layout["size"]:
                log.warn(f"range get of s3offset: {s3offset} s3size: {s3size} extends beyond end of contingous dataset for chunk_id: {chunk_id}")
            chunkinfo_map[chunk_id] = {"s3path": s3path, "s3offset": s3offset, "s3size": chunk_size}
    elif layout["class"] == 'H5D_CHUNKED_REF':
        s3path = layout["file_uri"]
        chunks = layout["chunks"]

        for chunk_id in chunk_ids:
            s3offset = 0
            s3size = 0
            chunk_key = getChunkSuffix(chunk_id)
            if chunk_key in chunks:
                item = chunks[chunk_key]
                s3offset = item[0]
                s3size = item[1]
            chunkinfo_map[chunk_id] = {"s3path": s3path, "s3offset": s3offset, "s3size": s3size}
    elif layout["class"] == 'H5D_CHUNKED_REF_INDIRECT':
        if "chunk_table" not in layout:
            log.error("Expected to find chunk_table in dataset layout")
            raise HTTPInternalServerError()
        chunktable_id = layout["chunk_table"]
        # get  state for dataset from DN.
        chunktable_json = await getObjectJson(app, chunktable_id, refresh=False)  
        log.debug(f"chunktable_json: {chunktable_json}")
        chunktable_dims = getShapeDims(chunktable_json["shape"])

        if len(chunktable_dims) != rank:
            msg = "Rank of chunktable should be same as the dataset"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
        
        # convert the list of chunk_ids into a set of points to query in the chunk table
        num_pts = len(chunk_ids)
        if rank == 1:
            arr_points = np.zeros((num_pts,), dtype=np.dtype('u8'))
        else:
            arr_points = np.zeros((num_pts,rank), dtype=np.dtype('u8'))
        for i in range(len(chunk_ids)):
            chunk_id = chunk_ids[i]
            log.debug(f"chunk_id for chunktable: {chunk_id}")
            indx = getChunkIndex(chunk_id)
            log.debug(f"get chunk indx: {indx}")
            if rank == 1:
                log.debug(f"convert: {indx[0]} to {indx}")
                indx = indx[0]
            arr_points[i] = indx
        log.debug(f"got chunktable points: {arr_points}, calling getPointData")
        point_data = await getPointData(app, chunktable_id, chunktable_json, arr_points)
        log.debug(f"got chunktable data: {point_data}")
        s3path = layout["file_uri"]

        for i in range(len(chunk_ids)):
            chunk_id = chunk_ids[i]
            item = point_data[i]
            s3offset = int(item[0])
            s3size = int(item[1])
            chunkinfo_map[chunk_id] = {"s3path": s3path, "s3offset": s3offset, "s3size": s3size}
    else:
        log.error(f"Unexpected chunk layout: {layout['class']}")
        raise HTTPInternalServerError()  

    log.debug(f"returning chunkinfo_map: {chunkinfo_map}")
    return chunkinfo_map



"""
 Handler for PUT /<dset_uuid>/value request
"""
async def PUT_Value(request):
    log.request(request)
    app = request.app 
    loop = app["loop"]
    body = None
    json_data = None
    params = request.rel_url.query
    append_rows = None # this is a append update or not
    append_dim = 0
    if "append" in params and params["append"]:
        try:
            append_rows = int(params["append"])
        except ValueError:
            msg = "invalid append query param"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
        log.info(f"append_rows: {append_rows}")
        if "select" in params:
            msg = "select query parameter can not be used with packet updates"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
    if "append_dim" in params and params["append_dim"]:
        try:
            append_dim = int(params["append_dim"])
        except ValueError:
            msg = "invalid append_dim"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
        log.info(f"append_dim: {append_dim}")

    dset_id = request.match_info.get('id')
    if not dset_id:
        msg = "Missing dataset id"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    if not isValidUuid(dset_id, "Dataset"):
        msg = "Invalid dataset id: {}".format(dset_id)
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    username, pswd = getUserPasswordFromRequest(request)
    await validateUserPassword(app, username, pswd)

    domain = getDomainFromRequest(request)
    if not isValidDomain(domain):
        msg = "Invalid host value: {}".format(domain)
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    
    request_type = "json"
    if "Content-Type" in request.headers:
        # client should use "application/octet-stream" for binary transfer
        content_type = request.headers["Content-Type"]
        if content_type not in ("application/json", "application/octet-stream"):
            msg = "Unknown content_type: {}".format(content_type)
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
        if content_type == "application/octet-stream":
            log.debug("PUT value - request_type is binary")
            request_type = "binary"
        else:
            log.debug("PUT value - request type is json")

    if not request.has_body:
        msg = "PUT Value with no body"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    if request_type == "json":
        body = await request.json()
        if "append" in body and body["append"]:
            try:
                append_rows = int(body["append"])
            except ValueError:
                msg = "invalid append value in body"
                log.warn(msg)
                raise HTTPBadRequest(reason=msg)
            log.info(f"append_rows: {append_rows}")
        if append_rows:
            for key in ("start", "stop", "step"):
                if key in body:
                    msg = f"body key {key} can not be used with append"
                    log.warn(msg)
                    raise HTTPBadRequest(reason=msg)

        if "append_dim" in body and body["append_dim"]:
            try:
                append_dim = int(body["append_dim"])
            except ValueError:
                msg = "invalid append_dim"
                log.warn(msg)
                raise HTTPBadRequest(reason=msg)
            log.info(f"append_dim: {append_dim}")
    
    # get  state for dataset from DN.
    dset_json = await getObjectJson(app, dset_id, refresh=False)  

    layout = None 
    datashape = dset_json["shape"]
    if datashape["class"] == 'H5S_NULL':
        msg = "Null space datasets can not be used as target for PUT value"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
        
    dims = getShapeDims(datashape)
    maxdims = getDsetMaxDims(dset_json)
    rank = len(dims)

    layout = getChunkLayout(dset_json)
    deflate_level = getDeflateLevel(dset_json)
     
    type_json = dset_json["type"]
    item_size = getItemSize(type_json)
    log.debug("item size: {}".format(item_size))
 
    if item_size == 'H5T_VARIABLE' and request_type != "json":
        msg = "Only JSON is supported for variable length data types"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    dset_dtype = createDataType(type_json)  # np datatype
    
    await validateAction(app, domain, dset_id, username, "update")
 
    binary_data = None
    np_shape = None  # expected shape of input data
    points = None # used for point selection writes
    np_shape = [] # shape of incoming data
    slices = []   # selection area to write to
    
    if append_rows:
        # shape must be extensible
        if not isExtensible(dims, maxdims):
            msg = "Dataset shape must be extensible for packet updates"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)        
        if append_dim < 0 or append_dim > rank-1:
            msg = "invalid append_dim"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
        maxdims = getDsetMaxDims(dset_json)
        if maxdims[append_dim] != 0 and dims[append_dim] + append_rows > maxdims[append_dim]:
            log.warn("unable to append to dataspace")
            raise HTTPConflict()
    
    # refetch the dims if the dataset is extensible 
    if isExtensible(dims, maxdims):
        dset_json = await getObjectJson(app, dset_id, refresh=True)
        dims = getShapeDims(dset_json["shape"]) 

    if request_type == "json":
        body_json = body
    else:
        body_json = None

    
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
            raise HTTPBadRequest(reason=msg)  

        # body could also contain a point selection specifier 
        if "points" in body:
            if append_rows:
                msg = "points not valid with packet update"
                log.warn(msg)
                raise HTTPBadRequest(reason=msg)

            json_points = body["points"]
            num_points = len(json_points)
            if rank == 1:
                point_shape = (num_points,)
                log.info("rank 1: point_shape: {}".format(point_shape))
            else:
                point_shape = (num_points, rank)
                log.info("rank >1: point_shape: {}".format(point_shape))
            try:
                dt = np.dtype(np.uint64) # use uint64 so we can address large array extents
                points = jsonToArray(point_shape, dt, json_points)
            except ValueError:
                msg = "Bad Request: point list not valid for dataset shape"
                log.warn(msg)
                raise HTTPBadRequest(reason=msg)
    else:
        # read binary data
        log.info(f"request content_length: {request.content_length}")
        max_request_size = int(config.get("max_request_size"))
        if isinstance(request.content_length, int) and request.content_length >= max_request_size:
            log.warn(f"Request size too large: {request.content_length} max: {max_request_size}")
            raise HTTPRequestEntityTooLarge(request.content_length, max_request_size)

        try :
            binary_data = await request_read(request)
        except HTTPRequestEntityTooLarge as tle:
            log.warn(f"Got HTTPRequestEntityTooLarge exception during binary read: {tle})")
            raise  # re-throw
        
        if len(binary_data) != request.content_length:
            msg = "Read {} bytes, expecting: {}".format(len(binary_data), request.content_length)
            log.error(msg)
            raise HTTPBadRequest(reason=msg)

    if append_rows:
        for i in range(rank):
            if i == append_dim:
                np_shape.append(append_rows)
                # this will be adjusted once the dataspace is extended
                slices.append(slice(0, append_rows, 1))  
            else:
                if dims[i] == 0:
                    dims[i] = 1  # need a non-zero extent for all dimensionas
                np_shape.append(dims[i])
                slices.append(slice(0, dims[i], 1))
        np_shape = tuple(np_shape)
                
    elif points is None:
        for dim in range(rank):    
            # if the selection region is invalid here, it's really invalid
            dim_slice = getSliceQueryParam(request, dim, dims[dim], body=body_json)
            slices.append(dim_slice)   
        # The selection parameters will determine expected put value shape
        log.debug(f"PUT Value selection: {slices}") 
        # not point selection, get hyperslab selection shape
        np_shape = getSelectionShape(slices)  
        num_elements = getNumElements(np_shape)
    else:
        # point update
        np_shape = (num_points,)     
        num_elements = num_points
    log.debug(f"selection shape: {np_shape}")

    num_elements = getNumElements(np_shape)
    log.debug(f"selection num elements: {num_elements}")
    if num_elements <= 0:
        msg = "Selection is empty"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    arr = None  # np array to hold request data
    if binary_data:
        if num_elements*item_size != len(binary_data):
            msg = "Expected: " + str(num_elements*item_size) + " bytes, but got: " + str(len(binary_data))
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
        arr = np.fromstring(binary_data, dtype=dset_dtype)
        try:
            arr = arr.reshape(np_shape)  # conform to selection shape    
        except ValueError:
            msg = "Bad Request: binary input data doesn't match selection" 
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)      
    else:
        #
        # data is json
        #
        try:
            msg = "input data doesn't match selection"
            arr = jsonToArray(np_shape, dset_dtype, json_data)
        except ValueError:
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
        except TypeError:
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
        except IndexError:
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
        log.debug("got json arr: {}".format(arr.shape))

    if append_rows:
        # extend the shape of the dataset 
        req = getDataNodeUrl(app, dset_id) + "/datasets/" + dset_id + "/shape"
        body = {"extend": append_rows, "extend_dim": append_dim}
        selection = None
        try:
            shape_rsp = await http_put(app, req, data=body)
            log.info(f"got shape put rsp: {shape_rsp}")
            if "selection" in shape_rsp:
                selection = shape_rsp["selection"]
        except HTTPConflict:
            log.warn("got 409 extending dataspace for PUT value")
            raise
        if not selection:
            log.error("expected to get selection in PUT shape response")
            raise HTTPInternalServerError()
        # selection should be in the format [:,n:m,:].  
        # extract n and m and use it to update the slice for the appending dimension
        if not selection.startswith("[") or not selection.endswith("]"):
            log.error("Unexpected selection in PUT shape response")
            raise HTTPInternalServerError()
        selection = selection[1:-1]  # strip off brackets
        parts = selection.split(',')
        for part in parts:
            if part == ":":
                continue
            bounds = part.split(':')
            if len(bounds) != 2:
                log.error("Unexpected selection in PUT shape response")
                raise HTTPInternalServerError()
            lb = ub = 0
            try:
                lb = int(bounds[0])
                ub = int(bounds[1])
            except ValueError:
                log.error("Unexpected selection in PUT shape response")
                raise HTTPInternalServerError()
            log.info(f"lb: {lb} ub: {ub}")
            # update the slices to indicate where to place the data
            slices[append_dim] = slice(lb, ub, 1)

    slices = tuple(slices)  # no more edits to slices

    if points is None:
        # for hyperslab selection, verify the input shape matches the
        # selection
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
            raise HTTPBadRequest(reason=msg)

        #log.info("got np array: {}".format(arr))
        num_chunks = getNumChunks(slices, layout)
        log.debug("num_chunks: {}".format(num_chunks))
        max_chunks = int(config.get('max_chunks_per_request'))
        if num_chunks > max_chunks:
            log.warn(f"PUT value too many chunks: {num_chunks}, {max_chunks}")
            raise HTTPRequestEntityTooLarge(num_chunks, max_chunks)
         
        try: 
            chunk_ids = getChunkIds(dset_id, slices, layout)
        except ValueError:
            log.warn("getChunkIds failed")
            raise HTTPInternalServerError()
        log.debug("chunk_ids: {}".format(chunk_ids))

        tasks = []
        task_batch_size = len(app["dn_urls"]) * 10
        for chunk_id in chunk_ids:
            task = asyncio.ensure_future(write_chunk_hyperslab(app, chunk_id, dset_json, slices, deflate_level, arr))
            tasks.append(task)
            if len(tasks) == task_batch_size:
                await asyncio.gather(*tasks, loop=loop)
                tasks = []
        if tasks:
            await asyncio.gather(*tasks, loop=loop)
    else:
        #
        # Do point PUT
        #
        log.debug("num_points: {}".format(num_points))
        
        chunk_dict = {}  # chunk ids to list of points in chunk

        for pt_indx in range(num_points):
            if rank == 1:
                point = int(points[pt_indx])
            else:
                point_tuple = points[pt_indx]
                point = []
                for i in range(len(point_tuple)):
                    point.append(int(point_tuple[i]))
            if rank == 1:
                if point < 0 or point >= dims[0]:
                    msg = "PUT Value point: {} is not within the bounds of the dataset"
                    msg = msg.format(point)
                    log.warn(msg)
                    raise HTTPBadRequest(reason=msg) 
            else:
                if len(point) != rank:
                    msg = "PUT Value point value did not match dataset rank"
                    log.warn(msg)
                    raise HTTPBadRequest(reason=msg) 
                for i in range(rank):
                    if point[i] < 0 or point[i] >= dims[i]:
                        msg = "PUT Value point: {} is not within the bounds of the dataset"
                        msg = msg.format(point)
                        log.warn(msg)
                        raise HTTPBadRequest(reason=msg) 
            chunk_id = getChunkId(dset_id, point, layout)
            # get the pt_indx element from the input data
            value = arr[pt_indx]
            if chunk_id not in chunk_dict:
                point_list = [point,]
                point_data = [value,]
                chunk_dict[chunk_id] = {"points": point_list, "values": point_data}
            else:
                item = chunk_dict[chunk_id]
                point_list = item["points"]
                point_list.append(point)
                point_data = item["values"]
                point_data.append(value)

        num_chunks = len(chunk_dict)
        log.debug("num_chunks: {}".format(num_chunks))
        max_chunks = int(config.get('max_chunks_per_request'))
        if num_chunks > max_chunks:
            msg = "PUT value request too large"
            log.warn(msg)
            raise HTTPRequestEntityTooLarge(num_chunks, max_chunks)
        tasks = []
        for chunk_id in chunk_dict.keys():
            item = chunk_dict[chunk_id]
            point_list = item["points"]
            point_data = item["values"]
            task = asyncio.ensure_future(write_point_sel(app, chunk_id, dset_json, 
                point_list, point_data))
            tasks.append(task)
        await asyncio.gather(*tasks, loop=loop)
         
    resp_json = {}
    resp = await jsonResponse(request, resp_json)
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
    params = request.rel_url.query

    dset_id = request.match_info.get('id')
    if not dset_id:
        msg = "Missing dataset id"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    if not isValidUuid(dset_id, "Dataset"):
        msg = "Invalid dataset id: {}".format(dset_id)
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    username, pswd = getUserPasswordFromRequest(request)
    if username is None and app['allow_noauth']:
        username = "default"
    else:
        await validateUserPassword(app, username, pswd)
     
    domain = getDomainFromRequest(request)
    if not isValidDomain(domain):
        msg = "Invalid host value: {}".format(domain)
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)  
   
    # get state for dataset from DN.
    dset_json = await getObjectJson(app, dset_id)  
    log.debug("got dset_json: {}".format(dset_json))
    
    datashape = dset_json["shape"]
    if datashape["class"] == 'H5S_NULL':
        msg = "Null space datasets can not be used as target for GET value"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    dims = getShapeDims(datashape)  # throws 400 for HS_NULL dsets
    maxdims = getDsetMaxDims(dset_json)
    rank = len(dims)
    layout = getChunkLayout(dset_json)
    
    await validateAction(app, domain, dset_id, username, "read")

    # refetch the dims if the dataset is extensible and requestor hasn't provided 
    # an explicit region
    if isExtensible(dims, maxdims) and "select" not in params:
        dset_json = await getObjectJson(app, dset_id, refresh=True)
        dims = getShapeDims(dset_json["shape"])  

    slices = None  # selection for read 
     
    # Get query parameter for selection
    if isExtensible:
        slices = []
        try:
            for dim in range(rank):
                dim_slice = getSliceQueryParam(request, dim, dims[dim])
                slices.append(dim_slice)   
        except HTTPBadRequest:
            # exception might be due to us having stale version of dims, refresh
            dset_json = await getObjectJson(app, dset_id, refresh=True)
            dims = getShapeDims(dset_json["shape"]) 
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
    log.debug(f"selection num elements: {npoints}")

    num_chunks = getNumChunks(slices, layout)
    log.debug("num_chunks: {}".format(num_chunks))
    max_chunks = int(config.get('max_chunks_per_request'))
    if num_chunks > max_chunks:
        msg = "GET value request too large"
        log.warn(msg)
        raise HTTPRequestEntityTooLarge(num_chunks, max_chunks)
    chunk_ids = getChunkIds(dset_id, slices, layout)
    # Get information about where chunks are located
    #   Will be None except for H5D_CHUNKED_REF_INDIRECT type
    chunkinfo = await getChunkInfoMap(app, dset_id, dset_json, chunk_ids)
    log.debug(f"chunkinfo_map: {chunkinfo}")


    if request.method == "OPTIONS":
        # skip doing any big data load for options request
        resp = await jsonResponse(request,  None)
    elif "query" in params:
        if rank > 1:
            msg = "Query string is not supported for multidimensional arrays"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
        try:
            resp = await doQueryRead(request, chunk_ids, dset_json, slices)
        except CancelledError as ce:
            log.warn(f"Cancelled error on query read: {ce}")
            resp = await jsonResponse(request, None)  # TBD: what do return if client cancels
    else:
        log.debug("chunk_ids: {}".format(chunk_ids))
        try:
            resp = await doHyperSlabRead(request, chunk_ids, dset_json, slices, chunk_map=chunkinfo)
        except CancelledError as ce:
            log.warn(f"Cancelled error on hyperslab read: {ce}")
            resp = await jsonResponse(request, None)  # TBD: what do return if client cancels
    log.response(request, resp=resp)
    return resp

async def doQueryRead(request, chunk_ids, dset_json,  slices):
    app = request.app 
    params = request.rel_url.query
    query = params["query"]
    log.info("Query request: {}".format(query))
    loop = app["loop"]

    type_json = dset_json["type"]
    item_size = getItemSize(type_json)
    
    log.debug("item size: {}".format(item_size))
    
    limit = 0
    if "Limit" in params:
        try:
            limit = int(params["Limit"])
        except ValueError:
            msg = "Invalid Limit query param"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)

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

async def doHyperSlabRead(request, chunk_ids, dset_json, slices, chunk_map=None):
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

    # check that the array size is reasonable
    request_size = np.prod(np_shape)
    if item_size == 'H5T_VARIABLE':
        request_size *= 512  # random guess of avg item_size
    else:
        request_size *= item_size
    log.debug("request_size: {}".format(request_size))
    max_request_size = int(config.get("max_request_size"))
    if request_size >= max_request_size:
        msg = "GET value request too large"
        log.warn(msg)
        raise HTTPRequestEntityTooLarge(request_size, max_request_size)

    arr = np.zeros(np_shape, dtype=dset_dtype, order='C')
    tasks = []
    for chunk_id in chunk_ids:
        task = asyncio.ensure_future(read_chunk_hyperslab(app, chunk_id, dset_json, slices, arr, chunk_map=chunk_map))
        tasks.append(task)
    await asyncio.gather(*tasks, loop=loop)

    log.debug("arr shape: {}".format(arr.shape))

    if response_type == "binary":
        output_data = arr.tobytes()
        log.debug("GET Value - returning {} bytes binary data".format(len(output_data)))
     
        # write response
        try:
            resp = StreamResponse()
            resp.headers['Content-Type'] = "application/octet-stream"
            # allow CORS
            resp.headers['Access-Control-Allow-Origin'] = '*'
            resp.headers['Access-Control-Allow-Methods'] = "GET, POST, DELETE, PUT, OPTIONS"
            resp.headers['Access-Control-Allow-Headers'] = "Content-Type, api_key, Authorization"
            resp.content_length = len(output_data)
            await resp.prepare(request)
            await resp.write(output_data)
        except Exception as e:
            log.error(f"Exception during binary data write: {e}")
        finally:
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
    body = None

    dset_id = request.match_info.get('id')
    if not dset_id:
        msg = "Missing dataset id"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    if not isValidUuid(dset_id, "Dataset"):
        msg = "Invalid dataset id: {}".format(dset_id)
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    log.info("POST_VALUE, id: {}".format(dset_id))

    username, pswd = getUserPasswordFromRequest(request)
    if username is None and app['allow_noauth']:
        username = "default"
    else:
        await validateUserPassword(app, username, pswd)
     
    domain = getDomainFromRequest(request)
    if not isValidDomain(domain):
        msg = "Invalid host value: {}".format(domain)
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)


    accept_type = getAcceptType(request)
    response_type = accept_type # will adjust later if binary not possible

    request_type = "json"
    if "Content-Type" in request.headers:
        # client should use "application/octet-stream" for binary transfer
        content_type = request.headers["Content-Type"]
        if content_type not in ("application/json", "application/octet-stream"):
            msg = "Unknown content_type: {}".format(content_type)
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
        if content_type == "application/octet-stream":
            log.debug("POST value - request_type is binary")
            request_type = "binary"
        else:
            log.debug("POST value - request type is json")

    if not request.has_body:
        msg = "POST Value with no body"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    
   
    # get  state for dataset from DN.
    dset_json = await getObjectJson(app, dset_id)  

    datashape = dset_json["shape"]
    if datashape["class"] == 'H5S_NULL':
        msg = "POST value not supported for datasets with NULL shape"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    if datashape["class"] == 'H5S_SCALAR':
        msg = "POST value not supported for datasets with SCALAR shape"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    dims = getShapeDims(datashape)
    rank = len(dims)
         
    type_json = dset_json["type"]
    item_size = getItemSize(type_json)
    log.debug("item size: {}".format(item_size))

    log.debug("got dset_json: {}".format(dset_json))
    await validateAction(app, domain, dset_id, username, "read")

    # read body data
    num_points = None
    point_dt = np.dtype('u8')  # use unsigned long for point index
    if request_type == "json":
        body = await request.json()
        if "points" not in body:
            msg = "Expected points key in request body"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
        points = body["points"]
        if not isinstance(points, list):
            msg = "POST Value expected list of points"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg) 
        num_points = len(points)

    else:
        # read binary data
        binary_data = await request_read(request)
        if len(binary_data) != request.content_length:
            msg = "Read {} bytes, expecting: {}".format(len(binary_data), request.content_length)
            log.error(msg)
            raise HTTPInternalServerError()
        if request.content_length % point_dt.itemsize != 0:
            msg = "Content length: {} not divisible by element size: {}".format(request.content_length, item_size)
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
        num_points = request.content_length // point_dt.itemsize
        log.debug("got {} num_points".format(num_points))
        points = np.fromstring(binary_data, dtype=point_dt)
        if rank > 1:
            if num_points % rank != 0:
                msg = "Number of points is not consistent with dataset rank"
                log.warn(msg)
                raise HTTPBadRequest(reason=msg)
            num_points //= rank
            points = points.reshape((num_points, rank))  # conform to point index shape

    arr_rsp = await getPointData(app, dset_id, dset_json, points)

    log.debug("arr shape: {}".format(arr_rsp.shape))

    if response_type == "binary":
        output_data = arr_rsp.tobytes()
        log.debug("POST Value - returning {} bytes binary data".format(len(output_data)))
     
        # write response
        try:
            resp = StreamResponse()
            resp.headers['Content-Type'] = "application/octet-stream"
            resp.content_length = len(output_data)
            await resp.prepare(request)
            await resp.write(output_data)
        except Exception as e:
            log.error(f"Exception during binary data write: {e}")
        finally:
            await resp.write_eof()
    else:
        log.debug("POST Value - returning JSON data")
        rsp_json = {}
        data = arr_rsp.tolist()
        log.debug("got rsp data {} points".format(len(data)))
        json_data = bytesArrayToList(data)
        rsp_json["value"] = json_data  
        resp = await jsonResponse(request, rsp_json)
    log.response(request, resp=resp)
    return resp
