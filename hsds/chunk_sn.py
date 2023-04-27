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
# handles dataset /value requests for service node
#

import base64
import math
import numpy as np
from json import JSONDecodeError
from asyncio import IncompleteReadError
from aiohttp.web_exceptions import HTTPException, HTTPBadRequest
from aiohttp.web_exceptions import HTTPRequestEntityTooLarge
from aiohttp.web_exceptions import HTTPConflict, HTTPInternalServerError
from aiohttp.web import StreamResponse

from .util.httpUtil import getHref, getAcceptType, getContentType, http_put
from .util.httpUtil import request_read, jsonResponse, isAWSLambda
from .util.idUtil import isValidUuid, getDataNodeUrl
from .util.domainUtil import getDomainFromRequest, isValidDomain
from .util.domainUtil import getBucketForDomain
from .util.hdf5dtype import getItemSize, createDataType
from .util.dsetUtil import getSelectionList, isNullSpace, getDatasetLayout
from .util.dsetUtil import getFillValue, isExtensible, getSelectionPagination
from .util.dsetUtil import getSelectionShape, getDsetMaxDims, getChunkLayout
from .util.dsetUtil import getDatasetCreationPropertyLayout 
from .util.chunkUtil import getNumChunks, getChunkIds, getChunkId
from .util.chunkUtil import getChunkIndex, getChunkSuffix
from .util.chunkUtil import getChunkCoverage, getDataCoverage
from .util.chunkUtil import getQueryDtype
from .util.arrayUtil import bytesArrayToList, jsonToArray, getShapeDims
from .util.arrayUtil import getNumElements, arrayToBytes, bytesToArray
from .util.arrayUtil import squeezeArray
from .util.authUtil import getUserPasswordFromRequest, validateUserPassword
from .util.boolparser import BooleanParser
from .servicenode_lib import getObjectJson, validateAction
from .chunk_crawl import ChunkCrawler
from . import config
from . import hsds_logger as log

CHUNK_REF_LAYOUTS = (
    "H5D_CONTIGUOUS_REF",
    "H5D_CHUNKED_REF",
    "H5D_CHUNKED_REF_INDIRECT",
)

VARIABLE_AVG_ITEM_SIZE = 512  # guess at avg variable type length


def get_hrefs(request, dset_json):
    """
    Convience function to set up hrefs for GET
    """
    hrefs = []
    dset_id = dset_json["id"]
    dset_uri = f"/datasets/{dset_id}"
    self_uri = f"{dset_uri}/value"
    hrefs.append({"rel": "self", "href": getHref(request, self_uri)})
    root_uri = "/groups/" + dset_json["root"]
    hrefs.append({"rel": "root", "href": getHref(request, root_uri)})
    hrefs.append({"rel": "home", "href": getHref(request, "/")})
    hrefs.append({"rel": "owner", "href": getHref(request, dset_uri)})
    return hrefs


async def get_slices(app, select, dset_json, bucket=None):
    """Get desired slices from selection query param string or json value.
    If select is none or empty, slices for entire datashape will be
    returned.
    Refretch dims if the dataset is extensible
    """

    dset_id = dset_json["id"]
    datashape = dset_json["shape"]
    if datashape["class"] == "H5S_NULL":
        msg = "Null space datasets can not be used as target for GET value"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    dims = getShapeDims(datashape)  # throws 400 for HS_NULL dsets
    maxdims = getDsetMaxDims(dset_json)

    # refetch the dims if the dataset is extensible and request or hasn't
    # provided an explicit region
    if isExtensible(dims, maxdims) and (select is None or not select):
        kwargs = {"bucket": bucket, "refresh": True}
        dset_json = await getObjectJson(app, dset_id, **kwargs)
        dims = getShapeDims(dset_json["shape"])

    slices = None  # selection for read
    if isExtensible and select:
        try:
            slices = getSelectionList(select, dims)
        except ValueError:
            # exception might be due to us having stale version of dims,
            # so use refresh
            kwargs = {"bucket": bucket, "refresh": True}
            dset_json = await getObjectJson(app, dset_id, **kwargs)
            dims = getShapeDims(dset_json["shape"])
            slices = None  # retry below

    if slices is None:
        try:
            slices = getSelectionList(select, dims)
        except ValueError:
            msg = f"Invalid selection: {select} on dims: {dims} "
            msg += f"for dataset: {dset_id}"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
    return slices


def use_http_streaming(request, rank):
    """ return boolean indicating whether http streaming should be used """
    if rank == 0:
        return False
    if isAWSLambda(request):
        return False
    if not config.get("http_streaming", default=True):
        return False
    return True


async def getChunkLocations(
    app, dset_id, dset_json, chunkinfo_map, chunk_ids, bucket=None
):
    """
    Get info for chunk locations (for reference layouts)
    """
    layout_class = getDatasetLayout(dset_json)

    if layout_class not in CHUNK_REF_LAYOUTS:
        msg = f"skip getChunkLocations for layout class: {layout_class}"
        log.debug(msg)
        return
    
    chunk_dims = None
    if "layout" in dset_json:
        dset_layout = dset_json["layout"]
        log.debug(f"dset_json layout: {dset_layout}")
        if "dims" in dset_layout:
            chunk_dims = dset_layout["dims"]
    if chunk_dims is None:
        msg = "no chunk dimensions set in dataset layout"
        log.error(msg)
        raise HTTPInternalServerError()

    datashape = dset_json["shape"]
    datatype = dset_json["type"]
    if isNullSpace(dset_json):
        log.error("H5S_NULL shape class used with reference chunk layout")
        raise HTTPInternalServerError()
    dims = getShapeDims(datashape)
    rank = len(dims)
    # chunk_ids = list(chunkinfo_map.keys())
    # chunk_ids.sort()
    num_chunks = len(chunk_ids)
    msg = f"getChunkLocations for dset: {dset_id} bucket: {bucket} "
    msg += f"rank: {rank} num chunk_ids: {num_chunks}"
    log.info(msg)
    log.debug(f"getChunkLocations layout: {layout_class}")

    def getChunkItem(chunkid):
        if chunk_id in chunkinfo_map:
            chunk_item = chunkinfo_map[chunk_id]
        else:
            chunk_item = {}
            chunkinfo_map[chunk_id] = chunk_item
        return chunk_item

    if layout_class == "H5D_CONTIGUOUS_REF":
        layout = getDatasetCreationPropertyLayout(dset_json)
        log.debug(f"cpl layout: {layout}")
        s3path = layout["file_uri"]
        s3size = layout["size"]
        if s3size == 0:
            msg = "getChunkLocations - H5D_CONTIGUOUS_REF layout size 0, "
            msg += "no allocation"
            log.info(msg)
            return
        item_size = getItemSize(datatype)
        chunk_size = item_size
        for dim in chunk_dims:
            chunk_size *= dim
        log.debug(f"using chunk_size: {chunk_size} for H5D_CONTIGUOUS_REF")

        for chunk_id in chunk_ids:
            log.debug(f"getChunkLocations - getting data for chunk: {chunk_id}")
            chunk_item = getChunkItem(chunk_id)
            chunk_index = getChunkIndex(chunk_id)
            if len(chunk_index) != rank:
                log.error("Unexpected chunk_index")
                raise HTTPInternalServerError()
            extent = item_size
            if "offset" not in layout:
                msg = "getChunkLocations - expected to find offset in chunk "
                msg += "layout for H5D_CONTIGUOUS_REF"
                log.error(msg)
                continue
            s3offset = layout["offset"]
            if not isinstance(s3offset, int):
                msg = "getChunkLocations - expected offset to be an int but "
                msg += f"got: {s3offset}"
                log.error(msg)
                continue
            log.debug(f"getChunkLocations s3offset: {s3offset}")
            for i in range(rank):
                dim = rank - i - 1
                index = chunk_index[dim]
                s3offset += index * chunk_dims[dim] * extent
                extent *= dims[dim]
            msg = f"setting chunk_info_map to s3offset: {s3offset} "
            msg == f"s3size: {s3size} for chunk_id: {chunk_id}"
            log.debug(msg)
            if s3offset > layout["offset"] + layout["size"]:
                msg = f"range get of s3offset: {s3offset} s3size: {s3size} "
                msg += "extends beyond end of contiguous dataset for "
                msg += f"chunk_id: {chunk_id}"
                log.warn(msg)
            chunk_item["s3path"] = s3path
            chunk_item["s3offset"] = s3offset
            chunk_item["s3size"] = chunk_size
    elif layout_class == "H5D_CHUNKED_REF":
        layout = getDatasetCreationPropertyLayout(dset_json)
        log.debug(f"cpl layout: {layout}")
        s3path = layout["file_uri"]
        chunks = layout["chunks"]

        for chunk_id in chunk_ids:
            chunk_item = getChunkItem(chunk_id)
            s3offset = 0
            s3size = 0
            chunk_key = getChunkSuffix(chunk_id)
            if chunk_key in chunks:
                item = chunks[chunk_key]
                s3offset = item[0]
                s3size = item[1]
            chunk_item["s3path"] = s3path
            chunk_item["s3offset"] = s3offset
            chunk_item["s3size"] = s3size

    elif layout_class == "H5D_CHUNKED_REF_INDIRECT":
        layout = getDatasetCreationPropertyLayout(dset_json)
        log.debug(f"cpl layout: {layout}")
        if "chunk_table" not in layout:
            log.error("Expected to find chunk_table in dataset layout")
            raise HTTPInternalServerError()
        chunktable_id = layout["chunk_table"]
        # get  state for dataset from DN.
        kwargs = {"bucket": bucket, "refresh": False}
        chunktable_json = await getObjectJson(app, chunktable_id, **kwargs)
        # log.debug(f"chunktable_json: {chunktable_json}")
        chunktable_dims = getShapeDims(chunktable_json["shape"])
        chunktable_layout = chunktable_json["layout"]
        if chunktable_layout.get("class") == "H5D_CHUNKED_REF_INDIRECT":
            # We don't support recursive chunked_ref_indirect classes
            msg = "chunktable layout: H5D_CHUNKED_REF_INDIRECT is invalid"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)

        if len(chunktable_dims) != rank:
            msg = "Rank of chunktable should be same as the dataset"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)

        # convert the list of chunk_ids into a set of points to query in
        # the chunk table
        if rank == 1:
            arr_points = np.zeros((num_chunks,), dtype=np.dtype("u8"))
        else:
            arr_points = np.zeros((num_chunks, rank), dtype=np.dtype("u8"))
        for i in range(num_chunks):
            chunk_id = chunk_ids[i]
            log.debug(f"chunk_id for chunktable: {chunk_id}")
            indx = getChunkIndex(chunk_id)
            log.debug(f"get chunk indx: {indx}")
            if rank == 1:
                log.debug(f"convert: {indx[0]} to {indx}")
                indx = indx[0]
            arr_points[i] = indx
        msg = f"got chunktable points: {arr_points}, calling getSelectionData"
        log.debug(msg)
        # this call won't lead to a circular loop of calls since we've checked
        # that the chunktable layout is not H5D_CHUNKED_REF_INDIRECT
        point_data = await getSelectionData(
            app, chunktable_id, chunktable_json, points=arr_points, bucket=bucket
        )

        log.debug(f"got chunktable data: {point_data}")
        if "file_uri" in layout:
            s3_layout_path = layout["file_uri"]
        else:
            s3_layout_path = None

        for i in range(num_chunks):
            chunk_id = chunk_ids[i]
            item = point_data[i]
            s3offset = int(item[0])
            s3size = int(item[1])
            if s3_layout_path is None:
                if len(item) < 3:
                    msg = "expected chunk table to have three fields"
                    log.warn(msg)
                    raise HTTPBadRequest(reason=msg)
                e = item[2]
                if e:
                    s3path = e.decode("utf-8")
                    log.debug(f"got s3path: {s3path}")
            else:
                s3path = s3_layout_path
            chunk_item = getChunkItem(chunk_id)
            chunk_item["s3path"] = s3path
            chunk_item["s3offset"] = s3offset
            chunk_item["s3size"] = s3size

    else:
        log.error(f"Unexpected chunk layout: {layout['class']}")
        raise HTTPInternalServerError()

    log.debug(f"returning chunkinfo_map: {chunkinfo_map}")
    return chunkinfo_map


def get_chunk_selections(chunk_map, chunk_ids, slices, dset_json):
    """Update chunk_map with chunk and data selections for the
    given set of slices
    """
    log.debug(f"get_chunk_selections - chunk_ids: {chunk_ids}")
    if not slices:
        log.debug("no slices set, returning")
        return  # nothing to do
    log.debug(f"slices: {slices}")
    layout = getChunkLayout(dset_json)
    for chunk_id in chunk_ids:
        if chunk_id in chunk_map:
            item = chunk_map[chunk_id]
        else:
            item = {}
            chunk_map[chunk_id] = item

        chunk_sel = getChunkCoverage(chunk_id, slices, layout)
        log.debug(
            f"get_chunk_selections - chunk_id: {chunk_id}, chunk_sel: {chunk_sel}"
        )
        item["chunk_sel"] = chunk_sel
        data_sel = getDataCoverage(chunk_id, slices, layout)
        log.debug(f"get_chunk_selections - data_sel: {data_sel}")
        item["data_sel"] = data_sel


async def PUT_Value(request):
    """
    Handler for PUT /<dset_uuid>/value request
    """
    log.request(request)
    app = request.app
    bucket = None
    body = None
    query = None
    json_data = None
    params = request.rel_url.query
    append_rows = None  # this is a append update or not
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

    if "query" in params:
        if "append" in params:
            msg = "Query string can not be used with append parameter"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
        query = params["query"]

    dset_id = request.match_info.get("id")
    if not dset_id:
        msg = "Missing dataset id"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    if not isValidUuid(dset_id, "Dataset"):
        msg = f"Invalid dataset id: {dset_id}"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    username, pswd = getUserPasswordFromRequest(request)
    await validateUserPassword(app, username, pswd)

    domain = getDomainFromRequest(request)
    if not isValidDomain(domain):
        msg = f"Invalid domain: {domain}"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    bucket = getBucketForDomain(domain)

    request_type = getContentType(request)

    log.debug(f"PUT value - request_type is {request_type}")

    if not request.has_body:
        msg = "PUT Value with no body"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    if request_type == "json":
        try:
            body = await request.json()
        except JSONDecodeError:
            msg = "Unable to load JSON body"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
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

    # get state for dataset from DN.
    dset_json = await getObjectJson(app, dset_id, bucket=bucket, refresh=False)

    layout = None
    datashape = dset_json["shape"]
    if datashape["class"] == "H5S_NULL":
        msg = "Null space datasets can not be used as target for PUT value"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    dims = getShapeDims(datashape)
    maxdims = getDsetMaxDims(dset_json)
    rank = len(dims)

    if query and rank > 1:
        msg = "Query string is not supported for multidimensional arrays"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    layout = getChunkLayout(dset_json)

    type_json = dset_json["type"]
    dset_dtype = createDataType(type_json)
    item_size = getItemSize(type_json)
    max_request_size = int(config.get("max_request_size"))

    if query:
        # divert here if we are doing a put query
        # returns array data like a GET query request
        log.debug(f"got query: {query}")
        try:
            parser = BooleanParser(query)
        except Exception:
            msg = f"query: {query} is not valid"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)

        field_names = set(dset_dtype.names)
        variables = parser.getVariables()
        for variable in variables:
            if variable not in field_names:
                msg = f"query variable {variable} not valid"
                log.warn(msg)
                raise HTTPBadRequest(reason=msg)

        select = params.get("select")
        slices = await get_slices(app, select, dset_json, bucket=bucket)
        if "Limit" in params:
            try:
                limit = int(params["Limit"])
            except ValueError:
                msg = "Limit param must be positive int"
                log.warning(msg)
                raise HTTPBadRequest(reason=msg)
        else:
            limit = 0

        arr_rsp = await getSelectionData(
            app,
            dset_id,
            dset_json,
            slices,
            query=query,
            bucket=bucket,
            limit=limit,
            query_update=body,
            method=request.method,
        )

        log.debug(f"arr shape: {arr_rsp.shape}")
        response_type = getAcceptType(request)

        if response_type == "binary":
            output_data = arr_rsp.tobytes()
            msg = f"PUT_Value query - returning {len(output_data)} bytes binary data"
            log.debug(msg)

            # write response
            try:
                resp = StreamResponse()
                if config.get("http_compression"):
                    log.debug("enabling http_compression")
                    resp.enable_compression()
                resp.headers["Content-Type"] = "application/octet-stream"
                resp.content_length = len(output_data)
                await resp.prepare(request)
                await resp.write(output_data)
                await resp.write_eof()
            except Exception as e:
                log.error(f"Exception during binary data write: {e}")
        else:
            log.debug("PUT Value query - returning JSON data")
            rsp_json = {}
            data = arr_rsp.tolist()
            log.debug(f"got rsp data {len(data)} points")
            json_query_data = bytesArrayToList(data)
            rsp_json["value"] = json_query_data
            rsp_json["hrefs"] = get_hrefs(request, dset_json)
            resp = await jsonResponse(request, rsp_json)
        log.response(request, resp=resp)
        return resp

    # Resume regular PUT_Value processing without query update
    dset_dtype = createDataType(type_json)  # np datatype
    binary_data = None
    points = None  # used for point selection writes
    np_shape = []  # shape of incoming data
    slices = []  # selection area to write to

    if item_size == 'H5T_VARIABLE' or not use_http_streaming(request, rank):
        http_streaming = False
    else:
        http_streaming = True

    # body could also contain a point selection specifier
    if body and "points" in body:
        if append_rows:
            msg = "points not valid with append update"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)

        json_points = body["points"]
        num_points = len(json_points)
        if rank == 1:
            point_shape = (num_points,)
            log.info(f"rank 1: point_shape: {point_shape}")
        else:
            point_shape = (num_points, rank)
            log.info(f"rank >1: point_shape: {point_shape}")
        try:
            # use uint64 so we can address large array extents
            dt = np.dtype(np.uint64)
            points = jsonToArray(point_shape, dt, json_points)
        except ValueError:
            msg = "Bad Request: point list not valid for dataset shape"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)

    if append_rows:
        # shape must be extensible
        if not isExtensible(dims, maxdims):
            msg = "Dataset shape must be extensible for packet updates"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
        if append_dim < 0 or append_dim > rank - 1:
            msg = "invalid append_dim"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
        maxdims = getDsetMaxDims(dset_json)
        if maxdims[append_dim] != 0:
            if dims[append_dim] + append_rows > maxdims[append_dim]:
                log.warn("unable to append to dataspace")
                raise HTTPConflict()

    # refetch the dims if the dataset is extensible
    if isExtensible(dims, maxdims):
        kwargs = {"bucket": bucket, "refresh": True}
        dset_json = await getObjectJson(app, dset_id, **kwargs)
        dims = getShapeDims(dset_json["shape"])

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
    else:
        # read binary data from request
        log.info(f"request content_length: {request.content_length}")

        if isinstance(request.content_length, int):
            if request.content_length >= max_request_size:
                if http_streaming:
                    # just do an info log that we'll be paginating over a large request
                    msg = f"will paginate over large request with {request.content_length} bytes"
                    log.info(msg)
                else:
                    msg = f"Request size {request.content_length} too large "
                    msg += f"for variable length data, max: {max_request_size}"
                    log.warn(msg)
                    raise HTTPRequestEntityTooLarge(request.content_length, max_request_size)

        if not http_streaming:
            # read the request data now
            # TBD: support streaming for variable length types
            try:
                binary_data = await request_read(request)
            except HTTPRequestEntityTooLarge as tle:
                msg = "Got HTTPRequestEntityTooLarge exception during "
                msg += f"binary read: {tle})"
                log.warn(msg)
                raise  # re-throw

            if len(binary_data) != request.content_length:
                msg = f"Read {len(binary_data)} bytes, expecting: "
                msg += f"{request.content_length}"
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
        log.debug(f"np_shape based on append_rows: {np_shape}")
        np_shape = tuple(np_shape)

    elif points is None:
        if body and "start" in body and "stop" in body:
            slices = await get_slices(app, body, dset_json, bucket=bucket)
        else:
            select = params.get("select")
            slices = await get_slices(app, select, dset_json, bucket=bucket)

        # The selection parameters will determine expected put value shape
        log.debug(f"PUT Value selection: {slices}")
        # not point selection, get hyperslab selection shape
        np_shape = getSelectionShape(slices)
    else:
        # point update
        np_shape = (num_points,)

    log.debug(f"selection shape: {np_shape}")
    num_elements = getNumElements(np_shape)
    log.debug(f"selection num elements: {num_elements}")
    if num_elements <= 0:
        msg = "Selection is empty"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    arr = None  # np array to hold request data
    if binary_data and isinstance(item_size, int):
        # binary, fixed item_size
        if num_elements * item_size != len(binary_data):
            msg = f"Expected: {num_elements*item_size} bytes, "
            msg += f"but got: {len(binary_data)}, "
            msg += f"num_elements: {num_elements}, item_size: {item_size}"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
        if num_elements * item_size > max_request_size:
            msg = f"read {num_elements*item_size} bytes, greater than {max_request_size}"
            log.warn(msg)
        arr = np.fromstring(binary_data, dtype=dset_dtype)
        try:
            arr = arr.reshape(np_shape)  # conform to selection shape
        except ValueError:
            msg = "Bad Request: binary input data doesn't match selection"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
        msg = f"PUT value - numpy array shape: {arr.shape} dtype: {arr.dtype}"
        log.debug(msg)
    elif binary_data and item_size == "H5T_VARIABLE":
        # binary variable length data
        try:
            arr = bytesToArray(binary_data, dset_dtype, np_shape)
        except ValueError as ve:
            log.warn(f"bytesToArray value error: {ve}")
            raise HTTPBadRequest()
    elif request_type == "json":
        # get array from json input
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
        log.debug(f"got json arr: {arr.shape}")
    else:
        log.debug("will using streaming for request data")

    if append_rows:
        # extend the shape of the dataset
        req = getDataNodeUrl(app, dset_id) + "/datasets/" + dset_id + "/shape"
        body = {"extend": append_rows, "extend_dim": append_dim}
        params = {}
        if bucket:
            params["bucket"] = bucket
        selection = None
        try:
            shape_rsp = await http_put(app, req, data=body, params=params)
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
        # extract n and m and use it to update the slice for the
        # appending dimension
        if not selection.startswith("[") or not selection.endswith("]"):
            log.error("Unexpected selection in PUT shape response")
            raise HTTPInternalServerError()
        selection = selection[1:-1]  # strip off brackets
        parts = selection.split(",")
        for part in parts:
            if part == ":":
                continue
            bounds = part.split(":")
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
    crawler_status = None  # will be set below
    if points is None:
        if arr is not None:
            # make a one page list to handle the write in one chunk crawler run
            # (larger write request should user binary streaming)
            pages = (slices,)
            log.debug(f"non-streaming data, setting page list to: {slices}")
        else:
            pages = getSelectionPagination(slices, dims, item_size, max_request_size)
            log.debug(f"getSelectionPagination returned: {len(pages)} pages")
        bytes_streamed = 0
        max_chunks = int(config.get("max_chunks_per_request", default=1000))

        for page_number in range(len(pages)):
            page = pages[page_number]
            msg = f"streaming request data for page: {page_number+1} of {len(pages)}, "
            msg += f"selection: {page}"
            log.info(msg)
            num_chunks = getNumChunks(page, layout)
            log.debug(f"num_chunks: {num_chunks}")
            if num_chunks > max_chunks:
                log.warn(
                    f"PUT value chunk count: {num_chunks} exceeds max_chunks: {max_chunks}"
                )
            select_shape = getSelectionShape(page)
            log.debug(f"got select_shape: {select_shape} for page: {page}")
            num_bytes = math.prod(select_shape) * item_size
            if arr is None or page_number > 0:
                log.debug(
                    f"page: {page_number} reading {num_bytes} from request stream"
                )
                # read page of data from input stream
                try:
                    page_bytes = await request_read(request, count=num_bytes)
                except HTTPRequestEntityTooLarge as tle:
                    msg = "Got HTTPRequestEntityTooLarge exception during "
                    msg += f"binary read: {tle})"
                    log.warn(msg)
                    raise  # re-throw
                except IncompleteReadError as ire:
                    msg = "Got asyncio.IncompleteReadError during binary "
                    msg += f"read: {ire}"
                    log.warn(msg)
                    raise HTTPBadRequest(reason=msg)
                log.debug(f"read {len(page_bytes)} for page: {page_number+1}")
                bytes_streamed += len(page_bytes)
                try:
                    arr = bytesToArray(page_bytes, dset_dtype, select_shape)
                except ValueError as ve:
                    msg = f"bytesToArray value error for page: {page_number+1}: {ve}"
                    log.warn(msg)
                    raise HTTPBadRequest(reason=msg)
                if len(select_shape) == 2:
                    log.debug(f"arr test value[0,0]: {arr[0,0]}")

            try:
                chunk_ids = getChunkIds(dset_id, page, layout)
            except ValueError:
                log.warn("getChunkIds failed")
                raise HTTPInternalServerError()
            log.debug(f"chunk_ids: {chunk_ids}")
            if len(chunk_ids) > max_chunks:
                log.warn(
                    f"got {len(chunk_ids)} for page: {page_number+1}.  max_chunks: {max_chunks} "
                )

            crawler = ChunkCrawler(
                app,
                chunk_ids,
                dset_json=dset_json,
                bucket=bucket,
                slices=page,
                arr=arr,
                action="write_chunk_hyperslab",
            )
            await crawler.crawl()

            crawler_status = crawler.get_status()

            if crawler_status not in (200, 201):
                log.warn(
                    f"crawler failed for page: {page_number+1} with status: {crawler_status}"
                )
            else:
                log.info("crawler write_chunk_hyperslab successful")

    else:
        #
        # Do point PUT
        #
        log.debug(f"num_points: {num_points}")

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
                    msg = f"PUT Value point: {point} is not within the "
                    msg += "bounds of the dataset"
                    log.warn(msg)
                    raise HTTPBadRequest(reason=msg)
            else:
                if len(point) != rank:
                    msg = "PUT Value point value did not match dataset rank"
                    log.warn(msg)
                    raise HTTPBadRequest(reason=msg)
                for i in range(rank):
                    if point[i] < 0 or point[i] >= dims[i]:
                        msg = f"PUT Value point: {point} is not within the "
                        msg += "bounds of the dataset"
                        log.warn(msg)
                        raise HTTPBadRequest(reason=msg)
            chunk_id = getChunkId(dset_id, point, layout)
            # get the pt_indx element from the input data
            value = arr[pt_indx]
            if chunk_id not in chunk_dict:
                point_list = [
                    point,
                ]
                point_data = [
                    value,
                ]
                chunk_dict[chunk_id] = {"indices": point_list, "points": point_data}
            else:
                item = chunk_dict[chunk_id]
                point_list = item["indices"]
                point_list.append(point)
                point_data = item["points"]
                point_data.append(value)

        num_chunks = len(chunk_dict)
        log.debug(f"num_chunks: {num_chunks}")
        max_chunks = int(config.get("max_chunks_per_request", default=1000))
        if num_chunks > max_chunks:
            msg = f"PUT value request with more than {max_chunks} chunks"
            log.warn(msg)

        chunk_ids = list(chunk_dict.keys())
        chunk_ids.sort()

        crawler = ChunkCrawler(
            app,
            chunk_ids,
            dset_json=dset_json,
            bucket=bucket,
            points=chunk_dict,
            action="write_point_sel",
        )
        await crawler.crawl()

        crawler_status = crawler.get_status()

    if crawler_status == 400:
        log.info(f"doWriteSelection raising BadRequest error:  {crawler_status}")
        raise HTTPBadRequest()
    if crawler_status not in (200, 201):
        log.info(
            f"doWriteSelection raising HTTPInternalServerError for status:  {crawler_status}"
        )
        raise HTTPInternalServerError()

    # write successful

    resp_json = {}
    resp = await jsonResponse(request, resp_json)
    return resp


async def GET_Value(request):
    """
    Handler for GET /<dset_uuid>/value request
    """
    log.request(request)
    app = request.app
    params = request.rel_url.query

    dset_id = request.match_info.get("id")
    if not dset_id:
        msg = "Missing dataset id"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    if not isValidUuid(dset_id, "Dataset"):
        msg = f"Invalid dataset id: {dset_id}"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    username, pswd = getUserPasswordFromRequest(request)
    if username is None and app["allow_noauth"]:
        username = "default"
    else:
        await validateUserPassword(app, username, pswd)

    domain = getDomainFromRequest(request)
    if not isValidDomain(domain):
        msg = f"Invalid domain: {domain}"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    bucket = getBucketForDomain(domain)

    # get state for dataset from DN.
    dset_json = await getObjectJson(app, dset_id, bucket=bucket)
    type_json = dset_json["type"]
    dset_dtype = createDataType(type_json)

    if isNullSpace(dset_json):
        msg = "Null space datasets can not be used as target for GET value"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    datashape = dset_json["shape"]
    dims = getShapeDims(datashape)
    log.debug(f"dset shape: {dims}")
    rank = len(dims)

    layout = getChunkLayout(dset_json)
    log.debug(f"chunk layout: {layout}")

    await validateAction(app, domain, dset_id, username, "read")

    # Get query parameter for selection
    select = params.get("select")
    if select:
        log.debug(f"select query param: {select}")
    slices = await get_slices(app, select, dset_json, bucket=bucket)
    log.debug(f"GET Value selection: {slices}")

    limit = 0
    if "Limit" in params:
        try:
            limit = int(params["Limit"])
            log.debug(f"limit: {limit}")
        except ValueError:
            msg = "Invalid Limit query param"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)

    if "ignore_nan" in params and params["ignore_nan"]:
        ignore_nan = True
    else:
        ignore_nan = False
    log.debug(f"ignore nan: {ignore_nan}")

    query = params.get("query")
    if query:
        log.debug(f"got query: {query}")
        try:
            parser = BooleanParser(query)
        except Exception:
            msg = f"query: {query} is not valid"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)

        field_names = set(dset_dtype.names)
        variables = parser.getVariables()
        for variable in variables:
            if variable not in field_names:
                msg = f"query variable {variable} not valid"
                log.warn(msg)
                raise HTTPBadRequest(reason=msg)

    response_type = getAcceptType(request)

    if response_type == "binary" and use_http_streaming(request, rank):
        stream_pagination = True
        log.debug("use stream_pagination")
    else:
        stream_pagination = False
        log.debug("no stream_pagination")

    # for non query requests with non-variable types we can fetch
    # the expected response bytes length now
    item_size = getItemSize(type_json)
    log.debug(f"item size: {item_size}")

    # get the shape of the response array
    np_shape = getSelectionShape(slices)
    log.debug(f"selection shape: {np_shape}")

    # check that the array size is reasonable
    request_size = math.prod(np_shape)
    if item_size == "H5T_VARIABLE":
        request_size *= VARIABLE_AVG_ITEM_SIZE  # random guess of avg item_size
    else:
        request_size *= item_size
    log.debug(f"request_size: {request_size}")
    max_request_size = int(config.get("max_request_size"))
    if isAWSLambda(request):
        # reduce max size to account for hex_encoding and other JSON content
        max_request_size -= 1000
        max_request_size /= 2
    if request_size >= max_request_size and not stream_pagination:
        msg = "GET value request too large"
        log.warn(msg)
        raise HTTPRequestEntityTooLarge(request_size, max_request_size)
    if item_size != "H5T_VARIABLE" and not query:
        # this is the exact number of bytes to be returned
        content_length = request_size
    else:
        content_length = None

    resp_json = {"status": 200}  # will over-write if there's a problem
    # write response
    try:
        resp = StreamResponse()
        if config.get("http_compression"):
            log.debug("enabling http_compression")
            resp.enable_compression()
        if response_type == "binary":
            resp.headers["Content-Type"] = "application/octet-stream"
            if content_length is None:
                log.debug("content_length could not be determined")
            else:
                resp.content_length = content_length
        else:
            resp.headers["Content-Type"] = "application/json"
        log.debug("prepare request")
        await resp.prepare(request)
        arr = None  # will be set based on returned data

        if stream_pagination:
            # example
            # get binary data a page at a time and write back to response
            if item_size == "H5T_VARIABLE":
                page_item_size = VARIABLE_AVG_ITEM_SIZE  # random guess of avg item_size
            else:
                page_item_size = item_size
            pages = getSelectionPagination(
                slices, dims, page_item_size, max_request_size
            )
            log.debug(f"getSelectionPagination returned: {len(pages)} pages")
            bytes_streamed = 0
            try:
                for page_number in range(len(pages)):
                    page = pages[page_number]
                    msg = f"streaming response data for page: {page_number+1} "
                    msg += f"of {len(pages)}, selection: {page}"
                    log.info(msg)

                    arr = await getSelectionData(
                        app,
                        dset_id,
                        dset_json,
                        page,
                        query=query,
                        bucket=bucket,
                        limit=limit,
                        method=request.method,
                    )

                    if arr is None or math.prod(arr.shape) == 0:
                        log.warn(f"no data returend for streaming page: {page_number}")
                        continue

                    log.debug("preparing binary response")
                    output_data = arrayToBytes(arr)
                    log.debug(f"got {len(output_data)} bytes for resp")
                    bytes_streamed += len(output_data)
                    log.debug("write request")
                    await resp.write(output_data)

                    if query and limit > 0:
                        query_rows = arr.shape[0]
                        log.debug(
                            f"streaming page {page_number} returned {query_rows} rows"
                        )
                        limit -= query_rows
                        if limit <= 0:
                            log.debug("skipping remaining pages, query limit reached")
                            break

            except HTTPException as he:
                # close the response stream
                log.error(f"got {type(he)} exception doing getSelectionData: {he}")
                resp_json["status"] = he.status_code
                # can't raise a HTTPException here since write is in progress
                #
            finally:
                msg = f"streaming data for {len(pages)} pages complete, "
                msg += f"{bytes_streamed} bytes written"
                log.info(msg)

                await resp.write_eof()
                return resp

        #
        # non-paginated response
        #

        try:
            arr = await getSelectionData(
                app,
                dset_id,
                dset_json,
                slices,
                query=query,
                bucket=bucket,
                limit=limit,
                method=request.method,
            )
        except HTTPException as he:
            # close the response stream
            log.error(f"got {type(he)} exception doing getSelectionData: {he}")
            resp_json["status"] = he.status_code
            # can't raise a HTTPException here since write is in progress

        if arr is None:
            # no array (OPTION request?)  Return empty json response
            log.warn("got None response from getSelectionData")

        elif not isinstance(arr, np.ndarray):
            msg = f"GET_Value - Expected ndarray but got: {type(arr)}"
            resp_json["status"] = 500
        elif response_type == "binary":
            if resp_json["status"] != 200:
                # write json with status_code
                # resp_json = resp_json.encode('utf-8')
                # await resp.write(resp_json)
                log.warn(f"GET Value - got error status: {resp_json['status']}")
            else:
                log.debug("preparing binary response")
                output_data = arrayToBytes(arr)
                log.debug(f"got {len(output_data)} bytes for resp")
                log.debug("write request")
                await resp.write(output_data)
        else:
            # return json
            log.debug("GET Value - returning JSON data")
            params = request.rel_url.query
            if "reduce_dim" in params and params["reduce_dim"]:
                arr = squeezeArray(arr)

            data = arr.tolist()
            json_data = bytesArrayToList(data)

            datashape = dset_json["shape"]

            if datashape["class"] == "H5S_SCALAR":
                # convert array response to value
                resp_json["value"] = json_data[0]
            else:
                resp_json["value"] = json_data
            resp_json["hrefs"] = get_hrefs(request, dset_json)
            resp_body = await jsonResponse(
                resp, resp_json, ignore_nan=ignore_nan, body_only=True
            )
            log.debug(f"jsonResponse returned: {resp_body}")
            resp_body = resp_body.encode("utf-8")
            await resp.write(resp_body)
        await resp.write_eof()
    except Exception as e:
        log.error(f"{type(e)} Exception during data write: {e}")
        import traceback

        tb = traceback.format_exc()
        print("traceback:", tb)
        raise HTTPInternalServerError()

    return resp


async def doReadSelection(
    app,
    chunk_ids,
    dset_json,
    slices=None,
    points=None,
    query=None,
    query_update=None,
    chunk_map=None,
    bucket=None,
    limit=0,
):
    """read selection utility function"""
    log.info(f"doReadSelection - number of chunk_ids: {len(chunk_ids)}")
    log.debug(f"doReadSelection - chunk_ids: {chunk_ids}")

    type_json = dset_json["type"]
    item_size = getItemSize(type_json)
    log.debug(f"item size: {item_size}")
    dset_dtype = createDataType(type_json)  # np datatype
    if query is None:
        query_dtype = None
    else:
        log.debug(f"query: {query} limit: {limit}")
        query_dtype = getQueryDtype(dset_dtype)

    # create array to hold response data
    arr = None

    if points is not None:
        # point selection
        np_shape = [
            len(points),
        ]
    elif query is not None:
        # return shape will be determined by number of matches
        np_shape = None
    elif slices is not None:
        log.debug(f"get np_shape for slices: {slices}")
        np_shape = getSelectionShape(slices)
    else:
        log.error("doReadSelection - expected points or slices to be set")
        raise HTTPInternalServerError()
    log.debug(f"selection shape: {np_shape}")

    if np_shape is not None:
        # check that the array size is reasonable
        request_size = math.prod(np_shape)
        if item_size == "H5T_VARIABLE":
            request_size *= 512  # random guess of avg item_size
        else:
            request_size *= item_size
            log.debug(f"request_size: {request_size}")
        max_request_size = int(config.get("max_request_size"))
        if request_size >= max_request_size:
            msg = f"Attempting to fetch {request_size} bytes (greater than "
            msg += f"{max_request_size} limit"
            log.error(msg)
            raise HTTPBadRequest(reason=msg)

        arr = np.zeros(np_shape, dtype=dset_dtype, order="C")
        fill_value = getFillValue(dset_json)
        if fill_value is not None:
            arr[...] = fill_value

    crawler = ChunkCrawler(
        app,
        chunk_ids,
        dset_json=dset_json,
        chunk_map=chunk_map,
        bucket=bucket,
        slices=slices,
        query=query,
        query_update=query_update,
        limit=limit,
        arr=arr,
        action="read_chunk_hyperslab",
    )
    await crawler.crawl()

    crawler_status = crawler.get_status()

    log.info(f"doReadSelection complete - status:  {crawler_status}")
    if crawler_status == 400:
        log.info(f"doReadSelection raising BadRequest error:  {crawler_status}")
        raise HTTPBadRequest()
    if crawler_status not in (200, 201):
        log.info(
            f"doReadSelection raising HTTPInternalServerError for status:  {crawler_status}"
        )
        raise HTTPInternalServerError()

    if query is not None:
        # combine chunk responses and return
        if limit > 0 and crawler._hits > limit:
            nrows = limit
        else:
            nrows = crawler._hits
        arr = np.empty((nrows,), dtype=query_dtype)
        start = 0
        for chunkid in chunk_ids:
            if chunkid not in chunk_map:
                continue
            chunk_item = chunk_map[chunkid]
            if "query_rsp" not in chunk_item:
                continue
            query_rsp = chunk_item["query_rsp"]
            if len(query_rsp) == 0:
                continue
            stop = start + len(query_rsp)
            if stop > nrows:
                rsp_stop = len(query_rsp) - (stop - nrows)
                arr[start:] = query_rsp[0:rsp_stop]
            else:
                arr[start:stop] = query_rsp[:]
            start = stop
            if start >= nrows:
                log.debug(f"got {nrows} rows for query, quitting")
                break
    return arr


async def getSelectionData(
    app,
    dset_id,
    dset_json,
    slices=None,
    points=None,
    query=None,
    query_update=None,
    bucket=None,
    limit=0,
    method="GET",
):
    """Read selected slices and return numpy array"""
    log.debug("getSelectionData")
    if slices is None and points is None:
        log.error("getSelectionData - expected either slices or points to be set")
        raise HTTPInternalServerError()

    layout = getChunkLayout(dset_json)

    chunkinfo = {}

    if slices is not None:
        num_chunks = getNumChunks(slices, layout)
        log.debug(f"num_chunks: {num_chunks}")

        max_chunks = int(config.get("max_chunks_per_request", default=1000))
        if num_chunks > max_chunks:
            msg = f"num_chunks over {max_chunks} limit, but will attempt to fetch with crawler"
            log.warn(msg)

        chunk_ids = getChunkIds(dset_id, slices, layout)
    else:
        # points - already checked it is not None
        num_points = len(points)
        chunk_ids = []
        for pt_indx in range(num_points):
            point = points[pt_indx]
            chunk_id = getChunkId(dset_id, point, layout)
            if chunk_id in chunkinfo:
                chunk_entry = chunkinfo[chunk_id]
            else:
                chunk_entry = {}
                chunkinfo[chunk_id] = chunk_entry
                chunk_ids.append(chunk_id)
            if "points" in chunk_entry:
                point_list = chunk_entry["points"]
            else:
                point_list = []
                chunk_entry["points"] = point_list
            if "indices" in chunk_entry:
                point_index = chunk_entry["indices"]
            else:
                point_index = []
                chunk_entry["indices"] = point_index

            point_list.append(point)
            point_index.append(pt_indx)

    # Get information about where chunks are located
    #   Will be None except for H5D_CHUNKED_REF_INDIRECT type
    await getChunkLocations(
        app, dset_id, dset_json, chunkinfo, chunk_ids, bucket=bucket
    )
    if slices is None:
        slices = await get_slices(app, None, dset_json, bucket=bucket)

    if points is None:
        # get chunk selections for hyperslab select
        get_chunk_selections(chunkinfo, chunk_ids, slices, dset_json)

    log.debug(f"chunkinfo_map: {chunkinfo}")

    if method == "OPTIONS":
        # skip doing any big data load for options request
        return None

    arr = await doReadSelection(
        app,
        chunk_ids,
        dset_json,
        slices=slices,
        points=points,
        query=query,
        query_update=query_update,
        limit=limit,
        chunk_map=chunkinfo,
        bucket=bucket,
    )

    return arr


async def POST_Value(request):
    """
    Handler for POST /<dset_uuid>/value request - point selection or hyperslab read
    """
    log.request(request)

    app = request.app
    body = None

    dset_id = request.match_info.get("id")
    if not dset_id:
        msg = "Missing dataset id"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    if not isValidUuid(dset_id, "Dataset"):
        msg = f"Invalid dataset id: {dset_id}"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    log.info(f"POST_Value, dataset id: {dset_id}")

    username, pswd = getUserPasswordFromRequest(request)
    if username is None and app["allow_noauth"]:
        username = "default"
    else:
        await validateUserPassword(app, username, pswd)

    domain = getDomainFromRequest(request)
    if not isValidDomain(domain):
        msg = f"Invalid domain: {domain}"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    bucket = getBucketForDomain(domain)

    accept_type = getAcceptType(request)
    response_type = accept_type  # will adjust later if binary not possible

    params = request.rel_url.query
    if "ignore_nan" in params and params["ignore_nan"]:
        ignore_nan = True
    else:
        ignore_nan = False

    request_type = getContentType(request)
    log.debug(f"POST value - request_type is {request_type}")

    if not request.has_body:
        msg = "POST Value with no body"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    # get  state for dataset from DN.
    dset_json = await getObjectJson(app, dset_id, bucket=bucket)

    datashape = dset_json["shape"]
    if datashape["class"] == "H5S_NULL":
        msg = "POST value not supported for datasets with NULL shape"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    if datashape["class"] == "H5S_SCALAR":
        msg = "POST value not supported for datasets with SCALAR shape"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    dims = getShapeDims(datashape)
    rank = len(dims)

    type_json = dset_json["type"]
    item_size = getItemSize(type_json)
    log.debug(f"item size: {item_size}")

    await validateAction(app, domain, dset_id, username, "read")

    # read body data
    slices = None  # this will be set for hyperslab selection
    points = None  # this will be set for point selection
    point_dt = np.dtype("u8")  # use unsigned long for point index
    if request_type == "json":
        try:
            body = await request.json()
        except JSONDecodeError:
            msg = "Unable to load JSON body"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
        if "points" in body:
            points_list = body["points"]
            if not isinstance(points_list, list):
                msg = "POST Value expected list of points"
                log.warn(msg)
                raise HTTPBadRequest(reason=msg)
            points = np.asarray(points_list, dtype=point_dt)
            log.debug(f"get {len(points)} points from json request")
        elif "select" in body:
            select = body["select"]
            log.debug(f"select: {select}")
            slices = await get_slices(app, select, dset_json, bucket=bucket)
            log.debug(f"got slices: {slices}")
        else:
            msg = "Expected points or select key in request body"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
    else:
        # read binary data
        binary_data = await request_read(request)
        if len(binary_data) != request.content_length:
            msg = f"Read {len(binary_data)} bytes, expecting: "
            msg += f"{request.content_length}"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
        if request.content_length % point_dt.itemsize != 0:
            msg = f"Content length: {request.content_length} not "
            msg += f"divisible by element size: {item_size}"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
        num_points = request.content_length // point_dt.itemsize
        points = np.fromstring(binary_data, dtype=point_dt)
        # reshape the data based on the rank (num_points x rank)
        if rank > 1:
            if len(points) % rank != 0:
                msg = "Number of point values is not consistent with dataset rank"
                log.warn(msg)
                raise HTTPBadRequest(reason=msg)
            num_points = len(points) // rank
            # conform to point index shape
            points = points.reshape((num_points, rank))

    if points is not None:
        log.debug(f"got {len(points)} num_points")

    # get expected content_length
    item_size = getItemSize(type_json)
    log.debug(f"item size: {item_size}")

    # get the shape of the response array
    if slices:
        # hyperslab post
        np_shape = getSelectionShape(slices)
    else:
        # point selection
        np_shape = [
            len(points),
        ]

    log.debug(f"selection shape: {np_shape}")

    # check that the array size is reasonable
    request_size = np.prod(np_shape)
    if item_size == "H5T_VARIABLE":
        request_size *= 512  # random guess of avg item_size
    else:
        request_size *= item_size
    log.debug(f"request_size: {request_size}")
    max_request_size = int(config.get("max_request_size"))
    if request_size >= max_request_size:
        msg = "POST value request too large"
        log.warn(msg)
        raise HTTPRequestEntityTooLarge(request_size, max_request_size)
    if item_size != "H5T_VARIABLE":
        # this is the exact number of bytes to be returned
        content_length = request_size
    else:
        # don't put content_length in response headers
        content_length = None

    if points is not None:
        # validate content of points input array
        for i in range(len(points)):
            point = points[i]
            if rank == 1:
                if point < 0 or point >= dims[0]:
                    msg = f"POST Value point: {point} is not within the bounds "
                    msg += "of the dataset"
                    log.warn(msg)
                    raise HTTPBadRequest(reason=msg)
            else:
                if len(point) != rank:
                    msg = "POST Value point value did not match dataset rank"
                    log.warn(msg)
                    raise HTTPBadRequest(reason=msg)
                for i in range(rank):
                    if point[i] < 0 or point[i] >= dims[i]:
                        msg = f"POST Value point: {point} is not within the "
                        msg += "bounds of the dataset"
                        log.warn(msg)
                        raise HTTPBadRequest(reason=msg)

    # write response
    resp = StreamResponse()
    try:
        if config.get("http_compression"):
            log.debug("enabling http_compression")
            resp.enable_compression()
        if response_type == "binary":
            resp.headers["Content-Type"] = "application/octet-stream"
            if content_length is None:
                log.debug("content_length could not be determined")
            else:
                resp.content_length = content_length
        else:
            resp.headers["Content-Type"] = "application/json"
        log.debug("prepare request...")
        await resp.prepare(request)

        kwargs = {"bucket": bucket}
        if slices is not None:
            kwargs["slices"] = slices
        if points is not None:
            kwargs["points"] = points
        log.debug(f"getSelectionData kwargs: {kwargs}")

        arr_rsp = await getSelectionData(app, dset_id, dset_json, **kwargs)
        if not isinstance(arr_rsp, np.ndarray):
            msg = f"POST_Value - Expected ndarray but got: {type(arr_rsp)}"
            log.error(msg)
            raise ValueError(msg)

        log.debug(f"arr shape: {arr_rsp.shape}")
        if response_type == "binary":
            log.debug("preparing binary response")
            output_data = arr_rsp.tobytes()
            msg = f"POST Value - returning {len(output_data)} bytes binary data"
            log.debug(msg)
            await resp.write(output_data)
        else:
            log.debug("POST Value - returning JSON data")
            resp_json = {}
            data = arr_rsp.tolist()
            log.debug(f"got rsp data {len(data)} points")
            json_data = bytesArrayToList(data)
            resp_json["value"] = json_data
            resp_json["hrefs"] = get_hrefs(request, dset_json)
            resp_body = await jsonResponse(
                resp, resp_json, ignore_nan=ignore_nan, body_only=True
            )
            log.debug(f"jsonResponse returned: {resp_body}")
            resp_body = resp_body.encode("utf-8")
            await resp.write(resp_body)
    except Exception as e:
        log.error(f"{type(e)} Exception during response write")
        import traceback

        tb = traceback.format_exc()
        print("traceback:", tb)

    # finalize response
    await resp.write_eof()

    log.response(request, resp=resp)
    return resp
