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

from .util.httpUtil import getHref, getAcceptType, getContentType
from .util.httpUtil import request_read, jsonResponse, isAWSLambda
from .util.idUtil import isValidUuid
from .util.domainUtil import getDomainFromRequest, isValidDomain
from .util.domainUtil import getBucketForDomain
from .util.hdf5dtype import getItemSize, getSubType, createDataType
from .util.dsetUtil import isNullSpace, isScalarSpace, get_slices, getShapeDims
from .util.dsetUtil import isExtensible, getSelectionPagination
from .util.dsetUtil import getSelectionShape, getDsetMaxDims, getChunkLayout
from .util.chunkUtil import getNumChunks, getChunkIds, getChunkId
from .util.arrayUtil import bytesArrayToList, jsonToArray
from .util.arrayUtil import getNumElements, arrayToBytes, bytesToArray
from .util.arrayUtil import squeezeArray, getBroadcastShape
from .util.authUtil import getUserPasswordFromRequest, validateUserPassword
from .servicenode_lib import getDsetJson, validateAction
from .dset_lib import getSelectionData, getParser, extendShape
from .chunk_crawl import ChunkCrawler
from . import config
from . import hsds_logger as log


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


def use_http_streaming(request, rank):
    """ return boolean indicating whether http streaming should be used """
    if rank == 0:
        return False
    if isAWSLambda(request):
        return False
    if not config.get("http_streaming", default=True):
        return False
    return True


def _isIgnoreNan(params, body=None):
    kw = "ignore_nan"
    if isinstance(body, dict) and kw in params and params[kw]:
        ignore_nan = bool(body)
    elif kw in params and params[kw]:
        ignore_nan = True
    else:
        ignore_nan = False
    return ignore_nan


def _isAppend(params, body=None):
    """ return True if append values are specified in params or body """
    kw = "append"
    if isinstance(body, dict) and kw in body and body[kw]:
        isAppend = True
    elif kw in params and params[kw]:
        isAppend = True
    else:
        isAppend = False
    return isAppend


def _getAppendDim(params, body=None):
    append_dim_param = None
    kw = "append_dim"
    if isinstance(body, dict):
        if kw in body:
            append_dim_param = body[kw]

    if append_dim_param is None:
        # check query param
        if kw in params:
            append_dim_param = params[kw]

    if append_dim_param is not None:
        try:
            append_dim = int(append_dim_param)
        except ValueError:
            msg = "invalid append_dim"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
        log.info(f"append_dim: {append_dim}")
    else:
        append_dim = 0

    return append_dim


def _getAppendRows(params, dset_json, body=None):
    """ get append rows value from query param or body """
    append_rows = None
    kw = "append"

    if isinstance(body, dict) and kw in body and body[kw]:
        try:
            append_rows = int(body[kw])
        except ValueError:
            msg = "invalid append value in body"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
    elif kw in params and params[kw]:
        try:
            append_rows = int(params[kw])
        except ValueError:
            msg = "invalid append query param"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
    else:
        append_rows = None

    if append_rows:
        log.info(f"append_rows: {append_rows}")
        datashape = dset_json["shape"]
        dims = getShapeDims(datashape)
        rank = len(dims)
        if rank == 0:
            msg = "append can't be used in scalar or null space datasets"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
        # select can't be used with append
        if _isSelect(params, body=body):
            msg = "select query parameter can not be used with append"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)

        # shape must be extensible
        datashape = dset_json["shape"]
        dims = getShapeDims(datashape)
        rank = len(dims)
        maxdims = getDsetMaxDims(dset_json)
        if not isExtensible(dims, maxdims):
            msg = "Dataset shape must be extensible for packet updates"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
        append_dim = _getAppendDim(params, body=body)
        if append_dim < 0 or append_dim > rank - 1:
            msg = "invalid append_dim"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)

        if maxdims[append_dim] != 0:
            if dims[append_dim] + append_rows > maxdims[append_dim]:
                log.warn("unable to append to dataspace")
                raise HTTPConflict()

    return append_rows


def _isSelect(params, body=None):
    """ return True if select param or select is set in request body
    """
    if "select" in params and params["select"]:
        return True

    if isinstance(body, dict):
        if "select" in body and body["select"]:
            return True
        for key in ("start", "stop", "step"):
            if key in body and body[key]:
                return True
    return False


def _getSelect(params, dset_json, body=None):
    """ return selection region if any as a list
      of slices. """
    slices = None
    log.debug(f"_getSelect  params: {params} body: {body}")
    try:
        if body and isinstance(body, dict):
            if "select" in body and body["select"]:
                select = body.get("select")
                slices = get_slices(select, dset_json)
            elif "start" in body and "stop" in body:
                slices = get_slices(body, dset_json)
        if "select" in params and params["select"]:
            select = params.get("select")
            if slices:
                msg = "select defined in both request body and query parameters"
                raise ValueError(msg)
            slices = get_slices(select, dset_json)
    except ValueError as ve:
        log.warn(f"Invalid selection: {ve}")
        raise HTTPBadRequest(reason="Invalid selection")

    if _isAppend(params, body=body) and slices:
        msg = "append can't be used with selection"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    if not slices:
        # just return the entire dataspace
        datashape = dset_json["shape"]
        dims = getShapeDims(datashape)
        slices = []
        for dim in dims:
            s = slice(0, dim, 1)
            slices.append(s)
    log.debug(f"_getSelect returning: {slices}")
    return slices


def _getSelectDtype(params, dset_dtype, body=None):
    """ if a field list is defined in params or body,
      create a sub-type of the dset dtype.  Else,
      just return the dset dtype. """
    select_fields = None
    kw = "fields"

    if isinstance(body, dict) and kw in body:
        select_fields = body[kw]
        log.debug(f"fields value in body: {select_fields}")
    elif kw in params:
        select_fields = params.get(kw)
        log.debug(f"fields param: {select_fields}")
    else:
        select_fields = None

    if select_fields:
        select_fields = select_fields.split(":")
        try:
            select_dtype = getSubType(dset_dtype, select_fields)
        except TypeError as te:
            msg = f"invalid fields selection: {te}"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
        log.debug(f"using select dtype: {select_dtype}")
    else:
        select_dtype = dset_dtype  # return all fields

    return select_dtype


def _getLimit(params, body=None):
    kw = "Limit"
    if isinstance(body, dict) and kw in body:
        limit = body[kw]
    elif kw in params:
        try:
            limit = int(params[kw])
        except ValueError:
            msg = "Limit param must be positive int"
            log.warning(msg)
            raise HTTPBadRequest(reason=msg)
    else:
        limit = 0
    return limit


def _getPoints(body, rank=1):
    """ return a set of points defined in the body
    as a numpy array.  Return None if no points are set. """
    if not body or "points" not in body:
        return None

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
    return points


def _getQuery(params, dtype, rank=1, body=None):
    """ get query parameter and validate if set """

    kw = "query"
    if isinstance(body, dict) and kw in body:
        query = body[kw]
    elif kw in params:
        query = params[kw]
    else:
        query = None

    if query:
        if _isAppend(params, body=body):
            msg = "Query string can not be used with append parameter"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
        # validate the query string
        if rank > 1:
            msg = "Query string is not supported for multidimensional datasets"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)

        if len(dtype) == 0:
            msg = "Query string is not supported for primitive type datasets"
            log.warn(msg)

        # following will throw HTTPBadRequest if query is malformed
        getParser(query, dtype)
    return query


def _getElementCount(params, body=None):
    """ get element count as query param or body key """
    kw = "element_count"

    if isinstance(body, dict) and kw in body:
        element_count = body[kw]
        if not isinstance(element_count, int):
            msg = f"expected int value for element_count, but got: {type(element_count)}"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
        log.debug(f"element_count body: {element_count}")
    elif kw in params:
        try:
            element_count = int(params[kw])
        except ValueError:
            msg = "invalid element_count"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
        log.debug(f"{kw} param: {element_count}")
    else:
        element_count = None

    return element_count


async def _getRequestData(request, http_streaming=True):
    """ get input data from request
        return dict for json input, bytes for non-streaming binary
        or None, for streaming """

    input_data = None
    request_type = getContentType(request)
    log.debug(f"_getRequestData - request_type: {request_type}")
    if request_type == "json":
        body = await request.json()
        log.debug(f"getRequestData - got json: {body}")
        if "value" in body:
            input_data = body["value"]
            log.debug("input_data: {input_data}")
        elif "value_base64" in body:
            base64_data = body["value_base64"]
            base64_data = base64_data.encode("ascii")
            input_data = base64.b64decode(base64_data)
        else:
            msg = "request has no value or value_base64 key in body"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
    else:
        # read binary data from request
        log.info(f"request content_length: {request.content_length}")

        if isinstance(request.content_length, int):
            max_request_size = int(config.get("max_request_size"))
            if request.content_length >= max_request_size:
                if http_streaming:
                    # just do an info log that we'll be paginating over a large request
                    msg = f"will paginate over large request with {request.content_length} bytes"
                    log.info(msg)
                else:
                    msg = f"Request size {request.content_length} too large "
                    msg += f"for variable length data, max: {max_request_size}"
                    log.warn(msg)
                    raise HTTPRequestEntityTooLarge(max_request_size, request.content_length)

        if not http_streaming:
            # read the request data now
            # TBD: support streaming for variable length types
            try:
                input_data = await request_read(request)
            except HTTPRequestEntityTooLarge as tle:
                msg = "Got HTTPRequestEntityTooLarge exception during "
                msg += f"binary read: {tle})"
                log.warn(msg)
                raise  # re-throw
    return input_data


async def arrayResponse(arr, request, dset_json):
    """ return the array as binary or json response based on accept type """
    response_type = getAcceptType(request)

    if response_type == "binary":
        output_data = arr.tobytes()
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
        data = arr.tolist()
        log.debug(f"got rsp data {len(data)} points")
        try:
            json_query_data = bytesArrayToList(data)
        except ValueError as err:
            msg = f"Cannot decode provided bytes to list: {err}"
            raise HTTPBadRequest(reason=msg)
        rsp_json["value"] = json_query_data
        rsp_json["hrefs"] = get_hrefs(request, dset_json)

        resp = await jsonResponse(request, rsp_json)
    return resp


async def _doPointWrite(app,
                        request,
                        points=None,
                        data=None,
                        dset_json=None,
                        bucket=None
                        ):
    """ write the given points to the dataset """

    num_points = len(points)
    log.debug(f"doPointWrite - num_points: {num_points}")
    dset_id = dset_json["id"]
    layout = getChunkLayout(dset_json)
    datashape = dset_json["shape"]
    dims = getShapeDims(datashape)
    rank = len(dims)

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
        value = data[pt_indx]
        if chunk_id not in chunk_dict:
            point_list = [point, ]
            point_data = [value, ]
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

    if crawler_status not in (200, 201):
        msg = f"doPointWritte raising HTTPInternalServerError for status: {crawler_status}"
        log.error(msg)
        raise HTTPInternalServerError()
    else:
        log.info("doPointWrite success")


async def _doHyperslabWrite(app,
                            request,
                            page_number=0,
                            page=None,
                            data=None,
                            dset_json=None,
                            select_dtype=None,
                            bucket=None
                            ):
    """ write the given page selection to the dataset """
    dset_id = dset_json["id"]
    log.info(f"_doHyperslabWrite on {dset_id} - page: {page_number}")
    type_json = dset_json["type"]
    item_size = getItemSize(type_json)

    if (select_dtype is not None):
        item_size = select_dtype.itemsize

    layout = getChunkLayout(dset_json)

    num_chunks = getNumChunks(page, layout)
    log.debug(f"num_chunks: {num_chunks}")
    max_chunks = int(config.get("max_chunks_per_request", default=1000))
    if num_chunks > max_chunks:
        msg = f"PUT value chunk count: {num_chunks} exceeds max_chunks: {max_chunks}"
        log.warn(msg)
    select_shape = getSelectionShape(page)
    log.debug(f"got select_shape: {select_shape} for page: {page_number}")
    num_bytes = math.prod(select_shape) * item_size
    if data is None:
        log.debug(f"reading {num_bytes} from request stream")
        # read page of data from input stream
        try:
            page_bytes = await request_read(request, count=num_bytes)
        except HTTPRequestEntityTooLarge as tle:
            msg = "Got HTTPRequestEntityTooLarge exception during "
            msg += f"binary read: {tle}) for page: {page_number}"
            log.warn(msg)
            raise  # re-throw
        except IncompleteReadError as ire:
            msg = "Got asyncio.IncompleteReadError during binary "
            msg += f"read: {ire} for page: {page_number}"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
        log.debug(f"read {len(page_bytes)} for page: {page_number}")
        try:
            arr = bytesToArray(page_bytes, select_dtype, select_shape)
        except ValueError as ve:
            msg = f"bytesToArray value error for page: {page_number}: {ve}"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
    else:
        arr = data  # use array provided to function

    try:
        chunk_ids = getChunkIds(dset_id, page, layout)
    except ValueError:
        log.warn("getChunkIds failed")
        raise HTTPInternalServerError()
    if len(chunk_ids) < 10:
        log.debug(f"chunk_ids: {chunk_ids}")
    else:
        log.debug(f"chunk_ids: {chunk_ids[:10]} ...")
    if len(chunk_ids) > max_chunks:
        msg = f"got {len(chunk_ids)} for page: {page_number}.  max_chunks: {max_chunks}"
        log.warn(msg)

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
        msg = f"crawler failed for page: {page_number} with status: {crawler_status}"
        log.warn(msg)
    else:
        log.info("crawler write_chunk_hyperslab successful")


async def PUT_Value(request):
    """
    Handler for PUT /<dset_uuid>/value request
    """
    app = request.app
    bucket = None
    body = None
    query = None
    params = request.rel_url.query
    append_rows = None  # this is a append update or not
    append_dim = 0
    num_elements = None
    element_count = None
    limit = None  # query limit

    arr_rsp = None  # array data to return if any

    if not request.has_body:
        msg = "PUT Value with no body"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    log.request(request)

    domain = getDomainFromRequest(request)
    if not isValidDomain(domain):
        msg = f"Invalid domain: {domain}"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    bucket = getBucketForDomain(domain)

    dset_id = request.match_info.get("id")
    if not dset_id:
        msg = "Missing dataset id"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    if not isValidUuid(dset_id, "Dataset"):
        msg = f"Invalid dataset id: {dset_id}"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    # authenticate and authorize action
    username, pswd = getUserPasswordFromRequest(request)
    await validateUserPassword(app, username, pswd)

    await validateAction(app, domain, dset_id, username, "update")

    request_type = getContentType(request)

    log.debug(f"PUT value - request_type is {request_type}")

    # get state for dataset from DN - will need this to validate
    # some of the query parameters
    dset_json = await getDsetJson(app, dset_id, bucket=bucket)

    datashape = dset_json["shape"]
    if isNullSpace(dset_json):
        msg = "Null space datasets can not be used as target for PUT value"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    dims = getShapeDims(datashape)
    rank = len(dims)

    type_json = dset_json["type"]
    dset_dtype = createDataType(type_json)
    item_size = getItemSize(type_json)

    if request_type == "json":
        try:
            body = await request.json()
        except JSONDecodeError:
            msg = "Unable to load JSON body"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)

        log.debug(f"got body: {body}")

    select_dtype = _getSelectDtype(params, dset_dtype, body=body)
    select_item_size = select_dtype.itemsize
    append_rows = _getAppendRows(params, dset_json, body=body)

    if append_rows:
        append_dim = _getAppendDim(params, body=body)
        log.debug(f"append_rows: {append_rows}, append_dim: {append_dim}")

    points = _getPoints(body, rank)
    if points is not None:
        log.debug(f"got points: {points.shape}")
        if append_rows:
            msg = "points not valid with append update"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)

    # if there's no selection parameter, this will return entire dataspace
    slices = _getSelect(params, dset_json, body=body)

    query = _getQuery(params, dset_dtype, rank=rank, body=body)

    element_count = _getElementCount(params, body=body)

    if item_size == 'H5T_VARIABLE' or element_count or not use_http_streaming(request, rank):
        http_streaming = False
    else:
        http_streaming = True

    if query:
        # divert here if we are doing a put query
        # returns array data like a GET query request
        log.debug(f"got query: {query}")
        limit = _getLimit(params, body=body)

        arr_rsp = await getSelectionData(
            app,
            dset_id,
            dset_json,
            slices=slices,
            query=query,
            bucket=bucket,
            limit=limit,
            query_update=body,
        )
        resp = await arrayResponse(arr_rsp, request, dset_json)
        log.response(request, resp=resp)
        return resp

    # regular PUT_Value processing without query update
    binary_data = None
    np_shape = []  # shape of incoming data
    bc_shape = []  # shape of broadcast array (if element_count is set)
    input_data = await _getRequestData(request, http_streaming=http_streaming)
    # could be int, list, str, bytes, or  None
    log.debug(f"got input data type: {type(input_data)}")

    if append_rows:
        # extend datashape and set slices to shape of appended region
        # make sure any other dimensions are non-zero
        for dim in range(rank):
            if dim == append_dim:
                continue
            extent = dims[dim]
            if extent == 0:
                msg = f"zero-extent datashape in dimension {dim} "
                msg += "must be extended before appending values"
                log.warn(msg)
                raise HTTPBadRequest(reason=msg)

        slices = await extendShape(app, dset_json, append_rows, axis=append_dim, bucket=bucket)
        np_shape = getSelectionShape(slices)
        log.debug(f"np_shape based on append_rows: {np_shape}")
    elif points is None:
        # The selection parameters will determine expected put value shape
        log.debug(f"PUT Value selection: {slices}")
        # not point selection, get hyperslab selection shape
        np_shape = getSelectionShape(slices)
    else:
        # point update
        np_shape = [len(points),]

    np_shape = tuple(np_shape)  # no more edits

    log.debug(f"selection shape: {np_shape}")
    if np.prod(np_shape) == 0:
        msg = "Selection is empty"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    if element_count is not None:
        # if this is set to something other than the number of
        # elements in np_shape, should be a value that can
        # be used for broadcasting
        bc_shape = getBroadcastShape(np_shape, element_count)

        if bc_shape is None:
            # this never got set, so element count must be invalid for this shape
            msg = f"element_count {element_count} not compatible with selection shape: {np_shape}"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
        # element_count will be what we expected to see
        num_elements = element_count
    else:
        # set num_elements based on selection shape
        num_elements = getNumElements(np_shape)
    log.debug(f"selection num elements: {num_elements}")

    arr = None  # np array to hold request data
    if isinstance(input_data, bytes):
        # non-streaming binary input
        if item_size == "H5T_VARIABLE":
            # binary variable length data
            try:
                arr = bytesToArray(input_data, select_dtype, [num_elements,])
            except ValueError as ve:
                log.warn(f"bytesToArray value error: {ve}")
                raise HTTPBadRequest()
        else:
            # fixed item size
            if len(input_data) % item_size != 0:
                msg = f"Expected request size to be a multiple of {item_size}, "
                msg += f"but {len(binary_data)} bytes received"
                log.warn(msg)
                raise HTTPBadRequest(reason=msg)

            if len(input_data) // item_size != num_elements:
                msg = f"expected {item_size * num_elements} bytes but got {len(binary_data)}"
                log.warn(msg)
                raise HTTPBadRequest(reason=msg)

            arr = np.fromstring(input_data, dtype=dset_dtype)
            log.debug(f"read fixed type array: {arr}")

        if bc_shape:
            # broadcast received data into numpy array
            arr = arr.reshape(bc_shape)
            if element_count == 1:
                log.debug("will send broadcast set to DN nodes")
            else:
                # need to instantiate the full np_shape since chunk boundries
                # will effect how individual chunks get set
                arr_tmp = np.zeros(np_shape, dtype=select_dtype)
                arr_tmp[...] = arr
                arr = arr_tmp

        if element_count != 1:
            try:
                arr = arr.reshape(np_shape)  # conform to selection shape
            except ValueError:
                msg = "Bad Request: binary input data doesn't match selection "
                msg += f"reshaping {arr.shape} to {np_shape}"
                log.warn(msg)
                raise HTTPBadRequest(reason=msg)

        msg = f"PUT value - numpy array shape: {arr.shape} dtype: {arr.dtype}"
        log.debug(msg)

    elif request_type == "json":
        # get array from json input
        try:
            msg = "input data doesn't match selection"
            # only enable broadcast if not appending

            if bc_shape:
                arr = jsonToArray(bc_shape, select_dtype, input_data)
            else:
                arr = jsonToArray(np_shape, select_dtype, input_data)

            if num_elements != np.prod(arr.shape):
                msg = f"expected {num_elements} elements, but got {np.prod(arr.shape)}"
                raise HTTPBadRequest(reason=msg)

            if bc_shape and element_count != 1:
                # broadcast to target
                arr_tmp = np.zeros(np_shape, dtype=select_dtype)
                arr_tmp[...] = arr
                arr = arr_tmp
        except ValueError:
            log.warn(f"ValueError: {msg}")
            raise HTTPBadRequest(reason=msg)
        except TypeError:
            log.warn(f"TypeError: {msg}")
            raise HTTPBadRequest(reason=msg)
        except IndexError:
            log.warn(f"IndexError: {msg}")
            raise HTTPBadRequest(reason=msg)
        log.debug(f"got json arr: {arr.shape}")
    else:
        log.debug("will use streaming for request data")

    slices = tuple(slices)  # no more edits to slices
    if points is None:
        # do a hyperslab write
        if arr is not None:
            # make a one page list to handle the write in one chunk crawler run
            # (larger write request should user binary streaming)
            pages = (slices,)
            log.debug(f"non-streaming data, setting page list to: {slices}")
        else:
            max_request_size = int(config.get("max_request_size"))
            pages = getSelectionPagination(slices, dims, select_item_size, max_request_size)
            log.debug(f"getSelectionPagination returned: {len(pages)} pages")

        for page_number in range(len(pages)):
            page = pages[page_number]
            msg = f"streaming request data for page: {page_number+1} of {len(pages)}, "
            msg += f"selection: {page}"
            log.info(msg)
            kwargs = {"page_number": page_number, "page": page}
            kwargs["dset_json"] = dset_json
            kwargs["bucket"] = bucket
            kwargs["select_dtype"] = select_dtype
            if arr is not None and page_number == 0:
                kwargs["data"] = arr
            else:
                kwargs["data"] = None
            # do write for one page selection
            await _doHyperslabWrite(app, request, **kwargs)
    else:
        #
        # Do point put
        #
        kwargs = {"points": points, "data": arr, "dset_json": dset_json, "bucket": bucket}
        await _doPointWrite(app, request, **kwargs)

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
    # Note - this will do a refresh if the dataset is extensible
    #   i.e. we need to make sure we have the correct shape dimensions

    dset_json = await getDsetJson(app, dset_id, bucket=bucket)
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
    slices = _getSelect(params, dset_json)

    # dtype for selection, or just dset_dtype if no fields are given
    select_dtype = _getSelectDtype(params, dset_dtype)

    log.debug(f"GET Value selection: {slices}")
    log.debug(f"dset_dtype: {dset_dtype}, select_dtype: {select_dtype}")

    limit = _getLimit(params)

    if "ignore_nan" in params and params["ignore_nan"]:
        ignore_nan = True
    else:
        ignore_nan = False
    log.debug(f"ignore nan: {ignore_nan}")

    query = _getQuery(params, dset_dtype, rank=rank)

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
        raise HTTPRequestEntityTooLarge(max_request_size, request_size)
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
            pages = getSelectionPagination(slices, dims, page_item_size, max_request_size)
            log.debug(f"getSelectionPagination returned: {len(pages)} pages")
            bytes_streamed = 0
            try:
                for page_number in range(len(pages)):
                    page = pages[page_number]
                    msg = f"streaming response data for page: {page_number+1} "
                    msg += f"of {len(pages)}, selection: {page}"
                    log.info(msg)

                    log.debug("calling getSelectionData!")

                    arr = await getSelectionData(
                        app,
                        dset_id,
                        dset_json,
                        slices=page,
                        select_dtype=select_dtype,
                        query=query,
                        bucket=bucket,
                        limit=limit
                    )

                    if arr is None or math.prod(arr.shape) == 0:
                        log.warn(f"no data returned for streaming page: {page_number}")
                        continue

                    log.debug("preparing binary response")
                    output_data = arrayToBytes(arr)
                    log.debug(f"got {len(output_data)} bytes for resp")
                    bytes_streamed += len(output_data)
                    log.debug("write request")
                    await resp.write(output_data)

                    if query and limit > 0:
                        query_rows = arr.shape[0]
                        msg = f"streaming page {page_number} returned {query_rows} rows"
                        log.debug(msg)
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
            except Exception as e:
                log.error(f"got {type(e)} exception doing getSelectionData: {e}")
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
                slices=slices,
                select_dtype=select_dtype,
                query=query,
                bucket=bucket,
                limit=limit
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
            try:
                json_data = bytesArrayToList(data)
            except ValueError as err:
                msg = f"Cannot decode bytes to list: {err}"
                raise HTTPBadRequest(reason=msg)
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
            log.debug(f"jsonResponse returned: {len(resp_body)} items")
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

    domain = getDomainFromRequest(request)
    if not isValidDomain(domain):
        msg = f"Invalid domain: {domain}"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    bucket = getBucketForDomain(domain)

    username, pswd = getUserPasswordFromRequest(request)
    if username is None and app["allow_noauth"]:
        username = "default"
    else:
        await validateUserPassword(app, username, pswd)
    await validateAction(app, domain, dset_id, username, "read")

    accept_type = getAcceptType(request)
    response_type = accept_type  # will adjust later if binary not possible

    params = request.rel_url.query
    ignore_nan = _isIgnoreNan(params)

    request_type = getContentType(request)
    log.debug(f"POST value - request_type is {request_type}")

    if not request.has_body:
        msg = "POST Value with no body"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    # get  state for dataset from DN.
    dset_json = await getDsetJson(app, dset_id, bucket=bucket)

    if isNullSpace(dset_json):
        msg = "POST value not supported for datasets with NULL shape"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    if isScalarSpace(dset_json):
        msg = "POST value not supported for datasets with SCALAR shape"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    datashape = dset_json["shape"]
    dims = getShapeDims(datashape)
    rank = len(dims)

    type_json = dset_json["type"]
    item_size = getItemSize(type_json)
    dset_dtype = createDataType(type_json)
    log.debug(f"item size: {item_size}")

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

    if _isSelect(params, body=body) and "points" in body:
        msg = "Unexpected points and select key in request body"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    slices = _getSelect(params, dset_json, body=body)
    select_dtype = _getSelectDtype(params, dset_dtype, body=body)
    log.debug(f"got select_dtype: {select_dtype}")

    if request_type == "json":
        if "points" in body:
            points_list = body["points"]
            if not isinstance(points_list, list):
                msg = "POST Value expected list of points"
                log.warn(msg)
                raise HTTPBadRequest(reason=msg)
            points = np.asarray(points_list, dtype=point_dt)
            log.debug(f"get {len(points)} points from json request")

        elif not _isSelect(params, body=body):
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

    # get the shape of the response array
    if _isSelect(params, body=body):
        # hyperslab post
        np_shape = getSelectionShape(slices)
    else:
        # point selection
        np_shape = [len(points), ]

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
        raise HTTPRequestEntityTooLarge(max_request_size, request_size)
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
        if points is None:
            kwargs["slices"] = slices
        else:
            kwargs["points"] = points
        kwargs["select_dtype"] = select_dtype
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
            try:
                json_data = bytesArrayToList(data)
            except ValueError as err:
                msg = f"Cannot decode bytes to list: {err}"
                raise HTTPBadRequest(reason=msg)
            resp_json["value"] = json_data
            resp_json["hrefs"] = get_hrefs(request, dset_json)
            resp_body = await jsonResponse(
                resp, resp_json, ignore_nan=ignore_nan, body_only=True
            )
            log.debug(f"jsonResponse returned: {len(resp_body)} items")
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
