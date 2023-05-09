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

from aiohttp.web_exceptions import HTTPBadRequest, HTTPInternalServerError
import math

from .. import hsds_logger as log

"""
Filters that are known to HSDS.
Format is:
  FILTER_CODE, FILTER_ID, Name

  H5Z_FILTER_FLETCHER32, H5Z_FILTER_SZIP, H5Z_FILTER_NBIT,
  and H5Z_FILTER_SCALEOFFSET, are not currently supported.

  Non-supported filters metadata will be stored, but are
  not (currently) used for compression/decompression.
"""

FILTER_DEFS = (
    ("H5Z_FILTER_NONE", 0, "none"),
    ("H5Z_FILTER_DEFLATE", 1, "gzip"),  # aka as "zlib" for blosc
    ("H5Z_FILTER_SHUFFLE", 2, "shuffle"),
    ("H5Z_FILTER_FLETCHER32", 3, "fletcher32"),
    ("H5Z_FILTER_SZIP", 4, "szip"),
    ("H5Z_FILTER_NBIT", 5, "nbit"),
    ("H5Z_FILTER_SCALEOFFSET", 6, "scaleoffet"),
    ("H5Z_FILTER_LZF", 32000, "lzf"),
    ("H5Z_FILTER_BLOSC", 32001, "blosclz"),
    ("H5Z_FILTER_SNAPPY", 32003, "snappy"),
    ("H5Z_FILTER_LZ4", 32004, "lz4"),
    ("H5Z_FILTER_LZ4HC", 32005, "lz4hc"),
    ("H5Z_FILTER_ZSTD", 32015, "zstd"),
)

COMPRESSION_FILTER_IDS = (
    "H5Z_FILTER_DEFLATE",
    "H5Z_FILTER_SZIP",
    "H5Z_FILTER_SCALEOFFSET",
    "H5Z_FILTER_LZF",
    "H5Z_FILTER_BLOSC",
    "H5Z_FILTER_SNAPPY",
    "H5Z_FILTER_LZ4",
    "H5Z_FILTER_LZ4HC",
    "H5Z_FILTER_ZSTD",
)

COMPRESSION_FILTER_NAMES = (
    "gzip",
    "szip",
    "lzf",
    "blosclz",
    "snappy",
    "lz4",
    "lz4hc",
    "zstd",
)

CHUNK_LAYOUT_CLASSES = (
    "H5D_CHUNKED",
    "H5D_CHUNKED_REF",
    "H5D_CHUNKED_REF_INDIRECT",
    "H5D_CONTIGUOUS_REF",
)


def getFilterItem(key):
    """
    Return filter code, id, and name, based on an id, a name or a code.
    """
    if key == "deflate":
        key = "gzip"  # use gzip as equivalent
    for item in FILTER_DEFS:
        for i in range(3):
            if key == item[i]:
                return {"class": item[0], "id": item[1], "name": item[2]}
    return None  # not found


def getFilters(dset_json):
    """Return list of filters, or empty list"""
    if "creationProperties" not in dset_json:
        return []
    creationProperties = dset_json["creationProperties"]
    if "filters" not in creationProperties:
        return []
    filters = creationProperties["filters"]
    return filters


def getCompressionFilter(dset_json):
    """Return compression filter from filters, or None"""
    filters = getFilters(dset_json)
    for filter in filters:
        if "class" not in filter:
            msg = f"filter option: {filter} with no class key"
            log.warn(msg)
            continue
        filter_class = filter["class"]
        if filter_class in COMPRESSION_FILTER_IDS:
            return filter
        if all(
            (
                filter_class == "H5Z_FILTER_USER",
                "name" in filter,
                filter["name"] in COMPRESSION_FILTER_NAMES,
            )
        ):
            return filter
    return None


def getShuffleFilter(dset_json):
    """Return shuffle filter, or None"""
    filters = getFilters(dset_json)
    for filter in filters:
        try:
            if filter["class"] == "H5Z_FILTER_SHUFFLE":
                log.debug(f"Shuffle filter is used: {filter}")
                return filter
        except KeyError:
            log.warn(f"filter option: {filter} with no class key")
            continue
    log.debug("Shuffle filter not used")
    return None


def getFilterOps(app, dset_json, item_size):
    """Get list of filter operations to be used for this dataset"""
    filter_map = app["filter_map"]
    dset_id = dset_json["id"]
    if dset_id in filter_map:
        log.debug(f"returning filter from filter_map {filter_map[dset_id]}")
        return filter_map[dset_id]

    compressionFilter = getCompressionFilter(dset_json)
    log.debug(f"got compressionFilter: {compressionFilter}")

    filter_ops = {}

    shuffleFilter = getShuffleFilter(dset_json)
    if shuffleFilter:
        filter_ops["use_shuffle"] = True

    if compressionFilter:
        if compressionFilter["class"] == "H5Z_FILTER_DEFLATE":
            filter_ops["compressor"] = "zlib"  # blosc compressor
            if shuffleFilter:
                filter_ops["use_shuffle"] = True
            else:
                # for HDF5-style compression, use shuffle only if it turned on
                filter_ops["use_shuffle"] = False
        else:
            if "name" in compressionFilter:
                filter_ops["compressor"] = compressionFilter["name"]
            else:
                filter_ops["compressor"] = "lz4"  # default to lz4
        if "level" not in compressionFilter:
            filter_ops["level"] = 5  # medium level
        else:
            filter_ops["level"] = int(compressionFilter["level"])

    if filter_ops:
        filter_ops["item_size"] = item_size
        if item_size == "H5T_VARIABLE":
            filter_ops["use_shuffle"] = False
        log.debug(f"save filter ops: {filter_ops} for {dset_id}")
        filter_map[dset_id] = filter_ops  # save
        return filter_ops
    else:
        return None


def getDsetRank(dset_json):
    """Get rank returning 0 for sclar or NULL datashapes"""
    datashape = dset_json["shape"]
    if datashape["class"] == "H5S_NULL":
        return 0
    if datashape["class"] == "H5S_SCALAR":
        return 0
    if "dims" not in datashape:
        log.warn(f"expected to find dims key in shape_json: {datashape}")
        return 0
    dims = datashape["dims"]
    rank = len(dims)
    return rank


def isNullSpace(dset_json):
    """Return true if this dataset is a null dataspace"""
    datashape = dset_json["shape"]
    if datashape["class"] == "H5S_NULL":
        return True
    else:
        return False


def getHyperslabSelection(dsetshape, start=None, stop=None, step=None):
    """
    Get slices given lists of start, stop, step values

    TBD: for step>1, adjust the slice to not extend beyond last
        data point returned
    """
    rank = len(dsetshape)
    if start:
        if not isinstance(start, (list, tuple)):
            start = [start]
        if len(start) != rank:
            msg = "Bad Request: start array length not equal to dataset rank"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
        for dim in range(rank):
            if start[dim] < 0 or start[dim] >= dsetshape[dim]:
                msg = "Bad Request: start index invalid for dim: " + str(dim)
                log.warn(msg)
                raise HTTPBadRequest(reason=msg)
    else:
        start = []
        for dim in range(rank):
            start.append(0)

    if stop:
        if not isinstance(stop, (list, tuple)):
            stop = [stop]
        if len(stop) != rank:
            msg = "Bad Request: stop array length not equal to dataset rank"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
        for dim in range(rank):
            if stop[dim] <= start[dim] or stop[dim] > dsetshape[dim]:
                msg = "Bad Request: stop index invalid for dim: " + str(dim)
                log.warn(msg)
                raise HTTPBadRequest(reason=msg)
    else:
        stop = []
        for dim in range(rank):
            stop.append(dsetshape[dim])

    if step:
        if not isinstance(step, (list, tuple)):
            step = [step]
        if len(step) != rank:
            msg = "Bad Request: step array length not equal to dataset rank"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
        for dim in range(rank):
            if step[dim] <= 0 or step[dim] > dsetshape[dim]:
                msg = "Bad Request: step index invalid for dim: " + str(dim)
                log.warn(msg)
                raise HTTPBadRequest(reason=msg)
    else:
        step = []
        for dim in range(rank):
            step.append(1)

    slices = []

    for dim in range(rank):

        try:
            s = slice(int(start[dim]), int(stop[dim]), int(step[dim]))
        except ValueError:
            msg = "Bad Request: invalid start/stop/step value"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
        slices.append(s)
    return tuple(slices)


def getSelectionShape(selection):
    """Return the shape of the given selection.
    Examples (selection -> returned shape):
    [(3,7,1)] -> [4]
    [(3, 7, 3)] -> [1]
    [(44, 52, 1), (48,52,1)] -> [8, 4]
    [[1,2,7]] ->
    """
    shape = []
    rank = len(selection)
    for i in range(rank):
        s = selection[i]
        if isinstance(s, slice):
            extent = 0
            if s.step and s.step > 1:
                step = s.step
            else:
                step = 1
            if s.stop > s.start:
                extent = s.stop - s.start
            if step > 1 and extent > 0:
                extent = extent // step
            if (s.stop - s.start) % step != 0:
                extent += 1
        else:
            # coordinate list
            extent = len(s)
        shape.append(extent)
    return shape


def getQueryParameter(request, query_name, body=None, default=None):
    """
    Herlper function, get query parameter value from request.
    If body is provided (as a JSON object) look in JSON and if not found
    look for query param.  Return default value (or None) if not found
    """
    # as a convience, look up different capitilizations of query name
    params = request.rel_url.query
    query_names = []
    query_names.append(query_name.lower())
    query_names.append(query_name.upper())
    query_names.append(query_name[0].upper() + query_name[1:].lower())
    if query_name not in query_names:
        query_names.insert(0, query_name)
    val = None
    if body is not None:
        for query_name in query_names:
            if query_name in body:
                val = body[query_name]
                break

    if val is None:
        # look for a query param
        for query_name in query_names:
            if query_name in params:
                val = params[query_name]

    if val and default is not None and isinstance(default, int):
        # convert to int Type
        try:
            val = int(val)
        except ValueError:
            msg = "Invalid request parameter: {}".format(query_name)
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
    if val is None:
        if default is not None:
            val = default
        else:
            msg = "Request parameter is missing: {}".format(query_name)
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
    return val


def _getSelectionStringFromRequestBody(body):
    """Join start, stop, and (optionally) stop keys
    to create an equivalent selection string"""
    if "start" not in body:
        raise KeyError("no start key")
    start_val = body["start"]
    if not isinstance(start_val, (list, tuple)):
        start_val = [
            start_val,
        ]
    rank = len(start_val)
    if "stop" not in body:
        raise KeyError("no stop key")
    stop_val = body["stop"]
    if not isinstance(stop_val, (list, tuple)):
        stop_val = [
            stop_val,
        ]
    if len(stop_val) != rank:
        raise ValueError("start and stop values have different ranks")
    if "step" in body:
        step_val = body["step"]
        if not isinstance(step_val, (list, tuple)):
            step_val = [
                step_val,
            ]
        if len(step_val) != rank:
            raise ValueError(
                "step values have differnt rank from start and stop selections"
            )
    else:
        step_val = None
    selection = []
    selection.append("[")
    for i in range(rank):
        dim_sel = f"{start_val[i]}:{stop_val[i]}"
        if step_val:
            dim_sel += f":{step_val[i]}"
        selection.append(dim_sel)
        if i + 1 < rank:
            selection.append(",")
    selection.append("]")
    return "".join(selection)


def _getSelectElements(select):
    """helper method - return array of queries for each
    dimension"""
    if isinstance(select, list) or isinstance(select, tuple):
        return select  # already listified
    select = select[1:-1]  # strip brackets
    query_array = []
    dim_query = []
    coord_list = False
    for ch in select:
        if ch.isspace():
            # ignore
            pass
        elif ch == ",":
            if coord_list:
                dim_query.append(ch)
            else:
                if len(dim_query) == 0:
                    # empty dimension
                    raise ValueError("invalid query")
                query_array.append("".join(dim_query))
                dim_query = []  # reset
        elif ch == "[":
            if coord_list:
                # can't have nested coordinates
                raise ValueError("invalid query")
            coord_list = True
            dim_query.append(ch)
        elif ch == "]":
            if not coord_list:
                # close bracket with no open
                raise ValueError("invalid query")
            dim_query.append(ch)
            coord_list = False
        elif ch == ":":
            if coord_list:
                # range not allowed in coord list
                raise ValueError("invalid query")
            dim_query.append(ch)
        else:
            dim_query.append(ch)
    if not dim_query:
        # empty dimension
        raise ValueError("invalid query")
    query_array.append("".join(dim_query))

    return query_array


def getSelectionList(select, dims):
    """Return tuple of slices and/or coordinate list for the given selection"""
    select_list = []

    if isinstance(select, dict):
        select = _getSelectionStringFromRequestBody(select)

    if select is None or len(select) == 0:
        """Return set of slices covering data space"""
        slices = []
        for extent in dims:
            s = slice(0, extent, 1)
            slices.append(s)
        return tuple(slices)

    # convert selection to list by dimension
    elements = _getSelectElements(select)
    rank = len(elements)
    if len(dims) != rank:
        raise ValueError("invalid rank for selection")
    for dim in range(rank):
        extent = dims[dim]
        element = elements[dim]
        is_list = isinstance(element, list)
        is_str = isinstance(element, str)
        if is_list or (is_str and element.startswith("[")):
            # list of coordinates
            if is_str:
                fields = element[1:-1].split(",")
            else:
                fields = element
            coords = []
            last_coord = None
            for field in fields:
                try:
                    coord = int(field)
                except ValueError:
                    raise ValueError(f"Invalid coordinate for dim {dim}")
                if coord < 0 or coord >= extent:
                    msg = f"out of range coordinate for dim {dim}, {coord} "
                    msg += f"not in range: 0-{extent-1}"
                    raise ValueError(msg)
                if last_coord is not None and coord <= last_coord:
                    raise ValueError("coordinates must be increasing")
                last_coord = coord
                coords.append(coord)
            select_list.append(coords)
        elif element == ":":
            s = slice(0, extent, 1)
            select_list.append(s)
        elif is_str and element.find(":") >= 0:
            fields = element.split(":")
            if len(fields) not in (2, 3):
                raise ValueError(f"Invalid selection format for dim {dim}")
            if len(fields[0]) == 0:
                start = 0
            else:
                try:
                    start = int(fields[0])
                except ValueError:
                    raise ValueError(f"Invalid selection - start value for dim {dim}")
                if start < 0 or start >= extent:
                    raise ValueError(
                        f"Invalid selection - start value out of range for dim {dim}"
                    )
            if len(fields[1]) == 0:
                stop = extent
            else:
                try:
                    stop = int(fields[1])
                except ValueError:
                    raise ValueError(f"Invalid selection - stop value for dim {dim}")
                if stop < 0 or stop > extent or stop <= start:
                    raise ValueError(
                        f"Invalid selection - stop value out of range for dim {dim}"
                    )
            if len(fields) == 3:
                # get step value
                if len(fields[2]) == 0:
                    step = 1
                else:
                    try:
                        step = int(fields[2])
                    except ValueError:
                        raise ValueError(
                            f"Invalid selection - step value for dim {dim}"
                        )
                    if step <= 0:
                        raise ValueError(
                            f"Invalid selection - step value out of range for dim {dim}"
                        )
            else:
                step = 1
            s = slice(start, stop, step)
            select_list.append(s)
        else:
            # expect single coordinate value
            try:
                index = int(element)
            except ValueError:
                raise ValueError(f"Invalid selection - index value for dim {dim}")
            if index < 0 or index >= extent:
                raise ValueError(
                    f"Invalid selection - index value out of range for dim {dim}"
                )

            s = slice(index, index + 1, 1)
            select_list.append(s)
    # end dimension loop
    return tuple(select_list)


def getSelectionPagination(select, dims, itemsize, max_request_size):
    """
    Paginate a select tupe into multiple selects where each
        select requires less than max_request_size bytes"""
    msg = f"getSelectionPagination - select: {select}, dims: {dims}, "
    msg += f"itemsize: {itemsize}, max_request_size: {max_request_size}"
    log.debug(msg)
    select_shape = getSelectionShape(select)
    log.debug(f"getSelectionPagination - select_shape: {select_shape}")
    select_size = math.prod(select_shape) * itemsize
    log.debug(f"getSelectionPagination - select_size: {select_size}")
    if select_size <= max_request_size:
        # No need to paginate, just return select as as a one item tuple
        log.debug("getSelectionPagination - not needed")
        return (select,)

    # get pagination dimension - first dimension with > 1 extent
    rank = len(dims)
    paginate_dim = None
    paginate_extent = None
    for i in range(rank):
        s = select[i]
        if isinstance(s, slice):
            paginate_extent = 0
            if s.step and s.stop > 1:
                step = s.step
            else:
                step = 1
            if s.stop > s.start:
                paginate_extent = s.stop - s.start
        else:
            # coordinate list
            paginate_extent = len(s)
        if paginate_extent > 1:
            paginate_dim = i
            break
    if paginate_dim is None:
        msg = "unable to determine pagination dimension"
        log.warn(msg)
        raise ValueError(msg)
    log.debug(f"getSelectionPagination - using pagination dimension: {paginate_dim}")

    # get the approx bytes per page by doing fractional dev with ceil
    page_count = select_size // max_request_size
    page_count += 1  # round up by one
    log.debug(f"getSelectionPagination - page_count: {page_count}")
    page_size = select_size // page_count
    log.debug(f"getSelectionPagination - page_size: {page_size}")

    s = select[paginate_dim]
    # log.debug(f"pagination dim: {paginate_dim} select: {s} paginate_extent: {paginate_extent}")
    # page_extent = -(-max_request_size // page_size)
    # log.debug(f"getSelectionPagination - page_extent: {page_extent}")
    # page_count = -(-paginate_extent // page_extent)
    if paginate_extent < page_count:
        msg = f"select pagination unable to paginate select dim: {paginate_dim} "
        msg += f"into {page_count} pages"
        log.warn(msg)
        raise ValueError(msg)
    page_extent = paginate_extent // page_count
    if page_extent < 1:
        page_extent = 1
    log.debug(f"getSelectionPagination - page_extent: {page_extent}")
    paginate_slices = []
    if isinstance(s, slice):
        start = s.start
        if s.step and s.stop > 1:
            step = s.step
        else:
            step = 1

        while start < s.stop:
            stop = start + page_extent
            if stop % step != 0:
                # adjust to fall on step boundry
                log.debug(f"pre-step adjust stop: {stop}")
                stop += step - (stop % step)
                log.debug(f"post-step adjust stop: {stop}")
            if stop > s.stop:
                stop = s.stop
            paginate_slices.append(slice(start, stop, step))
            start = stop
    else:
        # coordinate list
        start = 0  # first index
        while start < len(s):
            stop = start + page_extent
            if stop > len(s):
                stop = len(s)
            log.debug(f"page_coord s[{start}:{stop}]")
            page_coord = s[start:stop]
            log.debug(f"page coords: {page_coord}")
            paginate_slices.append(tuple(page_coord))
            start = stop
    # adjust page count to number to actual pagination
    page_count = len(paginate_slices)

    log.debug(f"got {page_count} paginate_slices")
    # return paginated selection list using paginate_slices for pagination
    # dimension, original selection for each other dimension
    pagination = []
    for page in range(page_count):
        s = []
        for i in range(rank):
            if i == paginate_dim:
                s.append(paginate_slices[page])
            else:
                s.append(select[i])
        pagination.append(tuple(s))
    pagination = tuple(pagination)
    # log.debug(f"returning pagination: {pagination}")
    return pagination


def getSliceQueryParam(sel):
    """
    Helper method - set query parameter for given shape + selection

    Query arg should be in the form: [<dim1>, <dim2>, ... , <dimn>]
        brackets are optional for one dimensional arrays.
         Each dimension, valid formats are:
            single integer: n
            start and end: n:m
            start, end, and stride: n:m:s
    """
    # pass dimensions, and selection as query params
    rank = len(sel)
    if rank > 0:
        sel_param = "["
        for i in range(rank):
            s = sel[i]
            if isinstance(s, slice):
                sel_param += str(s.start)
                sel_param += ":"
                sel_param += str(s.stop)
                if s.step > 1:
                    sel_param += ":"
                    sel_param += str(s.step)
            else:
                # coord selection
                sel_param += "["
                count = len(s)
                for j in range(count):
                    sel_param += str(s[j])
                    if j < count - 1:
                        sel_param += ","
                sel_param += "]"
            if i < rank - 1:
                sel_param += ","
        sel_param += "]"
        # log.debug(f"select query param: {sel_param}")
        if len(sel_param) > 500:
            log.warning(f"select param is {len(sel_param)} characters long")
        return sel_param


def setChunkDimQueryParam(params, dims):
    """
    Helper method - set chunk dim param
    Send the chunk dimensions as a query param
    Query arg should be in the form: [<dim1>, <dim2>, ... , <dimn>]
        brackets are optional for one dimensional arrays.
         Each dimension, valid formats are:
            single integer: n
            start and end: n:m
            start, end, and stride: n:m:s
    """
    # pass dimensions, and selection as query params
    rank = len(dims)
    if rank > 0:
        dim_param = "["
        for i in range(rank):
            extent = dims[i]
            dim_param += str(extent)
        dim_param += "]"
        log.debug("dim query param: {}".format(dim_param))
        params["dim"] = dim_param


def getDsetMaxDims(dset_json):
    """
    Get maxdims from a given shape.  Return [1,] for Scalar datasets

    Use with H5S_NULL datasets will throw a 400 error.
    """
    if "shape" not in dset_json:
        log.error("No shape found in dset_json")
        raise HTTPInternalServerError()
    shape_json = dset_json["shape"]
    maxdims = None
    if shape_json["class"] == "H5S_NULL":
        msg = "Expected shape class other than H5S_NULL"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    elif shape_json["class"] == "H5S_SCALAR":
        maxdims = [
            1,
        ]
    elif shape_json["class"] == "H5S_SIMPLE":
        if "maxdims" in shape_json:
            maxdims = shape_json["maxdims"]
    else:
        log.error("Unexpected shape class: {}".format(shape_json["class"]))
        raise HTTPInternalServerError()
    return maxdims


def getChunkLayout(dset_json):
    """Get chunk layout.  Throw 500 if used with non-H5D_CHUNKED layout"""
    if "layout" not in dset_json:
        log.error("No layout found in dset_json")
        raise HTTPInternalServerError()
    layout_json = dset_json["layout"]
    if layout_json["class"] not in CHUNK_LAYOUT_CLASSES:
        log.error("Unexpected shape layout: {}".format(layout_json["class"]))
        raise HTTPInternalServerError()
    layout = layout_json["dims"]
    return layout


def getPreviewQuery(dims):
    """
    Helper method - return query options for a "reasonable" size
        data preview selection. Return None if the dataset is small
        enough that a preview is not needed.
    """
    select = "select=["
    rank = len(dims)

    ncols = dims[rank - 1]
    if rank > 1:
        nrows = dims[rank - 2]
    else:
        nrows = 1

    # use some rough heuristics to define the selection
    # aim to return no more than 100 elements
    if ncols > 100:
        ncols = 100
    if nrows > 100:
        nrows = 100
    if nrows * ncols > 100:
        if nrows > ncols:
            nrows = 100 // ncols
        else:
            ncols = 100 // nrows

    for i in range(rank):
        if i == rank - 1:
            select += "0:" + str(ncols)
        elif i == rank - 2:
            select += "0:" + str(nrows) + ","
        else:
            select += "0:1,"
    select += "]"
    return select


def getFillValue(dset_json):
    """
    Return fill value if defined, otherwise return None
    """
    fill_value = None
    if "creationProperties" in dset_json:
        cprops = dset_json["creationProperties"]
        if "fillValue" in cprops:
            fill_value = cprops["fillValue"]
            if isinstance(fill_value, list):
                fill_value = tuple(fill_value)
    return fill_value


def isExtensible(dims, maxdims):
    """
    Determine if the dataset can be extended
    """
    if maxdims is None or len(dims) == 0:
        return False
    log.debug(f"isExtensible - dims: {dims} maxdims: {maxdims}")
    rank = len(dims)
    if len(maxdims) != rank:
        raise ValueError("rank of maxdims does not match dataset")
    for n in range(rank):
        # TBD - shouldn't have H5S_UNLIMITED in any new files.
        # Remove check once this is confirmed
        if maxdims[n] in (0, "H5S_UNLIMITED") or maxdims[n] > dims[n]:
            return True
    return False


def getDatasetCreationPropertyLayout(dset_json):
    """ return layout json from creation property list """
    cpl = None
    if "creationProperties" in dset_json:
        cp = dset_json["creationProperties"]
        if "layout" in cp:
            cpl = cp["layout"]
    if not cpl and "layout" in dset_json:
        # fallback to dset_json layout
        cpl = dset_json["layout"]
    if cpl is None:
        log.warn(f"no layout found for {dset_json}")
    return cpl


def getDatasetLayoutClass(dset_json):
    """ return layout class """
    chunk_layout = None
    cp_layout = getDatasetCreationPropertyLayout(dset_json)
    # check creation properties first
    if cp_layout:
        if "class" in cp_layout:
            chunk_layout = cp_layout["class"]
    # otherwise, get class prop from layout
    if chunk_layout is None and "layout" in dset_json:
        layout = dset_json["layout"]
        if "class" in layout:
            chunk_layout = layout["class"]
    return chunk_layout


class ItemIterator:
    """
    Class to iterator through items in a selection
    """

    def __init__(self, selection):
        self._selection = selection
        self._rank = len(selection)
        self._index = [
            0,
        ] * self._rank
        for i in range(self._rank):
            s = self._selection[i]
            self._index[i] = s.start

    def __iter__(self):
        return self

    def next(self):
        if self._index[0] >= self._selection[0].stop:
            # ran past last item, end iteration
            raise StopIteration()
        dim = self._rank - 1

        index = [
            0,
        ] * self._rank
        for i in range(self._rank):
            index[i] = self._index[i]
        while dim >= 0:
            s = self._selection[dim]
            self._index[dim] += s.step
            if self._index[dim] < s.stop:
                if self._rank == 1:
                    index = index[0]
                return index
            if dim > 0:
                self._index[dim] = s.start
            dim -= 1
        if self._rank == 1:
            index = index[0]  # return int, not list
        return index
