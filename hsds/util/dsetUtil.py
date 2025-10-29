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

from aiohttp.web_exceptions import HTTPBadRequest
import math

from h5json.shape_util import getShapeDims

from .. import hsds_logger as log

 
def getShapeJson(body):
    """ Return normalized json description of data space """

    dims = None
    maxdims = None
    shape_class = None

    if "shape" not in body:
        shape_class = "H5S_SCALAR"
        log.debug("not shape given - using H5S_SCALAR")
        return {"class": shape_class}

    body_shape = body["shape"]
    log.debug(f"got shape: {body_shape}")

    if isinstance(body_shape, int):
        shape_class = "H5S_SIMPLE"
        dims = [body_shape, ]
    elif isinstance(body_shape, str):
        # only valid string value is H5S_NULL or H5S_SCALAR
        shape_class = body_shape
    elif isinstance(body_shape, (tuple, list)):
        if len(body_shape) == 0:
            shape_class = "H5S_SCALAR"
        else:
            shape_class = "H5S_SIMPLE"
            dims = body_shape
    else:
        msg = f"invalid shape: {body_shape}"
        log.warn(msg)
        raise ValueError(msg)

    if shape_class not in ("H5S_NULL", "H5S_SCALAR", "H5S_SIMPLE"):
        msg = f"invalid shape class: {shape_class}"
        log.warn(msg)
        raise ValueError(msg)

    if shape_class in ("H5S_NULL", "H5S_SCALAR") and dims:
        msg = f"dims not valid for shape class: {body_shape}"
        log.warn(msg)
        raise ValueError(msg)

    if dims is None and shape_class == "H5S_SIMPLE":
        msg = "dims not specified for H5S_SIMPLE shape"
        log.warn(msg)
        raise ValueError(msg)

    if dims is not None:
        rank = len(dims)
        for i in range(rank):
            extent = dims[i]
            if not isinstance(extent, int):
                msg = f"Invalid shape dims: {dims}"
                log.warn(msg)
                raise ValueError(msg)
            if extent < 0:
                msg = f"shape dimension is negative for dims: {dims}"
                log.warn(msg)
                raise ValueError(msg)

    if "maxdims" in body:
        maxdims = body["maxdims"]
    elif isinstance(body_shape, dict) and "maxdims" in body_shape:
        maxdims = body_shape["maxdims"]
    else:
        maxdims = None

    # validate maxdims
    if maxdims:
        if dims is None:
            msg = f"maxdims cannot be supplied for space class: {shape_class}"
            log.warn(msg)
            raise ValueError(msg)

        if isinstance(maxdims, int):
            dim1 = maxdims
            maxdims = [dim1]
        elif isinstance(maxdims, list):
            pass  # can use as is
        else:
            msg = f"Bad Request: maxdims is invalid: {maxdims}"
            log.warn(msg)
            raise ValueError(msg)
        if len(dims) != len(maxdims):
            msg = "max_dims rank doesn't match dims"
            log.warn(msg)
            raise ValueError(msg)

    # return json description of shape
    shape_json = {"class": shape_class}
    if shape_class == "H5S_SIMPLE":
        shape_json["dims"] = dims
    if maxdims:
        shape_json["maxdims"] = maxdims
    log.debug(f"returning shape_json: {shape_json}")
    return shape_json


def getHyperslabSelection(dims, start=None, stop=None, step=None):
    """
    Get slices given lists of start, stop, step values

    TBD: for step>1, adjust the slice to not extend beyond last
        data point returned
    """
    rank = len(dims)
    if start:
        if not isinstance(start, (list, tuple)):
            start = [start]
        if len(start) != rank:
            msg = "Bad Request: start array length not equal to dataset rank"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
        for dim in range(rank):
            if start[dim] < 0 or start[dim] >= dims[dim]:
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
            if stop[dim] <= start[dim] or stop[dim] > dims[dim]:
                msg = "Bad Request: stop index invalid for dim: " + str(dim)
                log.warn(msg)
                raise HTTPBadRequest(reason=msg)
    else:
        stop = []
        for dim in range(rank):
            stop.append(dims[dim])

    if step:
        if not isinstance(step, (list, tuple)):
            step = [step]
        if len(step) != rank:
            msg = "Bad Request: step array length not equal to dataset rank"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
        for dim in range(rank):
            if step[dim] <= 0 or step[dim] > dims[dim]:
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
    coordinate_extent = None
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
            shape.append(extent)
        else:
            # coordinate list
            extent = len(s)
            if coordinate_extent is None:
                coordinate_extent = extent
                shape.append(extent)
            elif coordinate_extent != extent:
                msg = "shape mismatch: indexing arrays could not be broadcast together "
                msg += f"with shapes ({coordinate_extent},) ({extent},)"
                log.warn(msg)
                raise HTTPBadRequest(reason=msg)
            else:
                pass

    return shape


def isSelectAll(slices, dims):
    """ return True if the selection covers the entire dataspace """
    if len(slices) != len(dims):
        raise ValueError("isSelectAll - dimensions don't match")
    is_all = True
    for (s, dim) in zip(slices, dims):
        if s.step is not None and s.step != 1:
            is_all = False
            break
        if s.start != 0:
            is_all = False
            break
        if s.stop != dim:
            is_all = False
            break
    return is_all


def getQueryParameter(request, query_name, body=None, default=None):
    """
    Helper function, get query parameter value from request.
    If body is provided (as a JSON object) look in JSON and if not found
    look for query param.  Return default value (or None) if not found
    """
    # as a convenience, look up different capitalizations of query name
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
            msg = f"Invalid request parameter: {query_name}"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
    if val is None:
        if default is not None:
            val = default
        else:
            msg = f"Request parameter is missing: {query_name}"
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
        start_val = [start_val, ]
    rank = len(start_val)
    if "stop" not in body:
        raise KeyError("no stop key")
    stop_val = body["stop"]
    if not isinstance(stop_val, (list, tuple)):
        stop_val = [stop_val, ]
    if len(stop_val) != rank:
        raise ValueError("start and stop values have different ranks")
    if "step" in body:
        step_val = body["step"]
        if not isinstance(step_val, (list, tuple)):
            step_val = [step_val, ]
        if len(step_val) != rank:
            msg = "step values have differnt rank from start and stop selections"
            raise ValueError(msg)
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
    log.debug(f"getSelectionList, {select} dims: {dims}")
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
            for field in fields:
                try:
                    coord = int(field)
                except ValueError:
                    raise ValueError(f"Invalid coordinate for dim {dim}")
                if coord < 0 or coord >= extent:
                    msg = f"out of range coordinate for dim {dim}, {coord} "
                    msg += f"not in range: 0-{extent - 1}"
                    raise ValueError(msg)
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
                    msg = f"Invalid selection - start value out of range for dim {dim}"
                    raise ValueError(msg)
            if len(fields[1]) == 0:
                stop = extent
            else:
                try:
                    stop = int(fields[1])
                except ValueError:
                    raise ValueError(f"Invalid selection - stop value for dim {dim}")
                if stop < 0 or stop > extent or stop <= start:
                    msg = f"Invalid selection - stop value out of range for dim {dim}"
                    raise ValueError(msg)
            if len(fields) == 3:
                # get step value
                if len(fields[2]) == 0:
                    step = 1
                else:
                    try:
                        step = int(fields[2])
                    except ValueError:
                        msg = f"Invalid selection - step value for dim {dim}"
                        raise ValueError(msg)
                    if step <= 0:
                        msg = f"Invalid selection - step value out of range for dim {dim}"
                        raise ValueError(msg)
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
                msg = f"Invalid selection - index value out of range for dim {dim}"
                raise ValueError(msg)
            s = slice(index, index + 1, 1)
            select_list.append(s)
    # end dimension loop
    log.debug(f"select_list: {select_list}")
    return tuple(select_list)


def get_slices(select, dset_json):
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

    try:
        slices = getSelectionList(select, dims)
    except ValueError:
        msg = f"Invalid selection: {select} on dims: {dims} "
        msg += f"for dataset: {dset_id}"
        log.warn(msg)
        raise
    return slices


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
        log.debug(f"dim query param: {dim_param}")
        params["dim"] = dim_param


def getChunkInitializer(dset_json):
    """ get initializer application and arguments if set """
    initializer = None
    if "creationProperties" in dset_json:
        cprops = dset_json["creationProperties"]
        log.debug(f"get creationProperties: {cprops}")
        if "initializer" in cprops:
            initializer = cprops["initializer"]
            dset_id = dset_json["id"]
            log.debug(f"returning chunk initializer: {initializer} for dset: {dset_id}")
    return initializer


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


class ItemIterator:
    """
    Class to iterator through items in a selection
    """

    def __init__(self, selection):
        self._selection = selection
        self._rank = len(selection)
        self._index = [0,] * self._rank
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

        index = [0, ] * self._rank
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
