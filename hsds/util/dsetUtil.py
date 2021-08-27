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
from numcodecs import blosc

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
    ('H5Z_FILTER_NONE', 0, "none"),
    ('H5Z_FILTER_DEFLATE', 1, "gzip"),  # aka as "zlib" for blosc
    ('H5Z_FILTER_SHUFFLE', 2, "shuffle"),
    ('H5Z_FILTER_FLETCHER32', 3, "fletcher32"),
    ('H5Z_FILTER_SZIP', 4, "szip"),
    ('H5Z_FILTER_NBIT', 5, "nbit"),
    ('H5Z_FILTER_SCALEOFFSET', 6, "scaleoffet"),
    ('H5Z_FILTER_LZF', 32000, "lzf"),
    ('H5Z_FILTER_BLOSC', 32001, "blosclz"),
    ('H5Z_FILTER_SNAPPY', 32003, "snappy"),
    ('H5Z_FILTER_LZ4', 32004, "lz4"),
    ('H5Z_FILTER_LZ4HC', 32005, "lz4hc"),
    ('H5Z_FILTER_ZSTD', 32015, "zstd")
)

COMPRESSION_FILTER_IDS = (
    'H5Z_FILTER_DEFLATE',
    'H5Z_FILTER_SZIP',
    'H5Z_FILTER_SCALEOFFSET',
    'H5Z_FILTER_LZF',
    'H5Z_FILTER_BLOSC',
    'H5Z_FILTER_SNAPPY',
    'H5Z_FILTER_LZ4',
    'H5Z_FILTER_LZ4HC',
    'H5Z_FILTER_ZSTD'
)

COMPRESSION_FILTER_NAMES = (
    'gzip',
    'szip',
    'lzf',
    'blosclz',
    'snappy',
    'lz4',
    'lz4hc',
    'zstd'
)

CHUNK_LAYOUT_CLASSES = (
    'H5D_CHUNKED',
    'H5D_CHUNKED_REF',
    'H5D_CHUNKED_REF_INDIRECT',
    'H5D_CONTIGUOUS_REF'
)


def getFilterItem(key):
    """
    Return filter code, id, and name, based on an id, a name or a code.
    """
    for item in FILTER_DEFS:
        for i in range(3):
            if key == item[i]:
                return {"class": item[0], "id": item[1], "name": item[2]}
    return None  # not found


def getFilters(dset_json):
    """ Return list of filters, or empty list """
    if "creationProperties" not in dset_json:
        return []
    creationProperties = dset_json["creationProperties"]
    if "filters" not in creationProperties:
        return []
    filters = creationProperties["filters"]
    return filters


def getCompressionFilter(dset_json):
    """ Return compression filter from filters, or None """
    filters = getFilters(dset_json)
    for filter in filters:
        if 'class' not in filter:
            msg = f"filter option: {filter} with no class key"
            log.warn(msg)
            continue
        filter_class = filter["class"]
        if filter_class in COMPRESSION_FILTER_IDS:
            return filter
        if filter_class == 'H5Z_FILTER_USER' and 'name' in filter and \
                filter['name'] in COMPRESSION_FILTER_NAMES:
            return filter
    return None


def getShuffleFilter(dset_json):
    """ Return shuffle filter, or None """
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
    """ Get the Deflate compression value """
    filter_map = app['filter_map']
    dset_id = dset_json['id']
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
        if compressionFilter["class"] == 'H5Z_FILTER_DEFLATE':
            filter_ops["compressor"] = 'zlib'  # blosc compressor
            if shuffleFilter:
                filter_ops["use_shuffle"] = True
            else:
                # for HDF5-style compression, use shuffle only if it turned on
                filter_ops['use_shuffle'] = False
        else:
            if "name" in compressionFilter and \
                    compressionFilter["name"] in blosc.list_compressors():
                filter_ops["compressor"] = compressionFilter["name"]
            else:
                filter_ops["compressor"] = 'lz4'  # default to lz4
        if "level" not in compressionFilter:
            filter_ops['level'] = 5  # medium level
        else:
            filter_ops['level'] = int(compressionFilter["level"])

    if filter_ops:
        filter_ops['item_size'] = item_size
        if item_size == 'H5T_VARIABLE':
            filter_ops['use_shuffle'] = False
        log.debug(f"save filter ops: {filter_ops} for {dset_id}")
        filter_map[dset_id] = filter_ops  # save
        return filter_ops
    else:
        return None


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
    """ Return the shape of the given selection.
      Examples (selection -> returned shape):
      [(3,7,1)] -> [4]
      [(3, 7, 3)] -> [1]
      [(44, 52, 1), (48,52,1)] -> [8, 4]
    """
    shape = []
    rank = len(selection)
    for i in range(rank):
        s = selection[i]
        extent = 0
        if s.stop > s.start:
            extent = s.stop - s.start
        if s.step > 1 and extent > 0:
            extent = (extent // s.step)
        if (s.stop - s.start) % s.step != 0:
            extent += 1
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


def getSliceQueryParam(request, dim, extent, body=None):
    """
    Helper method - return slice for dim based on query params

    Query arg should be in the form: [<dim1>, <dim2>, ... , <dimn>]
        brackets are optional for one dimensional arrays.
         Each dimension, valid formats are:
            single integer: n
            start and end: n:m
            start, end, and stride: n:m:s
    """
    # Get optional query parameters for given dim
    log.debug("getSliceQueryParam: " + str(dim) + ", " + str(extent))
    params = request.rel_url.query

    start = 0
    stop = extent
    step = 1

    if body and "start" in body:
        # look for start params in body JSON
        start_val = body["start"]
        if isinstance(start_val, (list, tuple)):
            if len(start_val) < dim:
                msg = "Not enough dimensions supplied to body start key"
                log.arn(msg)
                raise HTTPBadRequest(reason=msg)
            start = start_val[dim]
        else:
            start = start_val

    if body and "stop" in body:
        stop_val = body["stop"]
        if isinstance(stop_val, (list, tuple)):
            if len(stop_val) < dim:
                msg = "Not enough dimensions supplied to body stop key"
                log.arn(msg)
                raise HTTPBadRequest(reason=msg)
            stop = stop_val[dim]
        else:
            stop = stop_val
    if body and "step" in body:
        step_val = body["step"]
        if isinstance(step_val, (list, tuple)):
            if len(step_val) < dim:
                msg = "Not enough dimensions supplied to body step key"
                log.arn(msg)
                raise HTTPBadRequest(reason=msg)
            step = step_val[dim]
        else:
            step = step_val

    if "select" in params:
        query = params["select"]
        log.debug("select query value:" + query)

        if not query.startswith('['):
            msg = "Bad Request: selection query missing start bracket"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
        if not query.endswith(']'):
            msg = "Bad Request: selection query missing end bracket"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)

        # now strip out brackets
        query = query[1:-1]

        query_array = query.split(',')
        if dim >= len(query_array):
            msg = "Not enough dimensions supplied to query argument"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
        dim_query = query_array[dim].strip()

        if dim_query.find(':') < 0:
            # just a number - return stop = start + 1 for this value
            try:
                start = int(dim_query)
            except ValueError:
                msg = "Bad Request: invalid selection parameter "
                msg += f"(can't convert to int) for dimension: {dim}"
                log.warn(msg)
                raise HTTPBadRequest(reason=msg)
            stop = start + 1
        elif dim_query == ':':
            # select everything
            pass
        else:
            fields = dim_query.split(":")
            log.debug("got fields: {}".format(fields))
            if len(fields) > 3:
                msg = "Bad Request: Too many ':' seperators for "
                msg += f"dimension: {dim}"
                log.warn(msg)
                raise HTTPBadRequest(reason=msg)
            try:
                if fields[0]:
                    start = int(fields[0])
                if fields[1]:
                    stop = int(fields[1])
                if len(fields) > 2 and fields[2]:
                    step = int(fields[2])
            except ValueError:
                msg = "Bad Request: invalid selection parameter "
                msg += f"(can't convert to int) for dimension: {dim}"
                log.info(msg)
                raise HTTPBadRequest(reason=msg)
    log.debug("start: {}, stop: {}, step: {}".format(start, stop, step))
    # now, validate whaterver start/stop/step values we got
    if start < 0 or start > extent:
        msg = "Bad Request: Invalid selection start parameter "
        msg += f"for dimension: {dim}"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    if stop > extent:
        msg = "Bad Request: Invalid selection stop parameter for "
        msg += f"dimension: {dim}"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    if step <= 0:
        msg = "Bad Request: invalid selection step parameter for "
        msg += f"dimension: {dim}"
        log.debug(msg)
        raise HTTPBadRequest(reason=msg)
    s = slice(start, stop, step)
    msg = f"dim query[{dim}] returning: start:{start} stop:{stop} step:{step}"
    log.debug(msg)
    return s


def setSliceQueryParam(params, sel):
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
            sel_param += str(s.start)
            sel_param += ':'
            sel_param += str(s.stop)
            if s.step > 1:
                sel_param += ':'
                sel_param += str(s.step)
            if i < rank - 1:
                sel_param += ','
        sel_param += ']'
        log.debug("select query param: {}".format(sel_param))
        params["select"] = sel_param


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
        dim_param += ']'
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
    if shape_json['class'] == 'H5S_NULL':
        msg = "Expected shape class other than H5S_NULL"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)
    elif shape_json['class'] == 'H5S_SCALAR':
        maxdims = [1, ]
    elif shape_json['class'] == 'H5S_SIMPLE':
        if "maxdims" in shape_json:
            maxdims = shape_json["maxdims"]
    else:
        log.error("Unexpected shape class: {}".format(shape_json['class']))
        raise HTTPInternalServerError()
    return maxdims


def getChunkLayout(dset_json):
    """ Get chunk layout.  Throw 500 if used with non-H5D_CHUNKED layout
    """
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

    ncols = dims[rank-1]
    if rank > 1:
        nrows = dims[rank-2]
    else:
        nrows = 1

    # use some rough heuristics to define the selection
    # aim to return no more than 100 elements
    if ncols > 100:
        ncols = 100
    if nrows > 100:
        nrows = 100
    if nrows*ncols > 100:
        if nrows > ncols:
            nrows = 100 // ncols
        else:
            ncols = 100 // nrows

    for i in range(rank):
        if i == rank-1:
            select += "0:" + str(ncols)
        elif i == rank-2:
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
    log.debug("isExtensible - dims: {} maxdims: {}".format(dims, maxdims))
    rank = len(dims)
    if len(maxdims) != rank:
        raise ValueError("rank of maxdims does not match dataset")
    for n in range(rank):
        # TBD - shouldn't have H5S_UNLIMITED in any new files.
        # Remove check once this is confirmed
        if maxdims[n] in (0, 'H5S_UNLIMITED') or maxdims[n] > dims[n]:
            return True
    return False


class ItemIterator:
    """
    Class to iterator through items in a selection
    """

    def __init__(self, selection):
        self._selection = selection
        self._rank = len(selection)
        self._index = [0, ] * self._rank
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
