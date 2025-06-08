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

from h5json.hdf5dtype import getItemSize, isVlen
from h5json.objid import isValidUuid

from .. import hsds_logger as log
from .. import config


CHUNK_MIN = 512 * 1024  # Soft lower limit (512k)
CHUNK_MAX = 2048 * 1024  # Hard upper limit (2M)
DEFAULT_TYPE_SIZE = 128  # Type size case when it is variable

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
    ("H5Z_FILTER_SCALEOFFSET", 6, "scaleoffset"),
    ("H5Z_FILTER_LZF", 32000, "lzf"),
    ("H5Z_FILTER_BLOSC", 32001, "blosclz"),
    ("H5Z_FILTER_SNAPPY", 32003, "snappy"),
    ("H5Z_FILTER_LZ4", 32004, "lz4"),
    ("H5Z_FILTER_LZ4HC", 32005, "lz4hc"),
    ("H5Z_FILTER_BITSHUFFLE", 32008, "bitshuffle"),
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


def get_dset_size(shape_json, typesize):
    """Return the size of the dataspace.  For
    any unlimited dimensions, assume a value of 1.
    (so the return size will be the absolute minimum)
    """
    if shape_json is None or shape_json["class"] == "H5S_NULL":
        return None
    if shape_json["class"] == "H5S_SCALAR":
        return typesize  # just return size for one item
    if typesize == "H5T_VARIABLE":
        typesize = DEFAULT_TYPE_SIZE  # just take a guess at the item size
    dset_size = typesize
    shape = shape_json["dims"]
    rank = len(shape)

    for n in range(rank):
        if shape[n] == 0:
            # extendable extent with value of 0
            continue  # assume this is one
        dset_size *= shape[n]
    return dset_size


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


def getFiltersJson(create_props, supported_filters=None):
    """ return standardized filter representation from creation properties
        raise bad request if invalid """

    # refer to https://hdf5-json.readthedocs.io/en/latest/bnf/\
    # filters.html#grammar-token-filter_list

    if "filters" not in create_props:
        return {}  # null set

    f_in = create_props["filters"]

    log.debug(f"filters provided in creation_prop: {f_in}")

    if not isinstance(f_in, list):
        msg = "Expected filters in creation_props to be a list"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    f_out = []
    for filter in f_in:
        if isinstance(filter, int) or isinstance(filter, str):
            item = getFilterItem(filter)
            if not item:
                msg = f"filter {filter} not recognized"
                log.warn(msg)
                raise HTTPBadRequest(reason=msg)

            if item["name"] not in supported_filters:
                msg = f"filter {filter} is not supported"
                log.warn(msg)
                raise HTTPBadRequest(reason=msg)
            f_out.append(item)
        elif isinstance(filter, dict):
            if "class" not in filter:
                msg = "expected 'class' key for filter property"
                log.warn(msg)
                raise HTTPBadRequest(reason=msg)
            if filter["class"] != "H5Z_FILTER_USER":
                item = getFilterItem(filter["class"])
            elif "id" in filter:
                item = getFilterItem(filter["id"])
            elif "name" in filter:
                item = getFilterItem(filter["name"])
            else:
                item = None
            if not item:
                msg = f"filter {filter['class']} not recognized"
                log.warn(msg)
                raise HTTPBadRequest(reason=msg)
            if "id" not in filter:
                filter["id"] = item["id"]
            elif item["id"] != filter["id"]:
                msg = f"Expected {filter['class']} to have id: "
                msg += f"{item['id']} but got {filter['id']}"
                log.warn(msg)
                raise HTTPBadRequest(reason=msg)
            if "name" not in filter:
                filter["name"] = item["name"]
            if filter["name"] not in supported_filters:
                msg = f"filter {filter} is not supported"
                log.warn(msg)
                raise HTTPBadRequest(reason=msg)

            f_out.append(filter)
        else:
            msg = f"Unexpected type for filter: {filter}"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)

    # return standardized filter representation
    log.debug(f"using filters: {f_out}")
    return f_out


def getFilters(dset_json):
    """Return list of filters, or empty list"""
    if "creationProperties" not in dset_json:
        return []
    creationProperties = dset_json["creationProperties"]
    if "filters" not in creationProperties:
        return []
    filters = creationProperties["filters"]
    return filters


def getCompressionFilter(filters):
    """Return compression filter from filters, or None"""
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


def getShuffleFilter(filters):
    """Return shuffle filter, or None"""
    FILTER_CLASSES = ("H5Z_FILTER_SHUFFLE", "H5Z_FILTER_BITSHUFFLE")
    for filter in filters:
        log.debug(f"filter: {filter}")
        if "class" not in filter:
            log.warn(f"filter option: {filter} with no class key")
            continue
        filter_class = filter["class"]
        if filter_class in FILTER_CLASSES:
            log.debug(f"found filter: {filter}")
            return filter

    log.debug("Shuffle filter not used")
    return None


def getFilterOps(app, dset_id, filters, dtype=None, chunk_shape=None):
    """Get list of filter operations to be used for this dataset"""
    filter_map = app["filter_map"]

    try:
        if dset_id in filter_map:
            log.debug(f"returning filter from filter_map for dset: {dset_id}")
            return filter_map[dset_id]
    except TypeError:
        log.error(f"getFilterOps TypeError - dset_id: {dset_id} filter_map: {filter_map}")
        raise

    compressionFilter = getCompressionFilter(filters)
    log.debug(f"got compressionFilter: {compressionFilter}")

    filter_ops = {}

    shuffleFilter = getShuffleFilter(filters)

    if shuffleFilter and not isVlen(dtype):
        shuffle_name = shuffleFilter["name"]
        if shuffle_name == "shuffle":
            filter_ops["shuffle"] = 1  # use regular shuffle
        elif shuffle_name == "bitshuffle":
            filter_ops["shuffle"] = 2  # use bitshuffle
        else:
            log.warn(f"unexpected shuffleFilter: {shuffle_name}")
            filter_ops["shuffle"] = 0  # no shuffle
    else:
        filter_ops["shuffle"] = 0  # no shuffle

    if compressionFilter:
        if compressionFilter["class"] == "H5Z_FILTER_DEFLATE":
            filter_ops["compressor"] = "zlib"  # blosc compressor
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
        # save the chunk shape and dtype
        filter_ops["chunk_shape"] = chunk_shape
        filter_ops["dtype"] = dtype
        log.debug(f"save filter ops for {dset_id}")
        filter_map[dset_id] = filter_ops  # save

        return filter_ops
    else:
        return None


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
        msg = "invalid shape: {body_shape}"
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


def getShapeClass(data_shape):
    """ Return shape class of the given data shape """

    if not isinstance(data_shape, dict):
        raise TypeError("expected dict object")

    if "class" not in data_shape:
        raise KeyError("expected 'class' key for data shape")\

    return data_shape["class"]


def getRank(data_shape):
    """ Return rank of given data shape_json """

    shape_class = getShapeClass(data_shape)

    if shape_class == "H5S_NULL":
        return 0
    elif shape_class == "H5S_SCALAR":
        return 0
    elif shape_class == "H5S_SIMPLE":
        if "dims" not in data_shape:
            raise KeyError("expected dims key for H5S_SIMPLE data shape")
        return len(data_shape["dims"])
    else:
        raise ValueError(f"unexpected data shape class: {shape_class}")


def getDsetRank(dset_json):
    """Get rank returning 0 for scalar or NULL data shapes"""
    data_shape = dset_json["shape"]
    return getRank(data_shape)


def isNullSpace(dset_json):
    """Return true if this dataset is a null data space"""
    shape_class = getShapeClass(dset_json["shape"])
    if shape_class == "H5S_NULL":
        return True
    else:
        return False


def isScalarSpace(dset_json):
    """ return true if this is a scalar dataset """

    data_shape = dset_json["shape"]
    shape_class = getShapeClass(data_shape)
    if shape_class == "H5S_NULL":
        return False

    rank = getRank(data_shape)
    return True if rank == 0 else False


def getContiguousLayout(shape_json, item_size, chunk_min=None, chunk_max=None):
    """
    create a chunk layout for datasets use contiguous storage.
    """
    if not isinstance(item_size, int):
        msg = "ContiguousLayout can only be used with fixed-length types"
        log.warn(msg)
        raise ValueError(msg)

    if chunk_min is None:
        msg = "chunk_min not set"
        log.warn(msg)
        raise ValueError(msg)
    if chunk_max is None:
        msg = "chunk_max not set"
        log.warn(msg)
        raise ValueError(msg)

    if chunk_max < chunk_min:
        raise ValueError("chunk_max cannot be less than chunk_min")

    if shape_json is None or shape_json["class"] == "H5S_NULL":
        return None
    if shape_json["class"] == "H5S_SCALAR":
        return (1,)  # just enough to store one item
    dims = shape_json["dims"]
    rank = len(dims)
    if rank == 0:
        raise ValueError("rank must be positive for Contiguous Layout")
    for dim in dims:
        if dim < 0:
            raise ValueError("extents must be positive for Contiguous Layout")
        if dim == 0:
            # data shape with no elements, just return dims as layout
            return dims

    nsize = item_size
    layout = [1,] * rank

    for i in range(rank):
        dim = rank - i - 1
        extent = dims[dim]
        if extent * nsize < chunk_max:
            # just use the full extent as layout
            layout[dim] = extent
            nsize *= extent
        else:
            n = extent
            while n > 1:
                n = -(-n // 2)  # use negatives so we round up on odds
                if n * nsize < chunk_max:
                    break
            layout[dim] = n
            break  # just use 1's for the rest of the layout

    return layout


def getChunkSize(layout, type_size):
    """Return chunk size given layout.
    i.e. just the product of the values in the list.
    """
    if type_size == "H5T_VARIABLE":
        type_size = DEFAULT_TYPE_SIZE

    chunk_size = type_size
    for n in layout:
        if n <= 0:
            raise ValueError("Invalid chunk layout")
        chunk_size *= n
    return chunk_size


def validateChunkLayout(shape_json, item_size, layout, chunk_table=None):
    """
    Use chunk layout given in the creationPropertiesList (if defined and
    layout is valid).
    Return chunk_layout_json
    """

    rank = 0
    space_dims = None
    chunk_dims = None
    max_dims = None

    if "dims" in shape_json:
        space_dims = shape_json["dims"]
        rank = len(space_dims)

    if "maxdims" in shape_json:
        max_dims = shape_json["maxdims"]
    if "dims" in layout:
        chunk_dims = layout["dims"]

    if chunk_dims:
        # validate that the chunk_dims are valid and correlates with the
        # dataset shape
        if isinstance(chunk_dims, int):
            chunk_dims = [
                chunk_dims,
            ]  # promote to array
        if len(chunk_dims) != rank:
            msg = "Layout rank does not match shape rank"
            log.warn(msg)
            raise ValueError(msg)
        for i in range(rank):
            dim_extent = space_dims[i]
            chunk_extent = chunk_dims[i]
            if not isinstance(chunk_extent, int):
                msg = "Layout dims must be integer or integer array"
                log.warn(msg)
                raise ValueError(msg)
            if chunk_extent <= 0:
                msg = "Invalid layout value"
                log.warn(msg)
                raise ValueError(msg)
            if max_dims is None:
                if chunk_extent > dim_extent:
                    msg = "Invalid layout value"
                    log.warn(msg)
                    raise HTTPBadRequest(reason=msg)
            elif max_dims[i] != 0:
                if chunk_extent > max_dims[i]:
                    msg = "Invalid layout value for extensible dimension"
                    log.warn(msg)
                    raise ValueError(msg)
            else:
                pass  # allow any positive value for unlimited dimensions

    if "class" not in layout:
        msg = "class key not found in layout for creation property list"
        log.warn(msg)
        raise ValueError(msg)

    layout_class = layout["class"]

    if layout_class == "H5D_CONTIGUOUS_REF":
        # reference to a dataset in a traditional HDF5 files with
        # contiguous storage
        if item_size == "H5T_VARIABLE":
            # can't be used with variable types...
            msg = "Datasets with variable types cannot be used with "
            msg += "reference layouts"
            log.warn(msg)
            raise ValueError(msg)
        if "file_uri" not in layout:
            # needed for H5D_CONTIGUOUS_REF
            msg = "'file_uri' key must be provided for "
            msg += "H5D_CONTIGUOUS_REF layout"
            log.warn(msg)
            raise ValueError(msg)
        if "offset" not in layout:
            # needed for H5D_CONTIGUOUS_REF
            msg = "'offset' key must be provided for "
            msg += "H5D_CONTIGUOUS_REF layout"
            log.warn(msg)
            raise ValueError(msg)
        if "size" not in layout:
            # needed for H5D_CONTIGUOUS_REF
            msg = "'size' key must be provided for "
            msg += "H5D_CONTIGUOUS_REF layout"
            log.warn(msg)
            raise ValueError(msg)
        if "dims" in layout:
            # used defined chunk layout not allowed for H5D_CONTIGUOUS_REF
            msg = "'dims' key can not be provided for "
            msg += "H5D_CONTIGUOUS_REF layout"
            log.warn(msg)
            raise ValueError(msg)
    elif layout_class == "H5D_CHUNKED_REF":
        # reference to a dataset in a traditional HDF5 files with
        # chunked storage
        if item_size == "H5T_VARIABLE":
            # can't be used with variable types..
            msg = "Datasets with variable types cannot be used with "
            msg += "reference layouts"
            log.warn(msg)
            raise ValueError(msg)
        if "file_uri" not in layout:
            # needed for H5D_CHUNKED_REF
            msg = "'file_uri' key must be provided for "
            msg += "H5D_CHUNKED_REF layout"
            log.warn(msg)
            raise ValueError(msg)
        if "dims" not in layout:
            # needed for H5D_CHUNKED_REF
            msg = "'dimns' key must be provided for "
            msg += "H5D_CHUNKED_REF layout"
            log.warn(msg)
            raise ValueError(msg)
        if "chunks" not in layout:
            msg = "'chunks' key must be provided for "
            msg += "H5D_CHUNKED_REF layout"
            log.warn(msg)
            raise ValueError(msg)
    elif layout_class == "H5D_CHUNKED_REF_INDIRECT":
        # reference to a dataset in a traditional HDF5 files with chunked
        # storage using an auxiliary dataset
        if item_size == "H5T_VARIABLE":
            # can't be used with variable types..
            msg = "Datasets with variable types cannot be used with "
            msg += "reference layouts"
            log.warn(msg)
            raise ValueError(msg)
        if "dims" not in layout:
            # needed for H5D_CHUNKED_REF_INDIRECT
            msg = "'dims' key must be provided for "
            msg += "H5D_CHUNKED_REF_INDIRECT layout"
            log.warn(msg)
            raise ValueError(msg)
        if "chunk_table" not in layout:
            msg = "'chunk_table' key must be provided for "
            msg += "H5D_CHUNKED_REF_INDIRECT layout"
            log.warn(msg)
            raise ValueError(msg)
        chunk_table_id = layout["chunk_table"]
        if not isValidUuid(chunk_table_id, "Dataset"):
            msg = f"Invalid chunk table id: {chunk_table_id}"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)

    elif layout_class == "H5D_CHUNKED":
        if "dims" not in layout:
            msg = "dims key not found in layout for creation property list"
            log.warn(msg)
            raise ValueError(msg)
        if shape_json["class"] != "H5S_SIMPLE":
            msg = "Bad Request: chunked layout not valid with shape class: "
            msg += f"{shape_json['class']}"
            log.warn(msg)
            raise ValueError(msg)
    elif layout_class == "H5D_CONTIGUOUS":
        if "dims" in layout:
            msg = "dims key found in layout for creation property list "
            msg += "for H5D_CONTIGUOUS storage class"
            log.warn(msg)
            raise ValueError(msg)
    elif layout_class == "H5D_COMPACT":
        if "dims" in layout:
            msg = "dims key found in layout for creation property list "
            msg += "for H5D_COMPACT storage class"
            log.warn(msg)
            raise ValueError(msg)
    else:
        msg = f"Unexpected layout: {layout_class}"
        log.warn(msg)
        raise ValueError(msg)


def expandChunk(layout, typesize, shape_json, chunk_min=CHUNK_MIN, layout_class="H5D_CHUNKED"):
    """Compute an increased chunk shape with a size in bytes greater than chunk_min."""
    if shape_json is None or shape_json["class"] == "H5S_NULL":
        return None
    if shape_json["class"] == "H5S_SCALAR":
        return (1,)  # just enough to store one item

    layout = list(layout)
    log.debug(f"expandChunk layout: {layout} typesize: {typesize}")
    dims = shape_json["dims"]
    rank = len(dims)
    extendable_dims = 0  # number of dimensions that are extenable
    maxdims = None
    if "maxdims" in shape_json:
        maxdims = shape_json["maxdims"]
        for n in range(rank):
            if maxdims[n] == 0 or maxdims[n] > dims[n]:
                extendable_dims += 1

    dset_size = get_dset_size(shape_json, typesize)
    if dset_size <= chunk_min and extendable_dims == 0:
        # just use the entire dataspace shape as one big chunk
        return tuple(dims)

    chunk_size = getChunkSize(layout, typesize)
    if chunk_size >= chunk_min:
        return tuple(layout)  # good already
    while chunk_size < chunk_min:
        # just adjust along extendable dimensions first
        old_chunk_size = chunk_size
        for n in range(rank):
            dim = rank - n - 1  # start from last dim

            if extendable_dims > 0:
                if maxdims[dim] == 0:
                    # infinitely extendable dimensions
                    layout[dim] *= 2
                    chunk_size = getChunkSize(layout, typesize)
                    if chunk_size > chunk_min:
                        break
                elif maxdims[dim] > layout[dim]:
                    # can only be extended so much
                    layout[dim] *= 2
                    if layout[dim] >= dims[dim]:
                        layout[dim] = maxdims[dim]  # trim back
                        extendable_dims -= 1  # one less extenable dimension

                    chunk_size = getChunkSize(layout, typesize)
                    if chunk_size > chunk_min:
                        break
                    else:
                        pass  # ignore non-extensible for now
            else:
                # no extendable dimensions
                if dims[dim] > layout[dim]:
                    # can expand chunk along this dimension
                    layout[dim] *= 2
                    if layout[dim] > dims[dim]:
                        layout[dim] = dims[dim]  # trim back
                    chunk_size = getChunkSize(layout, typesize)
                    if chunk_size > chunk_min:
                        break
                else:
                    pass  # can't extend chunk along this dimension
        if chunk_size <= old_chunk_size:
            # stop iteration if we haven't increased the chunk size
            log.debug("stopping expandChunk iteration")
            break
        elif chunk_size > chunk_min:
            break  # we're good
        else:
            pass  # do another round
    return tuple(layout)


def shrinkChunk(layout, typesize, chunk_max=CHUNK_MAX, layout_class="H5D_CHUNKED"):
    """Compute a reduced chunk shape with a size in bytes less than chunk_max."""
    layout = list(layout)
    chunk_size = getChunkSize(layout, typesize)
    if chunk_size <= chunk_max:
        return tuple(layout)  # good already
    log.debug(f"shrinkChunk layout: {layout} typesize: {typesize}")
    rank = len(layout)

    while chunk_size > chunk_max:
        # just adjust along extendable dimensions first
        old_chunk_size = chunk_size
        for dim in range(rank):
            if layout[dim] > 1:
                # tricky way to do  x // 2 with ceil
                layout[dim] = -(-layout[dim] // 2)
                chunk_size = getChunkSize(layout, typesize)
                if chunk_size <= chunk_max:
                    break
            else:
                pass  # can't shrink chunk along this dimension
        if chunk_size >= old_chunk_size:
            # reality check to see if we'll ever break out of the while loop
            log.warning("Unexpected error in shrink_chunk")
            break
        elif chunk_size <= chunk_max:
            break  # we're good
        else:
            pass  # do another round
    return tuple(layout)


def guessChunk(shape_json, typesize):
    """Guess an appropriate chunk layout for a dataset, given its shape and
    the size of each element in bytes.  Will allocate chunks only as large
    as MAX_SIZE.  Chunks are generally close to some power-of-2 fraction of
    each axis, slightly favoring bigger values for the last index.

    Undocumented and subject to change without warning.
    """
    if shape_json is None or shape_json["class"] == "H5S_NULL":
        return None
    if shape_json["class"] == "H5S_SCALAR":
        return (1,)  # just enough to store one item

    if "maxdims" in shape_json:
        shape = shape_json["maxdims"]
    else:
        shape = shape_json["dims"]

    if typesize == "H5T_VARIABLE":
        typesize = 128  # just take a guess at the item size

    # For unlimited dimensions we have to guess. use 1024
    shape = tuple((x if x != 0 else 1024) for i, x in enumerate(shape))

    return shape


def getLayoutJson(creation_props, shape=None, type_json=None, chunk_min=None, chunk_max=None):
    """ Get the layout json given by creation_props.
        Raise bad request error if invalid """

    min_chunk_size = int(config.get("min_chunk_size"))
    max_chunk_size = int(config.get("max_chunk_size"))

    item_size = getItemSize(type_json)
    if chunk_min is None:
        chunk_min = 1000 * 1000
    if chunk_max is None:
        chunk_max = 4 * 1000 * 1000

    if chunk_min > chunk_max:
        msg = "chunk_max must be larger than chunk_min"
        log.warn(msg)
        raise HTTPBadRequest(reason=msg)

    layout = None
    if "layout" in creation_props:
        layout_props = creation_props["layout"]
    else:
        layout_props = None

    if layout_props:
        if "class" not in layout_props:
            msg = "expected class key in layout props"
            log.warn(msg)
            raise HTTPBadRequest(reason=msg)
        layout_class = layout_props["class"]
        if layout_class == "H5D_CONTIGUOUS":
            # treat contiguous as chunked
            layout_class = "H5D_CHUNKED"
        else:
            layout_class = layout_props["class"]
    elif shape["class"] != "H5S_NULL":
        layout_class = "H5D_CHUNKED"
    else:
        layout_class = None

    if layout_class == "H5D_COMPACT":
        layout = {"class": "H5D_COMPACT"}
    elif layout_class:
        # initialize to H5D_CHUNKED
        layout = {"class": "H5D_CHUNKED"}
    else:
        # null space - no layout
        layout = None

    if layout_props and "dims" in layout_props:
        chunk_dims = layout_props["dims"]
    else:
        chunk_dims = None

    if layout_class == "H5D_CONTIGUOUS_REF":
        kwargs = {"chunk_min": min_chunk_size, "chunk_max": max_chunk_size}
        chunk_dims = getContiguousLayout(shape, item_size, **kwargs)
        layout["dims"] = chunk_dims
        log.debug(f"autoContiguous layout: {layout}")

    if layout_class == "H5D_CHUNKED" and chunk_dims is None:
        # do auto-chunking
        chunk_dims = guessChunk(shape, item_size)
        log.debug(f"initial autochunk layout: {chunk_dims}")

    if layout_class == "H5D_CHUNKED":
        chunk_size = getChunkSize(chunk_dims, item_size)

        msg = f"chunk_size: {chunk_size}, min: {min_chunk_size}, "
        msg += f"max: {max_chunk_size}"
        log.debug(msg)
        # adjust the chunk shape if chunk size is too small or too big
        adjusted_chunk_dims = None
        if chunk_size < min_chunk_size:
            msg = f"chunk size: {chunk_size} less than min size: "
            msg += f"{min_chunk_size}, expanding"
            log.debug(msg)
            kwargs = {"chunk_min": min_chunk_size, "layout_class": layout_class}
            adjusted_chunk_dims = expandChunk(chunk_dims, item_size, shape, **kwargs)
        elif chunk_size > max_chunk_size:
            msg = f"chunk size: {chunk_size} greater than max size: "
            msg += f"{max_chunk_size}, shrinking"
            log.debug(msg)
            kwargs = {"chunk_max": max_chunk_size}
            adjusted_chunk_dims = shrinkChunk(chunk_dims, item_size, **kwargs)
        if adjusted_chunk_dims:
            msg = f"requested chunk_dimensions: {chunk_dims} modified "
            msg += f"dimensions: {adjusted_chunk_dims}"
            log.debug(msg)
            layout["dims"] = adjusted_chunk_dims
        else:
            layout["dims"] = chunk_dims  # don't need to adjust chunk size

        # set partition_count if needed:
        max_chunks_per_folder = int(config.get("max_chunks_per_folder"))
        set_partition = False
        if max_chunks_per_folder > 0:
            if "dims" in shape and "dims" in layout:
                set_partition = True

        if set_partition:
            chunk_dims = layout["dims"]
            shape_dims = shape["dims"]
            if "maxdims" in shape:
                max_dims = shape["maxdims"]
            else:
                max_dims = None
            num_chunks = 1
            rank = len(shape_dims)
            unlimited_count = 0
            if max_dims:
                for i in range(rank):
                    if max_dims[i] == 0:
                        unlimited_count += 1
                msg = f"number of unlimited dimensions: {unlimited_count}"
                log.debug(msg)

            for i in range(rank):
                max_dim = 1
                if max_dims:
                    max_dim = max_dims[i]
                    if max_dim == 0:
                        # don't really know what the ultimate extent
                        # could be, but assume 10^6 for total number of
                        # elements and square-shaped array...
                        MAX_ELEMENT_GUESS = 10.0 ** 6
                        exp = 1 / unlimited_count
                        max_dim = int(math.pow(MAX_ELEMENT_GUESS, exp))
                else:
                    max_dim = shape_dims[i]
                num_chunks *= math.ceil(max_dim / chunk_dims[i])

            if num_chunks > max_chunks_per_folder:
                partition_count = math.ceil(num_chunks / max_chunks_per_folder)
                msg = f"set partition count to: {partition_count}, "
                msg += f"num_chunks: {num_chunks}"
                log.info(msg)
                layout["partition_count"] = partition_count
            else:
                msg = "do not need chunk partitions, num_chunks: "
                msg += f"{num_chunks} max_chunks_per_folder: "
                msg += f"{max_chunks_per_folder}"
                log.info(msg)

    if layout_class in ("H5D_CHUNKED_REF", "H5D_CHUNKED_REF_INDIRECT"):
        chunk_size = getChunkSize(chunk_dims, item_size)

        msg = f"chunk_size: {chunk_size}, min: {min_chunk_size}, "
        msg += f"max: {max_chunk_size}"
        log.debug(msg)
        # nothing to do about inefficiently small chunks, but large chunks
        # can be subdivided
        if chunk_size < min_chunk_size:
            msg = f"chunk size: {chunk_size} less than min size: "
            msg += f"{min_chunk_size} for {layout_class} dataset"
            log.warn(msg)
        elif chunk_size > max_chunk_size:
            msg = f"chunk size: {chunk_size} greater than max size: "
            msg += f"{max_chunk_size}, for {layout_class} dataset"
            log.warn(msg)
        layout["dims"] = chunk_dims


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


def getShapeDims(shape):
    """
    Get dims from a given shape json.  Return [1,] for Scalar datasets,
    None for null dataspaces
    """
    dims = None
    if isinstance(shape, int):
        dims = [shape, ]
    elif isinstance(shape, list) or isinstance(shape, tuple):
        dims = shape  # can use as is
    elif isinstance(shape, str):
        # only valid string value is H5S_NULL
        if shape != "H5S_NULL":
            raise ValueError("Invalid value for shape")
        dims = None
    elif isinstance(shape, dict):
        if "class" not in shape:
            raise ValueError("'class' key not found in shape")
        if shape["class"] == "H5S_NULL":
            dims = None
        elif shape["class"] == "H5S_SCALAR":
            dims = [1,]
        elif shape["class"] == "H5S_SIMPLE":
            if "dims" not in shape:
                raise ValueError("'dims' key expected for shape")
            dims = shape["dims"]
        else:
            raise ValueError("Unknown shape class: {}".format(shape["class"]))
    else:
        raise ValueError(f"Unexpected shape class: {type(shape)}")

    return dims


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
    if "class" not in layout_json:
        log.error(f"Expected class key for layout: {layout_json}")
        raise HTTPInternalServerError()
    layout_class = layout_json["class"]
    if layout_class not in CHUNK_LAYOUT_CLASSES:
        log.error(f"Unexpected shape layout: {layout_class}")
        raise HTTPInternalServerError()
    if "dims" not in layout_json:
        log.error(f"Expected dims key in layout: {layout_json}")
        raise HTTPInternalServerError()
    layout = layout_json["dims"]
    return layout


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


def isExtensible(dims, maxdims):
    """
    Determine if the dataset can be extended
    """
    if maxdims is None or len(dims) == 0:
        return False
    rank = len(dims)
    if len(maxdims) != rank:
        raise ValueError("rank of maxdims does not match dataset")
    for n in range(rank):
        # TBD - shouldn't have H5S_UNLIMITED in any new files.
        # Remove check once this is confirmed
        if maxdims[n] in (0, "H5S_UNLIMITED") or maxdims[n] > dims[n]:
            return True
    return False


def getDatasetLayout(dset_json):
    """ Return layout json from creation property list or layout json """
    layout = None

    if "creationProperties" in dset_json:
        cp = dset_json["creationProperties"]
        if "layout" in cp:
            layout = cp["layout"]
    if not layout and "layout" in dset_json:
        layout = dset_json["layout"]
    if not layout:
        log.warn(f"no layout for {dset_json}")
    return layout


def getDatasetLayoutClass(dset_json):
    """ return layout class """
    layout = getDatasetLayout(dset_json)
    if layout and "class" in layout:
        layout_class = layout["class"]
    else:
        layout_class = None
    return layout_class


def getChunkDims(dset_json):
    """ get chunk shape for given dset_json """

    layout = getDatasetLayout(dset_json)
    if layout and "dims" in layout:
        return layout["dims"]
    else:
        # H5D_COMPACT and H5D_CONTIGUOUS will not have a dims key
        # Check the layout dict in dset_json to see if it's
        # defined there
        if "layout" in dset_json:
            layout = dset_json["layout"]
            if "dims" in layout:
                return layout["dims"]
    return None


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
