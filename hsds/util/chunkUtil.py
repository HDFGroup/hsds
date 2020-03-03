import numpy as np
from .. import hsds_logger as log

CHUNK_BASE =  16*1024   # Multiplier by which chunks are adjusted
CHUNK_MIN =  512*1024   # Soft lower limit (512k)
CHUNK_MAX = 2048*1024   # Hard upper limit (2M)
DEFAULT_TYPE_SIZE = 128 # Type size case when it is variable
PRIMES = [29, 31, 37, 41, 43, 47, 53, 59, 61, 67] # for chunk partitioning


"""
Convert list that may contain bytes type elements to list of string elements

TBD: code copy from arrayUtil.py 
"""
def _bytesArrayToList(data):
    if type(data) in (bytes, str):
        is_list = False
    elif isinstance(data, (np.ndarray, np.generic)):
        if len(data.shape) == 0:
            is_list = False
            data = data.tolist()  # tolist will return a scalar in this case
            if type(data) in (list, tuple):
                is_list = True
            else:
                is_list = False
        else:
            is_list = True
    elif type(data) in (list, tuple):
        is_list = True
    else:
        is_list = False

    if is_list:
        out = []
        for item in data:
            out.append(_bytesArrayToList(item)) # recursive call
    elif type(data) is bytes:
        out = data.decode("utf-8")
    else:
        out = data

    return out

def getChunkSize(layout, type_size):
    """ Return chunk size given layout.
    i.e. just the product of the values in the list.
    """
    if type_size == 'H5T_VARIABLE':
        type_size = DEFAULT_TYPE_SIZE

    chunk_size = type_size
    for n in layout:
        if n <= 0:
            raise ValueError("Invalid chunk layout")
        chunk_size *= n
    return chunk_size

def get_dset_size(shape_json, typesize):
    """ Return the size of the dataspace.  For
        any unlimited dimensions, assume a value of 1.
        (so the return size will be the absolute minimum)
    """
    if shape_json is None or shape_json["class"] == 'H5S_NULL':
        return None
    if shape_json["class"] == 'H5S_SCALAR':
        return typesize  # just return size for one item
    if typesize == 'H5T_VARIABLE':
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

def expandChunk(layout, typesize, shape_json, chunk_min=CHUNK_MIN, layout_class='H5D_CHUNKED'):
    """ Extend the chunk shape until it is above the MIN target.
    """
    if shape_json is None or shape_json["class"] == 'H5S_NULL':
        return None
    if shape_json["class"] == 'H5S_SCALAR':
        return (1,)  # just enough to store one item

    layout = list(layout)
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
            dim = rank - n - 1 # start from

            if extendable_dims > 0:
                if maxdims[dim] == 0:
                    # infinately extendable dimensions
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
                        pass # ignore non-extensible for now
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
                    pass # can't extend chunk along this dimension
        if chunk_size <= old_chunk_size:
            # reality check to see if we'll ever break out of the while loop
            log.warn("Unexpected error in guess_chunk size")

            break
        elif chunk_size > chunk_min:
            break  # we're good
        else:
            pass  # do another round
    return tuple(layout)

def shrinkChunk(layout, typesize, chunk_max=CHUNK_MAX, layout_class='H5D_CHUNKED'):
    """ Shrink the chunk shape until it is less than the MAX target.
    """
    layout = list(layout)
    chunk_size = getChunkSize(layout, typesize)
    if chunk_size <= chunk_max:
        return tuple(layout)  # good already
    rank = len(layout)

    while chunk_size > chunk_max:
        # just adjust along extendable dimensions first
        old_chunk_size = chunk_size
        for dim in range(rank):
            if layout[dim] > 1:
                layout[dim] //= 2
                chunk_size = getChunkSize(layout, typesize)
                if chunk_size <= chunk_max:
                    break
            else:
                pass # can't shrink chunk along this dimension
        if chunk_size >= old_chunk_size:
            # reality check to see if we'll ever break out of the while loop
            log.warning("Unexpected error in shrink_chunk")
            break
        elif chunk_size <= chunk_max:
            break  # we're good
        else:
            pass   # do another round
    return tuple(layout)

def guessChunk(shape_json, typesize):
    """ Guess an appropriate chunk layout for a dataset, given its shape and
    the size of each element in bytes.  Will allocate chunks only as large
    as MAX_SIZE.  Chunks are generally close to some power-of-2 fraction of
    each axis, slightly favoring bigger values for the last index.

    Undocumented and subject to change without warning.
    """
    if shape_json is None or shape_json["class"] == 'H5S_NULL':
        return None
    if shape_json["class"] == 'H5S_SCALAR':
        return (1,)  # just enough to store one item

    if "maxdims" in shape_json:
        shape = shape_json["maxdims"]
    else:
        shape = shape_json["dims"]

    if typesize == 'H5T_VARIABLE':
        typesize = 128  # just take a guess at the item size


    # For unlimited dimensions we have to guess. use 1024
    shape = tuple((x if x!=0 else 1024) for i, x in enumerate(shape))

    return shape

def getContiguousLayout(shape_json, item_size, chunk_min=1000*1000, chunk_max=4*1000*1000):
    """
    create a chunklayout for datasets use continguous storage.
    """
    if not isinstance(item_size, int):
        raise ValueError("ContiguousLayout can only be used with fixed-length types")
    if chunk_max < chunk_min:
        raise ValueError("chunk_max cannot be less than chunk_min")
    if shape_json is None or shape_json["class"] == 'H5S_NULL':
        return None
    if shape_json["class"] == 'H5S_SCALAR':
        return (1,)  # just enough to store one item
    chunk_avg = (chunk_min + chunk_max) // 2
    dims = shape_json["dims"]
    rank = len(dims)
    nsize = item_size
    layout = [1,] * rank
    if rank == 1:
        # just divy up the dimension with whatever works best
        nsize *= dims[0]
        if nsize < chunk_max:
            layout[0] = dims[0]
        else:
            layout[0] = chunk_avg // item_size
    else:
        unit_chunk = False
        for i in range(rank):
            dim = rank - i - 1
            extent = dims[dim]
            nsize *= extent
            if unit_chunk:
                layout[dim] = 1
            else:
                layout[dim] = extent
                if nsize > chunk_max:
                    if i>0:
                        # make everything after first dimension 1
                        layout[dim] = 1
                    unit_chunk = 1
    return layout


def frac(x, d):
    """
    Utility func -- Works like fractional div, but returns ceiling rather than floor
    """
    return (x + (d-1)) // d

def slice_stop(s):
    """ Return the end of slice, accounting that for steps > 1, this may not
        be the slice stop value.
    """
    if s.step > 1:
        num_points = frac((s.stop-s.start), s.step)
        w = num_points * s.step - (s.step - 1)
    else:
        w = s.stop - s.start # selection width (>0)
    return s.start + w
 
def getNumChunks(selection, layout):
    """
    Get the number of chunks potentially required.
    If selection is provided (a list of slices), return the number
    of chunks that intersect with the selection.
    """

    # do a quick check that we don't have a null selection space'
    # TBD: this needs to be revise to do the right think with stride > 1
    for s in selection:
        if s.stop <= s.start:
            log.debug("null selection")
            return 0
    num_chunks = 1
    for i in range(len(selection)):
        s = selection[i]

        if s.step > 1:
            num_points = frac((s.stop-s.start), s.step)
            w = num_points * s.step - (s.step - 1)
        else:
            w = s.stop - s.start # selection width (>0)

        c = layout[i]   # chunk size

        lc = frac(s.start, c) * c

        if s.start + w <= lc:
            # looks like just we cross just one chunk along this deminsion
            continue

        rc = ((s.start + w) // c) * c
        m = rc - lc
        if c > s.step:
            count = m // c
        else:
            count = m // s.step
        if s.start < lc:
            count += 1  # hit one chunk on the left
        if s.start + w > rc:
            count += 1  # hit one chunk on the right

        num_chunks *= count
    return num_chunks

def getChunkId(dset_id, point, layout):
    """ get chunkid for given point in the dataset
    """

    chunk_id = "c-" + dset_id[2:] + '_'
    rank = len(layout)

    for dim in range(rank):
        coord = None
        if rank == 1:
            coord = point  # integer for 1d dataset
        else:
            coord = point[dim]
        c = layout[dim]
        chunk_index= int(coord) // c
        chunk_id +=  str(chunk_index)
        if dim + 1 < rank:
            chunk_id += '_' # seperate dimensions with underscores

    return chunk_id

def getDatasetId(chunk_id):
    """ Get dataset id given a chunk id
    """
    n = chunk_id.find('-') + 1
    if n <= 0:
        raise ValueError("Unexpected chunk id")
    obj_uuid = chunk_id[n:(36+n)]
    dset_id = "d-" + obj_uuid
    return dset_id

def getChunkIndex(chunk_id):
    """ given a chunk_id (e.g.: c-12345678-1234-1234-1234-1234567890ab_6_4)
    return the coordinates of the chunk. In this case (6,4)
    """
    # go to the first underscore
    n = chunk_id.find('_')  + 1
    if n == 0:
        raise ValueError("Invalid chunk_id: {}".format(chunk_id))
    suffix = chunk_id[n:]

    index = []
    parts = suffix.split('_')
    for part in parts:
        index.append(int(part))

    return index

def getChunkPartition(chunk_id):
    """ return partition (if any) for the given chunk id.
    Parition is encoded in digits after the initial 'c' character.
    E.g. for:  c56-12345678-1234-1234-1234-1234567890ab_6_4, the
    partition would be 56.
    For c-12345678-1234-1234-1234-1234567890ab_6_4, the
    partition would be None.
    """
    if not chunk_id or chunk_id[0] != 'c':
        raise ValueError("unexpected chunk id")
    n = chunk_id.find('-') # go to first underscore
    if n == 1:
        return None  # no partition
    partition = int(chunk_id[1:n])
    return partition



def getPartitionKey(chunk_id, partition_count):
    """ mixin the the partition specifier based on dataset shape and partition_count
    """
    if not partition_count or partition_count < 2:
        return chunk_id # no partition key needed

    chunk_index = getChunkIndex(chunk_id)
    rank = len(chunk_index)

    partition_index = 0
    for dim in range(rank):
        prime_factor = PRIMES[dim % len(PRIMES)]
        partition_index += chunk_index[dim] * prime_factor

    partition_index %= partition_count
    n = chunk_id.find("-")  # get the part after the first hyphen
    s = chunk_id[n:]
    chunk_id = 'c' + str(partition_index) + s
    return chunk_id


def getChunkIdForPartition(chunk_id, dset_json):
    """ Return the partition specific chunk id for given chunk
    """
    if "layout" not in dset_json:
        msg = "No layout found in dset_json"
        log.error(msg)
        raise KeyError(msg)
    layout_json = dset_json["layout"]
    if "partition_count" in layout_json:
        partition_count = layout_json["partition_count"]
        partition = getChunkPartition(chunk_id)
        if partition is None:
            # mix in the partition key
            chunk_id = getPartitionKey(chunk_id, partition_count)
    return chunk_id

def getChunkIds(dset_id, selection, layout, dim=0, prefix=None, chunk_ids=None):
    """ Get the all the chunk ids for chunks that lie in the selection of the
    given dataset.
    """
    num_chunks = getNumChunks(selection, layout)
    if num_chunks == 0:
        return []  # empty list
    if prefix is None:
        # construct a prefix using "c-" with the uuid of the dset_id
        if not dset_id.startswith("d-"):
            msg = "Bad Request: invalid dset id: {}".format(dset_id)
            log.warning(msg)
            raise ValueError(msg)
        prefix = "c-" + dset_id[2:] + '_'
    rank = len(selection)
    if chunk_ids is None:
        chunk_ids = []
    s = selection[dim]
    c = layout[dim]

    if s.step > c:
        # chunks may not be contiguous,  skip along the selection and add
        # whatever chunks we land in
        for i in range(s.start, s.stop, s.step):
            chunk_index = i // c
            chunk_id = prefix + str(chunk_index)
            if dim + 1 == rank:
                # we've gone through all the dimensions, add this id to the list
                chunk_ids.append(chunk_id)
            else:
                chunk_id += '_'  # seperator between dimensions
                # recursive call
                getChunkIds(dset_id, selection, layout, dim+1, chunk_id, chunk_ids)
    else:
        # get a contiguous set of chunks along the selection
        if s.step > 1:
            num_points = frac((s.stop-s.start), s.step)
            w = num_points * s.step - (s.step - 1)
        else:
            w = s.stop - s.start # selection width (>0)

        chunk_index_start = s.start // c
        chunk_index_end = frac((s.start + w), c)

        for i in range(chunk_index_start, chunk_index_end):
            chunk_id = prefix + str(i)
            if dim + 1 == rank:
                # we've gone through all the dimensions, add this id to the list
                chunk_ids.append(chunk_id)
            else:
                chunk_id += '_'  # seperator between dimensions
                # recursive call
                getChunkIds(dset_id, selection, layout, dim+1, chunk_id, chunk_ids)
    # got the complete list, return it!
    return chunk_ids

def getChunkSuffix(chunk_id):
    """ given a chunk_id (e.g.: c-12345678-1234-1234-1234-1234567890ab_6_4)
    return the coordinates as a string. In this case 6_4
    """
    # go to the first underscore
    n = chunk_id.find('_')  + 1
    if n == 0:
        raise ValueError("Invalid chunk_id: {}".format(chunk_id))
    suffix = chunk_id[n:]
    return suffix



def getChunkCoordinate(chunk_id, layout):
    """ given a chunk_id (e.g.: c-12345678-1234-1234-1234-1234567890ab_6_4)
    and a layout (e.g. (10,10))
    return the coordinates of the chunk in dataset space. In this case (60,40)
    """
    coord = getChunkIndex(chunk_id)
    for i in range(len(layout)):
        coord[i] *= layout[i]

    return coord


def getChunkSelection(chunk_id, slices, layout):
    """
    Return the intersection of the chunk with the given slices selection of the array.
    """
    chunk_index = getChunkIndex(chunk_id)
    rank = len(layout)
    sel = []
    for dim in range(rank):
        s = slices[dim]
        c = layout[dim]
        n = chunk_index[dim] * c
        if s.start >= n + c:
            return None  # null intersection
        #s_stop = slice_stop(s)
        if s.stop < n:
            return None  # null intersection
        if s.stop > n + c:
            stop = n + c
        else:
            stop = s.stop
        w = n - s.start
        if s.start < n:
            start = frac(w, s.step) * s.step + s.start
        else:
            start = s.start
        step = s.step
        cs = slice(start, stop, step)
        stop = slice_stop(cs)


        sel.append(slice(start, stop, step))
    return sel

def getChunkCoverage(chunk_id, slices, layout):
    """
    Get chunk-relative selection of the given chunk and selection.
    """
    chunk_index = getChunkIndex(chunk_id)
    chunk_sel = getChunkSelection(chunk_id, slices, layout)
    rank = len(layout)
    sel = []
    for dim in range(rank):
        s = chunk_sel[dim]
        w = layout[dim]
        offset = chunk_index[dim] * w
        start = s.start - offset
        if start < 0:
            msg = "Unexpected chunk selection"
            log.error(msg)
            raise ValueError(msg)
        stop = s.stop - offset
        if stop > w:
            msg = "Unexpected chunk selection"
            log.error(msg)
            raise ValueError(msg)
        step = s.step
        sel.append(slice(start, stop, step))
    return sel

def getDataCoverage(chunk_id, slices, layout):
    """
    Get data-relative selection of the given chunk and selection.
    """
    chunk_sel = getChunkSelection(chunk_id, slices, layout)
    rank = len(layout)
    sel = []
    for dim in range(rank):
        c = chunk_sel[dim]
        s = slices[dim]
        if c.step != s.step:
            raise ValueError("expecting step for chunk selection to be the same as data selection")
        start = (c.start - s.start) // s.step
        stop = frac((c.stop - s.start), s.step)
        step = 1
        sel.append(slice(start, stop, step))

    return tuple(sel)

def getChunkRelativePoint(chunkCoord, point):
    """
    Get chunk-relative coordinate of the given point

       chunkIndex: ndarray of chunk coordinates
       point: ndarray of element in dset
    Return: chunk-relative coordinates of point
    """
    tr = point.copy()
    for i in range(len(point)):
        if chunkCoord[i] > point[i]:
            msg = "unexpected point index"
            raise IndexError(msg)
        tr[i] = point[i] - chunkCoord[i]
    return tr

class ChunkIterator:
    """
    Class to iterate through list of chunks given dset_id, selection,
    and layout.
    """
    def __init__(self, dset_id, selection, layout):
        self._prefix = "c-" + dset_id[2:]
        self._layout = layout
        self._selection = selection
        self._rank = len(selection)
        self._chunk_index = [0,] * self._rank
        for i in range(self._rank):
            s = selection[i]
            c = layout[i]
            self._chunk_index[i] = s.start // c


    def __iter__(self):
        return self

    def next(self):
        if self._chunk_index[0] * self._layout[0] >= self._selection[0].stop:
            # ran past the last chunk, end iteration
            raise StopIteration()
        chunk_id = self._prefix
        # init to minimum chunk index for each dimension
        for i in range(self._rank):
            chunk_id += '_'
            chunk_id += str(self._chunk_index[i])
        # bump up the last index and carry forward if we run outside the selection
        dim = self._rank - 1
        while dim >= 0:
            c = self._layout[dim]
            s = self._selection[dim]
            self._chunk_index[dim] += 1

            chunk_end = self._chunk_index[dim] * c
            if chunk_end < s.stop:
                # we still have room to extend along this dimensions
                return chunk_id

            if dim > 0:
                # reset to the start and continue iterating with higher dimension
                self._chunk_index[dim] = s.start // c
            dim -= 1
        return chunk_id

"""
Return data from requested chunk and selection
"""
def chunkReadSelection(chunk_arr, slices=None):
    log.debug("chunkReadSelection")

    dims = chunk_arr.shape
    log.debug(f"got chunk dims: {dims}")
    rank = len(dims)
    if rank == 0:
        msg = "No dimension passed to chunkReadSelection"
        raise ValueError(msg)

    log.debug(f"got selection: {slices}")
    slices = tuple(slices)

    if len(slices) != rank:
        msg = "Selection rank does not match shape rank"
        raise ValueError(msg)

    dt = chunk_arr.dtype
    log.debug(f"dtype: {dt}")

    # get requested data
    output_arr = chunk_arr[slices]

    return output_arr

"""
Write data for requested chunk and selection
"""
def chunkWriteSelection(chunk_arr=None, slices=None, data=None):
    log.info(f"chunkWriteSelection")
    dims = chunk_arr.shape

    rank = len(dims)
 
    if rank == 0:
        msg = "No dimension passed to chunkReadSelection"
        raise ValueError(msg)
    if len(slices) != rank:
        msg = "Selection rank does not match dataset rank"
        raise ValueError(msg)
    if len(data.shape) != rank:
        msg = "Input arr does not match dataset rank"
        raise ValueError(msg)

    # update chunk array
    chunk_arr[slices] = data


"""
Read points from given chunk
"""
def chunkReadPoints(chunk_id=None, chunk_layout=None, chunk_arr=None, point_arr=None):
    log.debug(f"chunkReadPoints - chunk_id: {chunk_id}")

    dims = chunk_arr.shape
    log.debug(f"got dims: {dims}")
    chunk_coord = getChunkCoordinate(chunk_id, dims)
    log.debug(f"chunk_coord: {chunk_coord}")
    rank = len(dims)
    if rank == 0:
        msg = "No dimension passed to chunk read points"
        raise ValueError(msg)

    dset_dtype = chunk_arr.dtype
    log.debug(f"dset dtype: {dset_dtype}")

    # verify chunk_layout
    if len(chunk_layout) != rank:
        msg = "chunk layout doesn't match rank"
        raise ValueError(msg)

    # verify points array dtype
    points_dt = point_arr.dtype
    log.debug(f"points_dt: {points_dt}")
    log.debug(f"points_shape: {point_arr.shape}")
    if points_dt != np.dtype("uint64"):
        msg = "unexpected dtype for point array"
        raise ValueError(msg)
    if len(point_arr.shape) != 2:
        msg = "unexpected shape for point array"
        raise ValueError(msg)
    if point_arr.shape[1] != rank:
        msg = "unexpected shape for point array"
        raise ValueError(msg)
    num_points = point_arr.shape[0]

    log.debug(f"got {num_points} points")

    output_arr = np.zeros((num_points,), dtype=dset_dtype)

    chunk_coord = getChunkCoordinate(chunk_id, chunk_layout)

    for i in range(num_points):
        point = point_arr[i,:]
        tr_point = getChunkRelativePoint(chunk_coord, point)
        val = chunk_arr[tuple(tr_point)]
        output_arr[i] = val
    return output_arr

"""
Write points to given chunk
"""
def chunkWritePoints(chunk_id=None, chunk_layout=None, chunk_arr=None, point_arr=None):
    # writing point data
    log.debug(f"chunkWritePoints - chunk_id: {chunk_id}")
    dims = chunk_arr.shape

    log.debug(f"got dims: {dims}")
    rank = len(dims)
    if rank == 0:
        msg = "No dimension passed to chunkWritePoints"
        raise ValueError(msg)

    if len(point_arr.shape) != 1:
        msg = "Expected point array to be one dimensional"
        raise ValueError(msg)
    dset_dtype = chunk_arr.dtype
    log.debug(f"dtype: {dset_dtype}")
    log.debug(f"point_arr: {point_arr}")

    # point_arr should have the following type:
    #       (coord1, coord2, ...) | dset_dtype
    comp_dtype = point_arr.dtype
    if len(comp_dtype) != 2:
        msg = "expected compound type for point array"
        raise ValueError(msg)
    dt_0 = comp_dtype[0]
    if dt_0.base != np.dtype("uint64"):
        msg = "unexpected dtype for point array"
        raise ValueError(msg)
    if rank == 1:
        if dt_0.shape:
            msg = "unexpected dtype for point array"
            raise ValueError(msg)
    else:
        if dt_0.shape[0] != rank:
            msg = "unexpected shape for point array"
            raise ValueError(msg)
        dt_1 = comp_dtype[1]
        if dt_1 != dset_dtype:
            msg = "unexpected dtype for point array"
            raise ValueError(msg)

    num_points = len(point_arr)

    chunk_coord = getChunkCoordinate(chunk_id, chunk_layout)

    for i in range(num_points):
        elem = point_arr[i]
        log.debug(f"non-relative coordinate: {elem}")
        if rank == 1:
            coord = int(elem[0])
            coord = coord - chunk_coord[0] # adjust to chunk relative
            if coord < 0 or coord >= dims[0]:
                msg = f"chunkWritePoints - invalid index: {int(elem[0])}"
                log.warn(msg)
                raise IndexError(msg)
        else:
            coord = elem[0] # index to update
            for dim in range(rank):
                # adjust to chunk relative
                coord[dim] = int(coord[dim]) - chunk_coord[dim]
            coord = tuple(coord)  # need to convert to a tuple
        log.debug(f"relative coordinate: {coord}")

        val = elem[1]   # value
        chunk_arr[coord] = val # update the point



"""
_getEvalStr: Get eval string for given query

    Gets Eval string to use with numpy where method.
"""
def _getEvalStr(query, arr_name, field_names):
    i = 0
    eval_str = ""
    var_name = None
    end_quote_char = None
    var_count = 0
    paren_count = 0
    black_list = ( "import", ) # field names that are not allowed
    for item in black_list:
        if item in field_names:
            msg = "invalid field name"
            log.warn("Bad query: " + msg)
            raise ValueError(msg)
    while i < len(query):
        ch = query[i]
        if (i+1) < len(query):
            ch_next = query[i+1]
        else:
            ch_next = None
        if var_name and not ch.isalnum():
            # end of variable
            if var_name not in field_names:
                # invalid
                msg = "unknown field name"
                log.warn("Bad query: " + msg)
                raise ValueError(msg)
            eval_str += arr_name + "['" + var_name + "']"
            var_name = None
            var_count += 1

        if end_quote_char:
            if ch == end_quote_char:
                # end of literal
                end_quote_char = None
            eval_str += ch
        elif ch in ("'", '"'):
            end_quote_char = ch
            eval_str += ch
        elif ch.isalpha():
            if ch == 'b' and ch_next in ("'", '"'):
                eval_str += 'b' # start of a byte string literal
            elif var_name is None:
                var_name = ch  # start of a variable
            else:
                var_name += ch
        elif ch == '(' and end_quote_char is None:
            paren_count += 1
            eval_str += ch
        elif ch == ')' and end_quote_char is None:
            paren_count -= 1
            if paren_count < 0:
                msg = "Mismatched paren"
                log.warn("Bad query: " + msg)
                raise ValueError(msg)
            eval_str += ch
        else:
            # just add to eval_str
            eval_str += ch
        i = i+1
    if end_quote_char:
        msg = "no matching quote character"
        log.warn("Bad Query: " + msg)
        raise ValueError(msg)
    if var_count == 0:
        msg = "No field value"
        log.warn("Bad query: " + msg)
        raise ValueError(msg)
    if paren_count != 0:
        msg = "Mismatched paren"
        log.warn("Bad query: " + msg)
        raise ValueError(msg)
    log.debug(f"eval_str: {eval_str}")
    return eval_str


"""
Run query on chunk and selection
"""
def chunkQuery(chunk_id=None, chunk_layout=None, chunk_arr=None, slices=None,
        query=None, query_update=None, limit=0, return_json=False):
    log.debug(f"chunk_query - chunk_id: {chunk_id}")

    if not isinstance(chunk_arr, np.ndarray):
        raise TypeError("unexpected array type")

    if limit and not isinstance(limit, int):
        raise TypeError("unexpected limit")

    dims = chunk_arr.shape

    rank = len(dims)

    dset_dtype = chunk_arr.dtype
    log.debug(f"dtype: {dset_dtype}")
    log.debug(f"selection: {slices}")

    if rank != 1:
        msg = "Query operations only supported on one-dimensional datasets"
        raise ValueError(msg)
    if not slices:
        slices = [slice(0, dims[0], 1),]
    if len(slices) != rank:
        msg = "Selection rank does not match shape rank"
        raise ValueError(msg)
    slices = tuple(slices)

    chunk_coord = getChunkCoordinate(chunk_id, chunk_layout)

    values = []
    indices = []
    # do query selection
    field_names = list(dset_dtype.fields.keys())

    if query_update:
        replace_mask = [None,] * len(field_names)
        for i in range(len(field_names)):
            field_name = field_names[i]
            if field_name in query_update:
                replace_mask[i] = query_update[field_name]
        log.debug(f"replace_mask: {replace_mask}")
        if replace_mask == [None,] * len(field_names):
            msg = "no fields found in query_update"
            raise ValueError(msg)
    else:
        replace_mask = None

    x = chunk_arr[slices]
    # log.debug(f"chunkQuery - x: {x}")
    eval_str = _getEvalStr(query, "x", field_names)
    log.debug(f"chunkQuery - eval_str: {eval_str}")
    where_result = np.where(eval(eval_str))
    # log.debug(f"chunkQuery - where_result: {where_result}")
    where_result_index = where_result[0]
    #log.debug(f"chunkQuery - whare_result index: {where_result_index}")
    #log.debug(f"chunkQuery - boolean selection: {x[where_result_index]}")
    s = slices[0]
    count = 0
    for index in where_result_index:
        log.debug(f"chunkQuery - index: {index}")
        value = x[index].copy()
        if replace_mask:
            log.debug(f"chunkQuery - original value: {value}")
            for i in range(len(field_names)):
                if replace_mask[i] is not None:
                    value[i] = replace_mask[i]
            log.debug(f"chunkQuery - modified value: {value}")
            try:
                chunk_arr[index] = value
            except ValueError as ve:
                log.error(f"Numpy Value updating array: {ve}")
                raise

        log.debug(f"chunkQuery - got value: {value}")
        indices.append(int(index) * s.step + s.start + chunk_coord[0])  # adjust for selection
        values.append(value)
        count += 1
        if limit > 0 and count >= limit:
            log.debug("query update - got limit items")
            break

    if not values:
        return None   # no hits


    if return_json:
        # return JSON list
        result = {}
        result["index"] = indices
        result["value"] = _bytesArrayToList(values)

    else:
        # return the results as a numpy array
        if rank == 1:
            coord_type_str = "uint64"
        else:
            coord_type_str = f"({rank},)uint64"
        result_dtype = np.dtype([("coord", np.dtype(coord_type_str)), ("value", dset_dtype)])
        result = np.zeros((count,), dtype=result_dtype)
        for i in range(count):
            e = result[i]
            e[0] = indices[i] 
            e[1] = values[i]
            result[i] = e

        log.debug(f"chunkQuery returning: {count} rows")
    return result
