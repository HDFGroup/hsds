import numpy as np
from .. import hsds_logger as log
from .arrayUtil import ndarray_compare

CHUNK_BASE = 16 * 1024  # Multiplier by which chunks are adjusted
CHUNK_MIN = 512 * 1024  # Soft lower limit (512k)
CHUNK_MAX = 2048 * 1024  # Hard upper limit (2M)
DEFAULT_TYPE_SIZE = 128  # Type size case when it is variable
PRIMES = [29, 31, 37, 41, 43, 47, 53, 59, 61, 67]  # for chunk partitioning


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


def expandChunk(
    layout, typesize, shape_json, chunk_min=CHUNK_MIN, layout_class="H5D_CHUNKED"
):
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


def getContiguousLayout(
    shape_json, item_size, chunk_min=1000 * 1000, chunk_max=4 * 1000 * 1000
):
    """
    create a chunklayout for datasets use continguous storage.
    """
    if not isinstance(item_size, int):
        msg = "ContiguousLayout can only be used with fixed-length types"
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
            # datashape with no elements, just return dims as layout
            return dims

    nsize = item_size
    layout = [
        1,
    ] * rank

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


def frac(x, d):
    """
    Utility func -- Works like fractional div, but returns ceiling
    rather than floor
    """
    return (x + (d - 1)) // d


def slice_stop(s):
    """Return the end of slice, accounting that for steps > 1, this may not
    be the slice stop value.
    """
    if s.step > 1:
        num_points = frac((s.stop - s.start), s.step)
        w = num_points * s.step - (s.step - 1)
    else:
        w = s.stop - s.start  # selection width (>0)
    return s.start + w


def getNumChunks(selection, layout):
    """
    Get the number of chunks potentially required.
    If selection is provided (a list of slices), return the number
    of chunks that intersect with the selection.
    """
    rank = len(layout)
    if len(selection) != rank:
        msg = f"selection list has {len(selection)} items, but rank is {rank}"
        raise ValueError(msg)
    # do a quick check that we don't have a null selection space'
    # TBD: this needs to be revise to do the right think with stride > 1
    for s in selection:
        if isinstance(s, slice):
            if s.stop <= s.start:
                log.debug("null selection")
                return 0
        else:
            # coordinate list
            if len(s) == 0:
                return 0
    # first, get the number of chunks needed for any coordinate selection
    chunk_indices = []
    for i in range(len(selection)):
        s = selection[i]
        c = layout[i]
        if isinstance(s, slice):
            continue

        # coordinate list
        if chunk_indices:
            if len(s) != len(chunk_indices):
                msg = "shape mismatch: indexing arrays could not be broadcast together "
                msg += f"with shapes ({len(chunk_indices)},) ({len(s)},)"
                raise ValueError(msg)
        else:
            chunk_indices = ["",] * len(s)

        for j in range(len(s)):
            if chunk_indices[j]:
                chunk_indices[j] += "_"
            chunk_indices[j] += str(s[j] // layout[i])

    if chunk_indices:
        # number of chunks is the number of unique strings in the point list
        num_chunks = len(set(chunk_indices))
    else:
        num_chunks = 1

    # now deal with any slices in the selection
    for i in range(len(selection)):
        s = selection[i]
        c = layout[i]  # chunk size
        if not isinstance(s, slice):
            # ignore coordinate lists since we dealt with them above
            continue

        if s.step is None:
            s = slice(s.start, s.stop, 1)
        if s.step > 1:
            num_points = frac((s.stop - s.start), s.step)
            w = num_points * s.step - (s.step - 1)
        else:
            w = s.stop - s.start  # selection width (>0)

        lc = frac(s.start, c) * c

        if s.start + w <= lc:
            # looks like we just cross one chunk along this dimension
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
    """get chunkid for given point in the dataset"""

    chunk_id = "c-" + dset_id[2:] + "_"
    rank = len(layout)

    for dim in range(rank):
        coord = None
        if rank == 1:
            coord = point  # integer for 1d dataset
        else:
            coord = point[dim]
        c = layout[dim]
        chunk_index = int(coord) // c
        chunk_id += str(chunk_index)
        if dim + 1 < rank:
            chunk_id += "_"  # seperate dimensions with underscores

    return chunk_id


def getDatasetId(chunk_id):
    """Get dataset id given a chunk id"""
    n = chunk_id.find("-") + 1
    if n <= 0:
        raise ValueError("Unexpected chunk id")
    m = n + 36
    obj_uuid = chunk_id[n:m]
    dset_id = "d-" + obj_uuid
    return dset_id


def getChunkIndex(chunk_id):
    """given a chunk_id (e.g.: c-12345678-1234-1234-1234-1234567890ab_6_4)
    return the coordinates of the chunk. In this case (6,4)
    """
    # go to the first underscore
    n = chunk_id.find("_") + 1
    if n == 0:
        raise ValueError(f"Invalid chunk_id: {chunk_id}")
    suffix = chunk_id[n:]

    index = []
    parts = suffix.split("_")
    for part in parts:
        index.append(int(part))

    return index


def getChunkPartition(chunk_id):
    """return partition (if any) for the given chunk id.
    Parition is encoded in digits after the initial 'c' character.
    E.g. for:  c56-12345678-1234-1234-1234-1234567890ab_6_4, the
    partition would be 56.
    For c-12345678-1234-1234-1234-1234567890ab_6_4, the
    partition would be None.
    """
    if not chunk_id or chunk_id[0] != "c":
        raise ValueError("unexpected chunk id")
    n = chunk_id.find("-")  # go to first underscore
    if n == 1:
        return None  # no partition
    partition = int(chunk_id[1:n])
    return partition


def getPartitionKey(chunk_id, partition_count):
    """mixin the the partition specifier based on dataset shape and
    partition_count
    """
    if not partition_count or partition_count < 2:
        return chunk_id  # no partition key needed

    chunk_index = getChunkIndex(chunk_id)
    rank = len(chunk_index)

    partition_index = 0
    for dim in range(rank):
        prime_factor = PRIMES[dim % len(PRIMES)]
        partition_index += chunk_index[dim] * prime_factor

    partition_index %= partition_count
    n = chunk_id.find("-")  # get the part after the first hyphen
    s = chunk_id[n:]
    chunk_id = "c" + str(partition_index) + s
    return chunk_id


def getChunkIdForPartition(chunk_id, dset_json):
    """Return the partition specific chunk id for given chunk"""
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


def getChunkIds(dset_id, selection, layout, prefix=None):
    """Get the all the chunk ids for chunks that lie in the
    selection of the given dataset.
    """

    def chunk_index_to_id(indices):
        """ Convert chunk index list to string with '_' as seperator.
            None values will be replaced with '*' """
        items = []
        for x in indices:
            if x is None:
                items.append("*")
            else:
                items.append(str(x))
        return "_".join(items)

    def chunk_id_to_index(chunk_id):
        """ convert chunk_id to list of indices.
        Any '*' values will be replaced with None """
        indices = []
        items = chunk_id.split("_")
        for item in items:
            if item == "*":
                x = None
            else:
                x = int(item)
            indices.append(x)
        return indices

    num_chunks = getNumChunks(selection, layout)
    if num_chunks == 0:
        return []  # empty list
    if prefix is None:
        # construct a prefix using "c-" with the uuid of the dset_id
        if not dset_id.startswith("d-"):
            msg = f"Bad Request: invalid dset id: {dset_id}"
            log.warning(msg)
            raise ValueError(msg)
        prefix = "c-" + dset_id[2:] + "_"
    rank = len(selection)

    # initialize chunk_ids based on coordinate index, if any
    num_coordinates = None
    chunk_items = set()
    for s in selection:
        if isinstance(s, slice):
            continue
        elif num_coordinates is None:
            num_coordinates = len(s)
        else:
            if len(s) != num_coordinates:
                raise ValueError("coordinate length mismatch")

    if num_coordinates is None:
        # no coordinates, all slices
        num_coordinates = 1  # this will iniialize the list with one wildcard chunk index

    for i in range(num_coordinates):
        chunk_idx = []
        for dim in range(rank):
            s = selection[dim]
            c = layout[dim]
            if isinstance(s, slice):
                chunk_index = None
            else:
                chunk_index = s[i] // c
            chunk_idx.append(chunk_index)
        chunk_id = chunk_index_to_id(chunk_idx)
        chunk_items.add(chunk_id)
    chunk_ids = list(chunk_items)  # convert to a list, remove any dups
    # convert str ids back to indices
    chunk_items = []
    for chunk_id in chunk_ids:
        chunk_index = chunk_id_to_index(chunk_id)
        chunk_items.append(chunk_index)

    # log.debug(f"getChunkIds - selection: {selection}")
    for dim in range(rank):
        s = selection[dim]
        c = layout[dim]

        if not isinstance(s, slice):
            continue  # chunk indices for coordinate list already factored in

        # log.debug(f"getChunkIds - layout: {layout}")
        if s.step is None:
            s = slice(s.start, s.stop, 1)

        chunk_indices = []
        if s.step > c:
            # chunks may not be contiguous, skip along the selection and add
            # whatever chunks we land in
            for i in range(s.start, s.stop, s.step):
                chunk_index = i // c
                chunk_indices.append(chunk_index)
        else:
            # get a contiguous set of chunks along the selection
            if s.step > 1:
                num_points = frac((s.stop - s.start), s.step)
                w = num_points * s.step - (s.step - 1)
            else:
                w = s.stop - s.start  # selection width (>0)

            chunk_index_start = s.start // c
            chunk_index_end = frac((s.start + w), c)
            chunk_indices = list(range(chunk_index_start, chunk_index_end))

        # append the set of chunk_indices to our set of chunk_ids
        chunk_items_next = []
        for chunk_idx in chunk_items:
            for chunk_index in chunk_indices:
                chunk_idx_next = chunk_idx.copy()
                chunk_idx_next[dim] = chunk_index
                chunk_items_next.append(chunk_idx_next)
        chunk_items = chunk_items_next

    # convert chunk indices to chunk ids
    chunk_ids = []
    for chunk_idx in chunk_items:
        chunk_id = prefix + chunk_index_to_id(chunk_idx)
        chunk_ids.append(chunk_id)

    # got the complete list, return it!
    return chunk_ids


def getChunkSuffix(chunk_id):
    """given a chunk_id (e.g.: c-12345678-1234-1234-1234-1234567890ab_6_4)
    return the coordinates as a string. In this case 6_4
    """
    # go to the first underscore
    n = chunk_id.find("_") + 1
    if n == 0:
        raise ValueError(f"Invalid chunk_id: {chunk_id}")
    suffix = chunk_id[n:]
    return suffix


def getChunkCoordinate(chunk_id, layout):
    """given a chunk_id (e.g.: c-12345678-1234-1234-1234-1234567890ab_6_4)
    and a layout (e.g. (10,10))
    return the coordinates of the chunk in dataset space. In this case (60,40)
    """
    coord = getChunkIndex(chunk_id)
    for i in range(len(layout)):
        coord[i] *= layout[i]
    return coord


def getChunkSelection(chunk_id, slices, layout):
    """
    Return the intersection of the chunk with the given slices
    selection of the array.
    """
    chunk_index = getChunkIndex(chunk_id)
    rank = len(layout)
    sel = []

    coord_mask = None
    # compute a boolean mask for the coordinates that apply to the given chunk_id
    for dim in range(rank):
        s = slices[dim]
        c = layout[dim]
        n = chunk_index[dim] * c
        if isinstance(s, slice):
            continue
        if coord_mask is None:
            coord_mask = [True,] * len(s)
        if len(s) != len(coord_mask):
            raise ValueError("mismatched number of coordinates for fancy selection")

        for i in range(len(s)):
            if not coord_mask[i]:
                continue
            if s[i] < n or s[i] >= n + c:
                coord_mask[i] = False

    for dim in range(rank):
        s = slices[dim]
        c = layout[dim]
        n = chunk_index[dim] * c
        if isinstance(s, slice):
            if s.step is None:
                s = slice(s.start, s.stop, 1)
            if s.start >= n + c:
                return None  # null intersection
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
        else:
            # coord list
            coords = []
            for i in range(len(s)):
                if coord_mask[i]:
                    coords.append(s[i])
            sel.append(coords)

    return sel


def getChunkCoverage(chunk_id, slices, layout):
    """
    Get chunk-relative selection of the given chunk and selection.
    """
    chunk_index = getChunkIndex(chunk_id)
    chunk_sel = getChunkSelection(chunk_id, slices, layout)
    if not chunk_sel:
        log.warn(f"slices: {slices} does intersect chunk: {chunk_id}")
        return None

    rank = len(layout)
    if len(slices) != rank:
        raise ValueError(f"invalid slices value for dataset of rank: {rank}")
    sel = []
    for dim in range(rank):
        s = chunk_sel[dim]
        w = layout[dim]
        offset = chunk_index[dim] * w

        if isinstance(s, slice):
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
        else:
            coord = []
            for i in range(len(s)):
                coord.append(s[i] - offset)
            sel.append(tuple(coord))

    return sel


def getDataCoverage(chunk_id, slices, layout):
    """
    Get data-relative selection of the given chunk and selection.
    """

    chunk_sel = getChunkSelection(chunk_id, slices, layout)
    rank = len(layout)
    sel = []

    points = None
    coordinate_extent = None
    for dim in range(rank):
        c = chunk_sel[dim]
        s = slices[dim]
        if isinstance(s, slice):
            continue
        if isinstance(c, slice):
            msg = "expecting coordinate chunk selection for data "
            msg += "coord selection"
            raise ValueError(msg)
        if len(c) < 1:
            msg = "expected at least one chunk coordinate"
            raise ValueError(msg)
        if coordinate_extent is None:
            coordinate_extent = len(s)
        elif coordinate_extent != len(s):
            msg = "shape mismatch: indexing arrays could not be broadcast together "
            msg += f"with shapes ({coordinate_extent},) ({len(s)},)"
            raise ValueError(msg)
        else:
            pass

    if coordinate_extent is not None:
        points = np.zeros((coordinate_extent, rank), dtype=np.int64)
        points[:, :] = -1

    data_pts = None
    for dim in range(rank):
        c = chunk_sel[dim]
        s = slices[dim]
        if isinstance(s, slice):
            if s.step is None:
                s = slice(s.start, s.stop, 1)
            if c.step != s.step:
                msg = "expecting step for chunk selection to be the same as data selection"
                raise ValueError(msg)
            start = (c.start - s.start) // s.step
            stop = frac((c.stop - s.start), s.step)
            step = 1
            sel.append(slice(start, stop, step))
        else:
            # coordinate selection
            for i in range(len(s)):
                points[i, dim] = s[i]

            if data_pts is None:
                data_pts = []
                sel.append(data_pts)

    # now fill in the coordinate selection
    if data_pts is not None:
        chunk_coord = getChunkCoordinate(chunk_id, layout)
        for i in range(coordinate_extent):
            include_pt = True
            point = points[i]
            for dim in range(rank):
                point[dim]
                if point[dim] < 0:
                    continue  # this dim is a slice selection
                if point[dim] < chunk_coord[dim]:
                    include_pt = False
                    break
                if point[dim] >= chunk_coord[dim] + layout[dim]:
                    include_pt = False
                    break
            if include_pt:
                data_pts.append(i)

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


def get_chunktable_dims(shape_dims, chunk_dims):
    """
    Get the cannoncial size of the chunktable for a
    given dataset and chunk shape"""
    rank = len(shape_dims)
    table_dims = []
    for dim in range(rank):
        dset_extent = shape_dims[dim]
        chunk_extent = chunk_dims[dim]

        if dset_extent > 0 and chunk_extent > 0:
            # get integer ceil of dset and chunk extents
            table_extent = -(dset_extent // -chunk_extent)
        else:
            table_extent = 0
        table_dims.append(table_extent)
    table_dims = tuple(table_dims)
    return table_dims


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
        self._chunk_index = [
            0,
        ] * self._rank
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
            chunk_id += "_"
            chunk_id += str(self._chunk_index[i])
        # bump up the last index and carry forward if we run outside
        # the selection
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
                # reset to the start and continue iterating with
                # higher dimension
                self._chunk_index[dim] = s.start // c
            dim -= 1
        return chunk_id


def chunkReadSelection(chunk_arr, slices=None, select_dt=None):
    """
    Return data from requested chunk and selection
    """
    log.debug("chunkReadSelection")

    dims = chunk_arr.shape
    log.debug(f"got chunk dims: {dims}")
    rank = len(dims)
    if rank == 0:
        msg = "No dimension passed to chunkReadSelection"
        raise ValueError(msg)

    log.debug(f"got selection: {slices}")
    slices = tuple(slices)

    if select_dt is None:
        # no field selection
        select_dt = chunk_arr.dtype

    if len(slices) != rank:
        msg = "Selection rank does not match shape rank"
        raise ValueError(msg)

    dt = chunk_arr.dtype
    log.debug(f"dtype: {dt}")

    # get requested data
    output_arr = chunk_arr[slices]

    if len(select_dt) < len(dt):
        # do a field selection
        log.debug(f"select_dtype: {select_dt}")
        # create an array with just the given fields
        arr = np.zeros(output_arr.shape, select_dt)
        # slot in each of the given fields
        fields = select_dt.names
        if len(fields) > 1:
            for field in fields:
                arr[field] = output_arr[field]
        else:
            arr[...] = output_arr[fields[0]]
        output_arr = arr  # return this

    return output_arr


def chunkWriteSelection(chunk_arr=None, slices=None, data=None):
    """
    Write data for requested chunk and selection
    """

    log.debug(f"chunkWriteSelection for slices: {slices}")
    dims = chunk_arr.shape

    rank = len(dims)

    if rank == 0:
        msg = "No dimension passed to chunkWriteSelection"
        log.error(msg)
        raise ValueError(msg)
    if len(slices) != rank:
        msg = "Selection rank does not match dataset rank"
        log.error(msg)
        raise ValueError(msg)
    if len(data.shape) != rank:
        msg = "Input arr does not match dataset rank"
        log.error(msg)
        raise ValueError(msg)

    field_update = False
    if len(data.dtype) > 0:
        if len(data.dtype) < len(chunk_arr.dtype):
            field_update = True
            log.debug(f"ChunkWriteSelection for fields: {data.dtype.names}")
        else:
            log.debug("ChunkWriteSelection for all fields")

    updated = False
    try:
        if field_update:
            arr = chunk_arr[slices]
            # update each field of the selected region in the chunk
            updated = False
            field_updates = []
            for field in data.dtype.names:
                if not ndarray_compare(arr[field], data[field]):
                    # update the field
                    arr[field] = data[field]
                    updated = True
                    field_updates.append(field)
            if updated:
                # write back to the chunk
                chunk_arr[slices] = arr[...]
                log.debug(f"updated chunk arr for fields: {field_updates}")
        else:
            # check if the new data modifies the array or not
            # TBD - is this worth the cost of comparing two arrays element by element?
            if not ndarray_compare(chunk_arr[slices], data):
                # update chunk array
                chunk_arr[slices] = data
                updated = True
    except ValueError as ve:
        msg = f"array_equal ValueError, chunk_arr[{slices}]: {chunk_arr[slices]} "
        msg += f"data: {data}, data type: {type(data)} ve: {ve}"
        log.error(msg)
        raise

    log.debug(f"ChunkWriteSelection - chunk updated: {updated}")

    return updated


def chunkReadPoints(chunk_id=None,
                    chunk_layout=None,
                    chunk_arr=None,
                    point_arr=None,
                    select_dt=None
                    ):
    """
    Read points from given chunk
    """
    log.debug(f"chunkReadPoints - chunk_id: {chunk_id}")

    dims = chunk_arr.shape
    chunk_coord = getChunkCoordinate(chunk_id, dims)
    log.debug(f"chunk_coord: {chunk_coord}")
    rank = len(dims)
    if rank == 0:
        msg = "No dimension passed to chunk read points"
        raise ValueError(msg)

    dset_dtype = chunk_arr.dtype
    if select_dt is None:
        select_dt = dset_dtype  # no field selection

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

    output_arr = np.zeros((num_points,), dtype=select_dt)

    chunk_coord = getChunkCoordinate(chunk_id, chunk_layout)

    for i in range(num_points):
        # TBD: there's likely a better way to do this that
        # doesn't require iterating through each point...
        point = point_arr[i, :]
        tr_point = getChunkRelativePoint(chunk_coord, point)
        val = chunk_arr[tuple(tr_point)]
        if len(select_dt) < len(dset_dtype):
            # just update the relevant fields
            subfield_val = []
            for (x, field) in zip(val, dset_dtype.names):
                if field in select_dt.names:
                    subfield_val.append(x)
            val = tuple(subfield_val)
        output_arr[i] = val
    return output_arr


def chunkWritePoints(chunk_id=None,
                     chunk_layout=None,
                     chunk_arr=None,
                     point_arr=None,
                     select_dt=None):
    """
    Write points to given chunk
    """
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
    if select_dt is None:
        select_dt = dset_dtype  # no field selection
    log.debug(f"dtype: {dset_dtype}")

    # point_arr should have the following type:
    #       (coord1, coord2, ...) | select_dtype
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
        if dt_1 != select_dt:
            msg = "unexpected dtype for point array"
            raise ValueError(msg)

    num_points = len(point_arr)

    chunk_coord = getChunkCoordinate(chunk_id, chunk_layout)

    for i in range(num_points):
        elem = point_arr[i]
        log.debug(f"non-relative coordinate: {elem}")
        if rank == 1:
            coord = int(elem[0])
            coord = coord - chunk_coord[0]  # adjust to chunk relative
            if coord < 0 or coord >= dims[0]:
                msg = f"chunkWritePoints - invalid index: {int(elem[0])}"
                log.warn(msg)
                raise IndexError(msg)
        else:
            coord = elem[0]  # index to update
            for dim in range(rank):
                # adjust to chunk relative
                coord[dim] = int(coord[dim]) - chunk_coord[dim]
            coord = tuple(coord)  # need to convert to a tuple
        log.debug(f"relative coordinate: {coord}")

        val = elem[1]  # value
        if len(select_dt) < len(dset_dtype):
            # get the element from the chunk
            chunk_val = list(chunk_arr[coord])
            # and just update the relevant fields
            index = 0
            for (x, field) in zip(val, dset_dtype.names):
                if field in select_dt.names:
                    chunk_val[index] = x
                index += 1
            val = tuple(chunk_val)  # this will get written back

        chunk_arr[coord] = val  # update the point


def _getWhereFieldName(query):
    """
    Get the field name for a where clause.
    Returns None if no where statement
    """
    if query.startswith("where "):
        i = len("where ")
    else:
        i = query.find(" where ")
        if i > 0:
            i += len(" where ")
    if i < 0:
        # no where statement
        return None

    field_name = ""
    end_quote_char = None
    while i < len(query):
        ch = query[i]
        i += 1
        if end_quote_char and ch == end_quote_char:
            # end of variable
            end_quote_char = None
            break
        elif ch in ("'", '"'):
            end_quote_char = ch
            continue
        if field_name and not ch.isalnum() and not ch == "_" and not end_quote_char:
            # end of variable
            break
        if end_quote_char or ch.isalnum() or ch == "_":
            field_name += ch
    if not field_name:
        # got a where keyword, but no field name
        raise ValueError("query where with no fieldname")
    if end_quote_char:
        raise ValueError("unclosed quote")

    return field_name


def _getWhereElements(query):
    """
    Get the values from a where clause
    """

    n = query.find(" in ")
    if n < 0:
        raise ValueError("where query with no 'in' keyword")
    n += 4  # advance past " in "
    elements = []
    i = query[n:].find("(")
    if i < 0:
        raise ValueError("where in query with no '(' character)")
    i += n + 1  # advance past '('

    end_quote_char = None
    s = None

    while i < len(query):
        ch = query[i]
        i += 1
        if end_quote_char and ch == end_quote_char:
            # end of variable
            end_quote_char = None
            if s is None:
                s = ""
            elements.append(s)
            s = None
            continue
        if ch in ("'", '"'):
            end_quote_char = ch
            if s == "b":
                # use bytes not str
                s = b''
            else:
                s = ""
            continue
        if ch == ",":
            if s is not None:
                elements.append(s)
                s = None
            continue
        if ch == ")":
            if end_quote_char:
                raise ValueError("unclosed quote in 'where in' list")
            if s is not None:
                elements.append(s)
            break
        if ch.isspace():
            if end_quote_char:
                if isinstance(s, bytes):
                    ch = ch.encode('utf8')
                s += ch
            continue
        # anything else, just add to our variable
        if isinstance(s, bytes):
            ch = ch.encode('utf8')
        if s is None:
            s = ch
        else:
            s += ch

    if end_quote_char:
        raise ValueError("unclosed quote")

    return elements


def _getEvalStr(query, arr_name, field_names):
    """
    _getEvalStr: Get eval string for given query
    Gets Eval string to use with numpy where method.
    """
    i = 0
    eval_str = ""
    var_name = None
    end_quote_char = None
    var_count = 0
    paren_count = 0
    black_list = ("import",)  # field names that are not allowed
    for item in black_list:
        if item in field_names:
            msg = "invalid field name"
            log.warn("Bad query: " + msg)
            raise ValueError(msg)

    if query.startswith("where "):
        # no eval, return None
        return None
    # strip off any where clause after the query
    n = query.find(" where ")

    where_field = None
    if n > 0:
        where_field = _getWhereFieldName(query)
        log.debug(f"where field: [{where_field}]")
        log.debug(f"query orig: {query}")
        query = query[:n]
        log.debug(f"query adjusted: {query}")

    while i < len(query):
        ch = query[i]
        if (i + 1) < len(query):
            ch_next = query[i + 1]
        else:
            ch_next = None
        if var_name and not ch.isalnum() and not ch == "_":
            # end of variable
            if var_name not in field_names:
                # invalid
                msg = f"query variable: {var_name}"
                log.debug(f"field_names: {field_names}")
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
        elif ch.isalpha() or ch == "_":
            if ch == "b" and ch_next in ("'", '"'):
                eval_str += "b"  # start of a byte string literal
            elif var_name is None:
                var_name = ch  # start of a variable
            else:
                var_name += ch
        elif ch == "(" and end_quote_char is None:
            paren_count += 1
            eval_str += ch
        elif ch == ")" and end_quote_char is None:
            paren_count -= 1
            if paren_count < 0:
                msg = "Mismatched paren"
                log.warn("Bad query: " + msg)
                raise ValueError(msg)
            eval_str += ch
        else:
            # just add to eval_str
            eval_str += ch
        i = i + 1
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
    return eval_str


def getQueryDtype(dt):
    """make a dtype for query response"""
    field_names = dt.names
    #  make up a index field name that doesn't conflict with existing names
    index_name = "index"
    for i in range(len(field_names)):
        if index_name in field_names:
            index_name = "_" + index_name
        else:
            break

    dt_fields = [(index_name, "uint64"), ]
    for i in range(len(dt)):
        dt_fields.append((dt.names[i], dt[i]))
    query_dt = np.dtype(dt_fields)

    return query_dt


def chunkQuery(
    chunk_id=None,
    chunk_layout=None,
    chunk_arr=None,
    slices=None,
    query=None,
    query_update=None,
    select_dt=None,
    limit=0,
):
    """
    Run query on chunk and selection
    """
    msg = f"chunkQuery - chunk_id: {chunk_id} query: [{query}] slices: {slices}, "
    msg += f"limit: {limit} select_dt: {select_dt}"
    log.debug(msg)

    if not isinstance(chunk_arr, np.ndarray):
        raise TypeError("unexpected array type")

    dims = chunk_arr.shape

    rank = len(dims)

    dset_dt = chunk_arr.dtype
    if select_dt is None:
        select_dt = dset_dt

    if rank != 1:
        msg = "Query operations only supported on one-dimensional datasets"
        log.error(msg)
        raise ValueError(msg)

    if not slices:
        slices = [slice(0, dims[0], 1), ]
    log.debug(f"chunkQuery slices: {slices}")
    if len(slices) != rank:
        msg = "Selection rank does not match shape rank"
        log.error(msg)
        raise ValueError(msg)
    slices = tuple(slices)
    chunk_sel = chunk_arr[slices]

    chunk_coord = getChunkCoordinate(chunk_id, chunk_layout)

    # do query selection
    field_names = dset_dt.names

    # get the eval str
    eval_str = _getEvalStr(query, "chunk_sel", field_names)
    if eval_str:
        log.debug(f"eval_str: {eval_str}")
    else:
        log.debug("no eval_str")

    # check for a where in statement
    where_field = _getWhereFieldName(query)
    if where_field:
        log.debug(f"where_field: {where_field}")
        if where_field not in field_names:
            msg = f"where field {where_field} is not a member of dataset type"
            raise ValueError(msg)
        where_elements = _getWhereElements(query)
        if not where_elements:
            msg = "query: where key word with no elements"
            raise ValueError(msg)
        # convert to ndarray, checking that we can convert to our dtype along the way
        try:
            where_elements_arr = np.array(where_elements, dtype=dset_dt[where_field])
        except ValueError:
            msg = "where elements are not compatible with field datatype"
            raise ValueError(msg)
        isin_mask = np.isin(chunk_sel[where_field], where_elements_arr)

        if not np.any(isin_mask):
            # all false
            log.debug("query - no rows found for where elements")
            return None

        isin_indices = np.where(isin_mask)
        if not isinstance(isin_indices, tuple):
            log.warn(f"expected where_indices of tuple but got: {type(isin_indices)}")
            return None
        if len(isin_indices) == 0:
            log.warn("chunkQuery - got empty tuple where in result")
            return None

        isin_indices = isin_indices[0]
        if not isinstance(isin_indices, np.ndarray):
            log.warn(f"expected isin_indices of ndarray but got: {type(isin_indices)}")
            return None
        nrows = isin_indices.shape[0]
    elif eval_str:
        log.debug("no where keyword")
        isin_indices = None
    else:
        log.warn("query  - no eval and no where in, returning None")
        return None

    if query_update:
        if where_field:
            msg = "query update is not supported with where in"
            raise ValueError(msg)
        replace_mask = [None,] * len(field_names)
        for i in range(len(field_names)):
            field_name = field_names[i]
            if field_name in query_update:
                replace_mask[i] = query_update[field_name]
        log.debug(f"chunkQuery - replace_mask: {replace_mask}")
        replace_fields = [None, ] * len(field_names)
        if replace_mask == replace_fields:
            msg = "chunkQuery - no fields found in query_update"
            raise ValueError(msg)
    else:
        replace_mask = None

    if eval_str:
        where_indices = np.where(eval(eval_str))
        if not isinstance(where_indices, tuple):
            log.warn(f"expected where_indices of tuple but got: {type(where_indices)}")
            return None
        if len(where_indices) == 0:
            log.warn("chunkQuery - got empty tuple where result")
            return None

        where_indices = where_indices[0]
        if not isinstance(where_indices, np.ndarray):
            log.warn(f"expected where_indices of ndarray but got: {type(where_indices)}")
            return None
        nrows = where_indices.shape[0]
        log.debug(f"chunkQuery - {nrows} where rows found")
    else:
        where_indices = None

    if isin_indices is None:
        pass  # skip intersection
    else:
        if where_indices is None:
            # just use the isin_indices
            where_indices = isin_indices
        else:
            # interest the two sets of indices
            intersect = np.intersect1d(where_indices, isin_indices)

            nrows = intersect.shape[0]
            if nrows == 0:
                log.debug("chunkQuery - no rows found after intersect with is in")
                return None
            else:
                log.debug(f"chunkQuery - intersection, {nrows} found")
            # use the intsection as our new where index
            where_indices = intersect

    if limit > 0 and nrows > limit:
        # truncate to limit rows
        log.debug(f"limiting  response to {limit} rows")
        where_indices = where_indices[:limit]
        nrows = limit

    where_result = chunk_sel[where_indices].copy()

    if replace_mask and nrows > 0:
        log.debug(f"apply replace_mask: {replace_mask}")
        for i in range(len(field_names)):
            field = field_names[i]
            if replace_mask[i] is not None:
                where_result[field] = replace_mask[i]
        # update source array
        for i in range(nrows):
            index = where_indices[i]
            row = where_result[i]
            chunk_arr[index] = row

    # adjust the index to correspond with the dataset
    s = slices[0]
    if s.step is None:
        s = slice(s.start, s.stop, 1)
    start = s.start + chunk_coord[0]
    if start > 0:
        # can just increment every value by same amount
        where_indices += start
    if s.step and s.step > 1:
        for i in range(nrows):
            where_indices[i] = where_indices[i] + (s.step - 1) * i

    dt_rsp = getQueryDtype(select_dt)
    # construct response array
    rsp_arr = np.zeros((nrows,), dtype=dt_rsp)
    field_names = select_dt.names
    for field in field_names:
        rsp_arr[field] = where_result[field]
    index_name = dt_rsp.names[0]
    rsp_arr[index_name] = where_indices
    log.debug(f"chunkQuery returning {len(rsp_arr)} rows")

    return rsp_arr
