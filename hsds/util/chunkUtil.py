import numpy as np

from h5json.array_util import ndarray_compare
from h5json.dset_util import getDatasetLayout
from h5json import selections

from .. import hsds_logger as log

CHUNK_BASE = 16 * 1024  # Multiplier by which chunks are adjusted
CHUNK_MIN = 512 * 1024  # Soft lower limit (512k)
CHUNK_MAX = 2048 * 1024  # Hard upper limit (2M)
DEFAULT_TYPE_SIZE = 128  # Type size case when it is variable
PRIMES = [29, 31, 37, 41, 43, 47, 53, 59, 61, 67]  # for chunk partitioning


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


def _toArraySlice(s):
    """h5json normalizes slices to (start, start + num_points, step), so
    for step > 1 the stop value is a point count rather than a coordinate.
    Convert to an equivalent slice with a true coordinate stop, suitable
    for numpy indexing or coordinate-range arithmetic.
    """
    step = s.step if s.step is not None else 1
    if step == 1:
        return slice(s.start, s.stop, step)
    num_points = s.stop - s.start
    return slice(s.start, s.start + num_points * step, step)


def toNumpyIndex(selection):
    """Convert a selections.Selection into a tuple of slices/coordinate-lists
    usable for direct numpy array indexing (arr[toNumpyIndex(selection)]).
    """
    return tuple(_toArraySlice(s) if isinstance(s, slice) else s for s in selection.slices)


def getNumChunks(selection, layout):
    """
    Get the number of chunks potentially required.
    If selection is provided (a list of slices), return the number
    of chunks that intersect with the selection.
    """

    if not isinstance(selection, selections.Selection):
        msg = "Expected selection.Selection type"
        log.warning(msg)
        raise ValueError(msg)

    if selection.nselect == 0:
        # zero length selection
        return 0

    if selection.nselect == 1:
        # single point selection
        return 1

    rank = len(layout)
    if rank == 1 and layout[0] == 1:
        # scalar dataset
        return 1
    if len(selection.shape) != rank:
        msg = f"selection list has {len(selection.shape)} items, but rank is {rank}"
        raise ValueError(msg)

    # first, get the number of chunks needed for any coordinate selection
    chunk_indices = []
    for i in range(rank):
        s = selection.slices[i]
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
    for i in range(rank):
        s = selection.slices[i]
        c = layout[i]  # chunk size
        if not isinstance(s, slice):
            # ignore coordinate lists since we dealt with them above
            continue

        step = s.step if s.step is not None else 1
        # h5json normalizes slices to (start, start + num_points, step), so
        # (s.stop - s.start) is the number of points selected along this
        # dimension rather than a coordinate span
        num_points = s.stop - s.start
        if step > 1:
            w = num_points * step - (step - 1)
        else:
            w = num_points  # selection width (>0)

        lc = frac(s.start, c) * c

        if s.start + w <= lc:
            # looks like we just cross one chunk along this dimension
            continue

        rc = ((s.start + w) // c) * c
        m = rc - lc
        if c > step:
            count = m // c
        else:
            count = m // step
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
    log.debug(f"getChunkId - dset_id: {dset_id}, point: {point}, layout: {layout}")

    for dim in range(rank):
        coord = None
        if rank == 1 and not isinstance(point, (list, tuple, np.ndarray)):
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

    layout_json = getDatasetLayout(dset_json)
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

    log.debug(f"getChunkIds - dset_id: {dset_id}, selection: {selection}, layout: {layout}")
    if prefix:
        log.debug(f"prefix: {prefix}")
    if not isinstance(selection, selections.Selection):
        msg = "Expected selection.Selection type"
        log.warning(msg)
        raise ValueError(msg)

    num_chunks = getNumChunks(selection, layout)
    log.debug(f"getChunkIds - num_chunks: {num_chunks}")
    if num_chunks == 0:
        return []  # empty list
    if prefix is None:
        # construct a prefix using "c-" with the uuid of the dset_id
        if not dset_id.startswith("d-"):
            msg = f"Bad Request: invalid dset id: {dset_id}"
            log.warning(msg)
            raise ValueError(msg)
        prefix = "c-" + dset_id[2:] + "_"

    if selection.shape == ():
        # scalar dataset - single chunk, index 0 for each layout dimension
        return [prefix + chunk_index_to_id([0] * len(layout))]
    rank = len(selection.shape)

    # initialize chunk_ids based on coordinate index, if any
    num_coordinates = None
    chunk_items = set()
    for s in selection.slices:
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
            s = selection.slices[dim]
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
        s = selection.slices[dim]
        c = layout[dim]

        if not isinstance(s, slice):
            continue  # chunk indices for coordinate list already factored in

        # log.debug(f"getChunkIds - layout: {layout}")
        if s.step is None:
            s = slice(s.start, s.stop, 1)

        # h5json normalizes slices to (start, start + num_points, step), so
        # (s.stop - s.start) is the number of points selected along this
        # dimension rather than a coordinate span
        num_points = s.stop - s.start
        chunk_indices = []
        if s.step > c:
            # chunks may not be contiguous, skip along the selection and add
            # whatever chunks we land in
            for k in range(num_points):
                i = s.start + k * s.step
                chunk_index = i // c
                chunk_indices.append(chunk_index)
        else:
            # get a contiguous set of chunks along the selection
            if s.step > 1:
                w = num_points * s.step - (s.step - 1)
            else:
                w = num_points  # selection width (>0)

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


def getChunkSelection(chunk_id, selection, layout):
    """
    Return the intersection of the chunk with the given slices
    selection of the array.
    """
    chunk_index = getChunkIndex(chunk_id)
    rank = len(layout)
    sel = []

    coord_mask = None
    # compute a boolean mask for the coordinates that apply to the given chunk_id
    slices = selection.slices
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
            s = _toArraySlice(s)
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

    return selections.select(selection.shape, tuple(sel))


def getChunkCoverage(chunk_id, selection, layout):
    """
    Get chunk-relative selection of the given chunk and selection.
    """
    chunk_index = getChunkIndex(chunk_id)
    chunk_sel = getChunkSelection(chunk_id, selection, layout)
    if not chunk_sel:
        log.warn(f"selection: {selection} does intersect chunk: {chunk_id}")
        return None

    rank = len(layout)
    if len(selection.shape) != rank:
        raise ValueError(f"invalid slices value for dataset of rank: {rank}")
    sel = []
    for dim in range(rank):
        s = chunk_sel.slices[dim]
        w = layout[dim]
        offset = chunk_index[dim] * w

        if isinstance(s, slice):
            s = _toArraySlice(s)
            start = s.start - offset
            if start < 0:
                msg = "Unexpected chunk selection"
                log.error(msg)
                raise ValueError(msg)
            stop = slice_stop(s) - offset
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
            sel.append(coord)

    return selections.select(tuple(layout), tuple(sel))


def getDataCoverage(chunk_id, selection, layout):
    """
    Get data-relative selection of the given chunk and selection.
    """

    chunk_sel = getChunkSelection(chunk_id, selection, layout)
    chunk_slices = chunk_sel.slices
    rank = len(layout)
    sel = []

    points = None
    coordinate_extent = None
    slices = selection.slices
    for dim in range(rank):
        c = chunk_slices[dim]
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
        c = chunk_slices[dim]
        s = slices[dim]
        if isinstance(s, slice):
            s = _toArraySlice(s)
            c = _toArraySlice(c)
            c = slice(c.start, slice_stop(c), c.step)
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

    return selections.select(selection.mshape, tuple(sel))


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
        if not isinstance(selection, selections.Selection):
            msg = "Expected selection.Selection type"
            log.warning(msg)
            raise ValueError(msg)
        if selection.select_type != selections.H5S_SEL_HYPERSLABS:
            msg = "Expected hyperslab selection"
            log.warning(msg)
            raise ValueError(msg)
        self._selection = selection
        self._rank = len(selection.shape)
        self._chunk_index = [0,] * self._rank
        for i in range(self._rank):
            s = selection.slices[i]
            c = layout[i]
            self._chunk_index[i] = s.start // c

    def __iter__(self):
        return self

    def next(self):
        slices = self._selection.slices
        if self._chunk_index[0] * self._layout[0] >= slices[0].stop:
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
            s = slices[dim]
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


def chunkReadSelection(chunk_arr, selection=None, select_dt=None):
    """
    Return data from requested chunk and selection
    """
    log.debug("chunkReadSelection")

    if selection is None:
        selection = selections.select(chunk_arr.shape, ...)
    if not isinstance(selection, selections.Selection):
        msg = "Expected selection.Selection type"
        log.warning(msg)
        raise ValueError(msg)

    dims = chunk_arr.shape
    log.debug(f"got chunk dims: {dims}")
    rank = len(dims)
    if rank == 0:
        msg = "No dimension passed to chunkReadSelection"
        raise ValueError(msg)

    log.debug(f"got selection: {selection}")
    slices = toNumpyIndex(selection)

    if select_dt is None:
        # no field selection
        select_dt = chunk_arr.dtype

    if len(slices) != rank:
        msg = "Selection rank does not match shape rank"
        raise ValueError(msg)

    dt = chunk_arr.dtype

    # get requested data
    output_arr = chunk_arr[slices]

    if len(select_dt) < len(dt):
        # do a field selection
        if select_dt:
            if len(select_dt) < 10:
                log.debug(f"select_dtype: {select_dt}")
            else:
                log.debug(f"select_dtype: {len(select_dt)} from {len(dt)} fields")
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


def chunkWriteSelection(chunk_arr=None, selection=None, data=None):
    """
    Write data for requested chunk and selection
    """

    if selection is None:
        selection = selections.select(chunk_arr.shape, ...)
    if not isinstance(selection, selections.Selection):
        msg = "Expected selection.Selection type"
        log.warning(msg)
        raise ValueError(msg)
    if len(selection.shape) != len(chunk_arr.shape):
        msg = "Selection rank does not match dataset rank"
        log.error(msg)
        raise ValueError(msg)
    log.debug(f"chunkWriteSelection for selection: {selection}")
    dims = chunk_arr.shape

    rank = len(dims)

    if rank == 0:
        msg = "No dimension passed to chunkWriteSelection"
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
    slices = toNumpyIndex(selection)
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
    else:
        log.debug(f"select dtype: {dset_dtype}")

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
