import numpy as np
from aiohttp.errors import HttpBadRequest 
import hsds_logger as log

CHUNK_BASE = 16*1024    # Multiplier by which chunks are adjusted
CHUNK_MIN = 8*1024      # Soft lower limit (8k)
CHUNK_MAX = 1024*1024   # Hard upper limit (1M)

def guess_chunk(shape_json, typesize):
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
        shape = [] 
        maxdims = shape_json["maxdims"]
        # convert H5S_UNLIIMITED to 0
        for i in range(len(maxdims)):
            if maxdims[i] == 'H5S_UNLIMITED':
                shape.append(0)  
            else:
                shape.append(maxdims[i])
    else:
        shape = shape_json["dims"]

    if typesize == 'H5T_VARIABLE':
        typesize = 128  # just take a guess at the item size 

    # For unlimited dimensions we have to guess 1024
    shape = tuple((x if x!=0 else 1024) for i, x in enumerate(shape))

    ndims = len(shape)
    if ndims == 0:
        raise ValueError("Chunks not allowed for scalar datasets.")

    chunks = np.array(shape, dtype='=f8')
    if not np.all(np.isfinite(chunks)):
        raise ValueError("Illegal value in chunk tuple")

    # Determine the optimal chunk size in bytes using a PyTables expression.
    # This is kept as a float.
    dset_size = np.product(chunks)*typesize
    target_size = CHUNK_BASE * (2**np.log10(dset_size/(1024.*1024)))

    if target_size > CHUNK_MAX:
        target_size = CHUNK_MAX
    elif target_size < CHUNK_MIN:
        target_size = CHUNK_MIN

    idx = 0
    while True:
        # Repeatedly loop over the axes, dividing them by 2.  Stop when:
        # 1a. We're smaller than the target chunk size, OR
        # 1b. We're within 50% of the target chunk size, AND
        #  2. The chunk is smaller than the maximum chunk size

        chunk_bytes = np.product(chunks)*typesize

        if (chunk_bytes < target_size or \
         abs(chunk_bytes-target_size)/target_size < 0.5) and \
         chunk_bytes < CHUNK_MAX:
            break

        if np.product(chunks) == 1:
            break  # Element size larger than CHUNK_MAX

        chunks[idx%ndims] = np.ceil(chunks[idx%ndims] / 2.0)
        idx += 1

    return tuple(int(x) for x in chunks)

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
            log.info("null selection")
            return 0
    num_chunks = 1
    for i in range(len(selection)): 
        s = selection[i]
        w = s.stop - s.start # selection width (>0)
        c = layout[i]   # chunk size
        partial_left = (c - (s.start % c)) % c
        remainder = w - partial_left
        if remainder == 0:
            # if raminder is 0, then we just cross one chunk along this dimensions
            continue
        partial_right = remainder % c
        count = (remainder - partial_right) // c
        if partial_right > 0:
            count += 1
        if partial_left > 0:
            count += 1
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
        chunk_index= coord // c
        chunk_id +=  str(chunk_index)
        if dim + 1 < rank:
            chunk_id += '_' # seperate dimensions with underscores
    # got the complete chunk_id
    return chunk_id

def getChunkIds(dset_id, selection, layout, dim=0, prefix=None, chunk_ids=None):
    num_chunks = getNumChunks(selection, layout)
    if num_chunks == 0:
        return []  # empty list
    if prefix is None:
        # construct a prefix using "c-" with the uuid of the dset_id
        if not dset_id.startswith("d-"):
            msg = "Bad Request: invalid dset id: {}".format(dset_id)
            log.warn(msg)
            raise HttpBadRequest(message=msg)
        prefix = "c-" + dset_id[2:] + '_'
    rank = len(selection)
    if chunk_ids is None:
        chunk_ids = []
    s = selection[dim]
    c = layout[dim]
    chunk_index_start = s.start // c
    chunk_index_end = s.stop // c
    if s.stop % c != 0:
        chunk_index_end += 1
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
        w = layout[dim]
        n = chunk_index[dim] * w 
        if s.start < n:
            start = n
        else:
            start = s.start
        if s.start >= n + w:
            return None  # null intersection
        if s.stop >= n + w:
            stop = n + w
        else:
            stop = s.stop

        step = 1 # TBD - deal with steps > 1
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
        step = 1  # TBD - update for step > 1
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
        start = c.start - s.start
        stop = c.stop - s.start
        step = 1
        sel.append(slice(start, stop, step))
            
    return sel

def getChunkRelativePoint(chunkCoord, point):
    """
    Get chunk-relative coordinate of the given point

       chunkIndex: ndarray of chunk coordinates
       point: ndarray of element in dset
    Return: chunk-relative coordinates of point
    """
    tr = point.copy()
    for i in range(len(point)):
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
        
         

 

