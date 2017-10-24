from aiohttp.errors import HttpBadRequest 
import hsds_logger as log

CHUNK_BASE =  16*1024   # Multiplier by which chunks are adjusted
CHUNK_MIN =  512*1024   # Soft lower limit (512k)
CHUNK_MAX = 2048*1024   # Hard upper limit (2M) 
DEFAULT_TYPE_SIZE = 128 # Type size case when it is variable

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

def expandChunk(layout, typesize, shape_json, chunk_min=CHUNK_MIN):
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

def shrinkChunk(layout, typesize, chunk_max=CHUNK_MAX):
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
        chunk_index= coord // c
        chunk_id +=  str(chunk_index)
        if dim + 1 < rank:
            chunk_id += '_' # seperate dimensions with underscores
    # got the complete chunk_id
    return chunk_id

def getDatasetId(chunk_id):
    """ Get dataset id given a chunk id
    """
    obj_uuid = chunk_id[2:38]
    dset_id = "d-" + obj_uuid
    return dset_id
 

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
            raise HttpBadRequest(message=msg)
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
        
         

 

