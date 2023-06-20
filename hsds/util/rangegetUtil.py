from operator import attrgetter
from collections import namedtuple

ChunkLocation = namedtuple("ChunkLocation", ["index", "offset", "length"])

def _chunk_start(c):
    """ return start of byte range for given chunk or chunk list """
    start = None
    if isinstance(c, list):
        for e in c:
            if start is None or e.offset < start:
                start = e.offset 
    else:
        start = c.offset
    return start

def _chunk_end(c):
    """ return end of byte range for given chunk or chunk list """
    end = None
    if isinstance(c, list):
        for e in c:
            if end is None or e.offset + e.length > end:
                end = e.offset + e.length
    else:
        end = c.offset + c.length
    return end

def _chunk_dist(chunk_left, chunk_right):
    """ return byte seperation of two h5 chunks """
    left_start = _chunk_start(chunk_left)
    left_end = _chunk_end(chunk_left)
    right_start = _chunk_start(chunk_right)
    right_end = _chunk_end(chunk_right)

    if left_start < right_start:
        dist = right_start - left_end
    else:
        dist = left_start - right_end
    if dist < 0:
        raise ValueError("unexpected chunk position")
    return dist
        


def _find_min_pair(h5chunks, max_gap=None):
    """ Given a list of chunk_map entries which are sorted by offset,
        return the two chunks nearest to each other in the file.
        If max_gap is set, chunms must be within max_gap bytes   
    """
    num_chunks = len(h5chunks)

    if num_chunks < 2:
        return None
    
    min_pair = None
    min_dist = None

    for i in range(1, num_chunks):
        c1 = h5chunks[i-1]
        c2 = h5chunks[i]
        d = _chunk_dist(c1, c2)
        if d == 0:
            # short-circuit search and just return this pair
            return (i-1, i)
        if d > max_gap:
            continue
        if min_dist is None or d < min_dist:
            min_pair = (i-1, i)
            min_dist = d
    return min_pair

    
def chunkMunge(h5chunks, max_gap=1024):
    """ given a list of ChunkLocations, 
         return list of list of chunk items where
         items in the list our within max_gap of each other """
    
    # sort chunk locations by offset
    munged = sorted(h5chunks, key=attrgetter('offset'))
    while True:
        min_pair = _find_min_pair(munged, max_gap=max_gap)
        if min_pair is None:
            break
        left = min_pair[0]
        right = min_pair[1]
        
        # combine the left and right pair, taking care to
        # return a list of ChunkLocations
        chunk_left = munged[left]
        chunk_right = munged[right]

        if isinstance(chunk_left, list):
            combined = chunk_left
        else:
            combined = [chunk_left, ]
        if isinstance(chunk_right, list):
            combined.extend(chunk_right)
        else:
            combined.append(chunk_right)

        mungier = []
        if left > 0:
            mungier.extend(munged[:left])

        mungier.append(combined)

        if right < len(munged) - 1:
            mungier.extend(munged[(right+1):])

        munged = mungier

    return munged
        
                               
            


