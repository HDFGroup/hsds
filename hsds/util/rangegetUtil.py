from operator import attrgetter
from collections import namedtuple
import numpy as np

# HDF5 file chunk location named tuple
#  index: index of the chunk
#  offset: byte offset from start of HDF5 file
#  length: number of bytes used in the file
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


def getHyperChunkFactors(chunk_dims, hyper_dims):
    """ return list of rations betwen chunk and hyperchunkdims """

    factors = []
    rank = len(chunk_dims)
    if len(hyper_dims) != rank:
        raise ValueError("unexpected length for hyper_dims")
    for i in range(rank):
        chunk_extent = chunk_dims[i]
        hyper_extent = hyper_dims[i]
        if chunk_extent % hyper_extent != 0:
            raise ValueError("unexpected value for hyper_dims")
        factor = chunk_extent // hyper_extent
        factors.append(factor)
    return factors


def getHyperChunkIndex(i, factors):
    """ return index of ith hyperchunk based on the chunk factors
        e.g. for factors: [2,3,4], the 5th index will be: 0_1_1
    """

    rank = len(factors)
    index = []
    for dim in range(rank):
        factor = int(np.prod(factors[(dim + 1):]))
        n = (i // factor) % factors[dim]
        index.append(n)
    return tuple(index)


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
        return the indicies of the two chunks nearest to each other in the file.
        If max_gap is set, chunks must be within max_gap bytes
    """
    num_chunks = len(h5chunks)

    if num_chunks < 2:
        return None

    min_pair = None
    min_dist = None

    for i in range(1, num_chunks):
        c1 = h5chunks[i - 1]
        c2 = h5chunks[i]
        d = _chunk_dist(c1, c2)
        if d == 0:
            # short-circuit search and just return this pair
            return (i - 1, i)
        if d > max_gap:
            continue
        if min_dist is None or d < min_dist:
            min_pair = (i - 1, i)
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
            # no min_pair, so we are done
            break

        left = min_pair[0]
        right = min_pair[1]

        # combine the left and right pair, taking care to
        # return a list of ChunkLocations
        chunk_left = munged[left]
        chunk_right = munged[right]

        # combine one of:
        #   * chunk and chunk
        #   * chunk and list of chunks
        #   * list of chunks and chunk
        #   * list of chunks and list of chunks
        # result should be a list of chunks
        if isinstance(chunk_left, list):
            combined = chunk_left
        else:
            combined = [chunk_left, ]
        if isinstance(chunk_right, list):
            combined.extend(chunk_right)
        else:
            combined.append(chunk_right)

        # create a new list of the original entries
        # along with our combined element
        mungier = []
        if left > 0:
            mungier.extend(munged[:left])
        mungier.append(combined)
        if right < len(munged) - 1:
            mungier.extend(munged[(right + 1):])

        # repeat till we can't reduce the list size anymore
        munged = mungier

    return munged
