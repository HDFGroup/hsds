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
import unittest
import sys
import json
import numpy as np

sys.path.append('../..')
from hsds.util.dsetUtil import getHyperslabSelection
from hsds.util.chunkUtil import guessChunk, getNumChunks, getChunkIds, getChunkId, getPartitionKey, getChunkPartition
from hsds.util.chunkUtil import getChunkIndex, getChunkSelection, getChunkCoverage, getDataCoverage, ChunkIterator
from hsds.util.chunkUtil import getChunkSize, shrinkChunk, expandChunk, getDatasetId, getContiguousLayout, _getEvalStr
from hsds.util.chunkUtil import chunkReadSelection, chunkWriteSelection, chunkReadPoints, chunkWritePoints, chunkQuery


class ChunkUtilTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(ChunkUtilTest, self).__init__(*args, **kwargs)
        # main


    def testGuessChunk(self):

        typesize = 'H5T_VARIABLE'

        shape = {"class": 'H5S_NULL' }
        layout = guessChunk(shape, typesize)
        self.assertTrue(layout is None)

        shape = {"class": 'H5S_SCALAR' }
        layout = guessChunk(shape, typesize)
        self.assertEqual(layout, (1,))

        shape = {"class": 'H5S_SIMPLE', "dims": [100, 100]}
        layout = guessChunk(shape, typesize)
        self.assertTrue(len(layout), 2)
        for i in range(2):
            self.assertTrue(layout[i] >= 1)
            self.assertTrue(layout[i] <= 100)

        typesize = 8
        layout = guessChunk(shape, typesize)
        self.assertTrue(len(layout), 2)
        for i in range(2):
            self.assertTrue(layout[i] >= 1)
            self.assertTrue(layout[i] <= 100)

        shape = {"class": 'H5S_SIMPLE', "dims": [5]}
        layout = guessChunk(shape, typesize)
        self.assertEqual(layout, (5,))

        shape = {"class": 'H5S_SIMPLE', "dims": [100, 100, 100]}
        layout = guessChunk(shape, typesize)
        self.assertTrue(len(layout), 3)
        for i in range(3):
            self.assertTrue(layout[i] >= 1)
            self.assertTrue(layout[i] <= 100)

        shape = {"class": 'H5S_SIMPLE', "dims": [100, 0], "maxdims": [100, 0]}
        layout = guessChunk(shape, typesize)
        self.assertTrue(len(layout), 2)
        for i in range(2):
            self.assertTrue(layout[i] >= 1)
            self.assertTrue(layout[i] <= 1024)

        shape = {"class": 'H5S_SCALAR'}
        layout = guessChunk(shape, typesize)
        self.assertEqual(layout, (1,))

        shape = {"class": 'H5S_NULL'}
        layout = guessChunk(shape, typesize)
        self.assertEqual(layout, None)

    def testShrinkChunk(self):
        CHUNK_MIN = 500
        CHUNK_MAX = 5000
        typesize = 1
        layout = (1, 2, 3)
        shrunk = shrinkChunk(layout, typesize, chunk_max=CHUNK_MAX)
        self.assertEqual(shrunk, layout)

        layout = (100, 200, 300)
        num_bytes = getChunkSize(layout, typesize)
        self.assertTrue(num_bytes > CHUNK_MAX)
        shrunk = shrinkChunk(layout, typesize, chunk_max=CHUNK_MAX)
        rank = len(layout)
        for i in range(rank):
            self.assertTrue(shrunk[i] >= 1)
            self.assertTrue(shrunk[i] <= 1000*(i+1))
        num_bytes = getChunkSize(shrunk, typesize)
        self.assertTrue(num_bytes > CHUNK_MIN)
        self.assertTrue(num_bytes < CHUNK_MAX)

        layout = (300, 200, 100)
        num_bytes = getChunkSize(layout, typesize)
        self.assertTrue(num_bytes > CHUNK_MAX)
        shrunk = shrinkChunk(layout, typesize, chunk_max=CHUNK_MAX)
        rank = len(layout)
        for i in range(rank):
            self.assertTrue(shrunk[i] >= 1)
            self.assertTrue(shrunk[i] <= 1000*(3-i))
        num_bytes = getChunkSize(shrunk, typesize)
        self.assertTrue(num_bytes > CHUNK_MIN)
        self.assertTrue(num_bytes < CHUNK_MAX)

    def testExpandChunk(self):
        CHUNK_MIN = 5000
        CHUNK_MAX = 50000
        typesize = 1
        shape = {"class": 'H5S_SIMPLE', "dims": [10, 10, 10]}
        layout = (10, 10, 10)
        num_bytes = getChunkSize(layout, typesize)
        self.assertTrue(num_bytes < CHUNK_MIN)
        expanded = expandChunk(layout, typesize, shape, chunk_min=CHUNK_MIN)
        num_bytes = getChunkSize(expanded, typesize)
        # chunk layout can't be larger than dataspace
        self.assertTrue(num_bytes < CHUNK_MIN)
        self.assertEqual(expanded, (10, 10, 10))


        shape = {"class": 'H5S_SIMPLE', "dims": [1000, 2000, 3000]}
        layout = (10, 10, 10)
        num_bytes = getChunkSize(layout, typesize)
        self.assertTrue(num_bytes < CHUNK_MIN)
        expanded = expandChunk(layout, typesize, shape, chunk_min=CHUNK_MIN)
        num_bytes = getChunkSize(expanded, typesize)
        self.assertTrue(num_bytes > CHUNK_MIN)
        self.assertTrue(num_bytes < CHUNK_MAX)


        shape = {"class": 'H5S_SIMPLE', "dims": [1000, 10, 1000], "maxdims": [1000, 100, 1000]}
        layout = (10, 10, 10)
        num_bytes = getChunkSize(layout, typesize)
        self.assertTrue(num_bytes < CHUNK_MIN)
        expanded = expandChunk(layout, typesize, shape, chunk_min=CHUNK_MIN)
        num_bytes = getChunkSize(expanded, typesize)
        self.assertTrue(num_bytes > CHUNK_MIN)
        self.assertTrue(num_bytes < CHUNK_MAX)

        shape = {"class": 'H5S_SIMPLE', "dims": [1000, 0, 1000], "maxdims": [1000, 100, 1000]}
        layout = (10, 10, 10)
        num_bytes = getChunkSize(layout, typesize)
        self.assertTrue(num_bytes < CHUNK_MIN)
        expanded = expandChunk(layout, typesize, shape, chunk_min=CHUNK_MIN)
        num_bytes = getChunkSize(expanded, typesize)
        self.assertTrue(num_bytes > CHUNK_MIN)
        self.assertTrue(num_bytes < CHUNK_MAX)

        shape = {"class": 'H5S_SIMPLE', "dims": [1000, 10, 1000], "maxdims": [1000, 0, 1000]}
        layout = (10, 10, 10)
        num_bytes = getChunkSize(layout, typesize)
        self.assertTrue(num_bytes < CHUNK_MIN)
        expanded = expandChunk(layout, typesize, shape, chunk_min=CHUNK_MIN)
        num_bytes = getChunkSize(expanded, typesize)
        self.assertTrue(num_bytes > CHUNK_MIN)
        self.assertTrue(num_bytes < CHUNK_MAX)


    def testGetContiguiousLayout(self):

        typesize = 4
        chunk_min=400
        chunk_max=800

        try:
            shape = {"class": 'H5S_SIMPLE', "dims": [100, 100]}
            layout = getContiguousLayout(shape, 'H5T_VARIABLE')
            self.assertTrue(False)
        except ValueError:
            pass # expected

        shape = {"class": 'H5S_NULL' }
        layout = getContiguousLayout(shape, typesize)
        self.assertTrue(layout is None)

        shape = {"class": 'H5S_SCALAR' }
        layout = getContiguousLayout(shape, typesize)
        self.assertEqual(layout, (1,))

        for extent in (1, 100, 10000):
            dims = [extent,]
            shape = {"class": 'H5S_SIMPLE', "dims": dims}
            layout = getContiguousLayout(shape, typesize, chunk_min=chunk_min, chunk_max=chunk_max)
            self.assertTrue(len(layout), 1)
            chunk_bytes = layout[0]*typesize
            space_bytes = extent*typesize
            if space_bytes > chunk_min:
                self.assertTrue(chunk_bytes >= chunk_min)

            self.assertTrue(chunk_bytes <= chunk_max)

        for extent in (1, 10, 100):
            dims = [extent, extent]
            shape = {"class": 'H5S_SIMPLE', "dims": dims}
            layout = getContiguousLayout(shape, typesize, chunk_min=chunk_min, chunk_max=chunk_max)
            self.assertTrue(len(layout), 2)
            for i in range(2):
                self.assertTrue(layout[i] >= 1)
                self.assertTrue(layout[i] <= extent)
            self.assertEqual(layout[1], extent)

            chunk_bytes = layout[0]*layout[1]*typesize
            space_bytes = extent*extent*typesize
            if space_bytes > chunk_min:
                self.assertTrue(chunk_bytes >= chunk_min)
            self.assertTrue(chunk_bytes <= chunk_max)

        for extent in (1, 10, 100):
            dims = [extent, extent, 50]
            shape = {"class": 'H5S_SIMPLE', "dims": dims}
            layout = getContiguousLayout(shape, typesize, chunk_min=chunk_min, chunk_max=chunk_max)
            self.assertTrue(len(layout), 3)
            for i in range(3):
                self.assertTrue(layout[i] >= 1)
                self.assertTrue(layout[i] <= dims[i])

            chunk_bytes = layout[0]*layout[1]*layout[2]*typesize
            space_bytes = dims[0]*dims[1]*dims[2]*typesize
            if space_bytes > chunk_min:
                # chunk size maybe less than chunk_min in this case
                # self.assertTrue(chunk_bytes >= chunk_min)
                self.assertEqual(layout[0], 1)
            self.assertTrue(chunk_bytes <= chunk_max)

    def testGetNumChunks(self):
        datashape = [100,]
        layout = (10,)
        selection = getHyperslabSelection(datashape)
        count = getNumChunks(selection, layout)
        self.assertEqual(count, 10)
        selection = getHyperslabSelection(datashape, 12, 83)
        count = getNumChunks(selection, layout)
        self.assertEqual(count, 8)
        selection = getHyperslabSelection(datashape, 12, 80)
        count = getNumChunks(selection, layout)
        self.assertEqual(count, 7)
        selection = getHyperslabSelection(datashape, 10, 83)
        count = getNumChunks(selection, layout)
        self.assertEqual(count, 8)
        selection = getHyperslabSelection(datashape, 12, 17)
        count = getNumChunks(selection, layout)
        self.assertEqual(count, 1)

        # try with different increment
        selection = getHyperslabSelection(datashape, 0, 10, 5)
        count = getNumChunks(selection, layout)
        self.assertEqual(count, 1)
        selection = getHyperslabSelection(datashape, 0, 11, 5)
        count = getNumChunks(selection, layout)
        self.assertEqual(count, 2)
        selection = getHyperslabSelection(datashape, 6, 11, 5)
        count = getNumChunks(selection, layout)
        self.assertEqual(count, 1)
        selection = getHyperslabSelection(datashape, 12, 83, 2)
        count = getNumChunks(selection, layout)
        self.assertEqual(count, 8)
        selection = getHyperslabSelection(datashape, 12, 83, 20)
        count = getNumChunks(selection, layout)
        self.assertEqual(count, 4)
        selection = getHyperslabSelection(datashape, 10, 83, 20)
        count = getNumChunks(selection, layout)
        self.assertEqual(count, 4)


        datashape = [100,100]
        layout = (10,5)
        selection = getHyperslabSelection(datashape)
        count = getNumChunks(selection, layout)
        self.assertEqual(count, 200)
        selection = getHyperslabSelection(datashape, (41, 6), (49, 9))
        count = getNumChunks(selection, layout)
        self.assertEqual(count, 1)
        selection = getHyperslabSelection(datashape, (39, 4), (47, 7))
        count = getNumChunks(selection, layout)
        self.assertEqual(count, 4)
        # try with different increment
        selection = getHyperslabSelection(datashape, (39, 4), (47, 7), (3, 2))
        count = getNumChunks(selection, layout)
        self.assertEqual(count, 4)
        selection = getHyperslabSelection(datashape, (0, 0), (100, 100), (20, 40))
        count = getNumChunks(selection, layout)
        self.assertEqual(count, 15)



    def testGetChunkIds(self):
        # getChunkIds(dset_id, selection, layout, dim=0, prefix=None, chunk_ids=None):
        dset_id = "d-12345678-1234-1234-1234-1234567890ab"

        datashape = [1,]
        layout = (1,)
        selection = getHyperslabSelection(datashape)
        chunk_ids = getChunkIds(dset_id, selection, layout)
        self.assertEqual(len(chunk_ids), 1)
        chunk_id = chunk_ids[0]
        self.assertTrue(chunk_id.startswith("c-"))
        self.assertTrue(chunk_id.endswith('_0'))
        self.assertEqual(chunk_id[2:-2], dset_id[2:])
        self.assertEqual(len(chunk_id), 2+36+2)
        self.assertEqual(getDatasetId(chunk_id), dset_id)

        datashape = [100,]
        layout = (10,)
        selection = getHyperslabSelection(datashape)
        chunk_ids = getChunkIds(dset_id, selection, layout)
        partition_count = 10
        self.assertEqual(len(chunk_ids), 10)
        for i in range(10):
            chunk_id = chunk_ids[i]
            # chunk_id should look like:
            # c-12345678-1234-1234-1234-1234567890ab_n
            # where 'n' is in the range 0-9
            self.assertTrue(chunk_id.startswith("c-"))
            self.assertTrue(chunk_id.endswith('_' + str(i)))
            self.assertEqual(chunk_id[2:-2], dset_id[2:])
            self.assertEqual(len(chunk_id), 2+36+2)
            chunk_id = getPartitionKey(chunk_id, partition_count)

            partition = getChunkPartition(chunk_id)
            self.assertTrue(partition is not None)
            self.assertTrue(partition >= 0)
            self.assertTrue(partition < partition_count)


        selection = getHyperslabSelection(datashape, 20)
        chunk_ids = getChunkIds(dset_id, selection, layout)
        self.assertEqual(len(chunk_ids), 8)
        for i in range(8):
            chunk_id = chunk_ids[i]
            self.assertTrue(chunk_id.startswith("c-"))
            self.assertTrue(chunk_id.endswith('_' + str(i+2)))
            self.assertEqual(chunk_id[2:-2], dset_id[2:])
            self.assertEqual(len(chunk_id), 2+36+2)

        selection = getHyperslabSelection(datashape, 20, 81)
        chunk_ids = getChunkIds(dset_id, selection, layout)
        self.assertEqual(len(chunk_ids), 7)
        for i in range(7):
            chunk_id = chunk_ids[i]
            self.assertTrue(chunk_id.startswith("c-"))
            self.assertTrue(chunk_id.endswith('_' + str(i+2)))
            self.assertEqual(chunk_id[2:-2], dset_id[2:])
            self.assertEqual(len(chunk_id), 2+36+2)


        selection = getHyperslabSelection(datashape, 29, 81)
        chunk_ids = getChunkIds(dset_id, selection, layout)
        self.assertEqual(len(chunk_ids), 7)
        for i in range(7):
            chunk_id = chunk_ids[i]
            self.assertTrue(chunk_id.startswith("c-"))
            self.assertTrue(chunk_id.endswith('_' + str(i+2)))
            self.assertEqual(chunk_id[2:-2], dset_id[2:])
            self.assertEqual(len(chunk_id), 2+36+2)

        selection = getHyperslabSelection(datashape, 29, 81, 2)
        chunk_ids = getChunkIds(dset_id, selection, layout)
        self.assertEqual(len(chunk_ids), 6)
        for i in range(6):
            chunk_id = chunk_ids[i]
            self.assertTrue(chunk_id.startswith("c-"))
            self.assertTrue(chunk_id.endswith('_' + str(i+2)))
            self.assertEqual(chunk_id[2:-2], dset_id[2:])
            self.assertEqual(len(chunk_id), 2+36+2)

        selection = getHyperslabSelection(datashape, 29, 81, 20)
        chunk_ids = getChunkIds(dset_id, selection, layout)
        self.assertEqual(len(chunk_ids), 3)
        for i in range(3):
            chunk_id = chunk_ids[i]
            self.assertTrue(chunk_id.startswith("c-"))
            self.assertTrue(chunk_id.endswith('_' + str(i*2+2)))
            self.assertEqual(chunk_id[2:-2], dset_id[2:])
            self.assertEqual(len(chunk_id), 2+36+2)

        datashape = [3207353,]
        layout = (60000,)
        selection = getHyperslabSelection(datashape, 1234567, 1234568)
        chunk_ids = getChunkIds(dset_id, selection, layout)
        self.assertEqual(len(chunk_ids), 1)
        self.assertTrue(chunk_ids[0].endswith("_20") )


        datashape = [100,100]
        layout = (10,20)
        selection = getHyperslabSelection(datashape)
        chunk_ids = getChunkIds(dset_id, selection, layout)
        self.assertEqual(len(chunk_ids), 50)
        chunk_ids.reverse() # so we can pop off the front
        for i in range(10):
            for j in range(5):
                chunk_id = chunk_ids.pop()
                self.assertTrue(chunk_id.startswith("c-"))
                index1 = int(chunk_id[-3])
                index2 = int(chunk_id[-1])
                self.assertEqual(index1, i)
                self.assertEqual(index2, j)


        selection = getHyperslabSelection(datashape, (12, 23),(88,80))
        chunk_ids = getChunkIds(dset_id, selection, layout)
        self.assertEqual(len(chunk_ids), 24)
        chunk_ids.reverse() # so we can pop off the front
        for i in range(8):
            for j in range(3):
                chunk_id = chunk_ids.pop()
                self.assertTrue(chunk_id.startswith("c-"))
                index1 = int(chunk_id[-3])
                index2 = int(chunk_id[-1])
                self.assertEqual(index1, i+1)
                self.assertEqual(index2, j+1)

        selection = getHyperslabSelection(datashape, (12, 23),(88,80), (6,16))
        chunk_ids = getChunkIds(dset_id, selection, layout)
        self.assertEqual(len(chunk_ids), 24)
        chunk_ids.reverse() # so we can pop off the front
        for i in range(8):
            for j in range(3):
                chunk_id = chunk_ids.pop()
                self.assertTrue(chunk_id.startswith("c-"))
                index1 = int(chunk_id[-3])
                index2 = int(chunk_id[-1])
                self.assertEqual(index1, i+1)
                self.assertEqual(index2, j+1)

        selection = getHyperslabSelection(datashape, (12, 23),(88,80), (16,44))
        chunk_ids = getChunkIds(dset_id, selection, layout)
        self.assertEqual(len(chunk_ids), 10)
        chunk_ids.reverse() # so we can pop off the front
        xindex = (1,2,4,6,7)
        yindex = (1,3)
        for i in range(5):
            for j in range(2):
                chunk_id = chunk_ids.pop()
                self.assertTrue(chunk_id.startswith("c-"))
                index1 = int(chunk_id[-3])
                index2 = int(chunk_id[-1])
                self.assertEqual(index1, xindex[i])
                self.assertEqual(index2, yindex[j])

        # 3d test
        datashape = [365, 720, 1440]
        layout = (2, 180, 720)
        selection = getHyperslabSelection(datashape, (0, 0, 0), (1, 720, 1440))
        chunk_ids = getChunkIds(dset_id, selection, layout)
        self.assertEqual(len(chunk_ids), 8)
        chunk_ids.reverse() # so we can pop off the front
        for i in range(4):
            for j in range(2):
                chunk_id = chunk_ids.pop()
                self.assertTrue(chunk_id.startswith("c-"))
                index1 = int(chunk_id[-3])
                index2 = int(chunk_id[-1])
                self.assertEqual(index1, i)
                self.assertEqual(index2, j)

        selection = getHyperslabSelection(datashape, (0, 0, 0), (1, 720, 1440), (1, 25, 25))
        chunk_ids = getChunkIds(dset_id, selection, layout)
        self.assertEqual(len(chunk_ids), 8)
        chunk_ids.reverse() # so we can pop off the front
        for i in range(4):
            for j in range(2):
                chunk_id = chunk_ids.pop()
                self.assertTrue(chunk_id.startswith("c-"))
                index1 = int(chunk_id[-3])
                index2 = int(chunk_id[-1])
                self.assertEqual(index1, i)
                self.assertEqual(index2, j)




    def testGetChunkIndex(self):
        chunk_id = "c-12345678-1234-1234-1234-1234567890ab_6_4"
        index = getChunkIndex(chunk_id)
        self.assertEqual(index, [6,4])
        chunk_id = "c-12345678-1234-1234-1234-1234567890ab_64"
        index = getChunkIndex(chunk_id)
        self.assertEqual(index, [64,])


    def testGetChunkSelection(self):
        # 1-d test
        dset_id = "d-12345678-1234-1234-1234-1234567890ab"
        datashape = [100,]
        layout = (10,)
        selection = getHyperslabSelection(datashape, 42, 62)
        chunk_ids = getChunkIds(dset_id, selection, layout)
        self.assertEqual(len(chunk_ids), 3)

        chunk_id = chunk_ids[0]
        sel = getChunkSelection(chunk_id, selection, layout)
        self.assertEqual(sel[0].start, 42)
        self.assertEqual(sel[0].stop, 50)
        self.assertEqual(sel[0].step, 1)

        chunk_id = chunk_ids[1]
        sel = getChunkSelection(chunk_id, selection, layout)
        self.assertEqual(sel[0].start, 50)
        self.assertEqual(sel[0].stop, 60)
        self.assertEqual(sel[0].step, 1)

        chunk_id = chunk_ids[2]
        sel = getChunkSelection(chunk_id, selection, layout)
        self.assertEqual(sel[0].start, 60)
        self.assertEqual(sel[0].stop, 62)
        self.assertEqual(sel[0].step, 1)

        # 1-d with step
        selection = getHyperslabSelection(datashape, 42, 62, 4)
        chunk_ids = getChunkIds(dset_id, selection, layout)
        self.assertEqual(len(chunk_ids), 2)

        chunk_id = chunk_ids[0]
        sel = getChunkSelection(chunk_id, selection, layout)
        self.assertEqual(sel[0].start, 42)
        self.assertEqual(sel[0].stop, 47)
        self.assertEqual(sel[0].step, 4)

        chunk_id = chunk_ids[1]
        sel = getChunkSelection(chunk_id, selection, layout)
        self.assertEqual(sel[0].start, 50)
        self.assertEqual(sel[0].stop, 59)
        self.assertEqual(sel[0].step, 4)

        # another 1-d with step
        selection = getHyperslabSelection(datashape, 40, 63, 2)
        chunk_ids = getChunkIds(dset_id, selection, layout)
        self.assertEqual(len(chunk_ids), 3)

        chunk_id = chunk_ids[0]
        sel = getChunkSelection(chunk_id, selection, layout)
        self.assertEqual(sel[0].start, 40)
        self.assertEqual(sel[0].stop, 49)
        self.assertEqual(sel[0].step, 2)

        chunk_id = chunk_ids[1]
        sel = getChunkSelection(chunk_id, selection, layout)
        self.assertEqual(sel[0].start, 50)
        self.assertEqual(sel[0].stop, 59)
        self.assertEqual(sel[0].step, 2)

        chunk_id = chunk_ids[2]
        sel = getChunkSelection(chunk_id, selection, layout)
        self.assertEqual(sel[0].start, 60)
        self.assertEqual(sel[0].stop, 63)
        self.assertEqual(sel[0].step, 2)

        # test with step > chunk size
        selection = getHyperslabSelection(datashape, 0, 100, 15)
        chunk_ids = getChunkIds(dset_id, selection, layout)
        self.assertEqual(len(chunk_ids), 7)

        chunk_id = chunk_ids[0]
        sel = getChunkSelection(chunk_id, selection, layout)
        self.assertEqual(sel[0].start, 0)
        self.assertEqual(sel[0].stop, 1)
        self.assertEqual(sel[0].step, 15)

        chunk_id = chunk_ids[1]
        sel = getChunkSelection(chunk_id, selection, layout)
        self.assertEqual(sel[0].start, 15)
        self.assertEqual(sel[0].stop, 16)
        self.assertEqual(sel[0].step, 15)

        chunk_id = chunk_ids[2]
        sel = getChunkSelection(chunk_id, selection, layout)
        self.assertEqual(sel[0].start, 30)
        self.assertEqual(sel[0].stop, 31)
        self.assertEqual(sel[0].step, 15)

        # 2-d test
        datashape = [100,100]
        layout = (10,10)
        selection = getHyperslabSelection(datashape, (42, 46), (52, 58))
        chunk_ids = getChunkIds(dset_id, selection, layout)
        self.assertEqual(len(chunk_ids), 4)

        chunk_id = chunk_ids[0]
        sel = getChunkSelection(chunk_id, selection, layout)
        self.assertEqual(sel[0].start, 42)
        self.assertEqual(sel[0].stop, 50)
        self.assertEqual(sel[0].step, 1)
        self.assertEqual(sel[1].start, 46)
        self.assertEqual(sel[1].stop, 50)
        self.assertEqual(sel[1].step, 1)

        chunk_id = chunk_ids[1]
        sel = getChunkSelection(chunk_id, selection, layout)
        self.assertEqual(sel[0].start, 42)
        self.assertEqual(sel[0].stop, 50)
        self.assertEqual(sel[0].step, 1)
        self.assertEqual(sel[1].start, 50)
        self.assertEqual(sel[1].stop, 58)
        self.assertEqual(sel[1].step, 1)

        chunk_id = chunk_ids[2]
        sel = getChunkSelection(chunk_id, selection, layout)
        self.assertEqual(sel[0].start, 50)
        self.assertEqual(sel[0].stop, 52)
        self.assertEqual(sel[0].step, 1)
        self.assertEqual(sel[1].start, 46)
        self.assertEqual(sel[1].stop, 50)
        self.assertEqual(sel[1].step, 1)

        chunk_id = chunk_ids[3]
        sel = getChunkSelection(chunk_id, selection, layout)
        self.assertEqual(sel[0].start, 50)
        self.assertEqual(sel[0].stop, 52)
        self.assertEqual(sel[0].step, 1)
        self.assertEqual(sel[1].start, 50)
        self.assertEqual(sel[1].stop, 58)
        self.assertEqual(sel[1].step, 1)

        # 1-d test with fractional chunks
        datashape = [104,]
        layout = (10,)
        selection = getHyperslabSelection(datashape, 92, 102)
        chunk_ids = getChunkIds(dset_id, selection, layout)
        self.assertEqual(len(chunk_ids), 2)

        chunk_id = chunk_ids[0]
        sel = getChunkSelection(chunk_id, selection, layout)
        sel = sel[0]
        self.assertEqual(sel.start, 92)
        self.assertEqual(sel.stop, 100)
        self.assertEqual(sel.step, 1)

        chunk_id = chunk_ids[1]
        sel = getChunkSelection(chunk_id, selection, layout)
        sel = sel[0]
        self.assertEqual(sel.start, 100)
        self.assertEqual(sel.stop, 102)
        self.assertEqual(sel.step, 1)

        # 3d test
        datashape = [365, 720, 1440]
        layout = (2, 180, 720)
        selection = getHyperslabSelection(datashape, (0, 0, 0), (1, 200, 300))
        chunk_ids = getChunkIds(dset_id, selection, layout)
        self.assertEqual(len(chunk_ids), 2)

        chunk_id = chunk_ids[0]
        sel = getChunkSelection(chunk_id, selection, layout)
        self.assertEqual(sel[0].start, 0)
        self.assertEqual(sel[0].stop, 1)
        self.assertEqual(sel[0].step, 1)
        self.assertEqual(sel[1].start, 0)
        self.assertEqual(sel[1].stop, 180)
        self.assertEqual(sel[1].step, 1)
        self.assertEqual(sel[2].start, 0)
        self.assertEqual(sel[2].stop, 300)
        self.assertEqual(sel[2].step, 1)
        chunk_id = chunk_ids[1]
        sel = getChunkSelection(chunk_id, selection, layout)
        self.assertEqual(sel[0].start, 0)
        self.assertEqual(sel[0].stop, 1)
        self.assertEqual(sel[0].step, 1)
        self.assertEqual(sel[1].start, 180)
        self.assertEqual(sel[1].stop, 200)
        self.assertEqual(sel[1].step, 1)
        self.assertEqual(sel[2].start, 0)
        self.assertEqual(sel[2].stop, 300)
        self.assertEqual(sel[2].step, 1)


    def testGetChunkCoverage(self):
        # 1-d test
        dset_id = "d-12345678-1234-1234-1234-1234567890ab"
        datashape = [100,]
        layout = (10,)
        selection = getHyperslabSelection(datashape, 42, 62)
        chunk_ids = getChunkIds(dset_id, selection, layout)
        self.assertEqual(len(chunk_ids), 3)

        chunk_id = chunk_ids[0]
        sel = getChunkCoverage(chunk_id, selection, layout)
        self.assertEqual(sel[0].start, 2)
        self.assertEqual(sel[0].stop, 10)
        self.assertEqual(sel[0].step, 1)

        chunk_id = chunk_ids[1]
        sel = getChunkCoverage(chunk_id, selection, layout)
        self.assertEqual(sel[0].start, 0)
        self.assertEqual(sel[0].stop, 10)
        self.assertEqual(sel[0].step, 1)

        chunk_id = chunk_ids[2]
        sel = getChunkCoverage(chunk_id, selection, layout)
        self.assertEqual(sel[0].start, 0)
        self.assertEqual(sel[0].stop, 2)
        self.assertEqual(sel[0].step, 1)

        # 1-d with step
        selection = getHyperslabSelection(datashape, 42, 62, 4)
        chunk_ids = getChunkIds(dset_id, selection, layout)
        self.assertEqual(len(chunk_ids), 2)

        chunk_id = chunk_ids[0]
        sel = getChunkCoverage(chunk_id, selection, layout)
        self.assertEqual(sel[0].start, 2)
        self.assertEqual(sel[0].stop, 7)
        self.assertEqual(sel[0].step, 4)

        chunk_id = chunk_ids[1]
        sel = getChunkCoverage(chunk_id, selection, layout)
        self.assertEqual(sel[0].start, 0)
        self.assertEqual(sel[0].stop, 9)
        self.assertEqual(sel[0].step, 4)


        # 2-d test
        dset_id = "d-12345678-1234-1234-1234-1234567890ab"
        datashape = [100,100]
        layout = (10,10)
        selection = getHyperslabSelection(datashape, (42, 46), (52, 58))
        chunk_ids = getChunkIds(dset_id, selection, layout)
        self.assertEqual(len(chunk_ids), 4)

        chunk_id = chunk_ids[0]
        sel = getChunkCoverage(chunk_id, selection, layout)
        self.assertEqual(sel[0].start, 2)
        self.assertEqual(sel[0].stop, 10)
        self.assertEqual(sel[0].step, 1)
        self.assertEqual(sel[1].start, 6)
        self.assertEqual(sel[1].stop, 10)
        self.assertEqual(sel[1].step, 1)

        chunk_id = chunk_ids[1]
        sel = getChunkCoverage(chunk_id, selection, layout)
        self.assertEqual(sel[0].start, 2)
        self.assertEqual(sel[0].stop, 10)
        self.assertEqual(sel[0].step, 1)
        self.assertEqual(sel[1].start, 0)
        self.assertEqual(sel[1].stop, 8)
        self.assertEqual(sel[1].step, 1)

        chunk_id = chunk_ids[2]
        sel = getChunkCoverage(chunk_id, selection, layout)
        self.assertEqual(sel[0].start, 0)
        self.assertEqual(sel[0].stop, 2)
        self.assertEqual(sel[0].step, 1)
        self.assertEqual(sel[1].start, 6)
        self.assertEqual(sel[1].stop, 10)
        self.assertEqual(sel[1].step, 1)

        chunk_id = chunk_ids[3]
        sel = getChunkCoverage(chunk_id, selection, layout)
        self.assertEqual(sel[0].start, 0)
        self.assertEqual(sel[0].stop, 2)
        self.assertEqual(sel[0].step, 1)
        self.assertEqual(sel[1].start, 0)
        self.assertEqual(sel[1].stop, 8)
        self.assertEqual(sel[1].step, 1)

        # 2-d test - non-even chunks at boundry
        dset_id = "d-12345678-1234-1234-1234-1234567890ab"
        datashape = [45,54]
        layout = (10,10)
        selection = getHyperslabSelection(datashape, (22, 2), (23, 52))
        chunk_ids = getChunkIds(dset_id, selection, layout)
        self.assertEqual(len(chunk_ids), 6)

        chunk_id = chunk_ids[0]
        sel = getChunkCoverage(chunk_id, selection, layout)
        self.assertEqual(sel[0].start, 2)
        self.assertEqual(sel[0].stop, 3)
        self.assertEqual(sel[0].step, 1)
        self.assertEqual(sel[1].start, 2)
        self.assertEqual(sel[1].stop, 10)
        self.assertEqual(sel[1].step, 1)

        # the next 4 chunks will have same selection
        for i in range(1,4):
            chunk_id = chunk_ids[i]
            sel = getChunkCoverage(chunk_id, selection, layout)
            self.assertEqual(sel[0].start, 2)
            self.assertEqual(sel[0].stop, 3)
            self.assertEqual(sel[0].step, 1)
            self.assertEqual(sel[1].start, 0)
            self.assertEqual(sel[1].stop, 10)
            self.assertEqual(sel[1].step, 1)

        chunk_id = chunk_ids[5]
        sel = getChunkCoverage(chunk_id, selection, layout)
        self.assertEqual(sel[0].start, 2)
        self.assertEqual(sel[0].stop, 3)
        self.assertEqual(sel[0].step, 1)
        self.assertEqual(sel[1].start, 0)
        self.assertEqual(sel[1].stop, 2)
        self.assertEqual(sel[1].step, 1)


        # 1-d test with fractional chunks
        datashape = [104,]
        layout = (10,)
        selection = getHyperslabSelection(datashape, 92, 102)
        chunk_ids = getChunkIds(dset_id, selection, layout)
        self.assertEqual(len(chunk_ids), 2)

        chunk_id = chunk_ids[0]
        sel = getChunkCoverage(chunk_id, selection, layout)
        sel = sel[0]
        self.assertEqual(sel.start, 2)
        self.assertEqual(sel.stop, 10)
        self.assertEqual(sel.step, 1)

        chunk_id = chunk_ids[1]
        sel = getChunkCoverage(chunk_id, selection, layout)
        sel = sel[0]
        self.assertEqual(sel.start, 0)
        self.assertEqual(sel.stop, 2)
        self.assertEqual(sel.step, 1)


    def testGetDataCoverage(self):
        # 1-d test
        dset_id = "d-12345678-1234-1234-1234-1234567890ab"
        datashape = [100,]
        layout = (10,)
        selection = getHyperslabSelection(datashape, 42, 62)
        chunk_ids = getChunkIds(dset_id, selection, layout)
        self.assertEqual(len(chunk_ids), 3)

        chunk_id = chunk_ids[0]
        sel = getDataCoverage(chunk_id, selection, layout)
        self.assertEqual(sel[0].start, 0)
        self.assertEqual(sel[0].stop, 8)
        self.assertEqual(sel[0].step, 1)

        chunk_id = chunk_ids[1]
        sel = getDataCoverage(chunk_id, selection, layout)
        self.assertEqual(sel[0].start, 8)
        self.assertEqual(sel[0].stop, 18)
        self.assertEqual(sel[0].step, 1)

        chunk_id = chunk_ids[2]
        sel = getDataCoverage(chunk_id, selection, layout)
        self.assertEqual(sel[0].start, 18)
        self.assertEqual(sel[0].stop, 20)
        self.assertEqual(sel[0].step, 1)

        # test with step
        selection = getHyperslabSelection(datashape, 42, 68, 4)
        chunk_ids = getChunkIds(dset_id, selection, layout)
        self.assertEqual(len(chunk_ids), 3)

        chunk_id = chunk_ids[0]
        sel = getDataCoverage(chunk_id, selection, layout)
        self.assertEqual(sel[0].start, 0)
        self.assertEqual(sel[0].stop, 2)
        self.assertEqual(sel[0].step, 1)

        chunk_id = chunk_ids[1]
        sel = getDataCoverage(chunk_id, selection, layout)
        self.assertEqual(sel[0].start, 2)
        self.assertEqual(sel[0].stop, 5)
        self.assertEqual(sel[0].step, 1)

        chunk_id = chunk_ids[2]
        sel = getDataCoverage(chunk_id, selection, layout)
        self.assertEqual(sel[0].start, 5)
        self.assertEqual(sel[0].stop, 7)
        self.assertEqual(sel[0].step, 1)


        # 2-d test
        dset_id = "d-12345678-1234-1234-1234-1234567890ab"
        datashape = [100,100]
        layout = (10,10)
        selection = getHyperslabSelection(datashape, (42, 46), (52, 58))
        chunk_ids = getChunkIds(dset_id, selection, layout)
        self.assertEqual(len(chunk_ids), 4)

        chunk_id = chunk_ids[0]
        sel = getDataCoverage(chunk_id, selection, layout)
        self.assertEqual(sel[0].start, 0)
        self.assertEqual(sel[0].stop, 8)
        self.assertEqual(sel[0].step, 1)
        self.assertEqual(sel[1].start, 0)
        self.assertEqual(sel[1].stop, 4)
        self.assertEqual(sel[1].step, 1)

        chunk_id = chunk_ids[1]
        sel = getDataCoverage(chunk_id, selection, layout)
        self.assertEqual(sel[0].start, 0)
        self.assertEqual(sel[0].stop, 8)
        self.assertEqual(sel[0].step, 1)
        self.assertEqual(sel[1].start, 4)
        self.assertEqual(sel[1].stop, 12)
        self.assertEqual(sel[1].step, 1)

        chunk_id = chunk_ids[2]
        sel = getDataCoverage(chunk_id, selection, layout)
        self.assertEqual(sel[0].start, 8)
        self.assertEqual(sel[0].stop, 10)
        self.assertEqual(sel[0].step, 1)
        self.assertEqual(sel[1].start, 0)
        self.assertEqual(sel[1].stop, 4)
        self.assertEqual(sel[1].step, 1)

        chunk_id = chunk_ids[3]
        sel = getDataCoverage(chunk_id, selection, layout)
        self.assertEqual(sel[0].start, 8)
        self.assertEqual(sel[0].stop, 10)
        self.assertEqual(sel[0].step, 1)
        self.assertEqual(sel[1].start, 4)
        self.assertEqual(sel[1].stop, 12)
        self.assertEqual(sel[1].step, 1)

        # 2-d test, non-regular chunks
        dset_id = "d-12345678-1234-1234-1234-1234567890ab"
        datashape = [45,54]
        layout = (10,10)
        selection = getHyperslabSelection(datashape, (22, 2), (23, 52))
        chunk_ids = getChunkIds(dset_id, selection, layout)
        self.assertEqual(len(chunk_ids), 6)

        chunk_id = chunk_ids[0]
        sel = getDataCoverage(chunk_id, selection, layout)
        self.assertEqual(sel[0].start, 0)
        self.assertEqual(sel[0].stop, 1)
        self.assertEqual(sel[0].step, 1)
        self.assertEqual(sel[1].start, 0)
        self.assertEqual(sel[1].stop, 8)
        self.assertEqual(sel[1].step, 1)

        chunk_id = chunk_ids[1]
        sel = getDataCoverage(chunk_id, selection, layout)
        self.assertEqual(sel[0].start, 0)
        self.assertEqual(sel[0].stop, 1)
        self.assertEqual(sel[0].step, 1)
        self.assertEqual(sel[1].start, 8)
        self.assertEqual(sel[1].stop, 18)
        self.assertEqual(sel[1].step, 1)

        chunk_id = chunk_ids[5]
        sel = getDataCoverage(chunk_id, selection, layout)
        self.assertEqual(sel[0].start, 0)
        self.assertEqual(sel[0].stop, 1)
        self.assertEqual(sel[0].step, 1)
        self.assertEqual(sel[1].start, 48)
        self.assertEqual(sel[1].stop, 50)
        self.assertEqual(sel[1].step, 1)


        # 1-d test with fractional chunks
        datashape = [104,]
        layout = (10,)
        selection = getHyperslabSelection(datashape, 92, 102)
        chunk_ids = getChunkIds(dset_id, selection, layout)
        self.assertEqual(len(chunk_ids), 2)

        chunk_id = chunk_ids[0]
        sel = getDataCoverage(chunk_id, selection, layout)
        sel = sel[0]
        self.assertEqual(sel.start, 0)
        self.assertEqual(sel.stop, 8)
        self.assertEqual(sel.step, 1)

        chunk_id = chunk_ids[1]
        sel = getDataCoverage(chunk_id, selection, layout)
        sel = sel[0]
        self.assertEqual(sel.start, 8)
        self.assertEqual(sel.stop, 10)
        self.assertEqual(sel.step, 1)

    def testGetChunkId(self):
        # getChunkIds(dset_id, selection, layout, dim=0, prefix=None, chunk_ids=None):
        dset_id = "d-12345678-1234-1234-1234-1234567890ab"

        layout = (1,)
        chunk_id = getChunkId(dset_id, 0, layout)
        self.assertTrue(chunk_id.startswith("c-"))
        self.assertTrue(chunk_id.endswith('_0'))
        self.assertEqual(chunk_id[2:-2], dset_id[2:])
        self.assertEqual(len(chunk_id), 2+36+2)

        layout = (100,)
        chunk_id = getChunkId(dset_id, 2, layout)
        self.assertTrue(chunk_id.startswith("c-"))
        self.assertTrue(chunk_id.endswith('_0'))
        self.assertEqual(chunk_id[2:-2], dset_id[2:])
        self.assertEqual(len(chunk_id), 2+36+2)

        layout = (10,)
        chunk_id = getChunkId(dset_id, 23, layout)
        self.assertTrue(chunk_id.startswith("c-"))
        self.assertTrue(chunk_id.endswith('_2'))
        self.assertEqual(chunk_id[2:-2], dset_id[2:])
        self.assertEqual(len(chunk_id), 2+36+2)

        layout = (10,20)
        chunk_id = getChunkId(dset_id, (23,61), layout)
        self.assertTrue(chunk_id.startswith("c-"))
        self.assertTrue(chunk_id.endswith('_2_3'))
        self.assertEqual(chunk_id[2:-4], dset_id[2:])
        self.assertEqual(len(chunk_id), 2+36+4)


    def testDimQuery(self):
        request = {"dim_0": 23, "dim_1": 54, "dim_2": 2}
        dims = []
        dim = 0
        while True:
            k = "dim_{}".format(dim)
            if k in request:
                extent = int(request[k])
                dims.append(extent)
                dim += 1
            else:
                break


    def testChunkIterator1d(self):
        dset_id = "d-12345678-1234-1234-1234-1234567890ab"
        dims = [100]
        layout = [10,]

        selection = getHyperslabSelection(dims)
        it = ChunkIterator(dset_id, selection, layout)
        chunk_ids = set(getChunkIds(dset_id, selection, layout))
        count = 0

        while True:
            try:
                chunk_id = it.next()
                self.assertTrue(chunk_id) in chunk_ids
                count += 1
            except StopIteration:
                break

        self.assertEqual(count, 10)

    def testChunkIterator2d(self):
        dset_id = "d-12345678-1234-1234-1234-1234567890ab"
        dims = [100, 100,]
        layout = [50,50]
        selection = getHyperslabSelection(dims)
        it = ChunkIterator(dset_id, selection, layout)

        chunk_ids = set(getChunkIds(dset_id, selection, layout))
        count = 0

        while True:
            try:
                chunk_id = it.next()
                self.assertTrue(chunk_id) in chunk_ids
                count += 1
            except StopIteration:
                break

        self.assertEqual(count, 4)

    def testChunkIterator3d(self):
        dset_id = "d-12345678-1234-1234-1234-1234567890ab"
        dims = [100, 100, 20]
        layout = [50,50,5]
        selection = getHyperslabSelection(dims)
        it = ChunkIterator(dset_id, selection, layout)

        chunk_ids = set(getChunkIds(dset_id, selection, layout))
        count = 0

        while True:
            try:
                chunk_id = it.next()
                self.assertTrue(chunk_id) in chunk_ids
                count += 1
            except StopIteration:
                break

        self.assertEqual(count, 16)

    def testGetEvalStr(self):
        queries = { "date == 23": "rows['date'] == 23",
                    "wind == b'W 5'": "rows['wind'] == b'W 5'",
                    "temp > 61": "rows['temp'] > 61",
                    "(date >=22) & (date <= 24)": "(rows['date'] >=22) & (rows['date'] <= 24)",
                    "(date == 21) & (temp > 70)": "(rows['date'] == 21) & (rows['temp'] > 70)",
                    "(wind == b'E 7') | (wind == b'S 7')": "(rows['wind'] == b'E 7') | (rows['wind'] == b'S 7')" }

        fields = ["date", "wind", "temp"]

        for query in queries.keys():
            eval_str = _getEvalStr(query, "rows", fields)
            self.assertEqual(eval_str, queries[query])
                #print(query, "->", eval_str)

    def testBadQuery(self):
        queries = ( "foobar",    # no variable used
                "wind = b'abc",  # non-closed literal
                "(wind = b'N') & (temp = 32",  # missing paren
                "foobar > 42",                 # invalid field name
                "import subprocess; subprocess.call(['ls', '/'])")  # injection attack

        fields = ("date", "wind", "temp" )

        for query in queries:
            try:
                eval_str = _getEvalStr(query, "x", fields)
                self.assertTrue(False)  # shouldn't get here
            except Exception:
                pass  # ok



    def testChunkReadSelection(self):
        chunk_arr = np.array([2,3,5,7,11,13,17,19])
        arr = chunkReadSelection(chunk_arr, slices=((slice(3,5,1),)))
        self.assertEqual(arr.tolist(), [7,11])
        arr = chunkReadSelection(chunk_arr, slices=((slice(3,9,2),)))
        self.assertEqual(arr.tolist(), [7,13,19])
        chunk_arr = np.zeros((3,4))
        for i in range(3):
            chunk_arr[i] = list(range(i+1,i+1+4))
        arr = chunkReadSelection(chunk_arr, slices=((slice(1,2,1),slice(0,4,1))))
        self.assertEqual(arr.tolist(), [[2.0, 3.0, 4.0, 5.0]])
        arr = chunkReadSelection(chunk_arr, slices=((slice(0,3,1),slice(2,3,1))))
        self.assertEqual(arr.tolist(), [[3.0],[4.0],[5.0]])

    def testChunkWriteSelection(self):
        chunk_arr = np.zeros((8,))
        data = np.array([2,3,5,7,11,13,17,19])
        chunkWriteSelection(chunk_arr=chunk_arr, slices=(slice(0,8,1),), data=data)
        self.assertEqual(chunk_arr.tolist(), data.tolist())
        data = np.array([101, 121, 131])
        chunkWriteSelection(chunk_arr=chunk_arr, slices=(slice(3,6,1),), data=data)
        self.assertEqual(chunk_arr.tolist(), [2,3,5,101,121,131,17,19])

    def testChunkReadPoints1D(self):
        chunk_id = "c-00de6a9c-6aff5c35-15d5-3864dd-0740f8_12"
        chunk_layout = (100,)
        chunk_arr = np.array(list(range(100)))
        point_arr = np.array([[1200],[1299],[1244],[1222]], dtype=np.uint64)
        arr = chunkReadPoints(chunk_id=chunk_id, chunk_layout=chunk_layout, chunk_arr=chunk_arr, point_arr=point_arr)
        self.assertEqual(arr.tolist(), [0, 99, 44, 22])

        point_arr = np.array([[1200],[1299],[1244],[1322]], dtype=np.uint64)
        try:
            chunkReadPoints(chunk_id=chunk_id, chunk_layout=chunk_layout, chunk_arr=chunk_arr, point_arr=point_arr)
            self.assertTrue(False)  # expected exception
        except IndexError:
            pass # expected


    def testChunkReadPoints2D(self):
        chunk_id = "c-00de6a9c-6aff5c35-15d5-3864dd-0740f8_3_4"
        chunk_layout = (100,100)
        chunk_arr = np.zeros((100,100))
        chunk_arr[:,12] = 69
        chunk_arr[12,:] = 96

        point_arr = np.array([[312,498],[312,412],[355,412],[398,497]], dtype=np.uint64)
        arr = chunkReadPoints(chunk_id=chunk_id, chunk_layout=chunk_layout, chunk_arr=chunk_arr, point_arr=point_arr)
        self.assertEqual(arr.tolist(), [96,96,69,0])

        point_arr = np.array([[312,498],[312,412],[355,412],[398,397]], dtype=np.uint64)
        try:
            chunkReadPoints(chunk_id=chunk_id, chunk_layout=chunk_layout, chunk_arr=chunk_arr, point_arr=point_arr)
            self.assertTrue(False)  # expected exception
        except IndexError:
            pass # expected

    def testChunkWritePoints1D(self):
        chunk_id = "c-00de6a9c-6aff5c35-15d5-3864dd-0740f8_12"
        chunk_layout = (100,)
        chunk_arr = np.zeros((100,))
        rank = 1
        #       (coord1, coord2, ...) | dset_dtype
        point_dt = np.dtype([("coord", np.uint64), ("val", chunk_arr.dtype)])
        # point_dt = np.dtype([("coord", np.uint64, (rank,)), ("val", chunk_arr.dtype)])
        indexes = (1203,1245,1288,1212,1299)
        num_points = len(indexes)
        point_arr = np.zeros((num_points,), dtype=point_dt)
        print("point_arr.shape:", point_arr.shape)
        print("point_arr.dtype:", point_arr.dtype)
        for i in range(num_points):
            e = point_arr[i]
            e[0] = indexes[i]
            e[1] = 42
        chunkWritePoints(chunk_id=chunk_id, chunk_layout=chunk_layout, chunk_arr=chunk_arr, point_arr=point_arr)
        for i in range(100):
            if i + 1200 in indexes:
                self.assertEqual(chunk_arr[i], 42)
            else:
                self.assertEqual(chunk_arr[i], 0)

        e = point_arr[1]
        e[0] = 99  # index out of range
        try:
            chunkWritePoints(chunk_id=chunk_id, chunk_layout=chunk_layout, chunk_arr=chunk_arr, point_arr=point_arr)
            self.assertTrue(False)  # expected exception
        except IndexError:
            pass  # expected

    def testChunkWritePoints2D(self):
        chunk_id = "c-00de6a9c-6aff5c35-15d5-3864dd-0740f8_3_2"
        chunk_layout = (10,20)
        chunk_arr = np.zeros((10,20))
        rank = 2
        #       (coord1, coord2, ...) | dset_dtype
        point_dt = np.dtype([("coord", np.uint64, (2,)), ("val", chunk_arr.dtype)])
        indexes =((32,46),(38,52),(35,53))
        num_points = len(indexes)
        point_arr = np.zeros((num_points,), dtype=point_dt)
        for i in range(num_points):
            e = point_arr[i]
            e[0] = indexes[i]
            e[1] = 42
        chunkWritePoints(chunk_id=chunk_id, chunk_layout=chunk_layout, chunk_arr=chunk_arr, point_arr=point_arr)
        chunk_index = (30,40)
        for i in range(num_points):
            index = indexes[i]
            x = index[0]- chunk_index[0]
            y = index[1] - chunk_index[1]
            self.assertEqual(chunk_arr[x,y], 42)

        e = point_arr[0]
        e[0] = (42,46)  # index out of range
        try:
            chunkWritePoints(chunk_id=chunk_id, chunk_layout=chunk_layout, chunk_arr=chunk_arr, point_arr=point_arr)
            self.assertTrue(False)  # expected exception
        except IndexError:
            pass  # expected

    def testChunkQuery(self):
        chunk_id = "c-00de6a9c-6aff5c35-15d5-3864dd-0740f8_12"
        chunk_layout = (100,)
        value = [
            ("EBAY", "20170102", 3023, 3088),
            ("AAPL", "20170102", 3054, 2933),
            ("AMZN", "20170102", 2973, 3011),
            ("EBAY", "20170103", 3042, 3128),
            ("AAPL", "20170103", 3182, 3034),
            ("AMZN", "20170103", 3021, 2788),
            ("EBAY", "20170104", 2798, 2876),
            ("AAPL", "20170104", 2834, 2867),
            ("AMZN", "20170104", 2891, 2978),
            ("EBAY", "20170105", 2973, 2962),
            ("AAPL", "20170105", 2934, 3010),
            ("AMZN", "20170105", 3018, 3086)
        ]
        num_rows = len(value)
        chunk_dtype = np.dtype([("symbol", "S4"), ("date", "S8"), ("open", "i4"), ("close", "i4")])
        chunk_arr = np.zeros(chunk_layout, dtype=chunk_dtype)
        for i in range(num_rows):
            row = value[i]
            e = chunk_arr[i]
            for j in range(4):
                e[j] = row[j]
        #chunkQuery(chunk_id=None, chunk_arr=None, slices=None, query=None, query_update=None, limit=0, return_json=False):
        result = chunkQuery(chunk_id=chunk_id, chunk_layout=chunk_layout, chunk_arr=chunk_arr, query="symbol == b'AAPL'")
        self.assertTrue(isinstance(result, np.ndarray))
        result_dtype = result.dtype
        self.assertEqual(len(result_dtype), 2)
        self.assertEqual(result_dtype[0], np.dtype("u8"))
        self.assertEqual(len(result_dtype[1]), 4)
        self.assertEqual(len(result), 4)
        expected_indexes = (1201,1204,1207,1210)  # rows above with AAPL as symbol
        for i in range(4):
            item = result[i]
            self.assertEqual(len(item), 2)  # index and row values
            index = int(item[0])
            self.assertEqual(index, expected_indexes[i])
            row = item[1]
            chunk_index = index % chunk_layout[0]
            expected_row = chunk_arr[chunk_index]
            self.assertEqual(len(row), 4)
            self.assertEqual(row[0], b"AAPL")
            self.assertEqual(row, expected_row)

        # return JSON
        result = chunkQuery(chunk_id=chunk_id, chunk_layout=chunk_layout, chunk_arr=chunk_arr, query="symbol == b'AAPL'", return_json=True)
        json_str = json.dumps(result)  # test we can jsonfy the result
        self.assertTrue(len(json_str) > 100)
        print(result)
        self.assertTrue("index" in result)
        indexes = result["index"]
        self.assertTrue("value" in result)
        values = result["value"]
        for i in range(4):
            index = indexes[i]
            self.assertEqual(index, expected_indexes[i])
            row = values[i]
            chunk_index = index % chunk_layout[0]
            expected_row = chunk_arr[chunk_index]
            self.assertEqual(len(row), 4)
            self.assertEqual(row[0], "AAPL")  # note - string, not bytes
            for i in range(2,4):
                self.assertEqual(row[i], expected_row[i])
        # read just one row back
        result = chunkQuery(chunk_id=chunk_id, chunk_layout=chunk_layout, chunk_arr=chunk_arr, query="symbol == b'AAPL'", limit=1)
        self.assertTrue(isinstance(result, np.ndarray))
        self.assertEqual(len(result), 1)
        item = result[0]
        self.assertEqual(len(item), 2)
        index = item[0]
        self.assertEqual(index, 1201)
        row = item[1]
        self.assertEqual(row, chunk_arr[1])

        # query with no limit and selection
        slices = (slice(2,12,1),)
        result = chunkQuery(chunk_id=chunk_id, chunk_layout=chunk_layout, chunk_arr=chunk_arr,  slices=slices, query="symbol == b'AAPL'")
        self.assertTrue(isinstance(result, np.ndarray))
        self.assertEqual(len(result), 3)
        expected_indexes = (1204,1207,1210)
        for i in range(3):
            item = result[i]
            index = item[0]
            self.assertEqual(index, expected_indexes[i])

        # try bad Limit
        try:
            chunkQuery(chunk_id=chunk_id, chunk_layout=chunk_layout, chunk_arr=chunk_arr, query="symbol == b'AAPL'", limit="foobar")
            self.assertTrue(False)
        except TypeError:
            pass # expected

        # try invalid query string
        try:
            chunkQuery(chunk_id=chunk_id, chunk_layout=chunk_layout, chunk_arr=chunk_arr, query="foobar")
            self.assertTrue(False)
        except ValueError:
            pass # expected

        # try modifying one aapl row
        query_update = {"open": 999}
        result = chunkQuery(chunk_id=chunk_id, chunk_layout=chunk_layout, chunk_arr=chunk_arr, query="symbol == b'AAPL'", query_update=query_update)
        self.assertEqual(len(result), 4)
        for i in range(4):
            item = result[i]
            index = int(item[0]) - 1200
            row = item[1]
            self.assertEqual(row[0], b'AAPL')
            self.assertEqual(row[2], 999)
            # original array should have been modified
            row = chunk_arr[index]
            self.assertEqual(row[0], b'AAPL')
            self.assertEqual(row[2], 999)



















if __name__ == '__main__':
    #setup test files

    unittest.main()
