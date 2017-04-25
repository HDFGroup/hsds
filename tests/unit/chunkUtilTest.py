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
 
sys.path.append('../../hsds/util')
sys.path.append('../../hsds')
from dsetUtil import getHyperslabSelection
from chunkUtil import guessChunk, getNumChunks, getChunkCoordinate, getChunkIds, getChunkId
from chunkUtil import getChunkIndex, getChunkSelection, getChunkCoverage, getDataCoverage, ChunkIterator
from chunkUtil import CHUNK_MIN, CHUNK_MAX, getChunkSize, shrinkChunk, expandChunk


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
        typesize = 1
        layout = (1, 2, 3)
        shrunk = shrinkChunk(layout, typesize)
        self.assertEqual(shrunk, layout)

        layout = (1000, 2000, 3000)
        num_bytes = getChunkSize(layout, typesize)
        self.assertTrue(num_bytes > CHUNK_MAX)
        shrunk = shrinkChunk(layout, typesize)
        rank = len(layout)
        for i in range(rank):
            self.assertTrue(shrunk[i] >= 1)
            self.assertTrue(shrunk[i] <= 1000*(i+1))
        num_bytes = getChunkSize(shrunk, typesize)
        self.assertTrue(num_bytes > CHUNK_MIN)
        self.assertTrue(num_bytes < CHUNK_MAX)

        layout = (3000, 2000, 1000)
        num_bytes = getChunkSize(layout, typesize)
        self.assertTrue(num_bytes > CHUNK_MAX)
        shrunk = shrinkChunk(layout, typesize)
        rank = len(layout)
        for i in range(rank):
            self.assertTrue(shrunk[i] >= 1)
            self.assertTrue(shrunk[i] <= 1000*(3-i))
        num_bytes = getChunkSize(shrunk, typesize)
        self.assertTrue(num_bytes > CHUNK_MIN)
        self.assertTrue(num_bytes < CHUNK_MAX)

    def testExpandChunk(self):
        typesize = 1
        shape = {"class": 'H5S_SIMPLE', "dims": [10, 10, 10]}
        layout = (10, 10, 10)
        num_bytes = getChunkSize(layout, typesize)
        self.assertTrue(num_bytes < CHUNK_MIN)
        expanded = expandChunk(layout, typesize, shape)
        num_bytes = getChunkSize(expanded, typesize)
        # chunk layout can't be larger than dataspace
        self.assertTrue(num_bytes < CHUNK_MIN)
        self.assertEqual(expanded, (10, 10, 10))
         

        shape = {"class": 'H5S_SIMPLE', "dims": [1000, 2000, 3000]}
        layout = (10, 10, 10)
        num_bytes = getChunkSize(layout, typesize)
        self.assertTrue(num_bytes < CHUNK_MIN)
        expanded = expandChunk(layout, typesize, shape)
        num_bytes = getChunkSize(expanded, typesize)
        self.assertTrue(num_bytes > CHUNK_MIN)
        self.assertTrue(num_bytes < CHUNK_MAX)
         

        shape = {"class": 'H5S_SIMPLE', "dims": [1000, 10, 1000], "maxdims": [1000, 100, 1000]}
        layout = (10, 10, 10)
        num_bytes = getChunkSize(layout, typesize)
        self.assertTrue(num_bytes < CHUNK_MIN)
        expanded = expandChunk(layout, typesize, shape)
        num_bytes = getChunkSize(expanded, typesize)
        self.assertTrue(num_bytes > CHUNK_MIN)
        self.assertTrue(num_bytes < CHUNK_MAX)

        shape = {"class": 'H5S_SIMPLE', "dims": [1000, 0, 1000], "maxdims": [1000, 100, 1000]}
        layout = (10, 10, 10)
        num_bytes = getChunkSize(layout, typesize)
        self.assertTrue(num_bytes < CHUNK_MIN)
        expanded = expandChunk(layout, typesize, shape)
        num_bytes = getChunkSize(expanded, typesize)
        self.assertTrue(num_bytes > CHUNK_MIN)
        self.assertTrue(num_bytes < CHUNK_MAX)

        shape = {"class": 'H5S_SIMPLE', "dims": [1000, 10, 1000], "maxdims": [1000, 0, 1000]}
        layout = (10, 10, 10)
        num_bytes = getChunkSize(layout, typesize)
        self.assertTrue(num_bytes < CHUNK_MIN)
        expanded = expandChunk(layout, typesize, shape)
        num_bytes = getChunkSize(expanded, typesize)
        self.assertTrue(num_bytes > CHUNK_MIN)
        self.assertTrue(num_bytes < CHUNK_MAX)
 

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

        datashape = [100,]
        layout = (10,)
        selection = getHyperslabSelection(datashape)
        chunk_ids = getChunkIds(dset_id, selection, layout)
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

        datashape = [1,]
        layout = (1,)        
        chunk_id = getChunkId(dset_id, 0, layout)
        self.assertTrue(chunk_id.startswith("c-"))
        self.assertTrue(chunk_id.endswith('_0'))
        self.assertEqual(chunk_id[2:-2], dset_id[2:])
        self.assertEqual(len(chunk_id), 2+36+2)

        datashape = [100,]
        layout = (10,)
        chunk_id = getChunkId(dset_id, 23, layout)
        self.assertTrue(chunk_id.startswith("c-"))
        self.assertTrue(chunk_id.endswith('_2'))
        self.assertEqual(chunk_id[2:-2], dset_id[2:])
        self.assertEqual(len(chunk_id), 2+36+2)

        datashape = [100,100]
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
         
         
            
        
            
          
         

  
                                  
             
if __name__ == '__main__':
    #setup test files
    
    unittest.main()
    
