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
from dsetUtil import guess_chunk, getHyperslabSelection, getNumChunks
from dsetUtil import getChunkIds, getChunkIndex, getChunkSelection, getChunkCoverage


class DsetUtilTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(DsetUtilTest, self).__init__(*args, **kwargs)
        # main
    

    def testGuessChunk(self):       
        shape = [100, 100]
        typesize = 8
        layout = guess_chunk(shape, None, typesize)
        self.assertEqual(layout, (25, 50))

        shape = [5]
        layout = guess_chunk(shape, None, typesize)
        self.assertEqual(layout, (5,))

        shape = [100, 100, 100]
        layout = guess_chunk(shape, None, typesize)
        self.assertEqual(layout, (13,13,25))

        shape = [100, 0]
        layout = guess_chunk(shape, None, typesize)

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



    def testGetHyperslabSelection(self):
        # getHyperslabSelection(dsetshape, start, stop, step)
        # 1-D case
        datashape = [100,]
        slices = getHyperslabSelection(datashape)
        self.assertEqual(len(slices), 1)
        self.assertEqual(slices[0], slice(0, 100, 1))

        slices = getHyperslabSelection(datashape, 20)
        self.assertEqual(len(slices), 1)
        self.assertEqual(slices[0], slice(20, 100, 1))

        slices = getHyperslabSelection(datashape, 20, 80)
        self.assertEqual(len(slices), 1)
        self.assertEqual(slices[0], slice(20, 80, 1))

        slices = getHyperslabSelection(datashape, 20, 80, 2)
        self.assertEqual(len(slices), 1)
        self.assertEqual(slices[0], slice(20, 80, 2))

        datashape = [100, 50]
        slices = getHyperslabSelection(datashape)
        self.assertEqual(len(slices), 2)
        self.assertEqual(slices[0], slice(0, 100, 1))
        self.assertEqual(slices[1], slice(0, 50, 1))

        slices = getHyperslabSelection(datashape, (10, 20))
        self.assertEqual(len(slices), 2)
        self.assertEqual(slices[0], slice(10, 100, 1))
        self.assertEqual(slices[1], slice(20, 50, 1))

        slices = getHyperslabSelection(datashape, (10, 20), (90, 30))
        self.assertEqual(len(slices), 2)
        self.assertEqual(slices[0], slice(10, 90, 1))
        self.assertEqual(slices[1], slice(20, 30, 1))

        slices = getHyperslabSelection(datashape, (10, 20), (90, 30), (1,2))
        self.assertEqual(len(slices), 2)
        self.assertEqual(slices[0], slice(10, 90, 1))
        self.assertEqual(slices[1], slice(20, 30, 2))
    
    def testGetChunkIds(self):
        # getChunkIds(dset_id, selection, layout, dim=0, prefix=None, chunk_ids=None):
        dset_id = "d-12345678-1234-1234-1234-1234567890ab"
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
        cc = getChunkCoverage(chunk_id, sel, layout)
        self.assertEqual(cc[0].start, 2)
        self.assertEqual(cc[0].stop, 10)
        self.assertEqual(cc[0].step, 1)

        chunk_id = chunk_ids[1]
        sel = getChunkSelection(chunk_id, selection, layout)
        self.assertEqual(sel[0].start, 50)
        self.assertEqual(sel[0].stop, 60)
        self.assertEqual(sel[0].step, 1)
        cc = getChunkCoverage(chunk_id, sel, layout)
        self.assertEqual(cc[0].start, 0)
        self.assertEqual(cc[0].stop, 10)
        self.assertEqual(cc[0].step, 1)

        chunk_id = chunk_ids[2]
        sel = getChunkSelection(chunk_id, selection, layout)
        self.assertEqual(sel[0].start, 60)
        self.assertEqual(sel[0].stop, 62)
        self.assertEqual(sel[0].step, 1)
        cc = getChunkCoverage(chunk_id, sel, layout)
        self.assertEqual(cc[0].start, 0)
        self.assertEqual(cc[0].stop, 2)
        self.assertEqual(cc[0].step, 1)


        # 2-d test
        dset_id = "d-12345678-1234-1234-1234-1234567890ab"
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
        print(chunk_id)
        sel = getChunkSelection(chunk_id, selection, layout)
        self.assertEqual(sel[0].start, 42)
        self.assertEqual(sel[0].stop, 50)
        self.assertEqual(sel[0].step, 1)
        self.assertEqual(sel[1].start, 50)
        self.assertEqual(sel[1].stop, 58)
        self.assertEqual(sel[1].step, 1) 

        chunk_id = chunk_ids[2]
        print(chunk_id)
        sel = getChunkSelection(chunk_id, selection, layout)
        self.assertEqual(sel[0].start, 50)
        self.assertEqual(sel[0].stop, 52)
        self.assertEqual(sel[0].step, 1)
        self.assertEqual(sel[1].start, 46)
        self.assertEqual(sel[1].stop, 50)
        self.assertEqual(sel[1].step, 1)

        

        chunk_id = chunk_ids[3]
        print(chunk_id)
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
        print(chunk_id)
        sel = getChunkSelection(chunk_id, selection, layout)
        sel = sel[0]
        self.assertEqual(sel.start, 92)
        self.assertEqual(sel.stop, 100)
        self.assertEqual(sel.step, 1)

        chunk_id = chunk_ids[1]
        print(chunk_id)
        sel = getChunkSelection(chunk_id, selection, layout)
        sel = sel[0]
        self.assertEqual(sel.start, 100)
        self.assertEqual(sel.stop, 102)
        self.assertEqual(sel.step, 1)

             

        

                                  
             
if __name__ == '__main__':
    #setup test files
    
    unittest.main()
    
