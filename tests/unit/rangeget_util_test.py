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
import logging
import sys

sys.path.append("../..")
from hsds.util.rangegetUtil import (
    ChunkLocation,
    chunkMunge,
)

class RangegetutilTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(RangegetutilTest, self).__init__(*args, **kwargs)
        # main
        logging.getLogger().setLevel(logging.DEBUG)

    def testSimple(self):
        c1 = ChunkLocation(1, 100, 25)
        c2 = ChunkLocation(2, 200, 35)
        c3 = ChunkLocation(3, 300, 40)
        c4 = ChunkLocation(4, 340, 30)
        c_bad = ChunkLocation(5, 110, 40)  # overlaps c1
        
        h5chunks = []
        munged = chunkMunge(h5chunks)
        self.assertEqual(munged, h5chunks)
        
        h5chunks = [c1, ]
        munged = chunkMunge(h5chunks)
        self.assertEqual(munged, h5chunks)

        h5chunks = [c1, c2]
        munged = chunkMunge(h5chunks)
        self.assertEqual(len(munged), 1)
        self.assertTrue(isinstance(munged[0], list))
        self.assertEqual(len(munged[0]), 2)
        self.assertEqual(munged, [[c1, c2],])

        h5chunks = [c2, c1]
        munged = chunkMunge(h5chunks)
        self.assertEqual(len(munged), 1)
        self.assertTrue(isinstance(munged[0], list))
        self.assertEqual(len(munged[0]), 2)
        self.assertEqual(munged, [[c1, c2],])
        

        h5chunks = [c1, c2, c3, c4]
        munged = chunkMunge(h5chunks)
        self.assertEqual(len(munged), 1)
        self.assertTrue(isinstance(munged[0], list))
        self.assertEqual(len(munged[0]), 4)
        self.assertEqual(munged, [[c1, c2, c3, c4],])

        # test with max_gap = 0
        h5chunks = [c1, c2, c3, c4]
        munged = chunkMunge(h5chunks, max_gap=0)
        self.assertEqual(len(munged), 3)
        self.assertEqual(munged, [c1, c2, [c3, c4]])
        
        # test with max_gap = 70
        h5chunks = [c1, c2, c3, c4]
        munged = chunkMunge(h5chunks, max_gap=70)
        self.assertEqual(len(munged), 2)
        self.assertEqual(munged, [c1, [c2, c3, c4]])

        h5chunks = [c1, c_bad]
        try:
            munged = chunkMunge(h5chunks)
            self.assertTrue(False)
        except ValueError:
            pass # expected

        






if __name__ == "__main__":

    unittest.main()



