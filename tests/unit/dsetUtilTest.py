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
from dsetUtil import  getHyperslabSelection, getSelectionShape
from dsetUtil import  ItemIterator, getEvalStr

class DsetUtilTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(DsetUtilTest, self).__init__(*args, **kwargs)
        # main

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

    def testGetSelectionShape(self):
        sel = [ slice(3,7,1), ]
        shape = getSelectionShape(sel)
        self.assertEqual(shape, [4,])

        sel = [ slice(3,7,3), ]  # select points 3, 6
        shape = getSelectionShape(sel)
        self.assertEqual(shape, [2,])

        sel = [ slice(44,52,1), slice(48,52,1) ]
        shape = getSelectionShape(sel)
        self.assertEqual(shape, [8,4])

        sel = [ slice(0, 4, 2), ] # select points 0, 2
        shape = getSelectionShape(sel)
        self.assertEqual(shape, [2,])

        sel = [ slice(0, 5, 2), ] # select points 0, 2, 4
        shape = getSelectionShape(sel)
        self.assertEqual(shape, [3,])

    def testGetEvalStr(self):
        queries = { "date == 23": "rows['date'] == 23",
                    "wind == b'W 5'": "rows['wind'] == b'W 5'",
                    "temp > 61": "rows['temp'] > 61",
                    "(date >=22) & (date <= 24)": "(rows['date'] >=22) & (rows['date'] <= 24)",
                    "(date == 21) & (temp > 70)": "(rows['date'] == 21) & (rows['temp'] > 70)",
                    "(wind == b'E 7') | (wind == b'S 7')": "(rows['wind'] == b'E 7') | (rows['wind'] == b'S 7')" }

        fields = ["date", "wind", "temp"]

        for query in queries.keys():
            eval_str = getEvalStr(query, "rows", fields)
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
                eval_str = getEvalStr(query, "x", fields)
                self.assertTrue(False)  # shouldn't get here
            except Exception:
                pass  # ok

    def testItemIterator(self):
        # 1-D case
        datashape = [10,]
        slices = getHyperslabSelection(datashape)
        it = ItemIterator(slices)

        indices = []
        count = 0

        while True:
            try:
                index = it.next()
                count += 1
                indices.append(index)
            except StopIteration:
                break
        self.assertEqual(count, 10)
        self.assertEqual(indices, list(range(10)))

        # 2-D case
        datashape = [4, 5]
        slices = getHyperslabSelection(datashape)
        it = ItemIterator(slices)

        indices = []
        count = 0
        while True:
            try:
                index = it.next()
                self.assertTrue(len(index), 2)
                self.assertTrue(index[0] >= 0)
                self.assertTrue(index[0] < 4)
                self.assertTrue(index[1] >= 0)
                self.assertTrue(index[1] < 5)
                count += 1
                indices.append(index)
            except StopIteration:
                break
        self.assertEqual(count, 20)


if __name__ == '__main__':
    #setup test files

    unittest.main()

