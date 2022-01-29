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

sys.path.append('../..')
from hsds.util.dsetUtil import  getHyperslabSelection, getSelectionShape, getSelectionList, ItemIterator

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

        sel = [ [2,3,5,7,11]] # coordinate list
        shape = getSelectionShape(sel)
        self.assertEqual(shape, [5,])

        sel = [ slice(0,100,1), slice(50,51,1), [23,35,56] ]
        shape = getSelectionShape(sel)
        self.assertEqual(shape, [100,1,3])

    
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

    def testSelectionList1D(self):
        dims = [100,]

        for select in ("", []):
            selection = getSelectionList(select, dims)
            self.assertEqual(len(selection), 1)
            s1 = selection[0]
            self.assertTrue(isinstance(s1, slice))
            self.assertEqual(s1, slice(0, 100, 1))

        for select in ("[5]", [5,]):
            selection = getSelectionList(select, dims)
            self.assertEqual(len(selection), 1)
            s1 = selection[0]
            self.assertTrue(isinstance(s1, slice))
            self.assertEqual(s1, slice(5, 6, 1))

        for select in ("[:]", [":",]):
            selection = getSelectionList(select, dims)
            self.assertEqual(len(selection), 1)
            s1 = selection[0]
            self.assertTrue(isinstance(s1, slice))
            self.assertEqual(s1, slice(0, 100, 1))

        for select in ("[3:7]", ["3:7",]):
            selection = getSelectionList(select, dims)
            self.assertEqual(len(selection), 1)
            s1 = selection[0]
            self.assertTrue(isinstance(s1, slice))
            self.assertEqual(s1, slice(3,7,1))

        for select in ("[0:100]", ["0:100",]):
            selection = getSelectionList(select, dims)
            self.assertEqual(len(selection), 1)
            s1 = selection[0]
            self.assertTrue(isinstance(s1, slice))
            self.assertEqual(s1, slice(0,100,1))

        for select in ("[[3,4,7]]", ["[3,4,7]"], [[3,4,7]]):
            selection = getSelectionList(select, dims)
            self.assertEqual(len(selection), 1)
            s1 = selection[0]
            self.assertTrue(isinstance(s1, list))
            self.assertEqual(s1, [3,4,7])

        for select in ("[30:70:5]", ["30:70:5",]):
            selection = getSelectionList(select, dims)
            self.assertEqual(len(selection), 1)
            s1 = selection[0]
            self.assertTrue(isinstance(s1, slice))
            self.assertEqual(s1, slice(30,70,5))

        body = {"start": 3, "stop": 7}
        selection = getSelectionList(body, dims)
        self.assertEqual(len(selection), 1)
        s1 = selection[0]
        self.assertTrue(isinstance(s1, slice))
        self.assertEqual(s1, slice(3,7,1))

        body = {"start": 30, "stop": 70, "step": 5}
        selection = getSelectionList(body, dims)
        self.assertEqual(len(selection), 1)
        s1 = selection[0]
        self.assertTrue(isinstance(s1, slice))
        self.assertEqual(s1, slice(30,70,5))

    def testSelectionList2D(self):
        dims = [50, 100,]

        for select in ("", []):
            selection = getSelectionList(select, dims)
            self.assertEqual(len(selection), 2)
            s1 = selection[0]
            self.assertTrue(isinstance(s1, slice))
            self.assertEqual(s1, slice(0, 50, 1))
            s2 = selection[1]
            self.assertTrue(isinstance(s2, slice))
            self.assertEqual(s2, slice(0, 100, 1))

        for select in ("[5,40]", ["5", "40"], [5,40]):
            selection = getSelectionList(select, dims)
            self.assertEqual(len(selection), 2)
            s1 = selection[0]
            self.assertTrue(isinstance(s1, slice))
            self.assertEqual(s1, slice(5, 6, 1))
            s2 = selection[1]
            self.assertTrue(isinstance(s2, slice))
            self.assertEqual(s2, slice(40, 41, 1))

        for select in ("[3:7,12]", ["3:7", "12"], ["3:7", 12]):
            selection = getSelectionList(select, dims)
            self.assertEqual(len(selection), 2)
            s1 = selection[0]
            self.assertTrue(isinstance(s1, slice))
            self.assertEqual(s1, slice(3,7,1))
            s2 = selection[1]
            self.assertTrue(isinstance(s2, slice))
            self.assertEqual(s2, slice(12,13,1))

        for select in ("[:,[3,4,7]]", [":", "[3,4,7]"], [":", [3,4,7]]):
            selection = getSelectionList(select, dims)
            self.assertEqual(len(selection), 2)
            s1 = selection[0]
            self.assertTrue(isinstance(s1, slice))
            self.assertEqual(s1, slice(0, dims[0], 1))
            s2 = selection[1]
            self.assertTrue(isinstance(s2, list))
            self.assertEqual(s2, [3,4,7])

        for select in ("[1:20, 30:70:5]", ["1:20", "30:70:5"]):
            selection = getSelectionList(select, dims)
            self.assertEqual(len(selection), 2)
            s1 = selection[0]
            self.assertTrue(isinstance(s1, slice))
            self.assertEqual(s1, slice(1,20,1))
            s2 = selection[1]
            self.assertTrue(isinstance(s2, slice))
            self.assertEqual(s2, slice(30,70,5))

        for select in ("[0:50, 0:100]", ["0:50", "0:100"]):
            selection = getSelectionList(select, dims)
            self.assertEqual(len(selection), 2)
            s1 = selection[0]
            self.assertTrue(isinstance(s1, slice))
            self.assertEqual(s1, slice(0,50,1))
            s2 = selection[1]
            self.assertTrue(isinstance(s2, slice))
            self.assertEqual(s2, slice(0,100,1))

        body = {"start": [3,5], "stop": [7,9]}
        selection = getSelectionList(body, dims)
        self.assertEqual(len(selection), 2)
        s1 = selection[0]
        self.assertTrue(isinstance(s1, slice))
        self.assertEqual(s1, slice(3,7,1))
        s2 = selection[1]
        self.assertTrue(isinstance(s2, slice))
        self.assertEqual(s2, slice(5,9,1))

        body = {"start": [0,30], "stop": [10,70], "step": [1,5]}
        selection = getSelectionList(body, dims)
        self.assertEqual(len(selection), 2)
        s1 = selection[0]
        self.assertTrue(isinstance(s1, slice))
        self.assertEqual(s1, slice(0,10,1))
        s2 = selection[1]
        self.assertTrue(isinstance(s2, slice))
        self.assertEqual(s2, slice(30,70,5))


    def testInvalidSelectionList(self):
        dims = [50, 100,]

        try:
            # no bracket
            getSelectionList("2", dims)
            self.assertTrue(False)
        except ValueError:
            pass # expected

        try:
            # selection doesn't match dimension
            getSelectionList("[2]", dims)
            self.assertTrue(False)
        except ValueError:
            pass # expected

        try:
            # invalid character
            getSelectionList("[2,x]", dims)
            self.assertTrue(False)
        except ValueError:
            pass # expected

        try:
            # too many colons
            getSelectionList("[6, 1:2:3:4]", dims)
            self.assertTrue(False)
        except ValueError:
            pass # expected

        try:
            # out of bounds
            getSelectionList("[2, 101]", dims)
            self.assertTrue(False)
        except ValueError:
            pass # expected

        try:
            # out of bounds - range
            getSelectionList("[2, 22:101]", dims)
            self.assertTrue(False)
        except ValueError:
            pass # expected

        try:
            # out of bounds - coordinate list
            getSelectionList("[2, [1,2,3,101]]", dims)
            self.assertTrue(False)
        except ValueError:
            pass # expected

        try:
            # out of bounds - reversed selection
            getSelectionList("[2, 50:20]", dims)
            self.assertTrue(False)
        except ValueError:
            pass # expected

        try:
            # out of bounds - coordinate list non-increasing
            getSelectionList("[2, [1,2,2]]", dims)
            self.assertTrue(False)
        except ValueError:
            pass # expected

        try:
            # missing key
            getSelectionList({"start": [30,40]}, dims)
            self.assertTrue(False)
        except KeyError:
            pass # expected

        try:
            # out of bounds
            getSelectionList({"start": [30,40], "stop": [2,101]}, dims)
            self.assertTrue(False)
        except ValueError:
            pass # expected

        try:
            # wrong number of dimensions
            getSelectionList({"start": [30,40], "stop": [2,7,101]}, dims)
            self.assertTrue(False)
        except ValueError:
            pass # expected








if __name__ == '__main__':
    #setup test files

    unittest.main()
