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
 
sys.path.append('../../hsds/util')
sys.path.append('../../hsds')
from arrayUtil import bytesArrayToList, toTuple, getNumElements, jsonToArray


class ArrayUtilTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(ArrayUtilTest, self).__init__(*args, **kwargs)
        # main
    

    def testByteArrayToList(self): 
        data_items = (
            42, 
            "foo",
            b"foo",
            [1,2,3],
            (1,2,3),
            ["A", "B", "C"],
            [b"A", b"B", b"C"],
            [["A", "B"], [b'a', b'b', b'c']]
             
        )
        for data in data_items:
            json_data = bytesArrayToList(data)
            # will throw TypeError if not able to convert
            json.dumps(json_data)

    def testToTuple(self):
        data0d = 42  # scalar
        data1d1 = [1]  # one dimensional, one element list
        data1d = [1, 2, 3, 4, 5]  # list
        data2d1 = [[1,2],]  # two dimensional, one element
        data2d = [[1, 0.1], [2, 0.2], [3, 0.3], [4, 0.4]]  # list of two-element lists
        data3d = [[[0, 0.0], [1, 0.1]], [[2, 0.2], [3, 0.3]]]  # list of list of lists
        out = toTuple(0, data0d)
        self.assertEqual(data0d, out)
        out = toTuple(1, data1d1)
        self.assertEqual(data1d1, out)
        out = toTuple(1, data1d)
        self.assertEqual(data1d, out)
        out = toTuple(2, data2d)
        self.assertEqual(data2d, out)
        out = toTuple(1, data2d1)
        self.assertEqual([(1,2)], out)
        out = toTuple(3, data3d)
        self.assertEqual(data3d, out)
        out = toTuple(1, data2d)  # treat input as 1d array of two-field compound types
        self.assertEqual([(1, 0.1), (2, 0.2), (3, 0.3), (4, 0.4)], out)
        out = toTuple(2, data3d)  # treat input as 2d array of two-field compound types
        self.assertEqual([[(0, 0.0), (1, 0.1)], [(2, 0.2), (3, 0.3)]], out)
        out = toTuple(1, data3d)  # treat input a 1d array of compound type of compound types
        self.assertEqual([((0, 0.0), (1, 0.1)), ((2, 0.2), (3, 0.3))], out)

    def testGetNumElements(self):     
        shape = (4,)
        nelements = getNumElements(shape)
        self.assertEqual(nelements, 4)

        shape = [10,]
        nelements = getNumElements(shape)
        self.assertEqual(nelements, 10)

        shape = (10,8)
        nelements = getNumElements(shape)
        self.assertEqual(nelements, 80)

    def testJsonToArray(self):
        dt = np.dtype('i4')
        shape = [4,]
        data = [0,2,4,6]
        out = jsonToArray(shape, dt, data)
        
        self.assertTrue(isinstance(out, np.ndarray))
        self.assertEqual(out.shape, (4,))
        for i in range(4):
            self.assertEqual(out[i], i*2)

        dt = np.dtype([('a', 'i4'), ('b', 'S5')])
        shape = [2,]
        data = [[4, 'four'], [5, 'five']]
        out = jsonToArray(shape, dt, data)
        self.assertTrue(isinstance(out, np.ndarray))
        
        self.assertEqual(out.shape, (2,))
        e0 = out[0].tolist()
        self.assertEqual(e0, (4, b'four'))
        e1 = out[1].tolist()
        self.assertEqual(e1, (5, b'five'))

        shape = [1,]
        data = [[6, 'six'],]
        out = jsonToArray(shape, dt, data)
        e0 = out[0].tolist()
        self.assertEqual(e0, (6, b'six'))

        data = [6, 'six']
        out = jsonToArray(shape, dt, data)
        e0 = out[0].tolist()
        self.assertEqual(e0, (6, b'six'))

         

if __name__ == '__main__':
    #setup test files
    
    unittest.main()


