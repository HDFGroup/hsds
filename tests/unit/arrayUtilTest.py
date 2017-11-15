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
import hdf5dtype
from hdf5dtype import special_dtype
from hdf5dtype import check_dtype
from hdf5dtype import Reference
from hdf5dtype import createDataType


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
        self.assertTrue(isinstance(out[0], np.void))
        e0 = out[0].tolist()
        self.assertEqual(e0, (4, b'four'))
        self.assertTrue(isinstance(out[1], np.void))
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

        dt = special_dtype(vlen=np.dtype('int32'))
        shape = [4,]
        data = [[1,], [1,2], [1,2,3], [1,2,3,4]]
        out = jsonToArray(shape, dt, data)
        self.assertTrue(isinstance(out, np.ndarray))
        self.assertEqual(check_dtype(vlen=out.dtype), np.dtype('int32'))
        
        self.assertEqual(out.shape, (4,))
        self.assertEqual(out.dtype.kind, 'O')
        self.assertEqual(check_dtype(vlen=out.dtype), np.dtype('int32'))
        for i in range(4):
            e = out[i]  #.tolist()
            self.assertTrue(isinstance(e, tuple))
            self.assertEqual(e, tuple(range(1, i+2)))

        # create VLEN of obj ref's
        ref_type = {"class": "H5T_REFERENCE", 
                    "base": "H5T_STD_REF_OBJ"}
        vlen_type = {"class": "H5T_VLEN", "base": ref_type}
        dt = createDataType(vlen_type)  # np datatype
        
        id0 = 'g-a4f455b2-c8cf-11e7-8b73-0242ac110009'
        id1 = 'g-a50af844-c8cf-11e7-8b73-0242ac110009'
        id2 = 'g-a5236276-c8cf-11e7-8b73-0242ac110009'

        data = [ [id0,], [id0,id1], [id0,id1,id2] ]
        shape = [3,]
        out = jsonToArray(shape, dt, data)
        self.assertTrue(isinstance(out, np.ndarray))
        base_type = check_dtype(vlen=out.dtype)
        self.assertEqual(base_type.kind, 'S')
        self.assertEqual(base_type.itemsize, 38)
        
        self.assertEqual(out.shape, (3,))
        self.assertEqual(out.dtype.kind, 'O')
        self.assertEqual(check_dtype(vlen=out.dtype), np.dtype('S38'))

        e = out[0] 
        self.assertTrue(isinstance(e, tuple))
        self.assertEqual(e, (id0,))
        e = out[1] 
        self.assertTrue(isinstance(e, tuple))
        self.assertEqual(e, (id0,id1))
        e = out[2] 
        self.assertTrue(isinstance(e, tuple))
        self.assertEqual(e, (id0,id1,id2))
         

if __name__ == '__main__':
    #setup test files
    
    unittest.main()


