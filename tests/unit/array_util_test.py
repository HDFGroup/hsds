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
import json
import numpy as np

import sys

sys.path.append("../..")
from hsds.util.arrayUtil import (
    bytesArrayToList,
    toTuple,
    getNumElements,
    jsonToArray,
    arrayToBytes,
    bytesToArray,
    getByteArraySize,
)
from hsds.util.hdf5dtype import special_dtype
from hsds.util.hdf5dtype import check_dtype
from hsds.util.hdf5dtype import createDataType

# compare two numpy arrays.
# return true if the same (exclusive of null vs. empty array)
# false otherwise


def ndarray_compare(arr1, arr2):
    if not isinstance(arr1, np.ndarray) and not isinstance(arr2, np.ndarray):
        if not isinstance(arr1, np.void) and not isinstance(arr2, np.void):
            return arr1 == arr2
        if isinstance(arr1, np.void) and not isinstance(arr2, np.void):
            if arr1.size == 0 and not arr2:
                return True
            else:
                return False
        if not isinstance(arr1, np.void) and isinstance(arr2, np.void):
            if not arr1 and arr2.size == 0:
                return True
            else:
                return False
        # both np.voids
        if arr1.size != arr2.size:
            return False
        for i in range(arr1.size):
            if not ndarray_compare(arr1[i], arr2[i]):
                return False
        return True

    if isinstance(arr1, np.ndarray) and not isinstance(arr2, np.ndarray):
        # same only if arr1 is empty and arr2 is 0
        if arr1.size == 0 and not arr2:
            return True
        else:
            return False
    if not isinstance(arr1, np.ndarray) and isinstance(arr2, np.ndarray):
        # same only if arr1 is empty and arr2 is 0
        if not arr1 and not arr2.size == 0:
            return True
        else:
            return False

    # two ndarrays...
    if arr1.shape != arr2.shape:
        return False
    if arr2.dtype != arr2.dtype:
        return False
    nElements = np.prod(arr1.shape)
    arr1 = arr1.reshape((nElements,))
    arr2 = arr2.reshape((nElements,))
    for i in range(nElements):
        if not ndarray_compare(arr1[i], arr2[i]):
            return False
    return True


class ArrayUtilTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(ArrayUtilTest, self).__init__(*args, **kwargs)
        # main

    def testByteArrayToList(self):
        data_items = (
            42,
            "foo",
            b"foo",
            [1, 2, 3],
            (1, 2, 3),
            ["A", "B", "C"],
            [b"A", b"B", b"C"],
            [["A", "B"], [b"a", b"b", b"c"]],
        )
        for data in data_items:
            json_data = bytesArrayToList(data)
            # will throw TypeError if not able to convert
            json.dumps(json_data)

    def testToTuple(self):
        data0d = 42  # scalar
        data1d1 = [1]  # one dimensional, one element list
        data1d = [1, 2, 3, 4, 5]  # list
        data2d1 = [
            [1, 2],
        ]  # two dimensional, one element
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
        self.assertEqual([(1, 2)], out)
        out = toTuple(3, data3d)
        self.assertEqual(data3d, out)
        out = toTuple(1, data2d)  # treat input as 1d array of two-field compound types
        self.assertEqual([(1, 0.1), (2, 0.2), (3, 0.3), (4, 0.4)], out)
        out = toTuple(2, data3d)  # treat input as 2d array of two-field compound types
        self.assertEqual([[(0, 0.0), (1, 0.1)], [(2, 0.2), (3, 0.3)]], out)
        out = toTuple(
            1, data3d
        )  # treat input a 1d array of compound type of compound types
        self.assertEqual([((0, 0.0), (1, 0.1)), ((2, 0.2), (3, 0.3))], out)

    def testGetNumElements(self):
        shape = (4,)
        nelements = getNumElements(shape)
        self.assertEqual(nelements, 4)

        shape = [
            10,
        ]
        nelements = getNumElements(shape)
        self.assertEqual(nelements, 10)

        shape = (10, 8)
        nelements = getNumElements(shape)
        self.assertEqual(nelements, 80)

    def testJsonToArray(self):
        dt = np.dtype("i4")
        shape = [
            4,
        ]
        data = [0, 2, 4, 6]
        out = jsonToArray(shape, dt, data)

        self.assertTrue(isinstance(out, np.ndarray))
        self.assertEqual(out.shape, (4,))
        for i in range(4):
            self.assertEqual(out[i], i * 2)

        # compound type
        dt = np.dtype([("a", "i4"), ("b", "S5")])
        shape = [
            2,
        ]
        data = [[4, "four"], [5, "five"]]
        out = jsonToArray(shape, dt, data)
        self.assertTrue(isinstance(out, np.ndarray))

        self.assertEqual(out.shape, (2,))
        self.assertTrue(isinstance(out[0], np.void))
        e0 = out[0].tolist()
        self.assertEqual(e0, (4, b"four"))
        self.assertTrue(isinstance(out[1], np.void))
        e1 = out[1].tolist()
        self.assertEqual(e1, (5, b"five"))

        shape = [
            1,
        ]
        data = [
            [6, "six"],
        ]
        out = jsonToArray(shape, dt, data)
        e0 = out[0].tolist()
        self.assertEqual(e0, (6, b"six"))

        data = [6, "six"]
        out = jsonToArray(shape, dt, data)
        e0 = out[0].tolist()
        self.assertEqual(e0, (6, b"six"))

        # VLEN ascii
        dt = special_dtype(vlen=bytes)
        data = [b"one", b"two", b"three", "four", b"five"]
        shape = [
            5,
        ]
        out = jsonToArray(shape, dt, data)
        self.assertTrue("vlen" in out.dtype.metadata)
        self.assertEqual(out.dtype.metadata["vlen"], bytes)
        self.assertEqual(out.dtype.kind, "O")
        self.assertEqual(out.shape, (5,))
        # TBD: code does not actually enforce use of bytes vs. str,
        #  probably not worth the effort to fix
        self.assertEqual(out[2], b"three")
        self.assertEqual(out[3], "four")

        # VLEN str
        dt = special_dtype(vlen=str)
        data = [
            ["part 1 - section A", "part 1 - section B"],
            ["part 2 - section A", "part 2 - section B"],
        ]
        shape = [
            2,
        ]
        out = jsonToArray(shape, dt, data)
        self.assertTrue("vlen" in out.dtype.metadata)
        self.assertEqual(out.dtype.metadata["vlen"], str)
        self.assertEqual(out.dtype.kind, "O")
        self.assertEqual(out.shape, (2,))
        self.assertEqual(out[0], tuple(data[0]))
        self.assertEqual(out[1], tuple(data[1]))

        # VLEN Scalar str
        dt = special_dtype(vlen=str)
        data = "I'm a string!"
        shape = [
            1,
        ]
        out = jsonToArray(shape, dt, data)

        # VLEN unicode
        dt = special_dtype(vlen=bytes)
        data = ["one", "two", "three", "four", "five"]
        shape = [
            5,
        ]
        out = jsonToArray(shape, dt, data)
        self.assertTrue("vlen" in out.dtype.metadata)
        self.assertEqual(out.dtype.metadata["vlen"], bytes)
        self.assertEqual(out.dtype.kind, "O")
        # TBD: this should show up as bytes, but may not be worth the effort
        self.assertEqual(out[2], "three")

        # VLEN data
        dt = special_dtype(vlen=np.dtype("int32"))
        shape = [
            4,
        ]
        data = [
            [
                1,
            ],
            [1, 2],
            [1, 2, 3],
            [1, 2, 3, 4],
        ]
        out = jsonToArray(shape, dt, data)
        self.assertTrue(isinstance(out, np.ndarray))
        self.assertEqual(check_dtype(vlen=out.dtype), np.dtype("int32"))

        self.assertEqual(out.shape, (4,))
        self.assertEqual(out.dtype.kind, "O")
        self.assertEqual(check_dtype(vlen=out.dtype), np.dtype("int32"))
        for i in range(4):
            e = out[i]  # .tolist()
            self.assertTrue(isinstance(e, tuple))
            self.assertEqual(e, tuple(range(1, i + 2)))

        # VLEN 2D data
        dt = special_dtype(vlen=np.dtype("int32"))
        shape = [2, 2]
        data = [
            [
                [
                    0,
                ],
                [1, 2],
            ],
            [
                [
                    1,
                ],
                [2, 3],
            ],
        ]
        out = jsonToArray(shape, dt, data)
        self.assertTrue(isinstance(out, np.ndarray))
        self.assertEqual(check_dtype(vlen=out.dtype), np.dtype("int32"))

        self.assertEqual(out.shape, (2, 2))
        self.assertEqual(out.dtype.kind, "O")
        self.assertEqual(check_dtype(vlen=out.dtype), np.dtype("int32"))
        for i in range(2):
            for j in range(2):
                e = out[i, j]  # .tolist()
                self.assertTrue(isinstance(e, tuple))

        # create VLEN of obj ref's
        ref_type = {"class": "H5T_REFERENCE", "base": "H5T_STD_REF_OBJ"}
        vlen_type = {"class": "H5T_VLEN", "base": ref_type}
        dt = createDataType(vlen_type)  # np datatype

        id0 = "g-a4f455b2-c8cf-11e7-8b73-0242ac110009"
        id1 = "g-a50af844-c8cf-11e7-8b73-0242ac110009"
        id2 = "g-a5236276-c8cf-11e7-8b73-0242ac110009"

        data = [
            [
                id0,
            ],
            [id0, id1],
            [id0, id1, id2],
        ]
        shape = [
            3,
        ]
        out = jsonToArray(shape, dt, data)
        self.assertTrue(isinstance(out, np.ndarray))
        base_type = check_dtype(vlen=out.dtype)
        self.assertEqual(base_type.kind, "S")
        self.assertEqual(base_type.itemsize, 48)

        self.assertEqual(out.shape, (3,))
        self.assertEqual(out.dtype.kind, "O")
        self.assertEqual(check_dtype(vlen=out.dtype), np.dtype("S48"))

        e = out[0]
        self.assertTrue(isinstance(e, tuple))
        self.assertEqual(e, (id0,))
        e = out[1]
        self.assertTrue(isinstance(e, tuple))
        self.assertEqual(e, (id0, id1))
        e = out[2]
        self.assertTrue(isinstance(e, tuple))
        self.assertEqual(e, (id0, id1, id2))

    def testToBytes(self):
        # Simple array
        dt = np.dtype("<i4")
        arr = np.asarray((1, 2, 3, 4), dtype=dt)
        buffer = arrayToBytes(arr)
        self.assertEqual(buffer, arr.tobytes())

        # convert buffer back to arr
        arr_copy = bytesToArray(buffer, dt, (4,))
        # print("arr_copy: {}".format(arr_copy))
        self.assertTrue(np.array_equal(arr, arr_copy))

        # fixed length string
        dt = np.dtype("S8")
        arr = np.asarray(("abcdefgh", "ABCDEFGH", "12345678"), dtype=dt)
        buffer = arrayToBytes(arr)
        self.assertEqual(buffer, arr.tobytes())

        # convert back to arry
        arr_copy = bytesToArray(buffer, dt, (3,))
        self.assertTrue(ndarray_compare(arr, arr_copy))

        # Compound non-vlen
        dt = np.dtype([("x", "f8"), ("y", "i4")])
        arr = np.zeros((4,), dtype=dt)
        arr[0] = (3.12, 42)
        arr[3] = (1.28, 69)
        buffer = arrayToBytes(arr)
        self.assertEqual(buffer, arr.tobytes())

        # convert back to array
        arr_copy = bytesToArray(buffer, dt, (4,))
        self.assertTrue(ndarray_compare(arr, arr_copy))

        # VLEN of int32's
        dt = np.dtype("O", metadata={"vlen": np.dtype("int32")})
        arr = np.zeros((4,), dtype=dt)
        arr[0] = np.int32(
            [
                1,
            ]
        )
        arr[1] = np.int32([1, 2])
        arr[2] = 0  # test un-intialized value
        arr[3] = np.int32([1, 2, 3])
        buffer = arrayToBytes(arr)
        self.assertEqual(len(buffer), 40)

        # convert back to array
        arr_copy = bytesToArray(buffer, dt, (4,))
        self.assertTrue(ndarray_compare(arr, arr_copy))

        # VLEN of strings
        dt = np.dtype("O", metadata={"vlen": str})
        arr = np.zeros((5,), dtype=dt)
        arr[0] = "one: \u4e00"
        arr[1] = "two: \u4e8c"
        arr[2] = "three: \u4e09"
        arr[3] = "four: \u56db"
        arr[4] = 0
        buffer = arrayToBytes(arr)

        expected_length = 55
        expected = bytearray(expected_length)
        expected[0:4] = b"\x08\x00\x00\x00"
        expected[4:16] = b"one: \xe4\xb8\x80\x08\x00\x00\x00"
        expected[16:28] = b"two: \xe4\xba\x8c\n\x00\x00\x00"
        expected[28:42] = b"three: \xe4\xb8\x89\t\x00\x00\x00"
        expected[42:55] = b"four: \xe5\x9b\x9b\x00\x00\x00\x00"

        self.assertEqual(len(buffer), expected_length)

        self.assertEqual(buffer, expected)

        # convert back to array
        arr_copy = bytesToArray(buffer, dt, (5,))
        self.assertTrue(ndarray_compare(arr, arr_copy))
        # VLEN of bytes
        dt = np.dtype("O", metadata={"vlen": bytes})
        arr = np.zeros((5,), dtype=dt)
        arr[0] = b"Parting"
        arr[1] = b"is such"
        arr[2] = b"sweet"
        arr[3] = b"sorrow"
        arr[4] = 0

        buffer = arrayToBytes(arr)

        expected = bytearray(45)
        expected[0:11] = b"\x07\x00\x00\x00Parting"
        expected[11:22] = b"\x07\x00\x00\x00is such"
        expected[22:31] = b"\x05\x00\x00\x00sweet"
        expected[31:41] = b"\x06\x00\x00\x00sorrow"
        expected[41:45] = b"\x00\x00\x00\x00"

        self.assertEqual(len(buffer), len(expected))
        self.assertEqual(buffer, expected)  # same serialization as with str

        # convert back to array
        arr_copy = bytesToArray(buffer, dt, (5,))
        self.assertTrue(ndarray_compare(arr, arr_copy))

        #
        # Compound str vlen
        #
        dt_vstr = np.dtype("O", metadata={"vlen": str})
        dt = np.dtype([("x", "i4"), ("tag", dt_vstr), ("code", "S4")])
        arr = np.zeros((4,), dtype=dt)
        arr[0] = (42, "Hello", "X1")
        arr[3] = (84, "Bye", "XYZ")
        count = getByteArraySize(arr)
        buffer = arrayToBytes(arr)

        self.assertEqual(len(buffer), 56)
        self.assertEqual(buffer.find(b"Hello"), 8)
        self.assertEqual(buffer.find(b"Bye"), 49)
        self.assertEqual(buffer.find(b"X1"), 13)
        self.assertEqual(buffer.find(b"XYZ"), 52)

        # convert back to array
        arr_copy = bytesToArray(buffer, dt, (4,))
        self.assertTrue(ndarray_compare(arr, arr_copy))

        #
        # Compound int vlen
        #
        dt_vint = np.dtype("O", metadata={"vlen": "int32"})
        dt = np.dtype([("x", "int32"), ("tag", dt_vint)])
        arr = np.zeros((4,), dtype=dt)
        arr[0] = (42, np.array((), dtype="int32"))
        arr[3] = (84, np.array((1, 2, 3), dtype="int32"))
        count = getByteArraySize(arr)
        self.assertEqual(count, 44)
        buffer = arrayToBytes(arr)
        self.assertEqual(len(buffer), 44)
        buffer_expected = {0: 42, 24: 84, 28: 12, 32: 1, 36: 2, 40: 3}
        for i in range(44):
            if i in buffer_expected:
                self.assertEqual(buffer[i], buffer_expected[i])
            else:
                self.assertEqual(buffer[i], 0)

        # convert back to array
        arr_copy = bytesToArray(buffer, dt, (4,))
        self.assertTrue(ndarray_compare(arr, arr_copy))

        #
        # VLEN utf string with array type
        #
        dt_arr_str = np.dtype("(2,)O", metadata={"vlen": str})
        dt = np.dtype([("x", "i4"), ("tag", dt_arr_str)])
        arr = np.zeros((4,), dtype=dt)
        dt_str = np.dtype("O", metadata={"vlen": str})
        arr[0] = (42, np.asarray(["hi", "bye"], dtype=dt_str))
        arr[3] = (84, np.asarray(["hi-hi", "bye-bye"], dtype=dt_str))
        buffer = arrayToBytes(arr)
        self.assertEqual(len(buffer), 81)

        self.assertEqual(buffer.find(b"hi"), 8)
        self.assertEqual(buffer.find(b"bye"), 14)
        self.assertEqual(buffer.find(b"hi-hi"), 49)
        self.assertEqual(buffer.find(b"bye-bye"), 58)

        # convert back to array
        arr_copy = bytesToArray(buffer, dt, (4,))

        self.assertEqual(arr.dtype, arr_copy.dtype)
        self.assertEqual(arr.shape, arr_copy.shape)
        for i in range(4):
            e = arr[i]
            e_copy = arr_copy[i]
            self.assertTrue(np.array_equal(e, e_copy))
        #
        # VLEN ascii with array type
        #
        dt_arr_str = np.dtype("(2,)O", metadata={"vlen": bytes})
        dt = np.dtype([("x", "i4"), ("tag", dt_arr_str)])
        arr = np.zeros((4,), dtype=dt)
        dt_str = np.dtype("O", metadata={"vlen": bytes})
        arr[0] = (42, np.asarray([b"hi", b"bye"], dtype=dt_str))
        arr[3] = (84, np.asarray([b"hi-hi", b"bye-bye"], dtype=dt_str))
        buffer = arrayToBytes(arr)
        self.assertEqual(len(buffer), 81)

        self.assertEqual(buffer.find(b"hi"), 8)
        self.assertEqual(buffer.find(b"bye"), 14)
        self.assertEqual(buffer.find(b"hi-hi"), 49)
        self.assertEqual(buffer.find(b"bye-bye"), 58)
        # convert back to array

        arr_copy = bytesToArray(buffer, dt, (4,))
        self.assertTrue(ndarray_compare(arr, arr_copy))

    def testJsonToBytes(self):
        #
        # VLEN int
        #
        dt = special_dtype(vlen=np.dtype("int32"))
        shape = [
            4,
        ]
        data = [
            [
                1,
            ],
            [1, 2],
            [1, 2, 3],
            [1, 2, 3, 4],
        ]
        arr = jsonToArray(shape, dt, data)
        self.assertTrue(isinstance(arr, np.ndarray))
        self.assertEqual(check_dtype(vlen=arr.dtype), np.dtype("int32"))
        buffer = arrayToBytes(arr)
        self.assertEqual(len(buffer), 56)

        expected = bytearray(48)
        expected[0:8] = b"\x04\x00\x00\x00\x01\x00\x00\x00"
        expected[8:16] = b"\x08\x00\x00\x00\x01\x00\x00\x00"
        expected[16:24] = b"\x02\x00\x00\x00\x0c\x00\x00\x00"
        expected[24:32] = b"\x01\x00\x00\x00\x02\x00\x00\x00"
        expected[32:40] = b"\x03\x00\x00\x00\x10\x00\x00\x00"
        expected[40:48] = b"\x01\x00\x00\x00\x02\x00\x00\x00"
        expected[48:56] = b"\x03\x00\x00\x00\x04\x00\x00\x00"
        self.assertEqual(buffer, expected)

        # convert back to array
        arr_copy = bytesToArray(buffer, dt, (4,))
        # np.array_equal doesn't work for object arrays
        self.assertEqual(arr.dtype, arr_copy.dtype)
        self.assertEqual(arr.shape, arr_copy.shape)
        for i in range(4):
            e = arr[i]
            e_copy = arr_copy[i]
            self.assertTrue(np.array_equal(e, e_copy))
        #
        # Compound vlen
        #
        dt_str = np.dtype("O", metadata={"vlen": str})
        dt = np.dtype([("x", "i4"), ("tag", dt_str)])
        shape = [
            4,
        ]
        data = [[42, "Hello"], [0, 0], [0, 0], [84, "Bye"]]
        arr = jsonToArray(shape, dt, data)
        self.assertTrue(isinstance(arr, np.ndarray))
        buffer = arrayToBytes(arr)
        self.assertEqual(len(buffer), 40)

        expected = bytearray(40)
        expected[0:8] = b"*\x00\x00\x00\x05\x00\x00\x00"
        expected[8:19] = b"Hello\x00\x00\x00\x00\x00\x00"
        expected[19:26] = b"\x00\x00\x00\x00\x00\x00\x00"
        expected[26:40] = b"\x00\x00\x00T\x00\x00\x00\x03\x00\x00\x00Bye"

        self.assertEqual(buffer, expected)

        # convert back to array
        arr_copy = bytesToArray(buffer, dt, (4,))
        # np.array_equal doesn't work for object arrays
        self.assertEqual(arr.dtype, arr_copy.dtype)
        self.assertEqual(arr.shape, arr_copy.shape)
        for i in range(4):
            e = arr[i]
            e_copy = arr_copy[i]
            self.assertTrue(np.array_equal(e, e_copy))

        #
        # VLEN utf with array type
        #
        dt_arr_str = np.dtype("(2,)O", metadata={"vlen": str})
        dt = np.dtype([("x", "i4"), ("tag", dt_arr_str)])
        shape = [
            4,
        ]
        data = [
            [42, ["hi", "bye"]],
            [0, [0, 0]],
            [0, [0, 0]],
            [84, ["hi-hi", "bye-bye"]],
        ]
        arr = jsonToArray(shape, dt, data)
        self.assertTrue(isinstance(arr, np.ndarray))
        buffer = arrayToBytes(arr)
        self.assertEqual(len(buffer), 81)
        self.assertEqual(buffer.find(b"hi"), 8)
        self.assertEqual(buffer.find(b"bye"), 14)
        self.assertEqual(buffer.find(b"hi-hi"), 49)
        self.assertEqual(buffer.find(b"bye-bye"), 58)
        arr_copy = bytesToArray(buffer, dt, (4,))

        self.assertEqual(arr.dtype, arr_copy.dtype)
        self.assertEqual(arr.shape, arr_copy.shape)
        for i in range(4):
            e = arr[i]
            e_copy = arr_copy[i]
            self.assertTrue(np.array_equal(e, e_copy))

        #
        # VLEN ascii with array type
        #
        dt_arr_str = np.dtype("(2,)O", metadata={"vlen": bytes})
        dt = np.dtype([("x", "i4"), ("tag", dt_arr_str)])
        shape = [
            4,
        ]
        data = [
            [42, [b"hi", b"bye"]],
            [0, [0, 0]],
            [0, [0, 0]],
            [84, [b"hi-hi", b"bye-bye"]],
        ]
        arr = jsonToArray(shape, dt, data)
        self.assertTrue(isinstance(arr, np.ndarray))
        buffer = arrayToBytes(arr)
        self.assertEqual(len(buffer), 81)
        self.assertEqual(buffer.find(b"hi"), 8)
        self.assertEqual(buffer.find(b"bye"), 14)
        self.assertEqual(buffer.find(b"hi-hi"), 49)
        self.assertEqual(buffer.find(b"bye-bye"), 58)
        arr_copy = bytesToArray(buffer, dt, (4,))

        self.assertEqual(arr.dtype, arr_copy.dtype)
        self.assertEqual(arr.shape, arr_copy.shape)
        for i in range(4):
            e = arr[i]
            e_copy = arr_copy[i]
            self.assertTrue(np.array_equal(e, e_copy))


if __name__ == "__main__":
    # setup test files

    unittest.main()
