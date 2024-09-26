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
import numpy as np
import time

sys.path.append("../..")
from hsds.util.storUtil import _shuffle, _unshuffle, BIT_SHUFFLE, BYTE_SHUFFLE


class ShuffleUtilTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(ShuffleUtilTest, self).__init__(*args, **kwargs)
        # main

    def testByteShuffle(self):
        arr = np.zeros((3,), dtype="<u2")
        arr[0] = 0x0001
        arr[1] = 0x0002
        arr[2] = 0x0003
        data = arr.tobytes()
        fmt = "{:02X}{:02X} " * (len(data) // 2)
        self.assertEqual(fmt.format(*data), "0100 0200 0300 ")

        # Byte Shuffle
        shuffled = _shuffle(BYTE_SHUFFLE, data, chunk_shape=arr.shape, dtype=arr.dtype)
        self.assertEqual(fmt.format(*shuffled), "0102 0300 0000 ")
        unshuffled = _unshuffle(BYTE_SHUFFLE, shuffled, chunk_shape=arr.shape, dtype=arr.dtype)
        self.assertEqual(fmt.format(*data), "0100 0200 0300 ")
        for i in range(len(data)):
            self.assertEqual(data[i], unshuffled[i])

    def testBitShuffle(self):
        arr = np.array(list(range(100)), dtype="<u2")
        data = arr.tobytes()
        # Bit Shuffle
        shuffled = _shuffle(BIT_SHUFFLE, data, chunk_shape=arr.shape, dtype=arr.dtype)
        self.assertTrue(shuffled != data)
        unshuffled = _unshuffle(BIT_SHUFFLE, shuffled, chunk_shape=arr.shape, dtype=arr.dtype)
        arr_copy = np.frombuffer(unshuffled, dtype="<u2")
        self.assertTrue(np.array_equal(arr, arr_copy))

    def testTime(self):
        arr = np.random.rand(1000, 1000)
        now = time.time()
        data = arr.tobytes()
        shuffled = _shuffle(BYTE_SHUFFLE, data, chunk_shape=arr.shape, dtype=arr.dtype)

        self.assertEqual(len(data), len(shuffled))
        unshuffled = _unshuffle(BYTE_SHUFFLE, shuffled, chunk_shape=arr.shape, dtype=arr.dtype)
        elapsed = time.time() - now

        # this was taking ~0.04 s with an i7
        # without numba, time was 2.4s (60x slower)
        self.assertTrue(elapsed < 0.1, f"Elapsed time: {elapsed}")

        self.assertEqual(len(shuffled), len(unshuffled))
        self.assertEqual(data, unshuffled)


if __name__ == "__main__":
    # setup test files
    unittest.main()
