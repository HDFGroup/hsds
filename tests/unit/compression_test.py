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
import zlib

sys.path.append("../..")
from hsds.util.storUtil import _compress, _uncompress, getCompressors, BIT_SHUFFLE, BYTE_SHUFFLE


class CompressionUtilTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(CompressionUtilTest, self).__init__(*args, **kwargs)
        # main

    def testCompression(self):
        shape = 1_000_000
        dt = np.dtype("<i4")
        arr = np.random.randint(0, 200, shape, dtype=dt)

        data = arr.tobytes()
        compressors = getCompressors()
        print(compressors)

        kwargs = {"item_size": dt.itemsize}

        for compressor in compressors:

            print(f"testing compressor: {compressor}")
            kwargs["compressor"] = compressor

            kwargs["shuffle"] = 0
            cdata = _compress(data, **kwargs)
            self.assertTrue(len(cdata) != len(data))
            data_copy = _uncompress(cdata, **kwargs)
            self.assertEqual(data, data_copy)

            print(f"testing compressor: {compressor} with bit shuffle")
            kwargs["shuffle"] = BIT_SHUFFLE
            cdata = _compress(data, **kwargs)
            self.assertTrue(len(cdata) != len(data))
            data_copy = _uncompress(cdata, **kwargs)
            self.assertEqual(data, data_copy)

            print(f"testing compressor: {compressor} with byte shuffle")
            kwargs["shuffle"] = BYTE_SHUFFLE
            cdata = _compress(data, **kwargs)
            self.assertTrue(len(cdata) != len(data))
            data_copy = _uncompress(cdata, **kwargs)
            self.assertEqual(data, data_copy)

    def testZLibCompression(self):
        shape = 1_000_000
        dt = np.dtype("<i4")
        arr = np.random.randint(0, 200, shape, dtype=dt)

        data = arr.tobytes()
        # compress with zlib and verify we can uncompress the data again
        cdata = zlib.compress(data)
        kwargs = {"item_size": dt.itemsize, "compressor": "zlib"}
        data_copy = _uncompress(cdata, **kwargs)
        self.assertEqual(data, data_copy)


if __name__ == "__main__":
    # setup test files
    unittest.main()
