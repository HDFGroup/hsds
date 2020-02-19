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
import asyncio
import unittest
import sys
import numpy as np
sys.path.append('../../../hsds/util')
sys.path.append('../../../hsds')
sys.path.append('../../chunkread')
from idUtil import getRootObjId
from storUtil import releaseStorageClient
from chunkread import get_app, read_points
import config


class ReadPointsTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(ReadPointsTest, self).__init__(*args, **kwargs)
        # main

    async def read_points_test(self, app, params):

        point_arr = np.array([[0,1],[2,3],[4,5],[6,7],[8,9]], dtype=np.uint64)
        params["point_arr"] = point_arr
        arr = await read_points(app, params)
        self.assertEqual(arr.shape, (5,))
        self.assertEqual(arr.dtype, np.dtype('>i4'))
        self.assertEqual(list(arr[...]), list((0,6,20,42,72)))
        await releaseStorageClient(app)

    def testReadPoints(self):

        dset_id = config.get("dset111_id")
        print("dset_id:", dset_id)

        # these are the properties of the /g1/g1.1/dset1.1.1. dataset in tall.h5
        dset_json = {"id": dset_id}
        dset_json["root"] = getRootObjId(dset_id)
        dset_json["type"] = {"class": "H5T_INTEGER", "base": "H5T_STD_I32BE"}
        dset_json["shape"] = {"class": "H5S_SIMPLE", "dims": [10, 10], "maxdims": [10, 10]}
        dset_json["layout"] = {"class": "H5D_CHUNKED", "dims": [10, 10]}

        chunk_id = 'c' + dset_id[1:] + "_0_0"

        params = {}
        params["dset_json"] = dset_json
        params["chunk_id"] = chunk_id
        params["bucket"] = config.get("bucket")
        loop = asyncio.get_event_loop()
        app = get_app(loop=loop)
        loop.run_until_complete(self.read_points_test(app, params))

        loop.close()


if __name__ == '__main__':
    #setup test files

    unittest.main()
