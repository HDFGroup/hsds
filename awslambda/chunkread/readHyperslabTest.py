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
import numpy as np

import hsds.hsds_logger as log
from hsds.util.idUtil import getRootObjId
from hsds.util.storUtil import releaseStorageClient
from hsds.chunkread import get_app, read_hyperslab

# dataset: /g1/g1.1/dset1.1.1 from /home/jreadey/tall.h5
DSET111_id = "d-75b88d05-46db146e-8419-28925f-14d998"
BUCKET_NAME = "hdflab"

class ReadHyperslabTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(ReadHyperslabTest, self).__init__(*args, **kwargs)
        # main


    def testReadHyperslab(self):
        app = get_app()

        dset_id = DSET111_id
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
        params["bucket"] = BUCKET_NAME

        arr = read_hyperslab(app, params)
        self.assertEqual(arr.shape, (10,10))
        self.assertEqual(arr.dtype, np.dtype('>i4'))
        self.assertEqual(list(arr[1,:]), list(range(10)))
        self.assertEqual(list(arr[:,1]), list(range(10)))

        params["slices"]=((slice(1,2,1),slice(0,4,1)))
        arr = read_hyperslab(app, params)
        self.assertEqual(arr.shape, (1,4))
        self.assertEqual(arr.dtype, np.dtype('>i4'))
        self.assertEqual(list(arr[0,:]), list(range(4)))
        #releaseStorageClient(app)


if __name__ == '__main__':
    #setup test files

    unittest.main()
