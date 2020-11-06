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
import requests
import json
import time
import helper
import config

# min/max chunk size - these can be set by config, but
# practially the min config value should be larger than
# CHUNK_MIN and the max config value should less than
# CHUNK_MAX
CHUNK_MIN = 1024                # lower limit  (1024b)
CHUNK_MAX = 50*1024*1024        # upper limit (50M)

class RangeGetTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(RangeGetTest, self).__init__(*args, **kwargs)
        self.endpoint = helper.getRangeGetEndpoint()

        print("endpoint:", self.endpoint)

        # main

    def testRangeGetBytes(self):
        print("testRangeGetBytes")
     

        hdf5_sample_bucket = config.get("hdf5_sample_bucket")
        if not hdf5_sample_bucket:
            print("hdf5_sample_bucket config not set, skipping testRangeGetBytes")
            return

        tall_json = helper.getHDF5JSON("tall.json")
        if not tall_json:
            print("tall.json file not found, skipping testRangeGetBytes")
            return

        if "tall.h5" not in tall_json:
            self.assertTrue(False)

        chunk_info = tall_json["tall.h5"]
        if "/g1/g1.1/dset1.1.2" not in chunk_info:
            self.assertTrue(False)

        dset112_info = chunk_info["/g1/g1.1/dset1.1.2"]
        if "byteStreams" not in dset112_info:
            self.assertTrue(False)
        byteStreams = dset112_info["byteStreams"]

        # should be just one element for this contiguous dataset
        self.assertTrue(len(byteStreams), 1)
        byteStream = byteStreams[0]
        dset112_offset = byteStream["file_offset"]
        dset112_size = byteStream["size"]
        self.assertEqual(dset112_size, 80)

        
        req_headers = {"accept": "application/octet-stream"}
        req = self.endpoint + '/'

        params = {}
        params["bucket"] = hdf5_sample_bucket
        params["key"] = "/data/hdf5test/tall.h5"
        params["offset"] = dset112_offset
        params["length"] = dset112_size
        rsp = requests.get(req, headers=req_headers, params=params)
        self.assertEqual(rsp.status_code, 200)

        self.assertEqual(rsp.headers['Content-Type'], "application/octet-stream")
        data = rsp.content
        self.assertEqual(len(data), dset112_size)
        # content should be 4-byte little-endian integers 0 thru 19
        for i in range(dset112_size):
            if i % 4 == 3:
                self.assertEqual(data[i], i//4)
            else:
                self.assertEqual(data[i], 0)
 
if __name__ == '__main__':
    #setup test files

    unittest.main()
