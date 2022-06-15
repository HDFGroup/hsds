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
import math
import time
from tests.integ import helper
import config

# Test the binary PUTs and GETs work for request larger than
# max_request_size (by default 100MB)
# max_request_size should only apply to JSON streaming or
# variable length datatypes


class StreamTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(StreamTest, self).__init__(*args, **kwargs)
        self.base_domain = config.get("stream_test_domain")
        self.username = config.get("user_name")
        self.password = config.get("user_password")
        helper.setupDomain(
            self.base_domain, username=self.username, password=self.password
        )
        self.endpoint = helper.getEndpoint()

    def setUp(self):
        self.session = helper.getSession()

    def tearDown(self):
        if self.session:
            self.session.close()

    def getUUIDByPath(self, domain, h5path):
        return helper.getUUIDByPath(
            domain,
            h5path,
            username=self.username,
            password=self.password,
            session=self.session,
        )

    def getRootUUID(self, domain):
        return helper.getRootUUID(
            domain, username=self.username, password=self.password, session=self.session
        )

    def testStream2D(self):
        # write a large request for a 2d dataset
        print("testStream2D", self.base_domain)
        kwargs = {}
        if self.username:
            kwargs["username"] = self.username
        if self.password:
            kwargs["password"] = self.password
        headers = helper.getRequestHeaders(domain=self.base_domain, **kwargs)
        headers_bin_req = helper.getRequestHeaders(domain=self.base_domain, **kwargs)
        headers_bin_req["Content-Type"] = "application/octet-stream"
        headers_bin_rsp = helper.getRequestHeaders(domain=self.base_domain, **kwargs)
        headers_bin_rsp["accept"] = "application/octet-stream"

        req = self.endpoint + "/"

        # Get root uuid
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        root_uuid = rspJson["root"]
        helper.validateId(root_uuid)

        create_dataset = True
        dset_id = None
        dset_name = "dset2d"
        num_col = int(config.get("stream_test_ncols"))
        num_row = int(config.get("stream_test_nrows"))
        item_size = 8  # 8 bytes for H5T_STD_U64LE
        print(f"dataset shape: [{num_row}, {num_col}]")

        try:
            dset_id = self.getUUIDByPath(self.base_domain, "/dset2d")
            print("got dset_id:", dset_id)
            # get the dset json
            req = self.endpoint + "/datasets/" + dset_id
            rsp = self.session.get(req, headers=headers)
            self.assertEqual(rsp.status_code, 200)
            rspJson = json.loads(rsp.text)
            shape = rspJson["shape"]
            dims = shape["dims"]
            # can re-use this if the shape is what we need
            if len(dims) == 2 and dims[0] == num_row and dims[1] == num_col:
                create_dataset = False
            else:
                print("dims don't match - delete and create new dataset")

        except KeyError:
            pass  # will create a new dataset

        if create_dataset and dset_id:
            # delete the old datsaet
            print(f"deleting dataset: {dset_id}")
            req = self.endpoint + "/datasets/" + dset_id
            rsp = self.session.delete(req, headers=headers)
            self.assertEqual(rsp.status_code, 200)

            # delete the link
            req = self.endpoint + "/groups/" + root_uuid + "/links/" + dset_name
            rsp = self.session.delete(req, headers=headers)
            self.assertEqual(rsp.status_code, 200)

        if create_dataset:
            # create dataset
            print(f"create datset with shape: [{num_row}, {num_col}]")
            data = {"type": "H5T_STD_U64LE", "shape": [num_row, num_col]}

            req = self.endpoint + "/datasets"
            rsp = self.session.post(req, data=json.dumps(data), headers=headers)
            self.assertEqual(rsp.status_code, 201)
            rspJson = json.loads(rsp.text)
            dset_id = rspJson["id"]
            print(f"got dset_id: {dset_id}")

            # link new dataset
            req = self.endpoint + "/groups/" + root_uuid + "/links/" + dset_name
            payload = {"id": dset_id}
            rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
            self.assertEqual(rsp.status_code, 201)

        # initialize bytearray to test values
        num_bytes = item_size * num_row * num_col
        print(
            f"initializing test data ({num_bytes} bytes, {num_bytes/(1024*1024):.2f} MiB)"
        )
        bin_data = bytearray(num_bytes)
        exp = int(math.log10(num_col)) + 1
        for i in range(num_row):
            row_start_value = i * 10 ** exp
            for j in range(num_col):
                n = row_start_value + j + 1
                int_bytes = n.to_bytes(8, "little")
                offset_start = (i * num_col + j) * item_size
                offset_end = offset_start + item_size
                bin_data[offset_start:offset_end] = int_bytes

        print("writing...")
        ts = time.time()
        req = self.endpoint + "/datasets/" + dset_id + "/value"
        rsp = self.session.put(req, data=bin_data, headers=headers_bin_req)
        self.assertEqual(rsp.status_code, 200)
        elapsed = time.time() - ts
        mb_per_sec = num_bytes / (1024 * 1024 * elapsed)
        print(f" elapsed: {elapsed:.2f} s, {mb_per_sec:.2f} mb/s")

        # read back the data as binary
        print("reading...")
        ts = time.time()
        rsp = self.session.get(req, headers=headers_bin_rsp)
        self.assertEqual(rsp.status_code, 200)
        elapsed = time.time() - ts
        mb_per_sec = num_bytes / (1024 * 1024 * elapsed)
        print(f" elapsed: {elapsed:.2f} s, {mb_per_sec:.2f} mb/s")

        print("comparing sent vs. received")
        data = rsp.content
        self.assertEqual(len(data), num_bytes)
        self.assertEqual(data, bin_data)
        print("passed!")


if __name__ == "__main__":
    # setup test files
    unittest.main()
