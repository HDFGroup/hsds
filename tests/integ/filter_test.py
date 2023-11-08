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
import helper
import config


class FilterTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(FilterTest, self).__init__(*args, **kwargs)
        self.base_domain = helper.getTestDomainName(self.__class__.__name__)
        helper.setupDomain(self.base_domain)
        self.endpoint = helper.getEndpoint()

    def setUp(self):
        self.session = helper.getSession()

    def tearDown(self):
        if self.session:
            self.session.close()

    def getUUIDByPath(self, domain, h5path):
        return helper.getUUIDByPath(domain, h5path, session=self.session)

    def getRootUUID(self, domain, username=None, password=None):
        return helper.getRootUUID(
            domain, username=username, password=password, session=self.session
        )

    # main

    def testDeflateCompression(self):
        # test Dataset with creation property list
        print("testDeflateCompression", self.base_domain)
        headers = helper.getRequestHeaders(domain=self.base_domain)
        # get domain
        req = helper.getEndpoint() + "/"
        rsp = self.session.get(req, headers=headers)
        rspJson = json.loads(rsp.text)
        self.assertTrue("root" in rspJson)
        root_uuid = rspJson["root"]

        # create the dataset
        req = self.endpoint + "/datasets"

        # Create ~1MB dataset

        payload = {"type": "H5T_STD_I8LE", "shape": [1024, 1024]}
        # define deflate compression
        gzip_filter = {
            "class": "H5Z_FILTER_DEFLATE",
            "id": 1,
            "level": 9,
            "name": "deflate",
        }
        payload["creationProperties"] = {"filters": [gzip_filter]}
        req = self.endpoint + "/datasets"
        rsp = self.session.post(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)  # create dataset
        rspJson = json.loads(rsp.text)
        dset_uuid = rspJson["id"]
        self.assertTrue(helper.validateId(dset_uuid))

        # link new dataset as 'dset'
        name = "dset"
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
        payload = {"id": dset_uuid}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # write a horizontal strip of 22s
        req = self.endpoint + "/datasets/" + dset_uuid + "/value"
        data = [22] * 1024
        payload = {"start": [512, 0], "stop": [513, 1024], "value": data}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 200)

        # read back the 512,512 element
        req = self.endpoint + "/datasets/" + dset_uuid + "/value"  # test
        params = {"select": "[512:513,512:513]"}  # read  1 element
        rsp = self.session.get(req, params=params, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        value = rspJson["value"]
        self.assertEqual(len(value), 1)
        row = value[0]
        self.assertEqual(len(row), 1)
        self.assertEqual(row[0], 22)

    def testShuffleFilter(self):
        # test Dataset with creation property list
        print("testShuffleFilter", self.base_domain)
        headers = helper.getRequestHeaders(domain=self.base_domain)
        # get domain
        req = helper.getEndpoint() + "/"
        rsp = self.session.get(req, headers=headers)
        rspJson = json.loads(rsp.text)
        self.assertTrue("root" in rspJson)
        root_uuid = rspJson["root"]

        # create the dataset
        req = self.endpoint + "/datasets"

        # Create ~4MB dataset

        payload = {"type": "H5T_STD_I32LE", "shape": [1024, 1024]}
        # define sshufle compression
        shuffle_filter = {"class": "H5Z_FILTER_SHUFFLE", "id": 2, "name": "shuffle"}
        payload["creationProperties"] = {"filters": [shuffle_filter]}
        req = self.endpoint + "/datasets"
        rsp = self.session.post(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)  # create dataset
        rspJson = json.loads(rsp.text)
        dset_uuid = rspJson["id"]
        self.assertTrue(helper.validateId(dset_uuid))

        # link new dataset as 'dset'
        name = "dset"
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
        payload = {"id": dset_uuid}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # write a horizontal strip of 22s
        req = self.endpoint + "/datasets/" + dset_uuid + "/value"
        data = [22] * 1024
        payload = {"start": [512, 0], "stop": [513, 1024], "value": data}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 200)

        # read back the 512,512 element
        req = self.endpoint + "/datasets/" + dset_uuid + "/value"  # test
        params = {"select": "[512:513,512:513]"}  # read  1 element
        rsp = self.session.get(req, params=params, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        value = rspJson["value"]
        self.assertEqual(len(value), 1)
        row = value[0]
        self.assertEqual(len(row), 1)
        self.assertEqual(row[0], 22)

    def testShuffleAndDeflate(self):
        # test Dataset with creation property list
        print("testShuffleAndDeflate", self.base_domain)
        headers = helper.getRequestHeaders(domain=self.base_domain)
        # get domain
        req = helper.getEndpoint() + "/"
        rsp = self.session.get(req, headers=headers)
        rspJson = json.loads(rsp.text)
        self.assertTrue("root" in rspJson)
        root_uuid = rspJson["root"]

        # create the dataset
        req = self.endpoint + "/datasets"

        # Create ~1MB dataset

        payload = {"type": "H5T_STD_I32LE", "shape": [1024, 1024]}
        # define deflate compression
        gzip_filter = {
            "class": "H5Z_FILTER_DEFLATE",
            "id": 1,
            "level": 9,
            "name": "deflate",
        }
        # and shuffle compression
        shuffle_filter = {"class": "H5Z_FILTER_SHUFFLE", "id": 2, "name": "shuffle"}
        payload["creationProperties"] = {"filters": [shuffle_filter, gzip_filter]}
        req = self.endpoint + "/datasets"
        rsp = self.session.post(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)  # create dataset
        rspJson = json.loads(rsp.text)
        dset_uuid = rspJson["id"]
        self.assertTrue(helper.validateId(dset_uuid))

        # link new dataset as 'dset'
        name = "dset"
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
        payload = {"id": dset_uuid}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # write a horizontal strip of 22s
        req = self.endpoint + "/datasets/" + dset_uuid + "/value"
        data = [22] * 1024
        payload = {"start": [512, 0], "stop": [513, 1024], "value": data}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 200)

        # read back the 512,512 element
        req = self.endpoint + "/datasets/" + dset_uuid + "/value"  # test
        params = {"select": "[512:513,512:513]"}  # read  1 element
        rsp = self.session.get(req, params=params, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        value = rspJson["value"]
        self.assertEqual(len(value), 1)
        row = value[0]
        self.assertEqual(len(row), 1)
        self.assertEqual(row[0], 22)

    def testBitShuffleAndDeflate(self):
        # test Dataset with creation property list
        print("testBitShuffleAndDeflate", self.base_domain)
        headers = helper.getRequestHeaders(domain=self.base_domain)
        # get domain
        req = helper.getEndpoint() + "/"
        rsp = self.session.get(req, headers=headers)
        rspJson = json.loads(rsp.text)
        self.assertTrue("root" in rspJson)
        root_uuid = rspJson["root"]

        # create the dataset
        req = self.endpoint + "/datasets"

        # Create ~1MB dataset

        payload = {"type": "H5T_STD_I32LE", "shape": [1024, 1024]}
        # define deflate compression
        gzip_filter = {
            "class": "H5Z_FILTER_DEFLATE",
            "id": 1,
            "level": 9,
            "name": "deflate",
        }
        # and bit shuffle
        bitshuffle_filter = {"class": "H5Z_FILTER_BITSHUFFLE", "id": 32008, "name": "bitshuffle"}
        payload["creationProperties"] = {"filters": [bitshuffle_filter, gzip_filter]}
        req = self.endpoint + "/datasets"
        rsp = self.session.post(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)  # create dataset
        rspJson = json.loads(rsp.text)
        dset_uuid = rspJson["id"]
        self.assertTrue(helper.validateId(dset_uuid))

        # link new dataset as 'dset'
        name = "dset"
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
        payload = {"id": dset_uuid}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # write a horizontal strip of 22s
        req = self.endpoint + "/datasets/" + dset_uuid + "/value"
        data = [22] * 1024
        payload = {"start": [512, 0], "stop": [513, 1024], "value": data}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 200)

        # read back the 512,512 element
        req = self.endpoint + "/datasets/" + dset_uuid + "/value"  # test
        params = {"select": "[512:513,512:513]"}  # read  1 element
        rsp = self.session.get(req, params=params, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        value = rspJson["value"]
        self.assertEqual(len(value), 1)
        row = value[0]
        self.assertEqual(len(row), 1)
        self.assertEqual(row[0], 22)


    def testDeshuffling(self):
        """Test the shuffle filter implementation used with a known data file."""
        print("testDeshuffling", self.base_domain)
        headers = helper.getRequestHeaders(domain=self.base_domain)

        hdf5_sample_bucket = config.get("hdf5_sample_bucket")
        if not hdf5_sample_bucket:
            print("hdf5_sample_bucket config not set, skipping testShuffleFilter")
            return

        sample_json = helper.getHDF5JSON("sample-shuffle-data.json")
        if not sample_json:
            print("sample-shuffle-data.json file not found, skipping testDeshuffling")
            return

        if "sample-shuffle-data.h5" not in sample_json:
            self.assertTrue(False, 'JSON "sample-shuffle-data.h5" key not found')
        else:
            sample_json = sample_json["sample-shuffle-data.h5"]

        # Datasets in the test file...
        dset_info = [
            ("/float32", {"base": "H5T_IEEE_F32LE", "class": "H5T_FLOAT"}, "<f4"),
            ("/float64", {"base": "H5T_IEEE_F64LE", "class": "H5T_FLOAT"}, "<f8"),
            ("/int16", {"base": "H5T_STD_I16LE", "class": "H5T_INTEGER"}, "<i2"),
            ("/int32", {"base": "H5T_STD_I32LE", "class": "H5T_INTEGER"}, "<i4"),
            ("/int64", {"base": "H5T_STD_I64LE", "class": "H5T_INTEGER"}, "<i8"),
            ("/uint16", {"base": "H5T_STD_U16LE", "class": "H5T_INTEGER"}, "<u2"),
            ("/uint32", {"base": "H5T_STD_U32LE", "class": "H5T_INTEGER"}, "<u4"),
            ("/uint64", {"base": "H5T_STD_U64LE", "class": "H5T_INTEGER"}, "<u8"),
        ]

        # Verify chunk location info is available for all datasets...
        for name, undef, undef in dset_info:
            try:
                sample_json[name]["byteStreams"][0]["file_offset"]
                sample_json[name]["byteStreams"][0]["size"]
                sample_json[name]["byteStreams"][0]["array_offset"]
            except (KeyError, IndexError):
                self.assertFalse(True, f"{name}: Incomplete chunk location info")

        # Get domain
        req = helper.getEndpoint() + "/"
        rsp = self.session.get(req, headers=headers)
        rspJson = json.loads(rsp.text)
        self.assertTrue("root" in rspJson, '"root" JSON key missing')
        root_uuid = rspJson["root"]

        # Sample file URI...
        furi = f"{hdf5_sample_bucket}/data/hdf5test/sample-shuffle-data.h5"

        # Create HSDS datasets and test reading shuffled data from an HDF5 file...
        for name, dt, numpy_dt in dset_info:
            num_chunks = len(sample_json[name]["byteStreams"])
            chunk_info = dict()
            for i in range(num_chunks):
                this_chunk = sample_json[name]["byteStreams"][i]
                chunk_info[f'{this_chunk["index"]}'] = (
                    this_chunk["file_offset"],
                    this_chunk["size"],
                )

            # Create the HSDS dataset that points to the test shuffled data...
            payload = {
                "type": dt,
                "shape": [100],
                "creationProperties": {
                    "filters": [
                        {"class": "H5Z_FILTER_SHUFFLE", "id": 2, "name": "shuffle"}
                    ],
                    "layout": {
                        "class": "H5D_CHUNKED_REF",
                        "file_uri": furi,
                        "dims": [100],
                        "chunks": chunk_info,
                    },
                },
            }
            req = self.endpoint + "/datasets"
            rsp = self.session.post(req, data=json.dumps(payload), headers=headers)
            self.assertEqual(rsp.status_code, 201, rsp.text)
            rspJson = json.loads(rsp.text)
            dset_uuid = rspJson["id"]
            self.assertTrue(helper.validateId(dset_uuid))
            req = self.endpoint + "/groups/" + root_uuid + "/links/" + name[1:]
            payload = {"id": dset_uuid}
            rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
            self.assertEqual(rsp.status_code, 201, rsp.text)

            # Read dataset's test values...
            req = self.endpoint + "/datasets/" + dset_uuid + "/value"
            rsp = self.session.get(req, headers=headers)
            if rsp.status_code == 404:
                print(f"File object: {furi} not found, skipping " "shuffle filter test")
                return
            self.assertEqual(rsp.status_code, 200, rsp.text)
            rspJson = json.loads(rsp.text)
            self.assertTrue("hrefs" in rspJson, 'Missing "hrefs" JSON key')
            self.assertTrue("value" in rspJson, 'Missing "value" JSON key')
            value = rspJson["value"]
            self.assertTrue(
                np.array_equal(
                    np.fromiter(value, dtype=numpy_dt), np.arange(100, dtype=numpy_dt)
                ),
                f'Different values for "{name}" dataset',
            )


if __name__ == "__main__":
    # setup test files
    unittest.main()
