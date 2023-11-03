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


class ValueTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(ValueTest, self).__init__(*args, **kwargs)
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

    def checkVerbose(self, dset_id, headers=None, expected=None):
        # do a flush with rescan, then check the expected return values are correct
        req = f"{self.endpoint}/"
        params = {"flush": 1, "rescan": 1}
        rsp = self.session.put(req, params=params, headers=headers)
        # should get a NO_CONTENT code,
        self.assertEqual(rsp.status_code, 204)

        # do a get and verify the additional keys are
        req = f"{self.endpoint}/datasets/{dset_id}"
        params = {"verbose": 1}

        rsp = self.session.get(req, params=params, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)

        for k in expected:
            self.assertTrue(k in rspJson)
            self.assertEqual(rspJson[k], expected[k])

        # main

    def testPut1DDataset(self):
        # Test PUT value for 1d dataset
        print("testPut1DDataset", self.base_domain)

        headers = helper.getRequestHeaders(domain=self.base_domain)
        req = self.endpoint + "/"

        # Get root uuid
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        root_uuid = rspJson["root"]
        helper.validateId(root_uuid)

        # create dataset
        data = {"type": "H5T_STD_I32LE", "shape": 10}

        req = self.endpoint + "/datasets"
        rsp = self.session.post(req, data=json.dumps(data), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)
        dset_id = rspJson["id"]
        self.assertTrue(helper.validateId(dset_id))

        # add an attribute
        attr_payload = {"type": "H5T_STD_I32LE", "value": 42}
        attr_name = "attr1"
        req = self.endpoint + "/datasets/" + dset_id + "/attributes/" + attr_name
        rsp = self.session.put(req, data=json.dumps(attr_payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)  # created

        # link new dataset as 'dset1d'
        name = "dset1d"
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
        payload = {"id": dset_id}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # read values from dset (should be zeros)
        req = self.endpoint + "/datasets/" + dset_id + "/value"
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        expect_value = [
            0,
        ]
        expect_value *= data["shape"]
        self.assertEqual(rspJson["value"], expect_value)

        # write to the dset
        data = list(range(10))  # write 0-9
        payload = {"value": data}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 200)

        # read back the data
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        self.assertEqual(rspJson["value"], data)

        # read coordinate selection
        params = {"select": "[[0,1,3,7]]"}
        rsp = self.session.get(req, params=params, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        self.assertEqual(rspJson["value"], [0, 1, 3, 7])

        # read a selection
        params = {"select": "[2:8]"}  # read 6 elements, starting at index 2
        rsp = self.session.get(req, params=params, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        self.assertEqual(rspJson["value"], list(range(2, 8)))

        # read one element.  cf test for PR #84
        params = {"select": "[3]"}  # read 4th element
        rsp = self.session.get(req, params=params, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        self.assertEqual(rspJson["value"], [3])

        # try to read beyond the bounds of the array
        params = {"select": "[2:18]"}  # read 6 elements, starting at index 2
        rsp = self.session.get(req, params=params, headers=headers)
        self.assertEqual(rsp.status_code, 400)

        # check values we should get from a verbose query
        expected = {"num_chunks": 1, "allocated_size": 40}
        self.checkVerbose(dset_id, headers=headers, expected=expected)

    def testPut1DDatasetBinary(self):
        # Test PUT value for 1d dataset using binary data
        print("testPut1DDatasetBinary", self.base_domain)
        NUM_ELEMENTS = 10  # 1000000 - this value is hitting nginx request size limit

        headers = helper.getRequestHeaders(domain=self.base_domain)
        headers_bin_req = helper.getRequestHeaders(domain=self.base_domain)
        headers_bin_req["Content-Type"] = "application/octet-stream"
        headers_bin_rsp = helper.getRequestHeaders(domain=self.base_domain)
        headers_bin_rsp["accept"] = "application/octet-stream"

        req = self.endpoint + "/"

        # Get root uuid
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        root_uuid = rspJson["root"]
        helper.validateId(root_uuid)

        # create dataset
        data = {"type": "H5T_STD_I32LE", "shape": NUM_ELEMENTS}
        req = self.endpoint + "/datasets"
        rsp = self.session.post(req, data=json.dumps(data), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)
        dset_id = rspJson["id"]
        self.assertTrue(helper.validateId(dset_id))

        # link new dataset as 'dset1d'
        name = "dset1d"
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
        payload = {"id": dset_id}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # read values from dset (should be zeros)
        req = self.endpoint + "/datasets/" + dset_id + "/value"
        rsp = self.session.get(req, headers=headers_bin_rsp)
        self.assertEqual(rsp.status_code, 200)
        self.assertEqual(rsp.headers["Content-Type"], "application/octet-stream")
        data = rsp.content
        self.assertEqual(len(data), NUM_ELEMENTS * 4)
        for i in range(NUM_ELEMENTS):
            offset = i * 4
            self.assertEqual(data[offset + 0], 0)
            self.assertEqual(data[offset + 1], 0)
            self.assertEqual(data[offset + 2], 0)
            self.assertEqual(data[offset + 3], 0)

        # write to the dset
        # write 0-9 as four-byte little-endian integers
        data = bytearray(4 * NUM_ELEMENTS)
        for i in range(NUM_ELEMENTS):
            data[i * 4] = i % 256
        rsp = self.session.put(req, data=data, headers=headers_bin_req)
        self.assertEqual(rsp.status_code, 200)

        # read back the data
        rsp = self.session.get(req, headers=headers_bin_rsp)
        self.assertEqual(rsp.status_code, 200)
        data = rsp.content
        self.assertEqual(len(data), NUM_ELEMENTS * 4)
        for i in range(NUM_ELEMENTS):
            offset = i * 4
            self.assertEqual(data[offset + 0], i % 256)
            self.assertEqual(data[offset + 1], 0)
            self.assertEqual(data[offset + 2], 0)
            self.assertEqual(data[offset + 3], 0)

        # write a selection
        params = {"select": "[4:6]"}  # 4th and 5th elements
        data = bytearray(4 * 2)
        for i in range(2):
            data[i * 4] = 255
        rsp = self.session.put(req, data=data, params=params, headers=headers_bin_req)
        self.assertEqual(rsp.status_code, 200)

        # read a selection
        params = {"select": "[0:6]"}  # read first 6 elements
        rsp = self.session.get(req, params=params, headers=headers_bin_rsp)
        self.assertEqual(rsp.status_code, 200)
        data = rsp.content
        self.assertEqual(len(data), 24)
        for i in range(6):
            offset = i * 4
            if i >= 4:
                # these were updated by the previous selection
                self.assertEqual(data[offset + 0], 255)
            else:
                self.assertEqual(data[offset + 0], i)
            self.assertEqual(data[offset + 1], 0)
            self.assertEqual(data[offset + 2], 0)
            self.assertEqual(data[offset + 3], 0)

        # read one element.  cf test for PR #84
        params = {"select": "[3]"}  # read 4th element
        rsp = self.session.get(req, params=params, headers=headers_bin_rsp)
        self.assertEqual(rsp.status_code, 200)
        data = rsp.content
        self.assertEqual(len(data), 4)
        self.assertEqual(data[0], 3)
        self.assertEqual(data[1], 0)
        self.assertEqual(data[2], 0)
        self.assertEqual(data[3], 0)

        # check values we should get from a verbose query
        expected = {"num_chunks": 1, "allocated_size": 40}
        self.checkVerbose(dset_id, headers=headers, expected=expected)

    def testPut2DDataset(self):
        """Test PUT value for 2d dataset"""
        print("testPut2DDataset", self.base_domain)

        headers = helper.getRequestHeaders(domain=self.base_domain)
        req = self.endpoint + "/"

        # Get root uuid
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        root_uuid = rspJson["root"]
        helper.validateId(root_uuid)

        # create dataset
        num_col = 8
        num_row = 4
        data = {"type": "H5T_STD_I32LE", "shape": [num_row, num_col]}

        req = self.endpoint + "/datasets"
        rsp = self.session.post(req, data=json.dumps(data), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)
        dset_id = rspJson["id"]
        self.assertTrue(helper.validateId(dset_id))

        # link new dataset as 'dset1d'
        name = "dset2d"
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
        payload = {"id": dset_id}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # read values from dset (should be zeros)
        req = self.endpoint + "/datasets/" + dset_id + "/value"
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        for i in range(num_row):
            expected_value = [
                0,
            ]
            expected_value *= num_col
            self.assertEqual(rspJson["value"][i], expected_value)

        # write to the dset
        json_data = []
        for i in range(num_row):
            row = []
            for j in range(num_col):
                row.append(i * 10 + j)
            json_data.append(row)
        payload = {"value": json_data}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 200)

        # read back the data
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        self.assertEqual(rspJson["value"], json_data)

        # read a selection
        params = {"select": "[3:4,2:8]"}  # read 3 elements
        rsp = self.session.get(req, params=params, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        self.assertEqual(
            rspJson["value"],
            [
                json_data[3][2:8],
            ],
        )

        # write a coordinate selection
        json_data = [
            [120, 121, 122],
        ]
        payload = {"value": json_data}
        params = {"select": "[3:4,[0,2,5]]"}  # write 3 elements
        rsp = self.session.put(
            req, data=json.dumps(payload), params=params, headers=headers
        )
        self.assertEqual(rsp.status_code, 200)

        rsp = self.session.get(req, params=params, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        self.assertEqual(
            rspJson["value"],
            [
                [120, 121, 122],
            ],
        )
        # check values we should get from a verbose query
        expected = {"num_chunks": 1, "allocated_size": 128}
        self.checkVerbose(dset_id, headers=headers, expected=expected)

    def testPut2DDatasetBinary(self):
        # Test PUT value for 2d dataset
        print("testPut2DDatasetBinary", self.base_domain)

        headers = helper.getRequestHeaders(domain=self.base_domain)
        headers_bin_req = helper.getRequestHeaders(domain=self.base_domain)
        headers_bin_req["Content-Type"] = "application/octet-stream"
        headers_bin_rsp = helper.getRequestHeaders(domain=self.base_domain)
        headers_bin_rsp["accept"] = "application/octet-stream"

        req = self.endpoint + "/"

        # Get root uuid
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        root_uuid = rspJson["root"]
        helper.validateId(root_uuid)

        # create dataset
        num_col = 8
        num_row = 4
        data = {"type": "H5T_STD_I32LE", "shape": [num_row, num_col]}

        req = self.endpoint + "/datasets"
        rsp = self.session.post(req, data=json.dumps(data), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)
        dset_id = rspJson["id"]
        self.assertTrue(helper.validateId(dset_id))

        # link new dataset as 'dset2d'
        name = "dset2d"
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
        payload = {"id": dset_id}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # read values from dset (should be zeros)
        req = self.endpoint + "/datasets/" + dset_id + "/value"
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        for i in range(num_row):
            expected_value = [
                0,
            ]
            expected_value *= num_col
            self.assertEqual(rspJson["value"][i], expected_value)

        # initialize bytearray to test values
        bin_data = bytearray(4 * num_row * num_col)
        json_data = []
        for i in range(num_row):
            row = []
            for j in range(num_col):
                bin_data[(i * num_col + j) * 4] = i * 10 + j
                row.append(i * 10 + j)  # create json data for comparison
            json_data.append(row)
        rsp = self.session.put(req, data=bin_data, headers=headers_bin_req)
        self.assertEqual(rsp.status_code, 200)

        # read back the data as json
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        self.assertEqual(rspJson["value"], json_data)

        # read data as binary
        rsp = self.session.get(req, headers=headers_bin_rsp)
        self.assertEqual(rsp.status_code, 200)
        data = rsp.content
        self.assertEqual(len(data), num_row * num_col * 4)
        self.assertEqual(data, bin_data)

        # read a selection
        params = {"select": "[3:4,2:8]"}  # read 6 elements, starting at index 2
        rsp = self.session.get(req, params=params, headers=headers_bin_rsp)
        self.assertEqual(rsp.status_code, 200)
        data = rsp.content
        self.assertEqual(len(data), 6 * 4)
        for i in range(6):
            self.assertEqual(data[i * 4], 3 * 10 + i + 2)

        # write a coordinate selection
        json_data = [
            [120, 121, 122],
        ]
        bin_data = bytearray(4 * 3)
        bin_data[0] = 120
        bin_data[4] = 121
        bin_data[8] = 122
        params = {"select": "[3:4,[0,2,5]]"}  # write 3 elements
        rsp = self.session.put(
            req, data=bin_data, params=params, headers=headers_bin_req
        )
        self.assertEqual(rsp.status_code, 200)

        rsp = self.session.get(req, params=params, headers=headers_bin_rsp)
        self.assertEqual(rsp.status_code, 200)
        data = rsp.content
        self.assertEqual(len(data), 12)
        self.assertEqual(data, bin_data)

        # check values we should get from a verbose query
        expected = {"num_chunks": 1, "allocated_size": 128}
        self.checkVerbose(dset_id, headers=headers, expected=expected)

    def testPutSelection1DDataset(self):
        """Test PUT value with selection for 1d dataset"""
        print("testPutSelection1DDataset", self.base_domain)

        headers = helper.getRequestHeaders(domain=self.base_domain)
        req = self.endpoint + "/"

        # Get root uuid
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        root_uuid = rspJson["root"]
        helper.validateId(root_uuid)

        # create dataset
        dset_body = {"type": "H5T_STD_I32LE", "shape": 10}

        req = self.endpoint + "/datasets"
        rsp = self.session.post(req, data=json.dumps(dset_body), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)
        dset_id = rspJson["id"]
        self.assertTrue(helper.validateId(dset_id))

        # link new dataset as 'dset1d'
        name = "dset1d"
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
        payload = {"id": dset_id}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # write to dset
        req = self.endpoint + "/datasets/" + dset_id + "/value"
        data = list(range(10))  # write 0-9
        data_part1 = data[0:5]
        data_part2 = data[5:10]

        # write part 1
        payload = {"start": 0, "stop": 5, "value": data_part1}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 200)

        # write part 2
        payload = {"start": 5, "stop": 10, "value": data_part2}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 200)

        # read back the data
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        self.assertEqual(rspJson["value"], data)

        # write data with a step of 2
        payload = {"start": 0, "stop": 10, "step": 2, "value": data_part1}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 200)

        payload = {"start": 1, "stop": 10, "step": 2, "value": data_part2}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 200)

        # read back the data
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        value = rspJson["value"]
        for i in range(10):
            if i % 2 == 0:
                self.assertEqual(value[i], i // 2)
            else:
                self.assertEqual(value[i], (i // 2) + 5)

    def testPutSelection2DDataset(self):
        """Test PUT value with selection for 2d dataset"""
        print("testPutSelection2DDataset", self.base_domain)

        headers = helper.getRequestHeaders(domain=self.base_domain)

        req = self.endpoint + "/"

        # Get root uuid
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        root_uuid = rspJson["root"]
        helper.validateId(root_uuid)

        # create dataset
        # pass in layout specification so that we can test selection across chunk boundries
        data = {"type": "H5T_STD_I32LE", "shape": [45, 54]}
        data["creationProperties"] = {
            "layout": {"class": "H5D_CHUNKED", "dims": [10, 10]}
        }

        req = self.endpoint + "/datasets"
        rsp = self.session.post(req, data=json.dumps(data), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)
        dset_id = rspJson["id"]
        self.assertTrue(helper.validateId(dset_id))

        # link new dataset as 'dset2d'
        name = "dset2d"
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
        payload = {"id": dset_id}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # write a horizontal strip of 22s
        req = self.endpoint + "/datasets/" + dset_id + "/value"
        data = [
            22,
        ] * 50
        payload = {"start": [22, 2], "stop": [23, 52], "value": data}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 200)

        # read back a vertical strip that crossed the horizontal strip
        req = self.endpoint + "/datasets/" + dset_id + "/value"  # test
        params = {"select": "[20:25,21:22]"}  # read 6 elements, starting at index 20
        rsp = self.session.get(req, params=params, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        value = rspJson["value"]
        self.assertEqual(len(value), 5)
        self.assertEqual(value, [[0], [0], [22], [0], [0]])

        # write 44's to a region with a step value of 2 and 3
        req = self.endpoint + "/datasets/" + dset_id + "/value"
        data = [
            44,
        ] * 20
        payload = {"start": [10, 20], "stop": [20, 32], "step": [2, 3], "value": data}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 200)

        # read back a sub-block
        req = self.endpoint + "/datasets/" + dset_id + "/value"  # test
        params = {
            "select": "[12:13,23:26]"
        }  # read 6 elements, starting at index (12,14)
        rsp = self.session.get(req, params=params, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        value = rspJson["value"]
        self.assertEqual(len(value), 1)
        value = value[0]
        self.assertEqual(len(value), 3)
        self.assertEqual(value, [44, 0, 0])

    def testPutNullPadString(self):
        # Test PUT value for 1d dataset with fixed length string types
        print("testPutNullPadString", self.base_domain)

        headers = helper.getRequestHeaders(domain=self.base_domain)
        req = self.endpoint + "/"

        # Get root uuid
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        root_uuid = rspJson["root"]
        helper.validateId(root_uuid)

        # create dataset
        fixed_str_type = {
            "charSet": "H5T_CSET_ASCII",
            "class": "H5T_STRING",
            "length": 7,
            "strPad": "H5T_STR_NULLPAD",
        }
        data = {"type": fixed_str_type, "shape": 4}

        req = self.endpoint + "/datasets"
        rsp = self.session.post(req, data=json.dumps(data), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)
        dset_id = rspJson["id"]
        self.assertTrue(helper.validateId(dset_id))

        # link new dataset as 'dset_str'
        name = "dset_str"
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
        payload = {"id": dset_id}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # read values from dset (should be empty strings)
        req = self.endpoint + "/datasets/" + dset_id + "/value"
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        expected_value = [
            "",
        ]
        expected_value *= data["shape"]
        self.assertEqual(rspJson["value"], expected_value)

        # write to the dset
        data = ["Parting", "is such", "sweet", "sorrow."]
        payload = {"value": data}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 200)

        # read back the data
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        self.assertEqual(rspJson["value"], data)

        # read a selection
        params = {"select": "[1:3]"}  # read 2 elements, starting at index 1
        rsp = self.session.get(req, params=params, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        self.assertEqual(rspJson["value"], ["is such", "sweet"])

    def testPutNullPadStringBinary(self):
        # Test PUT value for 1d dataset with fixed length string types
        print("testPutNullPadStringBinary", self.base_domain)

        headers = helper.getRequestHeaders(domain=self.base_domain)
        headers_bin_req = helper.getRequestHeaders(domain=self.base_domain)
        headers_bin_req["Content-Type"] = "application/octet-stream"
        req = self.endpoint + "/"

        # Get root uuid
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        root_uuid = rspJson["root"]
        helper.validateId(root_uuid)

        STR_LENGTH = 7
        STR_COUNT = 4

        # create dataset
        fixed_str_type = {
            "charSet": "H5T_CSET_ASCII",
            "class": "H5T_STRING",
            "length": STR_LENGTH,
            "strPad": "H5T_STR_NULLPAD",
        }
        data = {"type": fixed_str_type, "shape": STR_COUNT}

        req = self.endpoint + "/datasets"
        rsp = self.session.post(req, data=json.dumps(data), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)
        dset_id = rspJson["id"]
        self.assertTrue(helper.validateId(dset_id))

        # link new dataset as 'dset_str'
        name = "dset_str"
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
        payload = {"id": dset_id}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # read values from dset (should be empty strings)
        req = self.endpoint + "/datasets/" + dset_id + "/value"
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        expected_value = [
            "",
        ]
        expected_value *= data["shape"]
        self.assertEqual(rspJson["value"], expected_value)

        # write to the dset
        strings = ["Parting", "is such", "sweet", "sorrow."]
        data = bytearray(STR_COUNT * STR_LENGTH)
        for i in range(STR_COUNT):
            string = strings[i]
            for j in range(STR_LENGTH):
                offset = i * STR_LENGTH + j
                if j < len(string):
                    data[offset] = ord(string[j])
                else:
                    data[offset] = 0  # null padd rest of the element

        payload = {"value": data}
        rsp = self.session.put(req, data=data, headers=headers_bin_req)
        self.assertEqual(rsp.status_code, 200)

        # read back the data
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        rsp_value = rspJson["value"]
        self.assertEqual(len(rsp_value), STR_COUNT)
        self.assertEqual(rsp_value, strings)

    def testPutOverflowFixedString(self):
        # Test PUT values to large for a fixed string datatype.
        # Server should accept PUT requests but silently truncate.
        print("testPutOverflowFixedString", self.base_domain)

        headers = helper.getRequestHeaders(domain=self.base_domain)
        req = self.endpoint + "/"

        # Get root uuid
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        root_uuid = rspJson["root"]
        helper.validateId(root_uuid)

        # create dataset
        fixed_str_type = {
            "charSet": "H5T_CSET_ASCII",
            "class": "H5T_STRING",
            "length": 7,
            "strPad": "H5T_STR_NULLPAD",
        }
        data = {"type": fixed_str_type, "shape": 4}

        req = self.endpoint + "/datasets"
        rsp = self.session.post(req, data=json.dumps(data), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)
        dset_id = rspJson["id"]
        self.assertTrue(helper.validateId(dset_id))

        # link new dataset as 'dset_str'
        name = "dset_str"
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
        payload = {"id": dset_id}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # read values from dset (should be empty strings)
        req = self.endpoint + "/datasets/" + dset_id + "/value"
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        expected_value = [
            "",
        ]
        expected_value *= data["shape"]
        self.assertEqual(rspJson["value"], expected_value)

        # write to the dset
        data = ["123456", "1234567", "12345678", "123456789"]
        payload = {"value": data}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 200)

        # read back the data
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)

        # read all values
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        values = rspJson["value"]
        self.assertEqual(len(values), 4)
        self.assertEqual(values[0], "123456")
        self.assertEqual(values[1], "1234567")
        self.assertEqual(values[2], "1234567")  # last character gets clipped
        self.assertEqual(values[2], "1234567")  # last two characters get clipped

    def testPutScalarDataset(self):
        # Test read/write to scalar dataset
        print("testPutScalarDataset", self.base_domain)

        headers = helper.getRequestHeaders(domain=self.base_domain)
        req = self.endpoint + "/"

        # Get root uuid
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        root_uuid = rspJson["root"]
        helper.validateId(root_uuid)

        # create a dataset obj
        str_type = {
            "charSet": "H5T_CSET_ASCII",
            "class": "H5T_STRING",
            "strPad": "H5T_STR_NULLPAD",
            "length": 40,
        }
        data = {"type": str_type}
        req = self.endpoint + "/datasets"
        rsp = self.session.post(req, data=json.dumps(data), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)
        self.assertEqual(rspJson["attributeCount"], 0)
        dset_id = rspJson["id"]
        self.assertTrue(helper.validateId(dset_id))

        # link new dataset as 'dset_scalar'
        name = "dset_scalar"
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
        payload = {"id": dset_id}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # read unintialized value from dataset
        req = self.endpoint + "/datasets/" + dset_id + "/value"
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        self.assertEqual(rspJson["value"], "")

        # write to the dataset
        data = "Hello, world"
        payload = {"value": data}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 200)

        # read back the value
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        self.assertEqual(rspJson["value"], "Hello, world")

    def testNullSpaceDataset(self):
        # Test attempted read/write to null space dataset
        print("testNullSpaceDataset", self.base_domain)

        headers = helper.getRequestHeaders(domain=self.base_domain)
        req = self.endpoint + "/"

        # Get root uuid
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        root_uuid = rspJson["root"]
        helper.validateId(root_uuid)

        # create a dataset obj
        str_type = {
            "charSet": "H5T_CSET_ASCII",
            "class": "H5T_STRING",
            "strPad": "H5T_STR_NULLPAD",
            "length": 40,
        }
        data = {"type": str_type, "shape": "H5S_NULL"}
        req = self.endpoint + "/datasets"
        rsp = self.session.post(req, data=json.dumps(data), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)
        dset_id = rspJson["id"]
        self.assertTrue(helper.validateId(dset_id))

        # link new dataset as 'dset_null'
        name = "dset_null"
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
        payload = {"id": dset_id}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # try reading from the dataset
        req = self.endpoint + "/datasets/" + dset_id + "/value"
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 400)

        # try writing to the dataset
        data = "Hello, world"
        payload = {"value": data}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 400)

    def testPutCompound(self):
        headers = helper.getRequestHeaders(domain=self.base_domain)
        req = self.endpoint + "/"

        # Get root uuid
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        root_uuid = rspJson["root"]
        helper.validateId(root_uuid)

        fields = (
            {"name": "temp", "type": "H5T_STD_I32LE"},
            {"name": "pressure", "type": "H5T_IEEE_F16LE"},
        )
        datatype = {"class": "H5T_COMPOUND", "fields": fields}

        #
        # create compound scalar dataset
        #
        payload = {"type": datatype}
        req = self.endpoint + "/datasets"
        rsp = self.session.post(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)  # create dataset

        rspJson = json.loads(rsp.text)
        dset0d_uuid = rspJson["id"]
        self.assertTrue(helper.validateId(dset0d_uuid))

        # verify the shape of the dataset
        req = self.endpoint + "/datasets/" + dset0d_uuid
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)  # get dataset
        rspJson = json.loads(rsp.text)
        shape = rspJson["shape"]
        self.assertEqual(shape["class"], "H5S_SCALAR")

        # link new dataset as 'dset0_compound'
        name = "dset0d"
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
        payload = {"id": dset0d_uuid}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # write entire array
        value = (42, 0.42)
        payload = {"value": value}
        req = self.endpoint + "/datasets/" + dset0d_uuid + "/value"
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 200)  # write value

        # read back the value
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)

        #
        # create 1d dataset
        #
        num_elements = 10
        payload = {"type": datatype, "shape": num_elements}
        req = self.endpoint + "/datasets"
        rsp = self.session.post(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)  # create dataset

        rspJson = json.loads(rsp.text)
        dset1d_uuid = rspJson["id"]
        self.assertTrue(helper.validateId(dset1d_uuid))

        # link new dataset as 'dset1'
        name = "dset1d"
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
        payload = {"id": dset1d_uuid}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # write entire array
        value = []
        for i in range(num_elements):
            item = (i * 10, i * 10 + i / 10.0)
            value.append(item)
        payload = {"value": value}

        req = self.endpoint + "/datasets/" + dset1d_uuid + "/value"
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 200)  # write value

        # selection write
        payload = {"start": 0, "stop": 1, "value": (42, 0.42)}
        req = self.endpoint + "/datasets/" + dset1d_uuid + "/value"
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 200)  # write value

        # read back the data
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)

        readData = rspJson["value"]

        self.assertEqual(readData[0][0], 42)
        self.assertEqual(readData[1][0], 10)

        #
        # create 2d dataset
        #
        dims = [2, 2]
        payload = {"type": datatype, "shape": dims}
        req = self.endpoint + "/datasets"
        rsp = self.session.post(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)  # create dataset

        rspJson = json.loads(rsp.text)
        dset2d_uuid = rspJson["id"]
        self.assertTrue(helper.validateId(dset2d_uuid))

        # link new dataset as 'dset2d'
        name = "dset2d"
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
        payload = {"id": dset2d_uuid}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # write entire array
        value = []
        for i in range(dims[0]):
            row = []
            for j in range(dims[1]):
                item = (i * 10, i * 10 + j / 2.0)
                row.append(item)
            value.append(row)
        payload = {"value": value}

        req = self.endpoint + "/datasets/" + dset2d_uuid + "/value"
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 200)  # write value

        # read back the data
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        readData = rspJson["value"]
        self.assertEqual(readData[0][0], [0, 0.0])
        self.assertEqual(readData[1][0], [10, 10.0])
        self.assertEqual(readData[0][1], [0, 0.5])
        self.assertEqual(readData[1][1], [10, 10.5])

    def testSimpleTypeFillValue(self):
        # test Dataset with simple type and fill value
        print("testSimpleTypeFillValue", self.base_domain)
        headers = helper.getRequestHeaders(domain=self.base_domain)

        # get domain
        req = helper.getEndpoint() + "/"
        rsp = self.session.get(req, headers=headers)
        rspJson = json.loads(rsp.text)
        self.assertTrue("root" in rspJson)
        root_uuid = rspJson["root"]

        # create the dataset
        req = self.endpoint + "/datasets"
        payload = {"type": "H5T_STD_I32LE", "shape": 10}
        creation_props = {"fillValue": 42}
        payload["creationProperties"] = creation_props

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

        # read back the data
        req = self.endpoint + "/datasets/" + dset_uuid + "/value"
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        expected_value = [
            42,
        ]
        expected_value *= 10
        self.assertEqual(rspJson["value"], expected_value)

        # write some values
        value = [
            24,
        ]
        value *= 5
        payload = {"start": 0, "stop": 5, "value": value}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 200)

        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        ret_values = rspJson["value"]
        for i in range(5):
            self.assertEqual(ret_values[i], 24)
            self.assertEqual(ret_values[i + 5], 42)

    def testCompoundFillValue(self):
        headers = helper.getRequestHeaders(domain=self.base_domain)
        req = self.endpoint + "/"

        # Get root uuid
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        root_uuid = rspJson["root"]
        helper.validateId(root_uuid)

        # ASCII 8-char fixed width
        str_type = {
            "charSet": "H5T_CSET_ASCII",
            "class": "H5T_STRING",
            "strPad": "H5T_STR_NULLPAD",
            "length": 8,
        }

        fields = (
            {"name": "tag", "type": str_type},
            {"name": "value", "type": "H5T_STD_I32LE"},
        )
        datatype = {"class": "H5T_COMPOUND", "fields": fields}
        fill_value = ["blank", -999]
        creationProperties = {"fillValue": fill_value}

        #
        # create compound dataset
        #
        payload = {
            "type": datatype,
            "shape": 40,
            "creationProperties": creationProperties,
        }
        req = self.endpoint + "/datasets"
        rsp = self.session.post(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)  # create dataset

        rspJson = json.loads(rsp.text)
        dset_uuid = rspJson["id"]

        # verify the shape of the dataset
        req = self.endpoint + "/datasets/" + dset_uuid
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)  # get dataset
        rspJson = json.loads(rsp.text)
        shape = rspJson["shape"]
        self.assertEqual(shape["class"], "H5S_SIMPLE")
        expected_value = [
            40,
        ]
        self.assertEqual(shape["dims"], expected_value)

        # read the default values
        req = self.endpoint + "/datasets/" + dset_uuid + "/value"
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)  # OK
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        value = rspJson["value"]
        for i in range(40):
            self.assertEqual(value[i], fill_value)

        # write some values
        new_value = ("mytag", 123)
        new_values = []
        for i in range(20):
            new_values.append(new_value)

        payload = {"start": 0, "stop": 20, "value": new_values}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 200)

        # read the values back
        req = self.endpoint + "/datasets/" + dset_uuid + "/value"
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)  # OK
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        value = rspJson["value"]
        for i in range(20):
            self.assertEqual(value[i], list(new_value))
            self.assertEqual(value[i + 20], fill_value)

    def testBigFillValue(self):
        # test Dataset with simple type and fill value that is very large
        # (i.e. a large string)
        print("testBigFillValue", self.base_domain)
        headers = helper.getRequestHeaders(domain=self.base_domain)

        # get domain
        req = helper.getEndpoint() + "/"
        rsp = self.session.get(req, headers=headers)
        rspJson = json.loads(rsp.text)
        self.assertTrue("root" in rspJson)
        root_uuid = rspJson["root"]

        item_length = 1000
        # ASCII fixed width
        str_type = {
            "charSet": "H5T_CSET_ASCII",
            "class": "H5T_STRING",
            "strPad": "H5T_STR_NULLPAD",
            "length": item_length,
        }

        fill_value = "X" * item_length
        # create the dataset
        req = self.endpoint + "/datasets"
        payload = {"type": str_type, "shape": 10}
        payload["creationProperties"] = {"fillValue": fill_value}
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

        # read back the data
        req = self.endpoint + "/datasets/" + dset_uuid + "/value"
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        expected_value = [
            fill_value,
        ]
        expected_value *= 10
        self.assertEqual(rspJson["value"], expected_value)

        # write some values
        value = [
            "hello",
        ]
        value *= 5
        payload = {"start": 0, "stop": 5, "value": value}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 200)

        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        ret_values = rspJson["value"]
        for i in range(5):
            self.assertEqual(ret_values[i], "hello")
            self.assertEqual(len(ret_values[i + 5]), len(fill_value))
            self.assertEqual(ret_values[i + 5], fill_value)

    def testNaNFillValue(self):
        # test Dataset with simple type and fill value of NaNs
        print("testNaNFillValue", self.base_domain)
        headers = helper.getRequestHeaders(domain=self.base_domain)

        # get domain
        req = helper.getEndpoint() + "/"
        rsp = self.session.get(req, headers=headers)
        rspJson = json.loads(rsp.text)
        self.assertTrue("root" in rspJson)
        root_uuid = rspJson["root"]

        # create the dataset
        req = self.endpoint + "/datasets"
        payload = {"type": "H5T_IEEE_F32LE", "shape": 10}
        creation_props = {"fillValue": np.NaN}
        payload["creationProperties"] = creation_props

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

        # read back the data
        req = self.endpoint + "/datasets/" + dset_uuid + "/value"
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        ret_values = rspJson["value"]
        self.assertEqual(len(ret_values), 10)
        for i in range(10):
            self.assertTrue(np.isnan(ret_values[i]))

        # read back data treating NaNs as null
        params = {"ignore_nan": 1}
        rsp = self.session.get(req, headers=headers, params=params)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        ret_values = rspJson["value"]
        self.assertEqual(len(ret_values), 10)
        for i in range(10):
            self.assertEqual(ret_values[i], None)

        # write some values
        value = [
            3.12,
        ]
        value *= 5
        payload = {"start": 0, "stop": 5, "value": value}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 200)

        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        ret_values = rspJson["value"]
        tol = 0.0001
        for i in range(10):
            ret_value = ret_values[i]
            if i < 5:
                self.assertTrue(ret_value > 3.12 - tol and ret_value < 3.12 + tol)
            else:
                self.assertTrue(np.isnan(ret_value))

        # read back data treating NaNs as null
        params = {"ignore_nan": 1}
        rsp = self.session.get(req, headers=headers, params=params)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        ret_values = rspJson["value"]
        tol = 0.0001
        for i in range(10):
            ret_value = ret_values[i]
            if i < 5:
                self.assertTrue(ret_value > 3.12 - tol and ret_value < 3.12 + tol)
            else:
                self.assertTrue(ret_value is None)

    def testPutObjRefDataset(self):
        # Test PUT obj ref values for 1d dataset
        print("testPutObjRefDataset", self.base_domain)

        headers = helper.getRequestHeaders(domain=self.base_domain)
        req = self.endpoint + "/"

        # Get root uuid
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        root_uuid = rspJson["root"]
        helper.validateId(root_uuid)

        # create new group
        payload = {"link": {"id": root_uuid, "name": "g1"}}
        req = helper.getEndpoint() + "/groups"
        rsp = self.session.post(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)
        g1_uuid = rspJson["id"]
        self.assertTrue(helper.validateId(g1_uuid))

        # create dataset
        ref_type = {"class": "H5T_REFERENCE", "base": "H5T_STD_REF_OBJ"}
        data = {"type": ref_type, "shape": 3}

        req = self.endpoint + "/datasets"
        rsp = self.session.post(req, data=json.dumps(data), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)
        dset_id = rspJson["id"]
        self.assertTrue(helper.validateId(dset_id))

        # link new dataset as 'dsetref'
        name = "dsetref"
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
        payload = {"id": dset_id}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # read values from dset (should be empty strings)
        req = self.endpoint + "/datasets/" + dset_id + "/value"
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        self.assertEqual(rspJson["value"], [""] * data["shape"])

        # write some values
        ref_values = ["groups/" + root_uuid, "", "groups/" + g1_uuid]
        payload = {"value": ref_values}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 200)

        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        ret_values = rspJson["value"]
        self.assertEqual(ret_values[0], "groups/" + root_uuid)
        self.assertEqual(ret_values[1], "")
        self.assertEqual(ret_values[2], "groups/" + g1_uuid)

    def testPutObjRefDatasetBinary(self):
        # Test PUT obj ref values for 1d dataset using binary transfer
        print("testPutObjRefDatasetBinary", self.base_domain)

        headers = helper.getRequestHeaders(domain=self.base_domain)
        headers_bin_req = helper.getRequestHeaders(domain=self.base_domain)
        headers_bin_req["Content-Type"] = "application/octet-stream"

        req = self.endpoint + "/"

        # Get root uuid
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        root_uuid = rspJson["root"]
        helper.validateId(root_uuid)

        # create new group
        payload = {"link": {"id": root_uuid, "name": "g1"}}
        req = helper.getEndpoint() + "/groups"
        rsp = self.session.post(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)
        g1_uuid = rspJson["id"]
        self.assertTrue(helper.validateId(g1_uuid))

        # create dataset
        ref_type = {"class": "H5T_REFERENCE", "base": "H5T_STD_REF_OBJ"}
        data = {"type": ref_type, "shape": 3}

        req = self.endpoint + "/datasets"
        rsp = self.session.post(req, data=json.dumps(data), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)
        dset_id = rspJson["id"]
        self.assertTrue(helper.validateId(dset_id))

        # link new dataset as 'dsetref'
        name = "dsetref"
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
        payload = {"id": dset_id}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # write some values
        ref_length = 48  # long enough to store any object ref
        data = bytearray(3 * ref_length)
        ref_values = ["groups/" + root_uuid, "", "groups/" + g1_uuid]
        for i in range(3):
            ref_value = ref_values[i]
            for j in range(len(ref_value)):
                offset = i * ref_length + j
                data[offset] = ord(ref_value[j])

        req = self.endpoint + "/datasets/" + dset_id + "/value"
        rsp = self.session.put(req, data=data, headers=headers_bin_req)
        self.assertEqual(rsp.status_code, 200)

        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        ret_values = rspJson["value"]
        self.assertEqual(ret_values[0], "groups/" + root_uuid)
        self.assertEqual(ret_values[1], "")
        self.assertEqual(ret_values[2], "groups/" + g1_uuid)

    def testGet(self):
        domain = helper.getTestDomain("tall.h5")
        print("testGetDomain", domain)
        headers = helper.getRequestHeaders(domain=domain)
        headers["Origin"] = "https://www.hdfgroup.org"  # test CORS
        headers_bin_rsp = helper.getRequestHeaders(domain=domain)
        headers_bin_rsp["accept"] = "application/octet-stream"

        # verify domain exists
        req = helper.getEndpoint() + "/"
        rsp = self.session.get(req, headers=headers)
        if rsp.status_code != 200:
            print(
                "WARNING: Failed to get domain: {}. Is test data setup?".format(domain)
            )
            return  # abort rest of test
        domainJson = json.loads(rsp.text)
        root_uuid = domainJson["root"]
        helper.validateId(root_uuid)

        # get the dataset uuid
        dset1_uuid = self.getUUIDByPath(domain, "/g1/g1.1/dset1.1.1")

        # read fancy selection
        params = {"select": "[0:4, [2,4,7]]"}
        req = helper.getEndpoint() + "/datasets/" + dset1_uuid + "/value"
        rsp = self.session.get(req, params=params, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("value" in rspJson)
        data = rspJson["value"]  # should be 4 x 3 array
        self.assertTrue(data[0], [0, 0, 0])
        self.assertTrue(data[1], [2, 4, 7])
        self.assertTrue(data[2], [4, 8, 14])
        self.assertTrue(data[3], [6, 12, 21])

        # read all the dataset values
        req = helper.getEndpoint() + "/datasets/" + dset1_uuid + "/value"
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)

        hrefs = rspJson["hrefs"]
        self.assertEqual(len(hrefs), 4)
        self.assertTrue("value" in rspJson)
        data = rspJson["value"]  # should be 10 x 10 array
        for j in range(10):
            row = data[j]
            for i in range(10):
                self.assertEqual(row[i], i * j)

        # read all the dataset values as binary

        rsp = self.session.get(req, headers=headers_bin_rsp)
        self.assertEqual(rsp.status_code, 200)
        self.assertEqual(rsp.headers["Content-Type"], "application/octet-stream")
        data = rsp.content
        self.assertEqual(len(data), 400)  # 10x10 4 byte array
        for j in range(10):
            for i in range(10):
                offset = (j * 10 + i) * 4
                self.assertEqual(data[offset], 0)
                self.assertEqual(data[offset + 1], 0)
                self.assertEqual(data[offset + 2], 0)
                self.assertEqual(data[offset + 3], i * j)

        # try same thing with a select param
        params = {"select": "[0:10, 0:10]"}
        rsp = self.session.get(req, params=params, headers=headers_bin_rsp)
        self.assertEqual(rsp.status_code, 200)
        self.assertEqual(rsp.headers["Content-Type"], "application/octet-stream")
        self.assertEqual(rsp.content, data)  # should get same values

        # equivalent select param
        params = {"select": "[::, ::]"}
        rsp = self.session.get(req, params=params, headers=headers_bin_rsp)
        self.assertEqual(rsp.status_code, 200)
        self.assertEqual(rsp.headers["Content-Type"], "application/octet-stream")
        self.assertEqual(rsp.content, data)  # should get same values

        # read 4x4 block from dataset
        params = {"select": "[0:4, 0:4]"}
        params["nonstrict"] = 1  # SN can read directly from S3 or DN node
        rsp = self.session.get(req, params=params, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("value" in rspJson)
        data = rspJson["value"]  # should be 4 x 4 array
        for j in range(4):
            row = data[j]
            for i in range(4):
                self.assertEqual(row[i], i * j)

        # read 2x2 block from dataset with step of 2
        params = {"select": "[0:4:2, 0:4:2]"}
        params["nonstrict"] = 1
        rsp = self.session.get(req, params=params, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("value" in rspJson)
        data = rspJson["value"]  # should be 2 x 2 array
        for j in range(2):
            row = data[j]
            for i in range(2):
                self.assertEqual(row[i], (i * 2) * (j * 2))

        # read 1x4 block from dataset
        row_index = 2
        params = {"select": f"[{row_index}:{row_index+1}, 0:4]"}
        params["nonstrict"] = 1  # SN can read directly from S3 or DN node
        rsp = self.session.get(req, params=params, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("value" in rspJson)
        data = rspJson["value"]  # should be 1 x 4 array
        self.assertTrue(len(data), 1)
        row = data[0]
        self.assertEqual(len(row), 4)
        for i in range(4):
            self.assertEqual(row[i], i * row_index)

        # read 1x4 block from dataset
        # use reduce_dim to return 4 element list instead of 1x4 array
        params["reduce_dim"] = 1
        rsp = self.session.get(req, params=params, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("value" in rspJson)
        row = rspJson["value"]  # should be 1 x 4 array
        self.assertEqual(len(row), 4)
        for i in range(4):
            self.assertEqual(row[i], i * row_index)

        # try a binary request
        headers["accept"] = "application/octet-stream"
        rsp = self.session.get(req, params=params, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        self.assertEqual(len(rsp.content), 16)  # 4 elements, 4 bytes each
        for i in range(16):
            byte_val = rsp.content[i]
            # should see (0,2,4,6) in the low-order byte for each 4-byte word
            if i % 4 == 3:
                self.assertEqual(byte_val, (i // 4) * 2)
            else:
                self.assertEqual(byte_val, 0)

        # try reading a selection that is out of bounds
        params = {"select": "[0:12, 0:12]"}
        params["nonstrict"] = 1
        rsp = self.session.get(req, params=params, headers=headers)
        self.assertEqual(rsp.status_code, 400)

    def testFancyIndexing(self):
        domain = helper.getTestDomain("tall.h5")
        print("testGetDomain", domain)
        headers = helper.getRequestHeaders(domain=domain)
        headers["Origin"] = "https://www.hdfgroup.org"  # test CORS
        headers_bin_rsp = helper.getRequestHeaders(domain=domain)
        headers_bin_rsp["accept"] = "application/octet-stream"

        # verify domain exists
        req = helper.getEndpoint() + "/"
        rsp = self.session.get(req, headers=headers)
        if rsp.status_code != 200:
            print(
                "WARNING: Failed to get domain: {}. Is test data setup?".format(domain)
            )
            return  # abort rest of test
        domainJson = json.loads(rsp.text)
        root_uuid = domainJson["root"]
        helper.validateId(root_uuid)

        # get the dataset uuid
        dset1_uuid = self.getUUIDByPath(domain, "/g1/g1.1/dset1.1.1")

        # read fancy selection
        params = {"select": "[1:3, [2,4,7]]"}
        req = helper.getEndpoint() + "/datasets/" + dset1_uuid + "/value"
        rsp = self.session.get(req, params=params, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("value" in rspJson)
        data = rspJson["value"]  # should be 2 x 3 array
        self.assertTrue(len(data), 2)
        self.assertTrue(data[0], [2, 4, 7])
        self.assertTrue(data[1], [4, 8, 14])

    def testResizable1DValue(self):
        # test read/write to resizable dataset
        print("testResizable1DValue", self.base_domain)
        headers = helper.getRequestHeaders(domain=self.base_domain)

        # get domain
        req = helper.getEndpoint() + "/"
        rsp = self.session.get(req, headers=headers)
        rspJson = json.loads(rsp.text)
        self.assertTrue("root" in rspJson)
        root_uuid = rspJson["root"]

        # create the dataset
        num_elements = 10
        req = self.endpoint + "/datasets"
        payload = {"type": "H5T_STD_I32LE", "shape": [num_elements], "maxdims": [0]}
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

        # write entire array
        value = list(range(num_elements))
        payload = {"value": value}
        req = self.endpoint + "/datasets/" + dset_uuid + "/value"
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 200)  # write value

        # resize the datasets elements
        orig_extent = num_elements
        num_elements *= 2  # double the extent
        req = self.endpoint + "/datasets/" + dset_uuid + "/shape"
        payload = {"shape": [num_elements]}

        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)

        # read values from the extended region
        req = self.endpoint + "/datasets/" + dset_uuid + "/value"
        params = {"select": "[{}:{}]".format(0, num_elements)}
        rsp = self.session.get(req, params=params, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        data = rspJson["value"]
        self.assertEqual(data[0:orig_extent], list(range(orig_extent)))

        # write to the extended region
        payload = {"value": value, "start": 10, "stop": 20}
        req = self.endpoint + "/datasets/" + dset_uuid + "/value"
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 200)  # write value

        # read values from the extended region
        req = self.endpoint + "/datasets/" + dset_uuid + "/value"
        params = {"select": "[{}:{}]".format(orig_extent, num_elements)}
        rsp = self.session.get(req, params=params, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        data = rspJson["value"]

        self.assertEqual(len(data), num_elements - orig_extent)

        # read all values back
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        data = rspJson["value"]
        # value handler still thinks there are num_elements
        self.assertEqual(len(data), num_elements)
        self.assertEqual(data[0:orig_extent], list(range(orig_extent)))
        # the extended area should be all zeros
        self.assertEqual(data[orig_extent:num_elements], list(range(orig_extent)))

    def testAppend1DJson(self):
        # test appending to resizable dataset
        print("testAppend1DJson", self.base_domain)
        headers = helper.getRequestHeaders(domain=self.base_domain)

        # get domain
        req = helper.getEndpoint() + "/"
        rsp = self.session.get(req, headers=headers)
        rspJson = json.loads(rsp.text)
        self.assertTrue("root" in rspJson)
        root_uuid = rspJson["root"]

        # create the dataset with a 0-sized shape
        req = self.endpoint + "/datasets"
        payload = {"type": "H5T_STD_I32LE", "shape": [0], "maxdims": [0]}
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

        # append values [0,10]
        num_elements = 10
        value = list(range(num_elements))
        payload = {"value": value, "append": num_elements}
        req = self.endpoint + "/datasets/" + dset_uuid + "/value"
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 200)  # write value

        # verify the shape in now (10,)
        req = self.endpoint + "/datasets/" + dset_uuid + "/shape"
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("shape" in rspJson)
        shape = rspJson["shape"]
        self.assertTrue("dims" in shape)
        self.assertEqual(shape["dims"], [num_elements])

        # read values from the extended region
        req = self.endpoint + "/datasets/" + dset_uuid + "/value"
        params = {"select": "[{}:{}]".format(0, num_elements)}
        rsp = self.session.get(req, params=params, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        data = rspJson["value"]
        self.assertEqual(data, value)

        # append values [10,20]
        num_elements = 10
        value = list(range(num_elements, num_elements * 2))
        payload = {"value": value, "append": num_elements}
        req = self.endpoint + "/datasets/" + dset_uuid + "/value"
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 200)  # write value

        # verify the shape in now (20,)
        req = self.endpoint + "/datasets/" + dset_uuid + "/shape"
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("shape" in rspJson)
        shape = rspJson["shape"]
        self.assertTrue("dims" in shape)
        self.assertEqual(shape["dims"], [num_elements * 2])

        # read values from the extended region
        req = self.endpoint + "/datasets/" + dset_uuid + "/value"
        params = {"select": "[{}:{}]".format(0, num_elements * 2)}
        rsp = self.session.get(req, params=params, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        data = rspJson["value"]
        self.assertEqual(data, list(range(num_elements * 2)))

        # test mis-match of append value and data
        value = list(range(num_elements, num_elements * 2))
        payload = {"value": value, "append": num_elements + 1}
        req = self.endpoint + "/datasets/" + dset_uuid + "/value"
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 400)  # write value

    def testAppend1DBinary(self):
        # test appending to resizable dataset using binary request
        print("testAppend1DBinary", self.base_domain)
        headers = helper.getRequestHeaders(domain=self.base_domain)
        headers_bin_req = helper.getRequestHeaders(domain=self.base_domain)
        headers_bin_req["Content-Type"] = "application/octet-stream"

        # get domain
        req = helper.getEndpoint() + "/"
        rsp = self.session.get(req, headers=headers)
        rspJson = json.loads(rsp.text)
        self.assertTrue("root" in rspJson)
        root_uuid = rspJson["root"]

        # create the dataset with a 0-sized shape
        req = self.endpoint + "/datasets"
        payload = {"type": "H5T_STD_I32LE", "shape": [0], "maxdims": [0]}
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

        # append values [0,10]
        # write 0-9 as four-byte little-endian integers
        num_elements = 10
        data = bytearray(4 * num_elements)
        for i in range(num_elements):
            data[i * 4] = i % 256
        params = {"append": num_elements}
        req = self.endpoint + "/datasets/" + dset_uuid + "/value"
        rsp = self.session.put(req, data=data, params=params, headers=headers_bin_req)
        self.assertEqual(rsp.status_code, 200)

        # verify the shape in now (10,)
        req = self.endpoint + "/datasets/" + dset_uuid + "/shape"
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("shape" in rspJson)
        shape = rspJson["shape"]
        self.assertTrue("dims" in shape)
        self.assertEqual(shape["dims"], [num_elements])

        # read values from the extended region
        req = self.endpoint + "/datasets/" + dset_uuid + "/value"
        params = {"select": "[{}:{}]".format(0, num_elements)}
        rsp = self.session.get(req, params=params, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        read_values = rspJson["value"]
        self.assertEqual(read_values, list(range(num_elements)))

        # append values [10,20]
        num_elements = 10
        data = bytearray(4 * num_elements)
        for i in range(num_elements):
            data[i * 4] = (i + num_elements) % 256
        req = self.endpoint + "/datasets/" + dset_uuid + "/value"
        params = {"append": num_elements}
        rsp = self.session.put(req, data=data, params=params, headers=headers_bin_req)
        self.assertEqual(rsp.status_code, 200)  # write value

        # verify the shape in now (20,)
        req = self.endpoint + "/datasets/" + dset_uuid + "/shape"
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("shape" in rspJson)
        shape = rspJson["shape"]
        self.assertTrue("dims" in shape)
        self.assertEqual(shape["dims"], [num_elements * 2])

        # read values from the extended region
        req = self.endpoint + "/datasets/" + dset_uuid + "/value"
        params = {"select": "[{}:{}]".format(0, num_elements * 2)}
        rsp = self.session.get(req, params=params, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        read_values = rspJson["value"]
        self.assertEqual(read_values, list(range(num_elements * 2)))

        # test mis-match of append value and data
        req = self.endpoint + "/datasets/" + dset_uuid + "/value"
        params = {"append": num_elements + 1}
        rsp = self.session.put(req, data=data, params=params, headers=headers_bin_req)
        self.assertEqual(rsp.status_code, 400)  # write value

    def testAppend2DJson(self):
        # test appending to resizable dataset
        print("testAppend2DJson", self.base_domain)
        headers = helper.getRequestHeaders(domain=self.base_domain)

        # get domain
        req = helper.getEndpoint() + "/"
        rsp = self.session.get(req, headers=headers)
        rspJson = json.loads(rsp.text)
        self.assertTrue("root" in rspJson)
        root_uuid = rspJson["root"]

        # create the dataset with a 0-sized shape
        req = self.endpoint + "/datasets"
        payload = {"type": "H5T_STD_I32LE", "shape": [0, 0], "maxdims": [0, 0]}
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

        # append values [0,10]
        num_elements = 10
        value = list(range(num_elements))
        payload = {"value": value, "append": num_elements, "append_dim": 1}
        req = self.endpoint + "/datasets/" + dset_uuid + "/value"
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 200)  # write value

        # verify the shape in now (1, 10)
        req = self.endpoint + "/datasets/" + dset_uuid + "/shape"
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("shape" in rspJson)
        shape = rspJson["shape"]
        self.assertTrue("dims" in shape)
        self.assertEqual(shape["dims"], [1, num_elements])

        # read values from the extended region
        req = self.endpoint + "/datasets/" + dset_uuid + "/value"
        params = {"select": "[0:1,{}:{}]".format(0, num_elements)}
        rsp = self.session.get(req, params=params, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        data = rspJson["value"]
        self.assertEqual(data, [value])

        # append values [10,20]
        num_elements = 10
        value = list(range(num_elements, num_elements * 2))
        payload = {"value": value, "append": num_elements, "append_dim": 1}
        req = self.endpoint + "/datasets/" + dset_uuid + "/value"
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 200)  # write value

        # verify the shape in now (1, 20)
        req = self.endpoint + "/datasets/" + dset_uuid + "/shape"
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("shape" in rspJson)
        shape = rspJson["shape"]
        self.assertTrue("dims" in shape)
        self.assertEqual(shape["dims"], [1, num_elements * 2])

        # read values from the extended region
        req = self.endpoint + "/datasets/" + dset_uuid + "/value"
        params = {"select": "[0:1,{}:{}]".format(0, num_elements * 2)}
        rsp = self.session.get(req, params=params, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        data = rspJson["value"]
        self.assertEqual(data, [list(range(num_elements * 2))])

        # append one row (20 elements) in the other dimension
        num_elements = 20
        value = list(range(num_elements))
        payload = {"value": value, "append": 1, "append_dim": 0}
        req = self.endpoint + "/datasets/" + dset_uuid + "/value"
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 200)  # write value

        # verify the shape in now (2, 20,)
        req = self.endpoint + "/datasets/" + dset_uuid + "/shape"
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("shape" in rspJson)
        shape = rspJson["shape"]
        self.assertTrue("dims" in shape)
        self.assertEqual(shape["dims"], [2, num_elements])

        # read all values from the dataset
        req = self.endpoint + "/datasets/" + dset_uuid + "/value"
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        data = rspJson["value"]
        self.assertEqual(len(data), 2)
        self.assertEqual(data[0], list(range(num_elements)))
        self.assertEqual(data[1], list(range(num_elements)))

        # test mis-match of append value and data
        value = list(range(num_elements, num_elements * 2))
        payload = {"value": value, "append": num_elements + 1}
        req = self.endpoint + "/datasets/" + dset_uuid + "/value"
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 400)  # write value

    def testContiguousRefDataset(self):
        test_name = "testContigousRefDataset"
        print(test_name, self.base_domain)
        headers = helper.getRequestHeaders(domain=self.base_domain)

        hdf5_sample_bucket = config.get("hdf5_sample_bucket")
        if not hdf5_sample_bucket:
            print(f"hdf5_sample_bucket config not set, skipping {test_name}")
            return

        tall_json = helper.getHDF5JSON("tall.json")
        if not tall_json:
            print(f"tall.json file not found, skipping {test_name}")
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

        if "/g2/dset2.2" not in chunk_info:
            self.assertTrue(False)
        dset22_info = chunk_info["/g2/dset2.2"]
        if "byteStreams" not in dset22_info:
            self.assertTrue(False)
        byteStreams = dset22_info["byteStreams"]
        self.assertTrue(len(byteStreams), 1)
        byteStream = byteStreams[0]
        dset22_offset = byteStream["file_offset"]
        dset22_size = byteStream["size"]
        self.assertEqual(dset22_size, 60)

        # get domain
        req = helper.getEndpoint() + "/"
        rsp = self.session.get(req, headers=headers)
        rspJson = json.loads(rsp.text)
        self.assertTrue("root" in rspJson)
        root_uuid = rspJson["root"]

        # create dataset for /g1/g1.1/dset1.1.2
        s3path = hdf5_sample_bucket + "/data/hdf5test" + "/tall.h5"
        data = {"type": "H5T_STD_I32BE", "shape": 20}
        layout = {
            "class": "H5D_CONTIGUOUS_REF",
            "file_uri": s3path,
            "offset": dset112_offset,
            "size": dset112_size,
        }
        data["creationProperties"] = {"layout": layout}

        req = self.endpoint + "/datasets"
        rsp = self.session.post(req, data=json.dumps(data), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)
        dset112_id = rspJson["id"]
        self.assertTrue(helper.validateId(dset112_id))

        # link new dataset as 'dset112'
        name = "dset112"
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
        payload = {"id": dset112_id}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # create dataset for /g2/dset2.2
        data = {"type": "H5T_IEEE_F32BE", "shape": [3, 5]}
        layout = {
            "class": "H5D_CONTIGUOUS_REF",
            "file_uri": s3path,
            "offset": dset22_offset,
            "size": dset22_size,
        }
        data["creationProperties"] = {"layout": layout}

        req = self.endpoint + "/datasets"
        rsp = self.session.post(req, data=json.dumps(data), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)
        dset22_id = rspJson["id"]
        self.assertTrue(helper.validateId(dset22_id))

        # link new dataset as 'dset22'
        name = "dset22"
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
        payload = {"id": dset22_id}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # read values from dset112 (should be the sequence 0 through 19)
        req = self.endpoint + "/datasets/" + dset112_id + "/value"
        rsp = self.session.get(req, headers=headers)

        if rsp.status_code == 404:
            print(f"s3object: {s3path} not found, skipping {test_name}")
            return

        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        value = rspJson["value"]
        self.assertEqual(len(value), 20)
        for i in range(20):
            self.assertEqual(value[i], i)

        # read values from dset22 (should be 3x5 array)
        req = self.endpoint + "/datasets/" + dset22_id + "/value"
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        value = rspJson["value"]
        self.assertEqual(len(value), 3)
        for i in range(3):
            self.assertEqual(len(value[i]), 5)

    def testGetSelectionChunkedRefDataset(self):
        test_name = "testGetSelectionChunkedRefDataset"
        print(test_name, self.base_domain)
        headers = helper.getRequestHeaders(domain=self.base_domain)

        hdf5_sample_bucket = config.get("hdf5_sample_bucket")

        if not hdf5_sample_bucket:
            print(f"hdf5_sample_bucket config not set, skipping {test_name}")
            return

        s3path = hdf5_sample_bucket + "/data/hdf5test" + "/snp500.h5"
        SNP500_ROWS = 3207353

        snp500_json = helper.getHDF5JSON("snp500.json")
        if not snp500_json:
            print(f"snp500.json file not found, skipping {test_name}")
            return

        if "snp500.h5" not in snp500_json:
            self.assertTrue(False)

        chunk_dims = [60000]  # chunk layout used in snp500.h5 file

        chunk_info = snp500_json["snp500.h5"]
        dset_info = chunk_info["/dset"]
        if "byteStreams" not in dset_info:
            self.assertTrue(False)
        byteStreams = dset_info["byteStreams"]

        # construct map of chunks
        chunks = {}
        for item in byteStreams:
            index = item["index"]
            chunk_key = str(index)
            chunks[chunk_key] = (item["file_offset"], item["size"])

        # get domain
        req = helper.getEndpoint() + "/"
        rsp = self.session.get(req, headers=headers)
        rspJson = json.loads(rsp.text)
        self.assertTrue("root" in rspJson)
        root_uuid = rspJson["root"]

        # define types we need
        s10_type = {
            "charSet": "H5T_CSET_ASCII",
            "class": "H5T_STRING",
            "length": 10,
            "strPad": "H5T_STR_NULLPAD",
        }
        s4_type = {
            "charSet": "H5T_CSET_ASCII",
            "class": "H5T_STRING",
            "length": 4,
            "strPad": "H5T_STR_NULLPAD",
        }
        fields = (
            {"name": "date", "type": s10_type},
            {"name": "symbol", "type": s4_type},
            {"name": "sector", "type": "H5T_STD_I8LE"},
            {"name": "open", "type": "H5T_IEEE_F32LE"},
            {"name": "high", "type": "H5T_IEEE_F32LE"},
            {"name": "low", "type": "H5T_IEEE_F32LE"},
            {"name": "volume", "type": "H5T_IEEE_F32LE"},
            {"name": "close", "type": "H5T_IEEE_F32LE"},
        )

        datatype = {"class": "H5T_COMPOUND", "fields": fields}

        data = {"type": datatype, "shape": [SNP500_ROWS]}
        layout = {
            "class": "H5D_CHUNKED_REF",
            "file_uri": s3path,
            "dims": chunk_dims,
            "chunks": chunks,
        }
        data["creationProperties"] = {"layout": layout}

        req = self.endpoint + "/datasets"
        rsp = self.session.post(req, data=json.dumps(data), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)
        dset_id = rspJson["id"]
        self.assertTrue(helper.validateId(dset_id))

        # link new dataset as 'dset'
        name = "dset"
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
        payload = {"id": dset_id}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # read a selection
        req = self.endpoint + "/datasets/" + dset_id + "/value"
        params = {
            "select": "[1234567:1234568]"
        }  # read 1 element, starting at index 1234567
        params["nonstrict"] = 1  # allow use of aws lambda if configured
        rsp = self.session.get(req, params=params, headers=headers)
        if rsp.status_code == 404:
            print(f"s3object: {s3path} not found, skipping {test_name}")
            return

        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        value = rspJson["value"]
        # should get one element back
        self.assertEqual(len(value), 1)
        item = value[0]
        # verify that this is what we expected to get
        self.assertEqual(len(item), len(fields))
        self.assertEqual(item[0], "1998.10.22")
        self.assertEqual(item[1], "MHFI")
        self.assertEqual(item[2], 3)
        # skip check rest of fields since float comparisons are trcky...

    def testChunkedRefIndirectDataset(self):
        test_name = "testChunkedRefIndirectDataset"
        print("testChunkedRefIndirectDataset", self.base_domain)
        headers = helper.getRequestHeaders(domain=self.base_domain)

        hdf5_sample_bucket = config.get("hdf5_sample_bucket")
        if not hdf5_sample_bucket:
            print(f"hdf5_sample_bucket config not set, skipping {test_name}")
            return

        s3path = hdf5_sample_bucket + "/data/hdf5test" + "/snp500.h5"
        SNP500_ROWS = 3207353

        snp500_json = helper.getHDF5JSON("snp500.json")
        if not snp500_json:
            print(f"snp500.json file not found, skipping {test_name}")
            return

        if "snp500.h5" not in snp500_json:
            self.assertTrue(False)

        chunk_dims = [60000]  # chunk layout used in snp500.h5 file
        num_chunks = (SNP500_ROWS // chunk_dims[0]) + 1

        chunk_info = snp500_json["snp500.h5"]
        dset_info = chunk_info["/dset"]
        if "byteStreams" not in dset_info:
            self.assertTrue(False)
        byteStreams = dset_info["byteStreams"]

        self.assertEqual(len(byteStreams), num_chunks)

        chunkinfo_data = [(0, 0)] * num_chunks

        # fill the numpy array with info from bytestreams data
        for i in range(num_chunks):
            item = byteStreams[i]
            index = item["index"]
            chunkinfo_data[index] = (item["file_offset"], item["size"])

        # get domain
        req = helper.getEndpoint() + "/"
        rsp = self.session.get(req, headers=headers)
        rspJson = json.loads(rsp.text)
        self.assertTrue("root" in rspJson)
        root_uuid = rspJson["root"]

        # create table to hold chunkinfo
        # create a dataset to store chunk info
        fields = (
            {"name": "offset", "type": "H5T_STD_I64LE"},
            {"name": "size", "type": "H5T_STD_I32LE"},
        )
        chunkinfo_type = {"class": "H5T_COMPOUND", "fields": fields}
        req = self.endpoint + "/datasets"
        # Store 40 chunk locations
        chunkinfo_dims = [num_chunks]
        payload = {"type": chunkinfo_type, "shape": chunkinfo_dims}
        req = self.endpoint + "/datasets"
        rsp = self.session.post(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)  # create dataset
        rspJson = json.loads(rsp.text)
        chunkinfo_uuid = rspJson["id"]
        self.assertTrue(helper.validateId(chunkinfo_uuid))

        # link new dataset as 'chunks'
        name = "chunks"
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
        payload = {"id": chunkinfo_uuid}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # define types we need
        s10_type = {
            "charSet": "H5T_CSET_ASCII",
            "class": "H5T_STRING",
            "length": 10,
            "strPad": "H5T_STR_NULLPAD",
        }
        s4_type = {
            "charSet": "H5T_CSET_ASCII",
            "class": "H5T_STRING",
            "length": 4,
            "strPad": "H5T_STR_NULLPAD",
        }
        fields = (
            {"name": "date", "type": s10_type},
            {"name": "symbol", "type": s4_type},
            {"name": "sector", "type": "H5T_STD_I8LE"},
            {"name": "open", "type": "H5T_IEEE_F32LE"},
            {"name": "high", "type": "H5T_IEEE_F32LE"},
            {"name": "low", "type": "H5T_IEEE_F32LE"},
            {"name": "volume", "type": "H5T_IEEE_F32LE"},
            {"name": "close", "type": "H5T_IEEE_F32LE"},
        )

        datatype = {"class": "H5T_COMPOUND", "fields": fields}

        data = {"type": datatype, "shape": [SNP500_ROWS]}
        layout = {
            "class": "H5D_CHUNKED_REF_INDIRECT",
            "file_uri": s3path,
            "dims": chunk_dims,
            "chunk_table": chunkinfo_uuid,
        }
        data["creationProperties"] = {"layout": layout}

        req = self.endpoint + "/datasets"
        rsp = self.session.post(req, data=json.dumps(data), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)
        dset_id = rspJson["id"]
        self.assertTrue(helper.validateId(dset_id))

        # link new dataset as 'dset'
        name = "dset"
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
        payload = {"id": dset_id}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # read a selection
        req = self.endpoint + "/datasets/" + dset_id + "/value"
        params = {
            "select": "[1234567:1234568]"
        }  # read 1 element, starting at index 1234567
        params["nonstrict"] = 1  # enable SN to invoke lambda func
        rsp = self.session.get(req, params=params, headers=headers)

        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        value = rspJson["value"]
        # should get one element back
        self.assertEqual(len(value), 1)
        item = value[0]
        # should be all zeros since we haven't updated the chunk table yet
        self.assertEqual(len(item), len(fields))
        self.assertEqual(item[0], "")
        self.assertEqual(item[1], "")
        self.assertEqual(item[2], 0)

        # write the chunk locations
        payload = {"value": chunkinfo_data}
        chunk_table_req = self.endpoint + "/datasets/" + chunkinfo_uuid + "/value"
        rsp = self.session.put(chunk_table_req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 200)  # write value

        # read the selection again
        rsp = self.session.get(req, params=params, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        value = rspJson["value"]
        # should get one element back
        self.assertEqual(len(value), 1)
        item = value[0]
        # verify that this is what we expected to get
        self.assertEqual(len(item), len(fields))
        self.assertEqual(item[0], "1998.10.22")
        self.assertEqual(item[1], "MHFI")
        self.assertEqual(item[2], 3)

        # skip check rest of fields since float comparisons are trcky...

    def testChunkedRefIndirectS3UriDataset(self):
        test_name = "testChunkedRefIndirectS3UriDataset"
        print(test_name, self.base_domain)
        headers = helper.getRequestHeaders(domain=self.base_domain)

        hdf5_sample_bucket = config.get("hdf5_sample_bucket")
        if not hdf5_sample_bucket:
            print(f"hdf5_sample_bucket config not set, skipping {test_name}")
            return

        s3path = hdf5_sample_bucket + "/data/hdf5test" + "/snp500.h5"
        SNP500_ROWS = 3207353

        snp500_json = helper.getHDF5JSON("snp500.json")
        if not snp500_json:
            print(f"snp500.json file not found, skipping {test_name}")
            return

        if "snp500.h5" not in snp500_json:
            self.assertTrue(False)

        chunk_dims = [60000]  # chunk layout used in snp500.h5 file
        num_chunks = (SNP500_ROWS // chunk_dims[0]) + 1

        chunk_info = snp500_json["snp500.h5"]
        dset_info = chunk_info["/dset"]
        if "byteStreams" not in dset_info:
            self.assertTrue(False)
        byteStreams = dset_info["byteStreams"]

        self.assertEqual(len(byteStreams), num_chunks)

        chunkinfo_data = [(0, 0)] * num_chunks

        # fill the numpy array with info from bytestreams data
        for i in range(num_chunks):
            item = byteStreams[i]
            index = item["index"]
            chunkinfo_data[index] = (item["file_offset"], item["size"], s3path)

        # get domain
        req = helper.getEndpoint() + "/"
        rsp = self.session.get(req, headers=headers)
        rspJson = json.loads(rsp.text)
        self.assertTrue("root" in rspJson)
        root_uuid = rspJson["root"]

        # create table to hold chunkinfo
        # create a dataset to store chunk info
        max_s3_uri_len = 40
        fixed_str_type = {
            "charSet": "H5T_CSET_ASCII",
            "class": "H5T_STRING",
            "length": max_s3_uri_len,
            "strPad": "H5T_STR_NULLPAD",
        }
        fields = (
            {"name": "offset", "type": "H5T_STD_I64LE"},
            {"name": "size", "type": "H5T_STD_I32LE"},
            {"name": "file_uri", "type": fixed_str_type},
        )
        chunkinfo_type = {"class": "H5T_COMPOUND", "fields": fields}
        req = self.endpoint + "/datasets"
        # Store 40 chunk locations
        chunkinfo_dims = [num_chunks]
        payload = {"type": chunkinfo_type, "shape": chunkinfo_dims}
        req = self.endpoint + "/datasets"
        rsp = self.session.post(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)  # create dataset
        rspJson = json.loads(rsp.text)
        chunkinfo_uuid = rspJson["id"]
        self.assertTrue(helper.validateId(chunkinfo_uuid))

        # link new dataset as 'chunks'
        name = "chunks"
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
        payload = {"id": chunkinfo_uuid}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # write to the chunkinfo dataset
        payload = {"value": chunkinfo_data}

        req = self.endpoint + "/datasets/" + chunkinfo_uuid + "/value"
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 200)  # write value

        # define types we need
        s10_type = {
            "charSet": "H5T_CSET_ASCII",
            "class": "H5T_STRING",
            "length": 10,
            "strPad": "H5T_STR_NULLPAD",
        }
        s4_type = {
            "charSet": "H5T_CSET_ASCII",
            "class": "H5T_STRING",
            "length": 4,
            "strPad": "H5T_STR_NULLPAD",
        }
        fields = (
            {"name": "date", "type": s10_type},
            {"name": "symbol", "type": s4_type},
            {"name": "sector", "type": "H5T_STD_I8LE"},
            {"name": "open", "type": "H5T_IEEE_F32LE"},
            {"name": "high", "type": "H5T_IEEE_F32LE"},
            {"name": "low", "type": "H5T_IEEE_F32LE"},
            {"name": "volume", "type": "H5T_IEEE_F32LE"},
            {"name": "close", "type": "H5T_IEEE_F32LE"},
        )

        datatype = {"class": "H5T_COMPOUND", "fields": fields}

        data = {"type": datatype, "shape": [SNP500_ROWS]}
        # don't provide s3path here, it will get picked up from the chunkinfo dataset
        layout = {
            "class": "H5D_CHUNKED_REF_INDIRECT",
            "dims": chunk_dims,
            "chunk_table": chunkinfo_uuid,
        }
        data["creationProperties"] = {"layout": layout}

        req = self.endpoint + "/datasets"
        rsp = self.session.post(req, data=json.dumps(data), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)
        dset_id = rspJson["id"]
        self.assertTrue(helper.validateId(dset_id))

        # link new dataset as 'dset'
        name = "dset"
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
        payload = {"id": dset_id}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # read a selection
        req = self.endpoint + "/datasets/" + dset_id + "/value"
        params = {
            "select": "[1234567:1234568]"
        }  # read 1 element, starting at index 1234567
        params["nonstrict"] = 1  # enable SN to invoke lambda func
        rsp = self.session.get(req, params=params, headers=headers)

        if rsp.status_code == 404:
            print(f"s3object: {s3path} not found, skipping {test_name}")
            return

        self.assertEqual(rsp.status_code, 200)

        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        value = rspJson["value"]
        # should get one element back
        self.assertEqual(len(value), 1)
        item = value[0]
        # verify that this is what we expected to get
        self.assertEqual(len(item), len(fields))
        self.assertEqual(item[0], "1998.10.22")
        self.assertEqual(item[1], "MHFI")
        self.assertEqual(item[2], 3)
        # skip check rest of fields since float comparisons are trcky...

    def testChunkInitializerDataset(self):
        test_name = "testChunkInitializerDataset"
        print(test_name, self.base_domain)
        headers = helper.getRequestHeaders(domain=self.base_domain)

        hdf5_sample_bucket = config.get("hdf5_sample_bucket")
        if not hdf5_sample_bucket:
            print(f"hdf5_sample_bucket config not set, skipping {test_name}")
            return

        file_path = "/data/hdf5test/snp500.h5"

        SNP500_ROWS = 3207353

        chunk_dims = [60000]  # chunk layout used in snp500.h5 file
        num_chunks = (SNP500_ROWS // chunk_dims[0]) + 1

        # get domain
        req = helper.getEndpoint() + "/"
        rsp = self.session.get(req, headers=headers)
        rspJson = json.loads(rsp.text)
        self.assertTrue("root" in rspJson)
        root_uuid = rspJson["root"]

        # create table to hold chunkinfo
        # create a dataset to store chunk info
        fields = (
            {"name": "offset", "type": "H5T_STD_I64LE"},
            {"name": "size", "type": "H5T_STD_I32LE"},
        )
        chunkinfo_type = {"class": "H5T_COMPOUND", "fields": fields}
        req = self.endpoint + "/datasets"
        # Store 40 chunk locations
        chunkinfo_dims = [num_chunks]
        layout = {"class": "H5D_CHUNKED"}
        layout["dims"] = chunkinfo_dims
        initializer = ["chunklocator",
                       "--h5path=/dset",
                       f"--filepath={file_path}",
                       f"--bucket={hdf5_sample_bucket}"]

        payload = {"type": chunkinfo_type, "shape": chunkinfo_dims}
        payload["creationProperties"] = {"layout": layout, "initializer": initializer}

        req = self.endpoint + "/datasets"
        rsp = self.session.post(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)  # create dataset
        rspJson = json.loads(rsp.text)
        chunkinfo_uuid = rspJson["id"]
        self.assertTrue(helper.validateId(chunkinfo_uuid))

        # link new dataset as 'chunks'
        name = "chunks"
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
        payload = {"id": chunkinfo_uuid}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # define types we need
        s10_type = {
            "charSet": "H5T_CSET_ASCII",
            "class": "H5T_STRING",
            "length": 10,
            "strPad": "H5T_STR_NULLPAD",
        }
        s4_type = {
            "charSet": "H5T_CSET_ASCII",
            "class": "H5T_STRING",
            "length": 4,
            "strPad": "H5T_STR_NULLPAD",
        }
        fields = (
            {"name": "date", "type": s10_type},
            {"name": "symbol", "type": s4_type},
            {"name": "sector", "type": "H5T_STD_I8LE"},
            {"name": "open", "type": "H5T_IEEE_F32LE"},
            {"name": "high", "type": "H5T_IEEE_F32LE"},
            {"name": "low", "type": "H5T_IEEE_F32LE"},
            {"name": "volume", "type": "H5T_IEEE_F32LE"},
            {"name": "close", "type": "H5T_IEEE_F32LE"},
        )

        datatype = {"class": "H5T_COMPOUND", "fields": fields}

        data = {"type": datatype, "shape": [SNP500_ROWS]}
        file_uri = f"{hdf5_sample_bucket}{file_path}"
        layout = {
            "class": "H5D_CHUNKED_REF_INDIRECT",
            "file_uri": file_uri,
            "dims": chunk_dims,
            "chunk_table": chunkinfo_uuid,
        }
        data["creationProperties"] = {"layout": layout}

        req = self.endpoint + "/datasets"
        rsp = self.session.post(req, data=json.dumps(data), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)
        dset_id = rspJson["id"]
        self.assertTrue(helper.validateId(dset_id))

        # link new dataset as 'dset'
        name = "dset"
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
        payload = {"id": dset_id}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # read a selection
        req = self.endpoint + "/datasets/" + dset_id + "/value"
        params = {
            "select": "[1234567:1234568]"
        }  # read 1 element, starting at index 1234567
        params["nonstrict"] = 1  # enable SN to invoke lambda func

        # read the selection
        rsp = self.session.get(req, params=params, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        value = rspJson["value"]
        # should get one element back
        self.assertEqual(len(value), 1)
        item = value[0]
        # verify that this is what we expected to get
        self.assertEqual(len(item), len(fields))
        self.assertEqual(item[0], "1998.10.22")
        self.assertEqual(item[1], "MHFI")
        self.assertEqual(item[2], 3)

    def testARangeInitializerDataset(self):
        test_name = "testARangeInitializerDataset"
        print(test_name, self.base_domain)
        headers = helper.getRequestHeaders(domain=self.base_domain)

        # get domain
        req = helper.getEndpoint() + "/"
        rsp = self.session.get(req, headers=headers)
        rspJson = json.loads(rsp.text)
        self.assertTrue("root" in rspJson)
        root_uuid = rspJson["root"]

        req = self.endpoint + "/datasets"

        extent = 1_000_000_000   # one billion elements
        dset_dims = [extent, ]
        layout = {"class": "H5D_CHUNKED"}
        layout["dims"] = dset_dims

        range_start = 0  # -0.25
        range_step = 1

        initializer = ["arange",
                       f"--start={range_start}",
                       f"--step={range_step}", ]

        payload = {"type": "H5T_STD_I64LE", "shape": dset_dims}
        payload["creationProperties"] = {"layout": layout, "initializer": initializer}

        req = self.endpoint + "/datasets"
        rsp = self.session.post(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)  # create dataset
        rspJson = json.loads(rsp.text)
        dset_id = rspJson["id"]
        self.assertTrue(helper.validateId(dset_id))

        # link new dataset as 'dset10'
        name = "dset10"
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
        payload = {"id": dset_id}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # read a selection
        req = self.endpoint + "/datasets/" + dset_id + "/value"
        count = 10
        sel_start = 19_531_260  # 20_000_000 # 123_456_789
        sel_stop = sel_start + count
        params = {"select": f"[{sel_start}:{sel_stop}]"}  # read 10 elements
        params["nonstrict"] = 1  # enable SN to invoke lambda func

        rsp = self.session.get(req, params=params, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        value = rspJson["value"]
        # should get extent elements back
        self.assertEqual(len(value), count)

        expected_val = (sel_start * range_step) + range_start
        for i in range(count):
            self.assertEqual(value[i], expected_val)
            expected_val += range_step

    def testIntelligentRangeGet(self):
        test_name = "testIntelligentRangeGet"
        print(test_name, self.base_domain)

        headers = helper.getRequestHeaders(domain=self.base_domain)

        hdf5_sample_bucket = config.get("hdf5_sample_bucket")
        if not hdf5_sample_bucket:
            print(f"hdf5_sample_bucket config not set, skipping {test_name}")
            return

        file_path = "/data/hdf5test/small1dchunk.h5"

        dset_rows = 2_000_000
        chunk_extent = 4000

        chunk_dims = [chunk_extent, ]  # file uses 16KB chunk size
        num_chunks = dset_rows // chunk_dims[0]

        # get domain
        req = helper.getEndpoint() + "/"
        rsp = self.session.get(req, headers=headers)
        rspJson = json.loads(rsp.text)
        self.assertTrue("root" in rspJson)
        root_uuid = rspJson["root"]

        req = self.endpoint + "/datasets"
        # Store chunk locations

        chunkinfo_dims = [num_chunks, ]
        fields = (
            {"name": "offset", "type": "H5T_STD_I64LE"},
            {"name": "size", "type": "H5T_STD_I32LE"},
        )
        chunkinfo_type = {"class": "H5T_COMPOUND", "fields": fields}
        layout = {"class": "H5D_CHUNKED"}
        layout["dims"] = chunkinfo_dims
        initializer = ["chunklocator",
                       "--h5path=/dset",
                       f"--filepath={file_path}",
                       f"--bucket={hdf5_sample_bucket}"]

        payload = {"type": chunkinfo_type, "shape": chunkinfo_dims}
        payload["creationProperties"] = {"layout": layout, "initializer": initializer}

        req = self.endpoint + "/datasets"
        rsp = self.session.post(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)  # create dataset
        rspJson = json.loads(rsp.text)
        chunkinfo_uuid = rspJson["id"]
        self.assertTrue(helper.validateId(chunkinfo_uuid))

        # link new dataset as 'chunktable'
        name = "chunktable"
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
        payload = {"id": chunkinfo_uuid}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        data = {"type": "H5T_STD_I32LE", "shape": [dset_rows, ]}
        file_uri = f"{hdf5_sample_bucket}{file_path}"

        # make the dataset chunk a multiple of linked chunk shape
        chunk_dims = [chunk_extent * 4, ]
        layout = {
            "class": "H5D_CHUNKED_REF_INDIRECT",
            "file_uri": file_uri,
            "dims": chunk_dims,
            "hyper_dims": [chunk_extent, ],
            "chunk_table": chunkinfo_uuid
        }
        # the linked dataset uses gzip, so set it here
        gzip_filter = {
            "class": "H5Z_FILTER_DEFLATE",
            "id": 1,
            "level": 9,
            "name": "deflate",
        }
        data["creationProperties"] = {"layout": layout, "filters": [gzip_filter, ]}

        req = self.endpoint + "/datasets"
        rsp = self.session.post(req, data=json.dumps(data), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)
        dset_id = rspJson["id"]
        self.assertTrue(helper.validateId(dset_id))

        # link new dataset as 'dset'
        name = "dset"
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
        payload = {"id": dset_id}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # read a selection
        req = self.endpoint + "/datasets/" + dset_id + "/value"
        start = 1234567
        stop = start + 10
        params = {"select": f"[{start}:{stop}]"}  # read 10 element, starting at index 1234567
        params["nonstrict"] = 1  # enable SN to invoke lambda func

        # read the selection
        rsp = self.session.get(req, params=params, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        value = rspJson["value"]
        # should get one element back
        self.assertEqual(len(value), 10)
        self.assertEqual(value, list(range(start, start + 10)))

    def testLargeCreationProperties(self):
        # test Dataset with artifically large creation_properties data
        print("testLargeCreationProperties", self.base_domain)
        headers = helper.getRequestHeaders(domain=self.base_domain)

        # get domain
        req = helper.getEndpoint() + "/"
        rsp = self.session.get(req, headers=headers)
        rspJson = json.loads(rsp.text)
        self.assertTrue("root" in rspJson)
        root_uuid = rspJson["root"]

        # create the dataset
        req = self.endpoint + "/datasets"
        payload = {"type": "H5T_STD_I32LE", "shape": 10}
        creation_props = {"fillValue": 42}
        foo_bar = {}
        for i in range(500):
            foo_bar[i] = f"this is a test {i}"
        creation_props["foo_bar"] = foo_bar

        payload["creationProperties"] = creation_props

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

        # read back the data
        req = self.endpoint + "/datasets/" + dset_uuid + "/value"
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        self.assertEqual(rspJson["value"], [42] * 10)

        # write some values
        payload = {"start": 0, "stop": 5, "value": [24] * 5}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 200)

        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        ret_values = rspJson["value"]
        for i in range(5):
            self.assertEqual(ret_values[i], 24)
            self.assertEqual(ret_values[i + 5], 42)

    def testValueReinitialization1D(self):
        # Test the dataset values get reset after a reduction and resize

        print("testValueReinitialization1D", self.base_domain)
        headers = helper.getRequestHeaders(domain=self.base_domain)

        # get domain
        req = f"{self.endpoint}/"
        rsp = self.session.get(req, headers=headers)
        rspJson = json.loads(rsp.text)
        self.assertTrue("root" in rspJson)
        root_uuid = rspJson["root"]

        # create the dataset
        req = f"{self.endpoint}/datasets"
        payload = {"type": "H5T_STD_I32LE", "shape": 10, "maxdims": 10}
        payload["creationProperties"] = {"fillValue": 42}
        req = self.endpoint + "/datasets"
        rsp = self.session.post(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)  # create dataset
        rspJson = json.loads(rsp.text)
        dset_uuid = rspJson["id"]
        self.assertTrue(helper.validateId(dset_uuid))

        # link new dataset as 'dset'
        name = "dset"
        req = f"{self.endpoint}/groups/{root_uuid}/links/{name}"
        payload = {"id": dset_uuid}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # write to the dset
        req = f"{self.endpoint}/datasets/{dset_uuid}/value"
        data = list(range(10))  # write 0-9
        payload = {"value": data[0:10]}
        params = {"select": "[0:10]"}

        rsp = self.session.put(req, data=json.dumps(payload), params=params, headers=headers)
        self.assertEqual(rsp.status_code, 200)

        # read back the data
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("value" in rspJson)
        self.assertEqual(rspJson["value"], data)

        # resize the dataset to 5 elements
        req = f"{self.endpoint}/datasets/{dset_uuid}/shape"
        payload = {"shape": 5}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)

        # read back the remaining elements
        req = f"{self.endpoint}/datasets/{dset_uuid}/value"
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("value" in rspJson)
        self.assertEqual(rspJson["value"], data[:5])

        # resize back to 10
        req = f"{self.endpoint}/datasets/{dset_uuid}/shape"
        payload = {"shape": 10}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)

        # read all 10 data values
        req = f"{self.endpoint}/datasets/{dset_uuid}/value"
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("value" in rspJson)
        value = rspJson["value"]
        self.assertEqual(value[0:5], data[0:5])
        self.assertEqual(value[5:10], [42,] * 5)

    def testShapeReinitialization2D(self):
        # Test the dataset values get reset after a reduction and resize

        print("testShapeReinitialization2D", self.base_domain)
        headers = helper.getRequestHeaders(domain=self.base_domain)

        # get domain
        req = f"{self.endpoint}/"
        rsp = self.session.get(req, headers=headers)
        rspJson = json.loads(rsp.text)
        self.assertTrue("root" in rspJson)
        root_uuid = rspJson["root"]

        # create the dataset
        req = f"{self.endpoint}/datasets"
        payload = {"type": "H5T_STD_I32LE", "shape": [12, 15], "maxdims": [12, 15]}
        req = self.endpoint + "/datasets"
        rsp = self.session.post(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)  # create dataset
        rspJson = json.loads(rsp.text)
        dset_uuid = rspJson["id"]
        self.assertTrue(helper.validateId(dset_uuid))

        # link new dataset as 'dset'
        name = "dset"
        req = f"{self.endpoint}/groups/{root_uuid}/links/{name}"
        payload = {"id": dset_uuid}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # write to the dset
        req = f"{self.endpoint}/datasets/{dset_uuid}/value"
        data = []
        for i in range(12):
            row = []
            for j in range(15):
                row.append(i * j)
            data.append(row)
        payload = {"value": data}
        params = {"select": "[0:12, 0:15]"}

        rsp = self.session.put(req, data=json.dumps(payload), params=params, headers=headers)
        self.assertEqual(rsp.status_code, 200)

        # read back the data
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("value" in rspJson)
        self.assertEqual(rspJson["value"], data)

        # resize the dataset to 10 x 10 array
        req = f"{self.endpoint}/datasets/{dset_uuid}/shape"
        payload = {"shape": [10, 10]}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)

        # read back the remaining elements
        req = f"{self.endpoint}/datasets/{dset_uuid}/value"
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("value" in rspJson)
        value = rspJson["value"]
        self.assertEqual(len(value), 10)
        for i in range(10):
            row = value[i]
            self.assertEqual(len(row), 10)
            for j in range(10):
                self.assertEqual(row[j], i * j)

        # resize back to 12, 15
        req = f"{self.endpoint}/datasets/{dset_uuid}/shape"
        payload = {"shape": [12, 15]}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)

        # read all the data values
        req = f"{self.endpoint}/datasets/{dset_uuid}/value"
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("value" in rspJson)
        value = rspJson["value"]

        # check that the re-extended area is zero's
        self.assertEqual(len(value), 12)
        for i in range(12):
            row = value[i]
            self.assertEqual(len(row), 15)
            for j in range(15):
                if j < 10 and i < 10:
                    self.assertEqual(row[j], i * j)
                else:
                    self.assertEqual(row[j], 0)

    def testShapeReinitialization3D(self):
        # Test the dataset values get reset after a reduction and resize

        print("testPointReinitialization3D", self.base_domain)
        headers = helper.getRequestHeaders(domain=self.base_domain)

        # get domain
        req = f"{self.endpoint}/"
        rsp = self.session.get(req, headers=headers)
        rspJson = json.loads(rsp.text)
        self.assertTrue("root" in rspJson)
        root_uuid = rspJson["root"]

        # define two different shapes that we'll switch between
        # min extent in each dimension is 20 for the point setup to work
        large_shape = (2200, 120, 130)
        small_shape = (55, 60, 70)

        # setup some points on the diagonal
        # space some points apart equally
        delta = (large_shape[0] // 10, large_shape[1] // 10, large_shape[2] // 10)
        offset = (5, 5, 5)
        points = []
        for i in range(10):
            if i == 0:
                pt = offset
            else:
                last_pt = points[i - 1]
                pt = (last_pt[0] + delta[0], last_pt[1] + delta[1], last_pt[2] + delta[2])
            for n in range(3):
                if pt[n] >= large_shape[n]:
                    raise ValueError("pt outside extent")
            points.append(pt)

        # create the dataset
        req = f"{self.endpoint}/datasets"
        payload = {"type": "H5T_STD_I32LE", "shape": large_shape, "maxdims": large_shape}
        req = self.endpoint + "/datasets"
        rsp = self.session.post(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)  # create dataset
        rspJson = json.loads(rsp.text)
        dset_uuid = rspJson["id"]
        self.assertTrue(helper.validateId(dset_uuid))

        # link new dataset as 'dset'
        name = "dset"
        req = f"{self.endpoint}/groups/{root_uuid}/links/{name}"
        payload = {"id": dset_uuid}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        value = [1, ] * 10  # set value of each pt to one

        # write 1's to all the point locations
        payload = {"points": points, "value": value}
        req = f"{self.endpoint}/datasets/{dset_uuid}/value"
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 200)

        # resize the dataset to the small shape
        req = f"{self.endpoint}/datasets/{dset_uuid}/shape"
        payload = {"shape": small_shape}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)

        # resize back to large shape
        req = f"{self.endpoint}/datasets/{dset_uuid}/shape"
        payload = {"shape": large_shape}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)

        # read all the data values
        req = f"{self.endpoint}/datasets/{dset_uuid}/value"
        body = {"points": points}
        # read selected points
        rsp = self.session.post(req, data=json.dumps(body), headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("value" in rspJson)
        ret_value = rspJson["value"]

        for i in range(10):
            pt = points[i]
            n = ret_value[i]
            if pt[0] >= small_shape[0] or pt[1] >= small_shape[1] or pt[2] >= small_shape[2]:
                self.assertEqual(n, 0)
            else:
                self.assertEqual(n, 1)

    def testPutFixedUTF8StringDataset(self):
        # Test PUT value for 1d attribute with fixed length UTF-8 string
        print("testPutFixedUTF8StringDataset", self.base_domain)
        headers = helper.getRequestHeaders(domain=self.base_domain)

        # get domain
        req = f"{self.endpoint}/"
        rsp = self.session.get(req, headers=headers)
        rspJson = json.loads(rsp.text)
        self.assertTrue("root" in rspJson)
        root_uuid = rspJson["root"]
        req = helper.getEndpoint() + "/"

        # Get root uuid
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("root" in rspJson)
        root_uuid = rspJson["root"]
        helper.validateId(root_uuid)

        # create dataset
        req = self.endpoint + "/datasets"

        text = "this is the chinese character for the number eight: \u516b"

        # size of datatype is in bytes
        byte_data = bytearray(text, "UTF-8")
        byte_length = len(byte_data)

        fixed_str_type = {
            "charSet": "H5T_CSET_UTF8",
            "class": "H5T_STRING",
            "length": byte_length + 1,
            "strPad": "H5T_STR_NULLTERM",
        }

        data = {"type": fixed_str_type, "shape": "H5S_SCALAR"}
        req = self.endpoint + "/datasets"
        rsp = self.session.post(req, data=json.dumps(data), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)
        dset_uuid = rspJson["id"]
        self.assertTrue(helper.validateId(dset_uuid))
        self.assertTrue("type" in rspJson)
        type_json = rspJson["type"]
        self.assertTrue("class" in type_json)
        self.assertEqual(type_json["class"], "H5T_STRING")
        self.assertTrue("length" in type_json)
        self.assertEqual(type_json["length"], byte_length + 1)
        self.assertTrue("strPad" in type_json)
        self.assertEqual(type_json["strPad"], "H5T_STR_NULLTERM")
        self.assertTrue("charSet" in type_json)
        self.assertEqual(type_json["charSet"], "H5T_CSET_UTF8")

        # link new dataset
        name = "fixed_utf8_str_dset"
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
        payload = {"id": dset_uuid}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # write fixed utf8 string to dset
        data = {"value": text}
        req = self.endpoint + "/datasets/" + dset_uuid + "/value"
        rsp = self.session.put(req, data=json.dumps(data), headers=headers)
        self.assertEqual(rsp.status_code, 200)

        # read value back from dset
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("value" in rspJson)
        self.assertEqual(rspJson["value"], text)

        # write different utf8 string of same overall byte length
        text = "this is the chinese character for the number eight: 888"
        new_byte_length = len(bytearray(text, "UTF-8"))
        self.assertEqual(byte_length, new_byte_length)

        data = {"value": text}
        req = self.endpoint + "/datasets/" + dset_uuid + "/value"
        rsp = self.session.put(req, data=json.dumps(data), headers=headers)
        self.assertEqual(rsp.status_code, 200)

        # read value back from dset
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("value" in rspJson)
        self.assertEqual(rspJson["value"], text)

    def testPutFixedUTF8StringDatasetBinary(self):
        # Test PUT value for 1d attribute with fixed length UTF-8 string in binary
        print("testPutFixedUTF8StringDatasetBinary", self.base_domain)
        headers = helper.getRequestHeaders(domain=self.base_domain)
        headers_bin_req = helper.getRequestHeaders(domain=self.base_domain)
        headers_bin_req["Content-Type"] = "application/octet-stream"
        headers_bin_rsp = helper.getRequestHeaders(domain=self.base_domain)
        headers_bin_rsp["accept"] = "application/octet-stream"

        req = helper.getEndpoint() + "/"

        # Get root uuid
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("root" in rspJson)
        root_uuid = rspJson["root"]
        helper.validateId(root_uuid)

        # create dataset
        req = self.endpoint + "/datasets"

        text = "this is the chinese character for the number eight: \u516b"
        # size of datatype is in bytes
        binary_text = bytearray(text, "UTF-8")
        byte_length = len(binary_text)

        fixed_str_type = {
            "charSet": "H5T_CSET_UTF8",
            "class": "H5T_STRING",
            "length": byte_length,
            "strPad": "H5T_STR_NULLTERM",
        }

        data = {"type": fixed_str_type, "shape": "H5S_SCALAR"}
        req = self.endpoint + "/datasets"
        rsp = self.session.post(req, data=json.dumps(data), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)
        dset_uuid = rspJson["id"]
        self.assertTrue(helper.validateId(dset_uuid))

        # link new dataset
        name = "fixed_utf8_str_dset_binary"
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
        payload = {"id": dset_uuid}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # write fixed utf8 binary string to dset
        req = self.endpoint + "/datasets/" + dset_uuid + "/value"
        rsp = self.session.put(req, data=binary_text, headers=headers_bin_req)
        self.assertEqual(rsp.status_code, 200)

        # read value back from dset as json
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        self.assertEqual(rspJson["value"], text)

        # read value back as binary
        rsp = self.session.get(req, headers=headers_bin_rsp)
        self.assertEqual(rsp.status_code, 200)
        self.assertEqual(rsp.text, text)

        # write different utf8 binary string of same overall byte length
        text = "this is the chinese character for the number eight: 888"
        binary_text = bytearray(text, "UTF-8")
        new_byte_length = len(binary_text)
        self.assertEqual(byte_length, new_byte_length)

        # read as JSON
        req = self.endpoint + "/datasets/" + dset_uuid + "/value"
        rsp = self.session.put(req, data=binary_text, headers=headers_bin_req)
        self.assertEqual(rsp.status_code, 200)

        # read as binary
        rsp = self.session.get(req, headers=headers_bin_rsp)
        self.assertEqual(rsp.status_code, 200)
        self.assertEqual(rsp.text, text)


if __name__ == "__main__":
    # setup test files
    unittest.main()
