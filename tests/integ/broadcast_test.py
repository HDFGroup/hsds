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
import helper


class BroadcastTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(BroadcastTest, self).__init__(*args, **kwargs)
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

    def testPut1DDataset(self):
        # Test PUT value with broadcast for 1d dataset
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

        # link new dataset as 'dset1d'
        name = "dset1d"
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
        payload = {"id": dset_id}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # write to the dset
        req = self.endpoint + "/datasets/" + dset_id + "/value"
        data = [42,]  # broadcast to [42, ..., 42]

        payload = {"value": data}
        params = {"element_count": 1}

        rsp = self.session.put(req, data=json.dumps(payload), params=params, headers=headers)
        self.assertEqual(rsp.status_code, 200)

        # read back the data
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("value" in rspJson)
        self.assertEqual(rspJson["value"], data * 10)

    def testPut1DDatasetBinary(self):
        # Test PUT value with broadcast for 1d dataset using binary data
        print("testPut1DDatasetBinary", self.base_domain)
        NUM_ELEMENTS = 10
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

        # write 42 as four-byte little endian integer
        # broadcast across the entire dataset
        req = self.endpoint + "/datasets/" + dset_id + "/value"
        data = bytearray(4)
        data[0] = 0x2a
        params = {"element_count": 1}
        rsp = self.session.put(req, data=data, params=params, headers=headers_bin_req)
        self.assertEqual(rsp.status_code, 200)

        # read back the data
        rsp = self.session.get(req, headers=headers_bin_rsp)
        self.assertEqual(rsp.status_code, 200)
        data = rsp.content
        self.assertEqual(len(data), NUM_ELEMENTS * 4)
        for i in range(NUM_ELEMENTS):
            offset = i * 4
            self.assertEqual(data[offset + 0], 0x2a)
            self.assertEqual(data[offset + 1], 0)
            self.assertEqual(data[offset + 2], 0)
            self.assertEqual(data[offset + 3], 0)

        # write a selection
        params = {"select": "[4:6]"}  # 4th and 5th elements
        params["element_count"] = 1  # broadcast
        data = bytearray(4)
        data[0] = 0x40  # 64
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
                self.assertEqual(data[offset + 0], 0x40)
            else:
                self.assertEqual(data[offset + 0], 0x2a)
            self.assertEqual(data[offset + 1], 0)
            self.assertEqual(data[offset + 2], 0)
            self.assertEqual(data[offset + 3], 0)

    def testPut2DDataset(self):
        """Test PUT value with broadcast for 2d dataset"""
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
        num_col = 5
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

        # broadcast one element to the dataset
        req = self.endpoint + "/datasets/" + dset_id + "/value"
        json_data = [42,]
        payload = {"value": json_data}
        params = {"element_count": 1}
        rsp = self.session.put(req, data=json.dumps(payload), params=params, headers=headers)
        self.assertEqual(rsp.status_code, 200)

        # read back the data
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("value" in rspJson)
        json_value = rspJson["value"]
        for row in json_value:
            for item in row:
                self.assertEqual(item, 42)

        # broadcast row to the dataset
        req = self.endpoint + "/datasets/" + dset_id + "/value"
        json_data = [1, 2, 3, 4, 5]
        payload = {"value": json_data}
        params = {"element_count": 5}
        rsp = self.session.put(req, data=json.dumps(payload), params=params, headers=headers)
        self.assertEqual(rsp.status_code, 200)

        # read back the data
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("value" in rspJson)
        json_value = rspJson["value"]
        for row in json_value:
            self.assertEqual(row, [1, 2, 3, 4, 5])

    def testPut2DDatasetBinary(self):
        # Test PUT value with broadcast for a 2d dataset
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
        num_col = 5
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

        # broadcast one value to entire datsaet
        bin_data = bytearray(4)
        bin_data[0] = 0x2a
        req = self.endpoint + "/datasets/" + dset_id + "/value"
        params = {"element_count": 1}
        rsp = self.session.put(req, data=bin_data, params=params, headers=headers_bin_req)
        self.assertEqual(rsp.status_code, 200)

        # read back the data as json
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("value" in rspJson)
        json_data = rspJson["value"]
        for row in json_data:
            self.assertEqual(row, [42, 42, 42, 42, 42])

        # broadcast a row to the entire dataset
        bin_data = bytearray(4 * 5)
        for i in range(5):
            bin_data[i * 4] = i

        params = {"element_count": 5}
        rsp = self.session.put(req, data=bin_data, params=params, headers=headers_bin_req)
        self.assertEqual(rsp.status_code, 200)

        # read back the data as json
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("value" in rspJson)
        json_data = rspJson["value"]
        for row in json_data:
            self.assertEqual(row, [0, 1, 2, 3, 4])

    def testPut3DDataset(self):
        """Test PUT value with broadcast for 3d dataset"""
        print("testPut3DDataset", self.base_domain)

        headers = helper.getRequestHeaders(domain=self.base_domain)

        req = self.endpoint + "/"

        # Get root uuid
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        root_uuid = rspJson["root"]
        helper.validateId(root_uuid)

        # create dataset
        data = {"type": "H5T_STD_I32LE", "shape": [2, 3, 5]}

        req = self.endpoint + "/datasets"
        rsp = self.session.post(req, data=json.dumps(data), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)
        dset_id = rspJson["id"]
        self.assertTrue(helper.validateId(dset_id))

        # link new dataset as 'dset3d'
        name = "dset3d"
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
        payload = {"id": dset_id}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # broadcast one element to the dataset
        req = self.endpoint + "/datasets/" + dset_id + "/value"
        json_data = [42,]
        payload = {"value": json_data}
        params = {"element_count": 1}
        rsp = self.session.put(req, data=json.dumps(payload), params=params, headers=headers)
        self.assertEqual(rsp.status_code, 200)

        # read back the data
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("value" in rspJson)
        json_value = rspJson["value"]
        for level in json_value:
            for row in level:
                self.assertEqual(row, [42, 42, 42, 42, 42])

        # broadcast row to the dataset
        req = self.endpoint + "/datasets/" + dset_id + "/value"
        json_data = [1, 2, 3, 4, 5]
        payload = {"value": json_data}
        params = {"element_count": 5}
        rsp = self.session.put(req, data=json.dumps(payload), params=params, headers=headers)
        self.assertEqual(rsp.status_code, 200)

        # read back the data
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("value" in rspJson)
        json_value = rspJson["value"]
        for level in json_value:
            for row in level:
                self.assertEqual(row, [1, 2, 3, 4, 5])

        # broadcast level (3x5 block) to the dataset
        req = self.endpoint + "/datasets/" + dset_id + "/value"
        test_data = [[1, 2, 3, 4, 5], [6, 7, 8, 9, 10], [11, 12, 13, 14, 15]]
        payload = {"value": test_data}
        params = {"element_count": 15}
        rsp = self.session.put(req, data=json.dumps(payload), params=params, headers=headers)
        self.assertEqual(rsp.status_code, 200)

        # read back the data
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("value" in rspJson)
        json_value = rspJson["value"]
        # test data should be repeated twice
        self.assertEqual(json_value[0], test_data)
        self.assertEqual(json_value[1], test_data)

    def testPut3DDatasetBinary(self):
        """Test PUT value with broadcast for 3d dataset"""
        print("testPut3DDatasetBinary", self.base_domain)

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
        data = {"type": "H5T_STD_I32LE", "shape": [2, 3, 5]}

        req = self.endpoint + "/datasets"
        rsp = self.session.post(req, data=json.dumps(data), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)
        dset_id = rspJson["id"]
        self.assertTrue(helper.validateId(dset_id))

        # link new dataset as 'dset3d'
        name = "dset3d"
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
        payload = {"id": dset_id}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # broadcast one value to entire datsaet
        bin_data = bytearray(4)
        bin_data[0] = 0x2a
        req = self.endpoint + "/datasets/" + dset_id + "/value"
        params = {"element_count": 1}
        rsp = self.session.put(req, data=bin_data, params=params, headers=headers_bin_req)
        self.assertEqual(rsp.status_code, 200)

        # read back the data
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("value" in rspJson)
        json_value = rspJson["value"]
        for level in json_value:
            for row in level:
                self.assertEqual(row, [42, 42, 42, 42, 42])

        # broadcast row to the dataset
        req = self.endpoint + "/datasets/" + dset_id + "/value"
        bin_data = bytearray(5 * 4)
        for i in range(5):
            bin_data[i * 4] = i + 1

        params = {"element_count": 5}
        rsp = self.session.put(req, data=bin_data, params=params, headers=headers_bin_req)
        self.assertEqual(rsp.status_code, 200)

        # read back the data
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("value" in rspJson)
        json_value = rspJson["value"]
        for level in json_value:
            for row in level:
                self.assertEqual(row, [1, 2, 3, 4, 5])

        # broadcast level (3x5 block) to the dataset
        req = self.endpoint + "/datasets/" + dset_id + "/value"
        bin_data = bytearray(5 * 3 * 4)
        for i in range(5 * 3):
            bin_data[i * 4] = i + 1
        params = {"element_count": 15}
        rsp = self.session.put(req, data=bin_data, params=params, headers=headers_bin_req)
        self.assertEqual(rsp.status_code, 200)

        # read back the data
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("value" in rspJson)
        json_value = rspJson["value"]

        for level in json_value:
            expected = 1
            for row in level:
                for item in row:
                    self.assertEqual(item, expected)
                    expected += 1


if __name__ == "__main__":
    # setup test files
    unittest.main()
