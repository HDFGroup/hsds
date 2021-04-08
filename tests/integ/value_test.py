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
import helper
import config

class ValueTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(ValueTest, self).__init__(*args, **kwargs)
        self.base_domain = helper.getTestDomainName(self.__class__.__name__)
        helper.setupDomain(self.base_domain)
        self.endpoint = helper.getEndpoint()

        # main

    def testPut1DDataset(self):
        # Test PUT value for 1d dataset
        print("testPut1DDataset", self.base_domain)

        headers = helper.getRequestHeaders(domain=self.base_domain)
        req = self.endpoint + '/'

        # Get root uuid
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        root_uuid = rspJson["root"]
        helper.validateId(root_uuid)

        # create dataset
        data = { "type": "H5T_STD_I32LE", "shape": 10 }

        req = self.endpoint + '/datasets'
        rsp = requests.post(req, data=json.dumps(data), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)
        dset_id = rspJson["id"]
        self.assertTrue(helper.validateId(dset_id))

        # add an attribute
        attr_payload = {'type': 'H5T_STD_I32LE', 'value': 42}
        attr_name = "attr1"
        req = self.endpoint + '/datasets/' + dset_id + "/attributes/" + attr_name
        rsp = requests.put(req, data=json.dumps(attr_payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)  # created

        # link new dataset as 'dset1d'
        name = "dset1d"
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
        payload = {"id": dset_id}
        rsp = requests.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # read values from dset (should be zeros)
        req = self.endpoint + "/datasets/" + dset_id + "/value"
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        self.assertEqual(rspJson["value"], [0,] * data["shape"])

        # write to the dset
        data = list(range(10))  # write 0-9
        payload = { 'value': data }
        rsp = requests.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 200)

        # read back the data
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        self.assertEqual(rspJson["value"], data)

        # read a selection
        params = {"select": "[2:8]"} # read 6 elements, starting at index 2
        rsp = requests.get(req, params=params, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        self.assertEqual(rspJson["value"], list(range(2,8)))

        # read one element.  cf test for PR #84    
        params = {"select": "[3]"} # read 4th element
        rsp = requests.get(req, params=params, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        self.assertEqual(rspJson["value"], [3])

        # try to read beyond the bounds of the array
        params = {"select": "[2:18]"} # read 6 elements, starting at index 2
        rsp = requests.get(req, params=params, headers=headers)
        self.assertEqual(rsp.status_code, 400)

    def testPut1DDatasetBinary(self):
        # Test PUT value for 1d dataset using binary data
        print("testPut1DDatasetBinary", self.base_domain)
        NUM_ELEMENTS=10     # 1000000 - this value is hitting nginx request size limit

        headers = helper.getRequestHeaders(domain=self.base_domain)
        headers_bin_req = helper.getRequestHeaders(domain=self.base_domain)
        headers_bin_req["Content-Type"] = "application/octet-stream"
        headers_bin_rsp = helper.getRequestHeaders(domain=self.base_domain)
        headers_bin_rsp["accept"] = "application/octet-stream"


        req = self.endpoint + '/'

        # Get root uuid
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        root_uuid = rspJson["root"]
        helper.validateId(root_uuid)

        # create dataset
        data = { "type": "H5T_STD_I32LE", "shape": NUM_ELEMENTS }
        req = self.endpoint + '/datasets'
        rsp = requests.post(req, data=json.dumps(data), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)
        dset_id = rspJson["id"]
        self.assertTrue(helper.validateId(dset_id))

        # link new dataset as 'dset1d'
        name = "dset1d"
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
        payload = {"id": dset_id}
        rsp = requests.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # read values from dset (should be zeros)
        req = self.endpoint + "/datasets/" + dset_id + "/value"
        rsp = requests.get(req, headers=headers_bin_rsp)
        self.assertEqual(rsp.status_code, 200)
        self.assertEqual(rsp.headers['Content-Type'], "application/octet-stream")
        data = rsp.content
        self.assertEqual(len(data), NUM_ELEMENTS * 4)
        for i in range(NUM_ELEMENTS):
            offset = i*4
            self.assertEqual(data[offset+0], 0)
            self.assertEqual(data[offset+1], 0)
            self.assertEqual(data[offset+2], 0)
            self.assertEqual(data[offset+3], 0)

        # write to the dset
        # write 0-9 as four-byte little-endian integers
        data = bytearray(4*NUM_ELEMENTS)
        for i in range(NUM_ELEMENTS):
            data[i*4] = i%256
        rsp = requests.put(req, data=data, headers=headers_bin_req)
        self.assertEqual(rsp.status_code, 200)

        # read back the data
        rsp = requests.get(req, headers=headers_bin_rsp)
        self.assertEqual(rsp.status_code, 200)
        data = rsp.content
        self.assertEqual(len(data), NUM_ELEMENTS*4)
        for i in range(NUM_ELEMENTS):
            offset = i*4
            self.assertEqual(data[offset+0], i%256)
            self.assertEqual(data[offset+1], 0)
            self.assertEqual(data[offset+2], 0)
            self.assertEqual(data[offset+3], 0)

        # write a selection
        params = {"select": "[4:6]"}  #4th and 5th elements
        data = bytearray(4*2)
        for i in range(2):
            data[i*4] = 255
        rsp = requests.put(req, data=data, params=params, headers=headers_bin_req)
        self.assertEqual(rsp.status_code, 200)


        # read a selection
        params = {"select": "[0:6]"} # read first 6 elements
        rsp = requests.get(req, params=params, headers=headers_bin_rsp)
        self.assertEqual(rsp.status_code, 200)
        data = rsp.content
        self.assertEqual(len(data), 24)
        for i in range(6):
            offset = i*4
            if i>=4:
                # these were updated by the previous selection
                self.assertEqual(data[offset+0], 255)
            else:
                self.assertEqual(data[offset+0], i)
            self.assertEqual(data[offset+1], 0)
            self.assertEqual(data[offset+2], 0)
            self.assertEqual(data[offset+3], 0)

        # read one element.  cf test for PR #84    
        params = {"select": "[3]"} # read 4th element
        rsp = requests.get(req, params=params, headers=headers_bin_rsp)
        self.assertEqual(rsp.status_code, 200)
        data = rsp.content
        self.assertEqual(len(data), 4)
        self.assertEqual(data[0], 3)
        self.assertEqual(data[1], 0)
        self.assertEqual(data[2], 0)
        self.assertEqual(data[3], 0)
         

    def testPut2DDataset(self):
        # Test PUT value for 2d dataset
        print("testPut2DDataset", self.base_domain)

        headers = helper.getRequestHeaders(domain=self.base_domain)
        req = self.endpoint + '/'

        # Get root uuid
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        root_uuid = rspJson["root"]
        helper.validateId(root_uuid)

        # create dataset
        num_col = 8
        num_row = 4
        data = { "type": "H5T_STD_I32LE", "shape": [num_row,num_col] }

        req = self.endpoint + '/datasets'
        rsp = requests.post(req, data=json.dumps(data), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)
        dset_id = rspJson["id"]
        self.assertTrue(helper.validateId(dset_id))

        # link new dataset as 'dset1d'
        name = "dset2d"
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
        payload = {"id": dset_id}
        rsp = requests.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # read values from dset (should be zeros)
        req = self.endpoint + "/datasets/" + dset_id + "/value"
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        for i in range(num_row):
            self.assertEqual(rspJson["value"][i], [0,] *  num_col)

        # write to the dset
        json_data = []
        for i in range(num_row):
            row = []
            for j in range(num_col):
                row.append(i*10 + j)
            json_data.append(row)
        payload = { 'value': json_data }
        rsp = requests.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 200)

        # read back the data
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        self.assertEqual(rspJson["value"], json_data)

        # read a selection
        params = {"select": "[3:4,2:8]"} # read 6 elements, starting at index 2
        rsp = requests.get(req, params=params, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        self.assertEqual(rspJson["value"], [json_data[3][2:8],])

    def testPut2DDatasetBinary(self):
        # Test PUT value for 2d dataset
        print("testPut2DDatasetBinary", self.base_domain)

        headers = helper.getRequestHeaders(domain=self.base_domain)
        headers_bin_req = helper.getRequestHeaders(domain=self.base_domain)
        headers_bin_req["Content-Type"] = "application/octet-stream"
        headers_bin_rsp = helper.getRequestHeaders(domain=self.base_domain)
        headers_bin_rsp["accept"] = "application/octet-stream"

        req = self.endpoint + '/'

        # Get root uuid
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        root_uuid = rspJson["root"]
        helper.validateId(root_uuid)

        # create dataset
        num_col = 8
        num_row = 4
        data = { "type": "H5T_STD_I32LE", "shape": [num_row,num_col] }

        req = self.endpoint + '/datasets'
        rsp = requests.post(req, data=json.dumps(data), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)
        dset_id = rspJson["id"]
        self.assertTrue(helper.validateId(dset_id))

        # link new dataset as 'dset2d'
        name = "dset2d"
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
        payload = {"id": dset_id}
        rsp = requests.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # read values from dset (should be zeros)
        req = self.endpoint + "/datasets/" + dset_id + "/value"
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        for i in range(num_row):
            self.assertEqual(rspJson["value"][i], [0,] *  num_col)

        # initialize bytearray to test values
        bin_data = bytearray(4*num_row*num_col)
        json_data = []
        for i in range(num_row):
            row = []
            for j in range(num_col):
                bin_data[(i*num_col+j)*4] = i*10 + j
                row.append(i*10 + j)  # create json data for comparison
            json_data.append(row)
        rsp = requests.put(req, data=bin_data, headers=headers_bin_req)
        self.assertEqual(rsp.status_code, 200)

        # read back the data as json
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        self.assertEqual(rspJson["value"], json_data)

        # read data as binary
        rsp = requests.get(req, headers=headers_bin_rsp)
        self.assertEqual(rsp.status_code, 200)
        data = rsp.content
        self.assertEqual(len(data), num_row*num_col*4)
        self.assertEqual(data, bin_data)

        # read a selection
        params = {"select": "[3:4,2:8]"} # read 6 elements, starting at index 2
        rsp = requests.get(req, params=params, headers=headers_bin_rsp)
        self.assertEqual(rsp.status_code, 200)
        data = rsp.content
        self.assertEqual(len(data), 6*4)
        for i in range(6):
            self.assertEqual(data[i*4], 3*10 + i+2)


    def testPutSelection1DDataset(self):
        # Test PUT value with selection for 1d dataset
        print("testPutSelection1DDataset", self.base_domain)

        headers = helper.getRequestHeaders(domain=self.base_domain)
        req = self.endpoint + '/'

        # Get root uuid
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        root_uuid = rspJson["root"]
        helper.validateId(root_uuid)

        # create dataset
        dset_body = { "type": "H5T_STD_I32LE", "shape": 10 }

        req = self.endpoint + '/datasets'
        rsp = requests.post(req, data=json.dumps(dset_body), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)
        dset_id = rspJson["id"]
        self.assertTrue(helper.validateId(dset_id))

        # link new dataset as 'dset1d'
        name = "dset1d"
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
        payload = {"id": dset_id}
        rsp = requests.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # write to dset
        req = self.endpoint + "/datasets/" + dset_id + "/value"
        data = list(range(10))  # write 0-9
        data_part1 = data[0:5]
        data_part2 = data[5:10]

        # write part 1
        payload = { 'start': 0, 'stop': 5, 'value': data_part1 }
        rsp = requests.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 200)

        # write part 2
        payload = { 'start': 5, 'stop': 10, 'value': data_part2 }
        rsp = requests.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 200)

        # read back the data
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        self.assertEqual(rspJson["value"], data)

        # write data with a step of 2
        payload = { 'start': 0, 'stop': 10,  'step': 2, 'value': data_part1 }
        rsp = requests.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 200)

        payload = { 'start': 1, 'stop': 10,  'step': 2, 'value': data_part2 }
        rsp = requests.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 200)

        # read back the data
        rsp = requests.get(req, headers=headers)
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
        # Test PUT value with selection for 2d dataset
        print("testPutSelection2DDataset", self.base_domain)

        headers = helper.getRequestHeaders(domain=self.base_domain)

        req = self.endpoint + '/'

        # Get root uuid
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        root_uuid = rspJson["root"]
        helper.validateId(root_uuid)

        # create dataset
        # pass in layout specification so that we can test selection across chunk boundries
        data = { "type": "H5T_STD_I32LE", "shape": [45,54] }
        data['creationProperties'] = {'layout': {'class': 'H5D_CHUNKED', 'dims': [10, 10] }}

        req = self.endpoint + '/datasets'
        rsp = requests.post(req, data=json.dumps(data), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)
        dset_id = rspJson["id"]
        self.assertTrue(helper.validateId(dset_id))

        # link new dataset as 'dset2d'
        name = "dset2d"
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
        payload = {"id": dset_id}
        rsp = requests.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # write a horizontal strip of 22s
        req = self.endpoint + "/datasets/" + dset_id + "/value"
        data = [22,] * 50
        payload = { 'start': [22, 2], 'stop': [23, 52], 'value': data }
        rsp = requests.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 200)

        # read back a vertical strip that crossed the horizontal strip
        req = self.endpoint + "/datasets/" + dset_id + "/value"  # test
        params = {"select": "[20:25,21:22]"} # read 6 elements, starting at index 20
        rsp = requests.get(req, params=params, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        value = rspJson["value"]
        self.assertEqual(len(value), 5)
        self.assertEqual(value, [[0,],[0,],[22,],[0,],[0,]])

        # write 44's to a region with a step value of 2 and 3
        req = self.endpoint + "/datasets/" + dset_id + "/value"
        data = [44,] * 20
        payload = { 'start': [10, 20], 'stop': [20, 32], 'step': [2, 3], 'value': data }
        rsp = requests.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 200)

        # read back a sub-block
        req = self.endpoint + "/datasets/" + dset_id + "/value"  # test
        params = {"select": "[12:13,23:26]"} # read 6 elements, starting at index (12,14)
        rsp = requests.get(req, params=params, headers=headers)
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
        req = self.endpoint + '/'

        # Get root uuid
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        root_uuid = rspJson["root"]
        helper.validateId(root_uuid)

        # create dataset
        fixed_str_type = {"charSet": "H5T_CSET_ASCII",
                "class": "H5T_STRING",
                "length": 7,
                "strPad": "H5T_STR_NULLPAD" }
        data = { "type": fixed_str_type, "shape": 4 }

        req = self.endpoint + '/datasets'
        rsp = requests.post(req, data=json.dumps(data), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)
        dset_id = rspJson["id"]
        self.assertTrue(helper.validateId(dset_id))

        # link new dataset as 'dset_str'
        name = "dset_str"
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
        payload = {"id": dset_id}
        rsp = requests.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # read values from dset (should be empty strings)
        req = self.endpoint + "/datasets/" + dset_id + "/value"
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        self.assertEqual(rspJson["value"], ['',] * data["shape"])

        # write to the dset
        data = ["Parting", "is such", "sweet", "sorrow."]
        payload = { 'value': data }
        rsp = requests.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 200)

        # read back the data
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        self.assertEqual(rspJson["value"], data)

        # read a selection
        params = {"select": "[1:3]"} # read 2 elements, starting at index 1
        rsp = requests.get(req, params=params, headers=headers)
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
        req = self.endpoint + '/'

        # Get root uuid
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        root_uuid = rspJson["root"]
        helper.validateId(root_uuid)

        STR_LENGTH = 7
        STR_COUNT = 4

        # create dataset
        fixed_str_type = {"charSet": "H5T_CSET_ASCII",
                "class": "H5T_STRING",
                "length": STR_LENGTH,
                "strPad": "H5T_STR_NULLPAD" }
        data = { "type": fixed_str_type, "shape": STR_COUNT }

        req = self.endpoint + '/datasets'
        rsp = requests.post(req, data=json.dumps(data), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)
        dset_id = rspJson["id"]
        self.assertTrue(helper.validateId(dset_id))

        # link new dataset as 'dset_str'
        name = "dset_str"
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
        payload = {"id": dset_id}
        rsp = requests.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # read values from dset (should be empty strings)
        req = self.endpoint + "/datasets/" + dset_id + "/value"
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        self.assertEqual(rspJson["value"], ['',] * data["shape"])

        # write to the dset
        strings = ["Parting", "is such", "sweet", "sorrow."]
        data = bytearray(STR_COUNT*STR_LENGTH)
        for i in range(STR_COUNT):
            string = strings[i]
            for j in range(STR_LENGTH):
                offset = i*STR_LENGTH + j
                if j < len(string):
                    data[offset] = ord(string[j])
                else:
                    data[offset] = 0  # null padd rest of the element


        payload = { 'value': data }
        rsp = requests.put(req, data=data, headers=headers_bin_req)
        self.assertEqual(rsp.status_code, 200)

        # read back the data
        rsp = requests.get(req, headers=headers)
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
        req = self.endpoint + '/'

        # Get root uuid
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        root_uuid = rspJson["root"]
        helper.validateId(root_uuid)

        # create dataset
        fixed_str_type = {"charSet": "H5T_CSET_ASCII",
                "class": "H5T_STRING",
                "length": 7,
                "strPad": "H5T_STR_NULLPAD" }
        data = { "type": fixed_str_type, "shape": 4 }

        req = self.endpoint + '/datasets'
        rsp = requests.post(req, data=json.dumps(data), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)
        dset_id = rspJson["id"]
        self.assertTrue(helper.validateId(dset_id))

        # link new dataset as 'dset_str'
        name = "dset_str"
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
        payload = {"id": dset_id}
        rsp = requests.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # read values from dset (should be empty strings)
        req = self.endpoint + "/datasets/" + dset_id + "/value"
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        self.assertEqual(rspJson["value"], ['',] * data["shape"])

        # write to the dset
        data = ["123456", "1234567", "12345678", "123456789"]
        payload = { 'value': data }
        rsp = requests.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 200)

        # read back the data
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)

        # read all values
        rsp = requests.get(req, headers=headers)
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
        req = self.endpoint + '/'

        # Get root uuid
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        root_uuid = rspJson["root"]
        helper.validateId(root_uuid)

        # create a dataset obj
        str_type = { 'charSet':   'H5T_CSET_ASCII',
                     'class':  'H5T_STRING',
                     'strPad': 'H5T_STR_NULLPAD',
                     'length': 40}
        data = { "type": str_type}
        req = self.endpoint + '/datasets'
        rsp = requests.post(req, data=json.dumps(data), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)
        self.assertEqual(rspJson["attributeCount"], 0)
        dset_id = rspJson["id"]
        self.assertTrue(helper.validateId(dset_id))

        # link new dataset as 'dset_scalar'
        name = "dset_scalar"
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
        payload = {"id": dset_id}
        rsp = requests.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # read unintialized value from dataset
        req = self.endpoint + "/datasets/" + dset_id + "/value"
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        self.assertEqual(rspJson["value"], '')

        # write to the dataset
        data = "Hello, world"
        payload = { 'value': data }
        rsp = requests.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 200)

        # read back the value
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        self.assertEqual(rspJson["value"], "Hello, world")

    def testNullSpaceDataset(self):
        # Test attempted read/write to null space dataset
        print("testNullSpaceDataset", self.base_domain)

        headers = helper.getRequestHeaders(domain=self.base_domain)
        req = self.endpoint + '/'

        # Get root uuid
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        root_uuid = rspJson["root"]
        helper.validateId(root_uuid)

        # create a dataset obj
        str_type = { 'charSet':   'H5T_CSET_ASCII',
                     'class':  'H5T_STRING',
                     'strPad': 'H5T_STR_NULLPAD',
                     'length': 40}
        data = { "type": str_type, 'shape': 'H5S_NULL'}
        req = self.endpoint + '/datasets'
        rsp = requests.post(req, data=json.dumps(data), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)
        dset_id = rspJson["id"]
        self.assertTrue(helper.validateId(dset_id))

        # link new dataset as 'dset_null'
        name = "dset_null"
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
        payload = {"id": dset_id}
        rsp = requests.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # try reading from the dataset
        req = self.endpoint + "/datasets/" + dset_id + "/value"
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 400)

        # try writing to the dataset
        data = "Hello, world"
        payload = { 'value': data }
        rsp = requests.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 400)


    def testPutCompound(self):
        headers = helper.getRequestHeaders(domain=self.base_domain)
        req = self.endpoint + '/'

        # Get root uuid
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        root_uuid = rspJson["root"]
        helper.validateId(root_uuid)

        fields = ({'name': 'temp', 'type': 'H5T_STD_I32LE'},
                    {'name': 'pressure', 'type': 'H5T_IEEE_F16LE'})
        datatype = {'class': 'H5T_COMPOUND', 'fields': fields }

        #
        #create compound scalar dataset
        #
        payload = {'type': datatype}
        req = self.endpoint + "/datasets"
        rsp = requests.post(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)  # create dataset

        rspJson = json.loads(rsp.text)
        dset0d_uuid = rspJson['id']
        self.assertTrue(helper.validateId(dset0d_uuid))

        # verify the shape of the dataset
        req = self.endpoint + "/datasets/" + dset0d_uuid
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)  # get dataset
        rspJson = json.loads(rsp.text)
        shape = rspJson["shape"]
        self.assertEqual(shape["class"], 'H5S_SCALAR')

        # link new dataset as 'dset0_compound'
        name = 'dset0d'
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
        payload = {"id": dset0d_uuid}
        rsp = requests.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # write entire array
        value = (42, 0.42)
        payload = {'value': value}
        req = self.endpoint + "/datasets/" + dset0d_uuid + "/value"
        rsp = requests.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 200)  # write value

        # read back the value
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)

        #
        #create 1d dataset
        #
        num_elements = 10
        payload = {'type': datatype, 'shape': num_elements}
        req = self.endpoint + "/datasets"
        rsp = requests.post(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)  # create dataset

        rspJson = json.loads(rsp.text)
        dset1d_uuid = rspJson['id']
        self.assertTrue(helper.validateId(dset1d_uuid))

        # link new dataset as 'dset1'
        name = 'dset1d'
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
        payload = {"id": dset1d_uuid}
        rsp = requests.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)


        # write entire array
        value = []
        for i in range(num_elements):
            item = (i*10, i*10+i/10.0)
            value.append(item)
        payload = {'value': value}
        req = self.endpoint + "/datasets/" + dset1d_uuid + "/value"
        rsp = requests.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 200)  # write value

        # selection write
        payload = { 'start': 0, 'stop': 1, 'value': (42, .42) }
        req = self.endpoint + "/datasets/" + dset1d_uuid + "/value"
        rsp = requests.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 200)  # write value

        # read back the data
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)

        readData = rspJson["value"]

        self.assertEqual(readData[0][0], 42)
        self.assertEqual(readData[1][0], 10)

        #
        #create 2d dataset
        #
        dims = [2,2]
        payload = {'type': datatype, 'shape': dims}
        req = self.endpoint + "/datasets"
        rsp = requests.post(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)  # create dataset

        rspJson = json.loads(rsp.text)
        dset2d_uuid = rspJson['id']
        self.assertTrue(helper.validateId(dset2d_uuid))

        # link new dataset as 'dset2d'
        name = 'dset2d'
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
        payload = {"id": dset2d_uuid}
        rsp = requests.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)


        # write entire array
        value = []
        for i in range(dims[0]):
            row = []
            for j in range(dims[1]):
                item = (i*10, i*10+j/2.0)
                row.append(item)
            value.append(row)
        payload = {'value': value}

        req = self.endpoint + "/datasets/" + dset2d_uuid + "/value"
        rsp = requests.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 200)  # write value

        # read back the data
        rsp = requests.get(req, headers=headers)
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
        req = helper.getEndpoint() + '/'
        rsp = requests.get(req, headers=headers)
        rspJson = json.loads(rsp.text)
        self.assertTrue("root" in rspJson)
        root_uuid = rspJson["root"]

        # create the dataset
        req = self.endpoint + "/datasets"
        payload = {'type': 'H5T_STD_I32LE', 'shape': 10}
        creation_props = {'fillValue': 42 }
        payload['creationProperties'] = creation_props

        req = self.endpoint + "/datasets"
        rsp = requests.post(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)  # create dataset
        rspJson = json.loads(rsp.text)
        dset_uuid = rspJson['id']
        self.assertTrue(helper.validateId(dset_uuid))

        # link new dataset as 'dset'
        name = 'dset'
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
        payload = {"id": dset_uuid}
        rsp = requests.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # read back the data
        req = self.endpoint + "/datasets/" + dset_uuid + "/value"
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        self.assertEqual(rspJson["value"], [42,]*10)

        # write some values
        payload = { 'start': 0, 'stop': 5, 'value': [24,]*5 }
        rsp = requests.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 200)

        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        ret_values = rspJson["value"]
        for i in range(5):
            self.assertEqual(ret_values[i], 24)
            self.assertEqual(ret_values[i+5], 42)

    def testCompoundFillValue(self):
        headers = helper.getRequestHeaders(domain=self.base_domain)
        req = self.endpoint + '/'

        # Get root uuid
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        root_uuid = rspJson["root"]
        helper.validateId(root_uuid)

        # ASCII 8-char fixed width
        str_type = { 'charSet':   'H5T_CSET_ASCII',
                     'class':  'H5T_STRING',
                     'strPad': 'H5T_STR_NULLPAD',
                     'length': 8}

        fields = ({'name': 'tag', 'type': str_type},
                    {'name': 'value', 'type': 'H5T_STD_I32LE'})
        datatype = {'class': 'H5T_COMPOUND', 'fields': fields }
        fill_value = ['blank', -999]
        creationProperties =  {'fillValue': fill_value }

        #
        #create compound dataset
        #
        payload = {'type': datatype, 'shape': 40, 'creationProperties': creationProperties}
        req = self.endpoint + "/datasets"
        rsp = requests.post(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)  # create dataset

        rspJson = json.loads(rsp.text)
        dset_uuid = rspJson['id']

        # verify the shape of the dataset
        req = self.endpoint + "/datasets/" + dset_uuid
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)  # get dataset
        rspJson = json.loads(rsp.text)
        shape = rspJson["shape"]
        self.assertEqual(shape["class"], 'H5S_SIMPLE')
        self.assertEqual(shape["dims"], [40,])

        # read the default values
        req = self.endpoint + "/datasets/" + dset_uuid + "/value"
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)  # OK
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        value = rspJson["value"]
        for i in range(40):
            self.assertEqual(value[i], fill_value)

        # write some values
        new_value = ('mytag', 123)
        payload = { 'start': 0, 'stop': 20, 'value': [new_value,]*20}
        rsp = requests.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 200)

        # read the values back
        req = self.endpoint + "/datasets/" + dset_uuid + "/value"
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)  # OK
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        value = rspJson["value"]
        for i in range(20):
            self.assertEqual(value[i], list(new_value))
            self.assertEqual(value[i+20], fill_value)

    def testBigFillValue(self):
        # test Dataset with simple type and fill value that is very large
        # (i.e. a large string)
        print("testBigFillValue", self.base_domain)
        headers = helper.getRequestHeaders(domain=self.base_domain)

        # get domain
        req = helper.getEndpoint() + '/'
        rsp = requests.get(req, headers=headers)
        rspJson = json.loads(rsp.text)
        self.assertTrue("root" in rspJson)
        root_uuid = rspJson["root"]

        item_length = 1000
        # ASCII fixed width
        str_type = { 'charSet':   'H5T_CSET_ASCII',
                     'class':  'H5T_STRING',
                     'strPad': 'H5T_STR_NULLPAD',
                     'length': item_length}

        fill_value = 'X'*item_length
        # create the dataset
        req = self.endpoint + "/datasets"
        payload = {'type': str_type, 'shape': 10}
        payload['creationProperties'] = {'fillValue': fill_value }
        req = self.endpoint + "/datasets"
        rsp = requests.post(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)  # create dataset
        rspJson = json.loads(rsp.text)
        dset_uuid = rspJson['id']
        self.assertTrue(helper.validateId(dset_uuid))

        # link new dataset as 'dset'
        name = 'dset'
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
        payload = {"id": dset_uuid}
        rsp = requests.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # read back the data
        req = self.endpoint + "/datasets/" + dset_uuid + "/value"
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        self.assertEqual(rspJson["value"], [fill_value,]*10)

        # write some values
        payload = { 'start': 0, 'stop': 5, 'value': ['hello',]*5 }
        rsp = requests.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 200)

        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        ret_values = rspJson["value"]
        for i in range(5):
            self.assertEqual(ret_values[i], 'hello')
            self.assertEqual(len(ret_values[i+5]), len(fill_value))
            self.assertEqual(ret_values[i+5], fill_value)

    def testPutObjRefDataset(self):
        # Test PUT obj ref values for 1d dataset
        print("testPutObjRefDataset", self.base_domain)

        headers = helper.getRequestHeaders(domain=self.base_domain)
        req = self.endpoint + '/'

        # Get root uuid
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        root_uuid = rspJson["root"]
        helper.validateId(root_uuid)

        # create new group
        payload = { 'link': { 'id': root_uuid, 'name': 'g1' } }
        req = helper.getEndpoint() + "/groups"
        rsp = requests.post(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)
        g1_uuid = rspJson["id"]
        self.assertTrue(helper.validateId(g1_uuid))

        # create dataset
        ref_type = {"class": "H5T_REFERENCE",
                    "base": "H5T_STD_REF_OBJ"}
        data = { "type": ref_type, "shape": 3 }

        req = self.endpoint + '/datasets'
        rsp = requests.post(req, data=json.dumps(data), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)
        dset_id = rspJson["id"]
        self.assertTrue(helper.validateId(dset_id))

        # link new dataset as 'dsetref'
        name = "dsetref"
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
        payload = {"id": dset_id}
        rsp = requests.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # read values from dset (should be empty strings)
        req = self.endpoint + "/datasets/" + dset_id + "/value"
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        self.assertEqual(rspJson["value"], ['',] * data["shape"])

        # write some values
        ref_values = ["groups/" + root_uuid, '', "groups/" + g1_uuid]
        payload = {  'value': ref_values }
        rsp = requests.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 200)

        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        ret_values = rspJson["value"]
        self.assertEqual(ref_values[0], "groups/" + root_uuid)
        self.assertEqual(ref_values[1], '')
        self.assertEqual(ref_values[2], "groups/" + g1_uuid)

    def testPutObjRefDatasetBinary(self):
        # Test PUT obj ref values for 1d dataset using binary transfer
        print("testPutObjRefDatasetBinary", self.base_domain)

        headers = helper.getRequestHeaders(domain=self.base_domain)
        headers_bin_req = helper.getRequestHeaders(domain=self.base_domain)
        headers_bin_req["Content-Type"] = "application/octet-stream"

        req = self.endpoint + '/'

        # Get root uuid
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        root_uuid = rspJson["root"]
        helper.validateId(root_uuid)

        # create new group
        payload = { 'link': { 'id': root_uuid, 'name': 'g1' } }
        req = helper.getEndpoint() + "/groups"
        rsp = requests.post(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)
        g1_uuid = rspJson["id"]
        self.assertTrue(helper.validateId(g1_uuid))

        # create dataset
        ref_type = {"class": "H5T_REFERENCE",
                    "base": "H5T_STD_REF_OBJ"}
        data = { "type": ref_type, "shape": 3 }

        req = self.endpoint + '/datasets'
        rsp = requests.post(req, data=json.dumps(data), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)
        dset_id = rspJson["id"]
        self.assertTrue(helper.validateId(dset_id))

        # link new dataset as 'dsetref'
        name = "dsetref"
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
        payload = {"id": dset_id}
        rsp = requests.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # write some values
        ref_length = 48  # long enough to store any object ref
        data = bytearray(3*ref_length)
        ref_values = ["groups/" + root_uuid, '', "groups/" + g1_uuid]
        for i in range(3):
            ref_value = ref_values[i]
            for j in range(len(ref_value)):
                offset = i*ref_length + j
                data[offset] = ord(ref_value[j])

        req = self.endpoint + "/datasets/" + dset_id + "/value"
        rsp = requests.put(req, data=data, headers=headers_bin_req)
        self.assertEqual(rsp.status_code, 200)

        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        ret_values = rspJson["value"]
        self.assertEqual(ref_values[0], "groups/" + root_uuid)
        self.assertEqual(ref_values[1], '')
        self.assertEqual(ref_values[2], "groups/" + g1_uuid)


    def testGet(self):
        domain = helper.getTestDomain("tall.h5")
        print("testGetDomain", domain)
        headers = helper.getRequestHeaders(domain=domain)

        # verify domain exists
        req = helper.getEndpoint() + '/'
        rsp = requests.get(req, headers=headers)
        if rsp.status_code != 200:
            print("WARNING: Failed to get domain: {}. Is test data setup?".format(domain))
            return  # abort rest of test
        domainJson = json.loads(rsp.text)
        root_uuid = domainJson["root"]

        # get the dataset uuid
        dset1_uuid = helper.getUUIDByPath(domain, "/g1/g1.1/dset1.1.1")

        # read all the dataset values
        req = helper.getEndpoint() + "/datasets/" + dset1_uuid + "/value"
        rsp = requests.get(req, headers=headers)
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
                self.assertEqual(row[i], i*j)

        # read 4x4 block from dataset
        params = {"select": "[0:4, 0:4]"}
        params["nonstrict"] = 1  # SN can read directly from S3 or DN node
        rsp = requests.get(req, params=params, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("value" in rspJson)
        data = rspJson["value"]  # should be 4 x 4 array
        for j in range(4):
            row = data[j]
            for i in range(4):
                self.assertEqual(row[i], i*j)


        # read 2x2 block from dataset with step of 2
        params = {"select": "[0:4:2, 0:4:2]"}
        params["nonstrict"] = 1  
        rsp = requests.get(req, params=params, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("value" in rspJson)
        data = rspJson["value"]  # should be 2 x 2 array
        for j in range(2):
            row = data[j]
            for i in range(2):
                self.assertEqual(row[i], (i*2)*(j*2))

        # try reading a selection that is out of bounds
        params = {"select": "[0:12, 0:12]"}
        params["nonstrict"] = 1
        rsp = requests.get(req, params=params, headers=headers)
        self.assertEqual(rsp.status_code, 400)


    def testResizable1DValue(self):
        # test read/write to resizable dataset
        print("testResizable1DValue", self.base_domain)
        headers = helper.getRequestHeaders(domain=self.base_domain)

        # get domain
        req = helper.getEndpoint() + '/'
        rsp = requests.get(req, headers=headers)
        rspJson = json.loads(rsp.text)
        self.assertTrue("root" in rspJson)
        root_uuid = rspJson["root"]

        # create the dataset
        num_elements = 10
        req = self.endpoint + "/datasets"
        payload = {'type': 'H5T_STD_I32LE', 'shape': [num_elements,], 'maxdims': [0,]}
        req = self.endpoint + "/datasets"
        rsp = requests.post(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)  # create dataset
        rspJson = json.loads(rsp.text)
        dset_uuid = rspJson['id']
        self.assertTrue(helper.validateId(dset_uuid))

        # link new dataset as 'dset'
        name = 'dset'
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
        payload = {"id": dset_uuid}
        rsp = requests.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # write entire array
        value = list(range(num_elements))
        payload = {'value': value}
        req = self.endpoint + "/datasets/" + dset_uuid + "/value"
        rsp = requests.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 200)  # write value

        # resize the datasets elements
        orig_extent = num_elements
        num_elements *= 2  # double the extent
        req = self.endpoint + "/datasets/" + dset_uuid + "/shape"
        payload = {"shape": [num_elements,]}

        rsp = requests.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)

        # read values from the extended region
        req = self.endpoint + "/datasets/" + dset_uuid + "/value"
        params = {"select": "[{}:{}]".format(0, num_elements)}
        rsp = requests.get(req, params=params, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        data = rspJson["value"]
        self.assertEqual(data[0:orig_extent], list(range(orig_extent)))

        # write to the extended region
        payload = {'value': value, 'start': 10, 'stop': 20}
        req = self.endpoint + "/datasets/" + dset_uuid + "/value"
        rsp = requests.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 200)  # write value

        # read values from the extended region
        req = self.endpoint + "/datasets/" + dset_uuid + "/value"
        params = {"select": "[{}:{}]".format(orig_extent, num_elements)}
        rsp = requests.get(req, params=params, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        data = rspJson["value"]

        self.assertEqual(len(data), num_elements-orig_extent)

        # read all values back
        rsp = requests.get(req, headers=headers)
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
        req = helper.getEndpoint() + '/'
        rsp = requests.get(req, headers=headers)
        rspJson = json.loads(rsp.text)
        self.assertTrue("root" in rspJson)
        root_uuid = rspJson["root"]

        # create the dataset with a 0-sized shape
        req = self.endpoint + "/datasets"
        payload = {'type': 'H5T_STD_I32LE', 'shape': [0,], 'maxdims': [0,]}
        req = self.endpoint + "/datasets"
        rsp = requests.post(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)  # create dataset
        rspJson = json.loads(rsp.text)
        dset_uuid = rspJson['id']
        self.assertTrue(helper.validateId(dset_uuid))

        # link new dataset as 'dset'
        name = 'dset'
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
        payload = {"id": dset_uuid}
        rsp = requests.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # append values [0,10]
        num_elements = 10
        value = list(range(num_elements))
        payload = {'value': value, 'append': num_elements}
        req = self.endpoint + "/datasets/" + dset_uuid + "/value"
        rsp = requests.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 200)  # write value

        # verify the shape in now (10,)
        req = self.endpoint + "/datasets/" + dset_uuid + "/shape"
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("shape" in rspJson)
        shape = rspJson["shape"]
        self.assertTrue("dims" in shape)
        self.assertEqual(shape["dims"], [num_elements,])

        # read values from the extended region
        req = self.endpoint + "/datasets/" + dset_uuid + "/value"
        params = {"select": "[{}:{}]".format(0, num_elements)}
        rsp = requests.get(req, params=params, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        data = rspJson["value"]
        self.assertEqual(data, value)

        # append values [10,20]
        num_elements = 10
        value = list(range(num_elements, num_elements*2))
        payload = {'value': value, 'append': num_elements}
        req = self.endpoint + "/datasets/" + dset_uuid + "/value"
        rsp = requests.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 200)  # write value

         # verify the shape in now (20,)
        req = self.endpoint + "/datasets/" + dset_uuid + "/shape"
        rsp = requests.get(req,  headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("shape" in rspJson)
        shape = rspJson["shape"]
        self.assertTrue("dims" in shape)
        self.assertEqual(shape["dims"], [num_elements*2,])

        # read values from the extended region
        req = self.endpoint + "/datasets/" + dset_uuid + "/value"
        params = {"select": "[{}:{}]".format(0, num_elements*2)}
        rsp = requests.get(req, params=params, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        data = rspJson["value"]
        self.assertEqual(data, list(range(num_elements*2)))

        # test mis-match of append value and data
        value = list(range(num_elements, num_elements*2))
        payload = {'value': value, 'append': num_elements+1}
        req = self.endpoint + "/datasets/" + dset_uuid + "/value"
        rsp = requests.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 400)  # write value

    def testAppend1DBinary(self):
        # test appending to resizable dataset using binary request
        print("testAppend1DBinary", self.base_domain)
        headers = helper.getRequestHeaders(domain=self.base_domain)
        headers_bin_req = helper.getRequestHeaders(domain=self.base_domain)
        headers_bin_req["Content-Type"] = "application/octet-stream"

        # get domain
        req = helper.getEndpoint() + '/'
        rsp = requests.get(req, headers=headers)
        rspJson = json.loads(rsp.text)
        self.assertTrue("root" in rspJson)
        root_uuid = rspJson["root"]

        # create the dataset with a 0-sized shape
        req = self.endpoint + "/datasets"
        payload = {'type': 'H5T_STD_I32LE', 'shape': [0,], 'maxdims': [0,]}
        req = self.endpoint + "/datasets"
        rsp = requests.post(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)  # create dataset
        rspJson = json.loads(rsp.text)
        dset_uuid = rspJson['id']
        self.assertTrue(helper.validateId(dset_uuid))

        # link new dataset as 'dset'
        name = 'dset'
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
        payload = {"id": dset_uuid}
        rsp = requests.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # append values [0,10]
        # write 0-9 as four-byte little-endian integers
        num_elements = 10
        data = bytearray(4*num_elements)
        for i in range(num_elements):
            data[i*4] = i%256
        params = {"append": num_elements}
        req = self.endpoint + "/datasets/" + dset_uuid + "/value"
        rsp = requests.put(req, data=data, params=params, headers=headers_bin_req)
        self.assertEqual(rsp.status_code, 200)

        # verify the shape in now (10,)
        req = self.endpoint + "/datasets/" + dset_uuid + "/shape"
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("shape" in rspJson)
        shape = rspJson["shape"]
        self.assertTrue("dims" in shape)
        self.assertEqual(shape["dims"], [num_elements,])

        # read values from the extended region
        req = self.endpoint + "/datasets/" + dset_uuid + "/value"
        params = {"select": "[{}:{}]".format(0, num_elements)}
        rsp = requests.get(req, params=params, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        read_values = rspJson["value"]
        self.assertEqual(read_values, list(range(num_elements)))

        # append values [10,20]
        num_elements = 10
        data = bytearray(4*num_elements)
        for i in range(num_elements):
            data[i*4] = (i+num_elements)%256
        req = self.endpoint + "/datasets/" + dset_uuid + "/value"
        params = {"append": num_elements}
        rsp = requests.put(req, data=data, params=params, headers=headers_bin_req)
        self.assertEqual(rsp.status_code, 200)  # write value

        # verify the shape in now (20,)
        req = self.endpoint + "/datasets/" + dset_uuid + "/shape"
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("shape" in rspJson)
        shape = rspJson["shape"]
        self.assertTrue("dims" in shape)
        self.assertEqual(shape["dims"], [num_elements*2,])

        # read values from the extended region
        req = self.endpoint + "/datasets/" + dset_uuid + "/value"
        params = {"select": "[{}:{}]".format(0, num_elements*2)}
        rsp = requests.get(req, params=params, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        read_values = rspJson["value"]
        self.assertEqual(read_values, list(range(num_elements*2)))

        # test mis-match of append value and data
        req = self.endpoint + "/datasets/" + dset_uuid + "/value"
        params = {"append": num_elements+1}
        rsp = requests.put(req, data=data, params=params, headers=headers_bin_req)
        self.assertEqual(rsp.status_code, 400)  # write value

    def testAppend2DJson(self):
        # test appending to resizable dataset
        print("testAppend2DJson", self.base_domain)
        headers = helper.getRequestHeaders(domain=self.base_domain)

        # get domain
        req = helper.getEndpoint() + '/'
        rsp = requests.get(req, headers=headers)
        rspJson = json.loads(rsp.text)
        self.assertTrue("root" in rspJson)
        root_uuid = rspJson["root"]

        # create the dataset with a 0-sized shape
        req = self.endpoint + "/datasets"
        payload = {'type': 'H5T_STD_I32LE', 'shape': [0,0], 'maxdims': [0,0]}
        req = self.endpoint + "/datasets"
        rsp = requests.post(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)  # create dataset
        rspJson = json.loads(rsp.text)
        dset_uuid = rspJson['id']
        self.assertTrue(helper.validateId(dset_uuid))

        # link new dataset as 'dset'
        name = 'dset'
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
        payload = {"id": dset_uuid}
        rsp = requests.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # append values [0,10]
        num_elements = 10
        value = list(range(num_elements))
        payload = {'value': value, 'append': num_elements, 'append_dim': 1}
        req = self.endpoint + "/datasets/" + dset_uuid + "/value"
        rsp = requests.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 200)  # write value

        # verify the shape in now (1, 10)
        req = self.endpoint + "/datasets/" + dset_uuid + "/shape"
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("shape" in rspJson)
        shape = rspJson["shape"]
        self.assertTrue("dims" in shape)
        self.assertEqual(shape["dims"], [1, num_elements])

        # read values from the extended region
        req = self.endpoint + "/datasets/" + dset_uuid + "/value"
        params = {"select": "[0:1,{}:{}]".format(0, num_elements)}
        rsp = requests.get(req, params=params, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        data = rspJson["value"]
        self.assertEqual(data, [value,])

        # append values [10,20]
        num_elements = 10
        value = list(range(num_elements, num_elements*2))
        payload = {'value': value, 'append': num_elements, 'append_dim': 1}
        req = self.endpoint + "/datasets/" + dset_uuid + "/value"
        rsp = requests.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 200)  # write value

         # verify the shape in now (1, 20)
        req = self.endpoint + "/datasets/" + dset_uuid + "/shape"
        rsp = requests.get(req,  headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("shape" in rspJson)
        shape = rspJson["shape"]
        self.assertTrue("dims" in shape)
        self.assertEqual(shape["dims"], [1, num_elements*2])

        # read values from the extended region
        req = self.endpoint + "/datasets/" + dset_uuid + "/value"
        params = {"select": "[0:1,{}:{}]".format(0, num_elements*2)}
        rsp = requests.get(req, params=params, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        data = rspJson["value"]
        self.assertEqual(data, [list(range(num_elements*2)),])

        # append one row (20 elements) in the other dimension
        num_elements = 20
        value = list(range(num_elements))
        payload = {'value': value, 'append': 1, 'append_dim': 0}
        req = self.endpoint + "/datasets/" + dset_uuid + "/value"
        rsp = requests.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 200)  # write value

         # verify the shape in now (2, 20,)
        req = self.endpoint + "/datasets/" + dset_uuid + "/shape"
        rsp = requests.get(req,  headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("shape" in rspJson)
        shape = rspJson["shape"]
        self.assertTrue("dims" in shape)
        self.assertEqual(shape["dims"], [2, num_elements])

        # read all values from the dataset
        req = self.endpoint + "/datasets/" + dset_uuid + "/value"
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        data = rspJson["value"]
        self.assertEqual(len(data), 2)
        self.assertEqual(data[0], list(range(num_elements)))
        self.assertEqual(data[1], list(range(num_elements)))


        # test mis-match of append value and data
        value = list(range(num_elements, num_elements*2))
        payload = {'value': value, 'append': num_elements+1}
        req = self.endpoint + "/datasets/" + dset_uuid + "/value"
        rsp = requests.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 400)  # write value


    def testDeflateCompression(self):
        # test Dataset with creation property list
        print("testDeflateCompression", self.base_domain)
        headers = helper.getRequestHeaders(domain=self.base_domain)
        # get domain
        req = helper.getEndpoint() + '/'
        rsp = requests.get(req, headers=headers)
        rspJson = json.loads(rsp.text)
        self.assertTrue("root" in rspJson)
        root_uuid = rspJson["root"]

        # create the dataset
        req = self.endpoint + "/datasets"

        # Create ~1MB dataset

        payload = {'type': 'H5T_STD_I8LE', 'shape': [1024, 1024]}
        # define deflate compression
        gzip_filter = {'class': 'H5Z_FILTER_DEFLATE', 'id': 1, 'level': 9, 'name': 'deflate'}
        payload['creationProperties'] = {'filters': [gzip_filter,] }
        req = self.endpoint + "/datasets"
        rsp = requests.post(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)  # create dataset
        rspJson = json.loads(rsp.text)
        dset_uuid = rspJson['id']
        self.assertTrue(helper.validateId(dset_uuid))

        # link new dataset as 'dset'
        name = 'dset'
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
        payload = {"id": dset_uuid}
        rsp = requests.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # write a horizontal strip of 22s
        req = self.endpoint + "/datasets/" + dset_uuid + "/value"
        data = [22,] * 1024
        payload = { 'start': [512, 0], 'stop': [513, 1024], 'value': data }
        rsp = requests.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 200)

        # read back the 512,512 element
        req = self.endpoint + "/datasets/" + dset_uuid + "/value"  # test
        params = {"select": "[512:513,512:513]"} # read  1 element
        rsp = requests.get(req, params=params, headers=headers)
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
        req = helper.getEndpoint() + '/'
        rsp = requests.get(req, headers=headers)
        rspJson = json.loads(rsp.text)
        self.assertTrue("root" in rspJson)
        root_uuid = rspJson["root"]

        # create the dataset
        req = self.endpoint + "/datasets"

        # Create ~4MB dataset

        payload = {'type': 'H5T_STD_I32LE', 'shape': [1024, 1024]}
        # define sshufle compression
        shuffle_filter = {'class': 'H5Z_FILTER_SHUFFLE', 'id': 2, 'name': 'shuffle'}
        payload['creationProperties'] = {'filters': [shuffle_filter,] }
        req = self.endpoint + "/datasets"
        rsp = requests.post(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)  # create dataset
        rspJson = json.loads(rsp.text)
        dset_uuid = rspJson['id']
        self.assertTrue(helper.validateId(dset_uuid))

        # link new dataset as 'dset'
        name = 'dset'
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
        payload = {"id": dset_uuid}
        rsp = requests.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # write a horizontal strip of 22s
        req = self.endpoint + "/datasets/" + dset_uuid + "/value"
        data = [22,] * 1024
        payload = { 'start': [512, 0], 'stop': [513, 1024], 'value': data }
        rsp = requests.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 200)

        # read back the 512,512 element
        req = self.endpoint + "/datasets/" + dset_uuid + "/value"  # test
        params = {"select": "[512:513,512:513]"} # read  1 element
        rsp = requests.get(req, params=params, headers=headers)
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
        req = helper.getEndpoint() + '/'
        rsp = requests.get(req, headers=headers)
        rspJson = json.loads(rsp.text)
        self.assertTrue("root" in rspJson)
        root_uuid = rspJson["root"]

        # create the dataset
        req = self.endpoint + "/datasets"

        # Create ~1MB dataset

        payload = {'type': 'H5T_STD_I32LE', 'shape': [1024, 1024]}
        # define deflate compression
        gzip_filter = {'class': 'H5Z_FILTER_DEFLATE', 'id': 1, 'level': 9, 'name': 'deflate'}
        # and shuffle compression
        shuffle_filter = {'class': 'H5Z_FILTER_SHUFFLE', 'id': 2, 'name': 'shuffle'}
        payload['creationProperties'] = {'filters': [shuffle_filter, gzip_filter] }
        req = self.endpoint + "/datasets"
        rsp = requests.post(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)  # create dataset
        rspJson = json.loads(rsp.text)
        dset_uuid = rspJson['id']
        self.assertTrue(helper.validateId(dset_uuid))

        # link new dataset as 'dset'
        name = 'dset'
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
        payload = {"id": dset_uuid}
        rsp = requests.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # write a horizontal strip of 22s
        req = self.endpoint + "/datasets/" + dset_uuid + "/value"
        data = [22,] * 1024
        payload = { 'start': [512, 0], 'stop': [513, 1024], 'value': data }
        rsp = requests.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 200)

        # read back the 512,512 element
        req = self.endpoint + "/datasets/" + dset_uuid + "/value"  # test
        params = {"select": "[512:513,512:513]"} # read  1 element
        rsp = requests.get(req, params=params, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        value = rspJson["value"]
        self.assertEqual(len(value), 1)
        row = value[0]
        self.assertEqual(len(row), 1)
        self.assertEqual(row[0], 22)

    def testContiguousRefDataset(self):
        print("testConfiguousRefDataset", self.base_domain)
        headers = helper.getRequestHeaders(domain=self.base_domain)

        hdf5_sample_bucket = config.get("hdf5_sample_bucket")
        if not hdf5_sample_bucket:
            print("hdf5_sample_bucket config not set, skipping testContiguousRefDataset")
            return


        tall_json = helper.getHDF5JSON("tall.json")
        if not tall_json:
            print("tall.json file not found, skipping testContiguousRefDataset")
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
        req = helper.getEndpoint() + '/'
        rsp = requests.get(req, headers=headers)
        rspJson = json.loads(rsp.text)
        self.assertTrue("root" in rspJson)
        root_uuid = rspJson["root"]

        # create dataset fodr /g1/g1.1/dset1.1.2
        s3path = "s3://" + hdf5_sample_bucket + "/data/hdf5test" + "/tall.h5"
        data = { "type": 'H5T_STD_I32BE', "shape": 20 }
        layout = {"class": 'H5D_CONTIGUOUS_REF', "file_uri": s3path, "offset": dset112_offset, "size": dset112_size }
        data['creationProperties'] = {'layout': layout}

        req = self.endpoint + '/datasets'
        rsp = requests.post(req, data=json.dumps(data), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)
        dset112_id = rspJson["id"]
        self.assertTrue(helper.validateId(dset112_id))

        # link new dataset as 'dset112'
        name = "dset112"
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
        payload = {"id": dset112_id}
        rsp = requests.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # create dataset for /g2/dset2.2
        data = { "type": 'H5T_IEEE_F32BE', "shape": [3, 5] }
        layout = {"class": 'H5D_CONTIGUOUS_REF', "file_uri": s3path, "offset": dset22_offset, "size": dset22_size }
        data['creationProperties'] = {'layout': layout}

        req = self.endpoint + '/datasets'
        rsp = requests.post(req, data=json.dumps(data), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)
        dset22_id = rspJson["id"]
        self.assertTrue(helper.validateId(dset22_id))

        # link new dataset as 'dset22'
        name = "dset22"
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
        payload = {"id": dset22_id}
        rsp = requests.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)


        # read values from dset112 (should be the sequence 0 through 19)
        req = self.endpoint + "/datasets/" + dset112_id + "/value"
        rsp = requests.get(req, headers=headers)

        if rsp.status_code == 404:
            print("s3object: {} not found, skipping hyperslab read chunk contiguous reference test".format(s3path))
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
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        value = rspJson["value"]
        self.assertEqual(len(value), 3)
        for i in range(3):
            self.assertEqual(len(value[i]), 5)

    def testGetSelectionChunkedRefDataset(self):
        print("testGetSelectionChunkedRefDataset", self.base_domain)
        headers = helper.getRequestHeaders(domain=self.base_domain)

        hdf5_sample_bucket = config.get("hdf5_sample_bucket")

        if not hdf5_sample_bucket:
            print("hdf5_sample_bucket config not set, skipping testChunkedRefDataset")
            return

        s3path = "s3://" + hdf5_sample_bucket + "/data/hdf5test" + "/snp500.h5"
        SNP500_ROWS = 3207353

        snp500_json = helper.getHDF5JSON("snp500.json")
        if not snp500_json:
            print("snp500.json file not found, skipping testChunkedRefDataset")
            return

        if "snp500.h5" not in snp500_json:
            self.assertTrue(False)

        chunk_dims = [60000,]  # chunk layout used in snp500.h5 file

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
        req = helper.getEndpoint() + '/'
        rsp = requests.get(req, headers=headers)
        rspJson = json.loads(rsp.text)
        self.assertTrue("root" in rspJson)
        root_uuid = rspJson["root"]

        # define types we need

        s10_type = {"charSet": "H5T_CSET_ASCII",
                "class": "H5T_STRING",
                "length": 10,
                "strPad": "H5T_STR_NULLPAD" }
        s4_type = {"charSet": "H5T_CSET_ASCII",
                "class": "H5T_STRING",
                "length": 4,
                "strPad": "H5T_STR_NULLPAD" }

        fields = ({'name': 'date', 'type': s10_type},
                  {'name': 'symbol', 'type': s4_type},
                  {'name': 'sector', 'type': 'H5T_STD_I8LE'},
                  {'name': 'open', 'type': 'H5T_IEEE_F32LE'},
                  {'name': 'high', 'type': 'H5T_IEEE_F32LE'},
                  {'name': 'low', 'type': 'H5T_IEEE_F32LE'},
                  {'name': 'volume', 'type': 'H5T_IEEE_F32LE'},
                  {'name': 'close', 'type': 'H5T_IEEE_F32LE'})


        datatype = {'class': 'H5T_COMPOUND', 'fields': fields }

        data = { "type": datatype, "shape": [SNP500_ROWS,] }
        layout = {"class": 'H5D_CHUNKED_REF', "file_uri": s3path, "dims": chunk_dims, "chunks": chunks }
        data['creationProperties'] = {'layout': layout}

        req = self.endpoint + '/datasets'
        rsp = requests.post(req, data=json.dumps(data), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)
        dset_id = rspJson["id"]
        self.assertTrue(helper.validateId(dset_id))

        # link new dataset as 'dset'
        name = "dset"
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
        payload = {"id": dset_id}
        rsp = requests.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # read a selection
        req = self.endpoint + "/datasets/" + dset_id + "/value"
        params = {"select": "[1234567:1234568]"} # read 1 element, starting at index 1234567
        params["nonstrict"] = 1  # allow use of aws lambda if configured
        rsp = requests.get(req, params=params, headers=headers)
        if rsp.status_code == 404:
            print("s3object: {} not found, skipping hyperslab read chunk reference test".format(s3path))
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
        self.assertEqual(item[0], '1998.10.22')
        self.assertEqual(item[1], 'MHFI')
        self.assertEqual(item[2], 3)
        # skip check rest of fields since float comparisons are trcky...



    def testChunkedRefIndirectDataset(self):
        print("testChunkedRefIndirectDataset", self.base_domain)
        headers = helper.getRequestHeaders(domain=self.base_domain)

        hdf5_sample_bucket = config.get("hdf5_sample_bucket")
        if not hdf5_sample_bucket:
            print("hdf5_sample_bucket config not set, skipping testChunkedRefIndirectDataset")
            return

        s3path = "s3://" + hdf5_sample_bucket + "/data/hdf5test" + "/snp500.h5"
        SNP500_ROWS = 3207353

        snp500_json = helper.getHDF5JSON("snp500.json")
        if not snp500_json:
            print("snp500.json file not found, skipping testChunkedRefDataset")
            return

        if "snp500.h5" not in snp500_json:
            self.assertTrue(False)

        chunk_dims = [60000,]  # chunk layout used in snp500.h5 file
        num_chunks = (SNP500_ROWS // chunk_dims[0]) + 1

        chunk_info = snp500_json["snp500.h5"]
        dset_info = chunk_info["/dset"]
        if "byteStreams" not in dset_info:
            self.assertTrue(False)
        byteStreams = dset_info["byteStreams"]

        self.assertEqual(len(byteStreams), num_chunks)

        chunkinfo_data = [(0,0)]*num_chunks

        # fill the numpy array with info from bytestreams data
        for i in range(num_chunks):
            item = byteStreams[i]
            index = item["index"]
            chunkinfo_data[index] = (item["file_offset"], item["size"])

        # get domain
        req = helper.getEndpoint() + '/'
        rsp = requests.get(req, headers=headers)
        rspJson = json.loads(rsp.text)
        self.assertTrue("root" in rspJson)
        root_uuid = rspJson["root"]

        # create table to hold chunkinfo
        # create a dataset to store chunk info
        fields = ({'name': 'offset', 'type': 'H5T_STD_I64LE'},
                  {'name': 'size', 'type': 'H5T_STD_I32LE'})
        chunkinfo_type = {'class': 'H5T_COMPOUND', 'fields': fields }
        req = self.endpoint + "/datasets"
        # Store 40 chunk locations
        chunkinfo_dims = [num_chunks,]
        payload = {'type': chunkinfo_type, 'shape': chunkinfo_dims }
        req = self.endpoint + "/datasets"
        rsp = requests.post(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)  # create dataset
        rspJson = json.loads(rsp.text)
        chunkinfo_uuid = rspJson['id']
        self.assertTrue(helper.validateId(chunkinfo_uuid))

        # link new dataset as 'chunks'
        name = "chunks"
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
        payload = {"id": chunkinfo_uuid}
        rsp = requests.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # write to the chunkinfo dataset
        payload = {'value': chunkinfo_data}

        req = self.endpoint + "/datasets/" + chunkinfo_uuid + "/value"
        rsp = requests.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 200)  # write value


        # define types we need

        s10_type = {"charSet": "H5T_CSET_ASCII",
                "class": "H5T_STRING",
                "length": 10,
                "strPad": "H5T_STR_NULLPAD" }
        s4_type = {"charSet": "H5T_CSET_ASCII",
                "class": "H5T_STRING",
                "length": 4,
                "strPad": "H5T_STR_NULLPAD" }

        fields = ({'name': 'date', 'type': s10_type},
                  {'name': 'symbol', 'type': s4_type},
                  {'name': 'sector', 'type': 'H5T_STD_I8LE'},
                  {'name': 'open', 'type': 'H5T_IEEE_F32LE'},
                  {'name': 'high', 'type': 'H5T_IEEE_F32LE'},
                  {'name': 'low', 'type': 'H5T_IEEE_F32LE'},
                  {'name': 'volume', 'type': 'H5T_IEEE_F32LE'},
                  {'name': 'close', 'type': 'H5T_IEEE_F32LE'})


        datatype = {'class': 'H5T_COMPOUND', 'fields': fields }

        data = { "type": datatype, "shape": [SNP500_ROWS,] }
        layout = {"class": 'H5D_CHUNKED_REF_INDIRECT', "file_uri": s3path, "dims": chunk_dims, "chunk_table": chunkinfo_uuid}
        data['creationProperties'] = {'layout': layout}

        req = self.endpoint + '/datasets'
        rsp = requests.post(req, data=json.dumps(data), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)
        dset_id = rspJson["id"]
        self.assertTrue(helper.validateId(dset_id))

        # link new dataset as 'dset'
        name = "dset"
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
        payload = {"id": dset_id}
        rsp = requests.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # read a selection
        req = self.endpoint + "/datasets/" + dset_id + "/value"
        params = {"select": "[1234567:1234568]"} # read 1 element, starting at index 1234567
        params["nonstrict"] = 1 # enable SN to invoke lambda func
        rsp = requests.get(req, params=params, headers=headers)

        if rsp.status_code == 404:
            print("s3object: {} not found, skipping hyperslab read chunk reference indirect test".format(s3path))
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
        self.assertEqual(item[0], '1998.10.22')
        self.assertEqual(item[1], 'MHFI')
        self.assertEqual(item[2], 3)
        # skip check rest of fields since float comparisons are trcky...

    def testChunkedRefIndirectS3UriDataset(self):
        print("testChunkedRefIndirectS3UriDataset", self.base_domain)
        headers = helper.getRequestHeaders(domain=self.base_domain)

        hdf5_sample_bucket = config.get("hdf5_sample_bucket")
        if not hdf5_sample_bucket:
            print("hdf5_sample_bucket config not set, skipping testChunkedRefIndirectDataset")
            return

        s3path = "s3://" + hdf5_sample_bucket + "/data/hdf5test" + "/snp500.h5"
        SNP500_ROWS = 3207353

        snp500_json = helper.getHDF5JSON("snp500.json")
        if not snp500_json:
            print("snp500.json file not found, skipping testChunkedRefDataset")
            return

        if "snp500.h5" not in snp500_json:
            self.assertTrue(False)

        chunk_dims = [60000,]  # chunk layout used in snp500.h5 file
        num_chunks = (SNP500_ROWS // chunk_dims[0]) + 1

        chunk_info = snp500_json["snp500.h5"]
        dset_info = chunk_info["/dset"]
        if "byteStreams" not in dset_info:
            self.assertTrue(False)
        byteStreams = dset_info["byteStreams"]

        self.assertEqual(len(byteStreams), num_chunks)

        chunkinfo_data = [(0,0)]*num_chunks

        # fill the numpy array with info from bytestreams data
        for i in range(num_chunks):
            item = byteStreams[i]
            index = item["index"]
            chunkinfo_data[index] = (item["file_offset"], item["size"], s3path)

        # get domain
        req = helper.getEndpoint() + '/'
        rsp = requests.get(req, headers=headers)
        rspJson = json.loads(rsp.text)
        self.assertTrue("root" in rspJson)
        root_uuid = rspJson["root"]

        # create table to hold chunkinfo
        # create a dataset to store chunk info
        max_s3_uri_len = 40
        fixed_str_type = {"charSet": "H5T_CSET_ASCII",
                "class": "H5T_STRING",
                "length": max_s3_uri_len,
                "strPad": "H5T_STR_NULLPAD" }
        fields = ({'name': 'offset', 'type': 'H5T_STD_I64LE'},
                  {'name': 'size', 'type': 'H5T_STD_I32LE'},
                  {'name': 'file_uri', 'type': fixed_str_type})
        chunkinfo_type = {'class': 'H5T_COMPOUND', 'fields': fields }
        req = self.endpoint + "/datasets"
        # Store 40 chunk locations
        chunkinfo_dims = [num_chunks,]
        payload = {'type': chunkinfo_type, 'shape': chunkinfo_dims }
        req = self.endpoint + "/datasets"
        rsp = requests.post(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)  # create dataset
        rspJson = json.loads(rsp.text)
        chunkinfo_uuid = rspJson['id']
        self.assertTrue(helper.validateId(chunkinfo_uuid))

        # link new dataset as 'chunks'
        name = "chunks"
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
        payload = {"id": chunkinfo_uuid}
        rsp = requests.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # write to the chunkinfo dataset
        payload = {'value': chunkinfo_data}

        req = self.endpoint + "/datasets/" + chunkinfo_uuid + "/value"
        rsp = requests.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 200)  # write value


        # define types we need

        s10_type = {"charSet": "H5T_CSET_ASCII",
                "class": "H5T_STRING",
                "length": 10,
                "strPad": "H5T_STR_NULLPAD" }
        s4_type = {"charSet": "H5T_CSET_ASCII",
                "class": "H5T_STRING",
                "length": 4,
                "strPad": "H5T_STR_NULLPAD" }

        fields = ({'name': 'date', 'type': s10_type},
                  {'name': 'symbol', 'type': s4_type},
                  {'name': 'sector', 'type': 'H5T_STD_I8LE'},
                  {'name': 'open', 'type': 'H5T_IEEE_F32LE'},
                  {'name': 'high', 'type': 'H5T_IEEE_F32LE'},
                  {'name': 'low', 'type': 'H5T_IEEE_F32LE'},
                  {'name': 'volume', 'type': 'H5T_IEEE_F32LE'},
                  {'name': 'close', 'type': 'H5T_IEEE_F32LE'})


        datatype = {'class': 'H5T_COMPOUND', 'fields': fields }

        data = { "type": datatype, "shape": [SNP500_ROWS,] }
        # don't provide s3path here, it will get picked up from the chunkinfo dataset
        layout = {"class": 'H5D_CHUNKED_REF_INDIRECT', "dims": chunk_dims, "chunk_table": chunkinfo_uuid}
        data['creationProperties'] = {'layout': layout}

        req = self.endpoint + '/datasets'
        rsp = requests.post(req, data=json.dumps(data), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)
        dset_id = rspJson["id"]
        self.assertTrue(helper.validateId(dset_id))

        # link new dataset as 'dset'
        name = "dset"
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
        payload = {"id": dset_id}
        rsp = requests.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # read a selection
        req = self.endpoint + "/datasets/" + dset_id + "/value"
        params = {"select": "[1234567:1234568]"} # read 1 element, starting at index 1234567
        params["nonstrict"] = 1 # enable SN to invoke lambda func
        rsp = requests.get(req, params=params, headers=headers)

        if rsp.status_code == 404:
            print("s3object: {} not found, skipping hyperslab read chunk reference indirect test".format(s3path))
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
        self.assertEqual(item[0], '1998.10.22')
        self.assertEqual(item[1], 'MHFI')
        self.assertEqual(item[2], 3)
        # skip check rest of fields since float comparisons are trcky...

    def testLargeCreationProperties(self):
        # test Dataset with artifically large creation_properties data
        print("testLargeCreationProperties", self.base_domain)
        headers = helper.getRequestHeaders(domain=self.base_domain)

        # get domain
        req = helper.getEndpoint() + '/'
        rsp = requests.get(req, headers=headers)
        rspJson = json.loads(rsp.text)
        self.assertTrue("root" in rspJson)
        root_uuid = rspJson["root"]

        # create the dataset
        req = self.endpoint + "/datasets"
        payload = {'type': 'H5T_STD_I32LE', 'shape': 10}
        creation_props = {'fillValue': 42 }
        foo_bar = {}
        for i in range(500):
            foo_bar[i] = f"this is a test {i}"
        creation_props['foo_bar'] = foo_bar

        payload['creationProperties'] = creation_props

        req = self.endpoint + "/datasets"
        rsp = requests.post(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)  # create dataset
        
        rspJson = json.loads(rsp.text)
        dset_uuid = rspJson['id']
        self.assertTrue(helper.validateId(dset_uuid))

        # link new dataset as 'dset'
        name = 'dset'
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
        payload = {"id": dset_uuid}
        rsp = requests.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # read back the data
        req = self.endpoint + "/datasets/" + dset_uuid + "/value"
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        self.assertEqual(rspJson["value"], [42,]*10)
        

        # write some values
        payload = { 'start': 0, 'stop': 5, 'value': [24,]*5 }
        rsp = requests.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 200)

        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        ret_values = rspJson["value"]
        for i in range(5):
            self.assertEqual(ret_values[i], 24)
            self.assertEqual(ret_values[i+5], 42)
        


if __name__ == '__main__':
    #setup test files

    unittest.main()
