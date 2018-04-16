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
import json
import requests
import unittest
import config
import helper

class ValueTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(ValueTest, self).__init__(*args, **kwargs)
        self.base_domain = helper.getTestDomainName(self.__class__.__name__)
        helper.setupDomain(self.base_domain)
        self.endpoint = helper.getEndpoint()
        self.headers = helper.getRequestHeaders(domain=self.base_domain)

    def testRank1Dataset(self):
        dset_size = 10

        # create dataset
        dset_payload = {
            "type": "H5T_STD_I32LE",
            "shape": [dset_size],
        }
        dset_id = helper.postDataset(self.base_domain, dset_payload)

        # read values from dset (should be zeros)
        data = [0 for _ in range(dset_size)]
        req = f"{self.endpoint}/datasets/{dset_id}/value"
        rsp = requests.get(req, headers=self.headers)
        self.assertEqual(rsp.status_code, 200, "problem reading initial data")
        rspJson = rsp.json()
        self.assertEqual(rspJson["value"], data)

        # write to the dset
        data = list(range(dset_size)) # [0..9]
        payload = { "value": data }
        rsp = requests.put(req, data=json.dumps(payload), headers=self.headers)
        self.assertEqual(rsp.status_code, 200, "problem writing data")

        # read back the data
        rsp = requests.get(req, headers=self.headers)
        self.assertEqual(rsp.status_code, 200, "problem getting data")
        rspJson = rsp.json()
        self.assertEqual(rspJson["value"], data)

        # read a selection
        params = {"select": "[2:8]"} # read 6 elements, starting at index 2
        rsp = requests.get(req, params=params, headers=self.headers)
        self.assertEqual(rsp.status_code, 200, "problem getting selection")
        rspJson = rsp.json()
        self.assertEqual(rspJson["value"], [2,3,4,5,6,7])

        # try to read beyond the bounds of the array
        params = {"select": "[2:18]"}
        rsp = requests.get(req, params=params, headers=self.headers)
        self.assertEqual(rsp.status_code, 400, "read past extent should fail")

    def testRank1DatasetBinary(self):
        array_len = 10
        aos = "application/octet-stream" # for convenience
        headers = helper.getRequestHeaders(domain=self.base_domain)
        headers_bin_req = helper.getRequestHeaders(domain=self.base_domain) 
        headers_bin_req["Content-Type"] = aos
        headers_bin_rsp = helper.getRequestHeaders(domain=self.base_domain)
        headers_bin_rsp["accept"] = aos

        # create dataset
        dset_payload = {
            "type": "H5T_STD_I32LE",
            "shape": array_len,
        }
        dset_id = helper.postDataset(self.base_domain, dset_payload)

        # read values from dset (should be zeros)
        req = f"{self.endpoint}/datasets/{dset_id}/value" 
        rsp = requests.get(req, headers=headers_bin_rsp)
        self.assertEqual(rsp.status_code, 200)
        self.assertEqual(rsp.headers['Content-Type'], aos)
        data = rsp.content
        self.assertEqual(len(data), 4 * array_len)
        for i in range(10):
            offset = i*4
            self.assertEqual(data[offset+0], 0)
            self.assertEqual(data[offset+1], 0)
            self.assertEqual(data[offset+2], 0)
            self.assertEqual(data[offset+3], 0)

        # write 0-9 as four-byte little-endian integers
        data = bytearray(4 * array_len)
        for i in range(array_len):
            data[i*4] = i
        rsp = requests.put(req, data=data, headers=headers_bin_req)
        self.assertEqual(rsp.status_code, 200)

        # read back the data
        rsp = requests.get(req, headers=headers_bin_rsp)
        self.assertEqual(rsp.status_code, 200)
        data = rsp.content
        self.assertEqual(len(data), 4 * array_len)
        for i in range(array_len):
            offset = i*4
            self.assertEqual(data[offset+0], i)
            self.assertEqual(data[offset+1], 0)
            self.assertEqual(data[offset+2], 0)
            self.assertEqual(data[offset+3], 0)

        # write a selection
        params = {"select": "[4:6]"}  #4th and 5th elements
        data = bytearray(4*2)
        for i in range(2):
            data[i*4] = 255
        rsp = requests.put(
            req,
            data=data,
            params=params,
            headers=headers_bin_req)
        self.assertEqual(rsp.status_code, 200)

        # read a selection
        params = {"select": "[0:6]"} # read first 6 elements 
        rsp = requests.get(req, params=params, headers=headers_bin_rsp)
        self.assertEqual(rsp.status_code, 200)
        data = rsp.content
        self.assertEqual(len(data), 4*6)
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

    def testRank2Dataset(self):
        headers = helper.getRequestHeaders(domain=self.base_domain)

        # create dataset
        num_col = 8
        num_row = 4
        data = { "type": "H5T_STD_I32LE", "shape": [num_row,num_col] }
        dset_id = helper.postDataset(self.base_domain, data)

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
        headers = helper.getRequestHeaders(domain=self.base_domain)
        headers_bin_req = helper.getRequestHeaders(domain=self.base_domain) 
        headers_bin_req["Content-Type"] = "application/octet-stream"
        headers_bin_rsp = helper.getRequestHeaders(domain=self.base_domain)
        headers_bin_rsp["accept"] = "application/octet-stream"

        # create dataset
        num_col = 8
        num_row = 4
        data = { "type": "H5T_STD_I32LE", "shape": [num_row,num_col] }
        dset_id = helper.postDataset(self.base_domain, data)

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
        headers = helper.getRequestHeaders(domain=self.base_domain)

        # create dataset
        dset_body = { "type": "H5T_STD_I32LE", "shape": 10 }
        dset_id = helper.postDataset(self.base_domain, dset_body)

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
        headers = helper.getRequestHeaders(domain=self.base_domain)

        # create dataset
        # pass in layout specification so that we can test selection across
        # chunk boundries
        payload = {
            "type": "H5T_STD_I32LE",
            "shape": [45,54],
            "creationProperties": {
                "layout": {
                    "class": "H5D_CHUNKED",
                    "dims": [10, 10],
                 }
            }
        }
        dset_id = helper.postDataset(self.base_domain, payload)

        # write a horizontal strip of 22s
        req = self.endpoint + "/datasets/" + dset_id + "/value" 
        data = [22,] * 50
        payload = { 'start': [22, 2], 'stop': [23, 52], 'value': data }
        rsp = requests.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 200)

        # read back a vertical strip that crossed the horizontal strip
        req = self.endpoint + "/datasets/" + dset_id + "/value"  # test
        # read 6 elements, starting at index 20
        params = {"select": "[20:25,21:22]"}
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
        payload = {
            'start': [10, 20],
            'stop': [20, 32],
            'step': [2, 3],
            'value': data,
        }
        rsp = requests.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 200)

        # read back a sub-block
        req = self.endpoint + "/datasets/" + dset_id + "/value"  # test
        # read 6 elements, starting at index (12,14)
        params = {"select": "[12:13,23:26]"}
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
        headers = helper.getRequestHeaders(domain=self.base_domain)

        # create dataset
        fixed_str_type = {"charSet": "H5T_CSET_ASCII", 
                "class": "H5T_STRING", 
                "length": 7, 
                "strPad": "H5T_STR_NULLPAD" }
        data = { "type": fixed_str_type, "shape": 4 }
        dset_id = helper.postDataset(self.base_domain, data)

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
        headers = helper.getRequestHeaders(domain=self.base_domain)
        headers_bin_req = helper.getRequestHeaders(domain=self.base_domain) 
        headers_bin_req["Content-Type"] = "application/octet-stream"

        STR_LENGTH = 7
        STR_COUNT = 4

        # create dataset
        fixed_str_type = {"charSet": "H5T_CSET_ASCII", 
                "class": "H5T_STRING", 
                "length": STR_LENGTH, 
                "strPad": "H5T_STR_NULLPAD" }
        data = { "type": fixed_str_type, "shape": STR_COUNT }
        dset_id = helper.postDataset(self.base_domain, data)

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
        headers = helper.getRequestHeaders(domain=self.base_domain)

        # create dataset
        fixed_str_type = {"charSet": "H5T_CSET_ASCII", 
                "class": "H5T_STRING", 
                "length": 7, 
                "strPad": "H5T_STR_NULLPAD" }
        data = { "type": fixed_str_type, "shape": 4 }
        dset_id = helper.postDataset(self.base_domain, data)

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
        self.assertEqual(values[2], "1234567", "clip last character")
        self.assertEqual(values[2], "1234567", "clip last two characters")

    def testPutScalarDataset(self):
        headers = helper.getRequestHeaders(domain=self.base_domain)

        # create a dataset obj
        str_type = { 'charSet':   'H5T_CSET_ASCII', 
                     'class':  'H5T_STRING', 
                     'strPad': 'H5T_STR_NULLPAD', 
                     'length': 40}
        data = { "type": str_type}
        dset_id = helper.postDataset(self.base_domain, data)

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
        headers = helper.getRequestHeaders(domain=self.base_domain)

        # create a dataset obj
        str_type = {
            "charSet": "H5T_CSET_ASCII", 
            "class": "H5T_STRING", 
            "strPad": "H5T_STR_NULLPAD", 
            "length": 40,
        }
        data = { "type": str_type, "shape": "H5S_NULL"}
        dset_id = helper.postDataset(self.base_domain, data)

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

        fields = ({'name': 'temp', 'type': 'H5T_STD_I32LE'}, 
                    {'name': 'pressure', 'type': 'H5T_IEEE_F32LE'}) 
        datatype = {'class': 'H5T_COMPOUND', 'fields': fields }

        # create compound scalar dataset
        payload = {'type': datatype}
        dset0d_uuid = helper.postDataset(self.base_domain, payload)

        # verify the shape of the dataset
        req = self.endpoint + "/datasets/" + dset0d_uuid
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)  # get dataset
        rspJson = json.loads(rsp.text)
        shape = rspJson["shape"]
        self.assertEqual(shape["class"], 'H5S_SCALAR')

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

        # create 1d dataset
        num_elements = 10
        payload = {'type': datatype, 'shape': num_elements}
        dset1d_uuid = helper.postDataset(self.base_domain, payload)

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

        # create 2d dataset
        dims = [2,2]
        payload = {'type': datatype, 'shape': dims}
        dset2d_uuid = helper.postDataset(self.base_domain, payload)

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
        headers = helper.getRequestHeaders(domain=self.base_domain)

        # create the dataset 
        req = self.endpoint + "/datasets"
        payload = {'type': 'H5T_STD_I32LE', 'shape': 10}
        payload['creationProperties'] = {'fillValue': 42 }
        dset_uuid = helper.postDataset(self.base_domain, payload)

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

        # create compound dataset
        # ASCII 8-char fixed width
        str_type = {
            "charSet":   "H5T_CSET_ASCII", 
            "class":  "H5T_STRING", 
            "strPad": "H5T_STR_NULLPAD", 
            "length": 8,
        }
        fields = (
            {"name": "tag", "type": str_type}, 
            {"name": "value", "type": "H5T_STD_I32LE"}
        ) 
        datatype = {"class": "H5T_COMPOUND", "fields": fields }
        fill_value = ["blank", -999]
        creationProperties =  {"fillValue": fill_value }
        payload = {
            "type": datatype,
            "shape": 40,
            "creationProperties": creationProperties
        }
        dset_uuid = helper.postDataset(self.base_domain, payload)

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
        headers = helper.getRequestHeaders(domain=self.base_domain)
        item_length = 1000

        # create the dataset 
        # ASCII fixed width
        str_type = {
            "charSet": "H5T_CSET_ASCII", 
            "class": "H5T_STRING", 
            "strPad": "H5T_STR_NULLPAD", 
            "length": item_length,
        }
        fill_value = 'X' * item_length
        payload = {
            "type": str_type,
            "shape": [10],
             "creationProperties": {"fillValue": fill_value},
        }
        dset_uuid = helper.postDataset(self.base_domain, payload)

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
        headers = helper.getRequestHeaders(domain=self.base_domain)
        root_uuid = helper.getRootUUID(self.base_domain)

        # create new group  
        g1_uuid = helper.postGroup(self.base_domain)
        ref_values = [
            f"groups/{root_uuid}",
            '',
            f"groups/{g1_uuid}",
        ]
        array_len = len(ref_values)

        # create dataset
        ref_type = {
                "class": "H5T_REFERENCE", 
                "base": "H5T_STD_REF_OBJ",
        }
        payload = {
            "type": ref_type,
            "shape": [array_len]
        }
        dset_id = helper.postDataset(self.base_domain, payload)

        # read values from dset (should be empty strings)
        req = self.endpoint + "/datasets/" + dset_id + "/value" 
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        self.assertEqual(rspJson["value"], ['' for _ in range(array_len)])

        # write some values
        payload = { "value": ref_values }
        rsp = requests.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 200)

        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        self.assertListEqual(rspJson["value"], ref_values)

    def testPutObjRefDatasetBinary(self):
        # Test PUT obj ref values for 1d dataset using binary transfer
        headers = helper.getRequestHeaders(domain=self.base_domain)
        headers_bin_req = helper.getRequestHeaders(domain=self.base_domain) 
        headers_bin_req["Content-Type"] = "application/octet-stream"
        root_uuid = helper.getRootUUID(self.base_domain)

        # create new group  
        g1_uuid = helper.postGroup(self.base_domain)
        ref_values = [
            f"groups/{root_uuid}",
            '',
            f"groups/{g1_uuid}",
        ]
        array_len = len(ref_values)

        # create dataset
        ref_type = {
                "class": "H5T_REFERENCE", 
                "base": "H5T_STD_REF_OBJ",
        }
        payload = {
            "type": ref_type,
            "shape": [array_len],
        }
        dset_id = helper.postDataset(self.base_domain, payload)

        # write some values
        ref_length = 48  # long enough to store any object ref
        data = bytearray(3*ref_length)
        ref_values = ["groups/" + root_uuid, '', "groups/" + g1_uuid]
        for i in range(array_len):
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
        self.assertListEqual(rspJson["value"], ref_values)

    def testResizable1DValue(self):
        # test read/write to resizable dataset
        headers = helper.getRequestHeaders(domain=self.base_domain)

        # create the dataset 
        num_elements = 10
        req = self.endpoint + "/datasets"
        payload = {
            "type": "H5T_STD_I32LE",
            "shape": [num_elements],
            "maxdims": [0],
        }
        dset_uuid = helper.postDataset(self.base_domain, payload)

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
        self.assertListEqual(
                data[orig_extent:num_elements],
                list(range(orig_extent)))

    def testDeflateCompression(self):
        headers = helper.getRequestHeaders(domain=self.base_domain)

        # Create ~1MB dataset with compression
        gzip_filter = {
            "class": "H5Z_FILTER_DEFLATE",
            "id": 1,
            "level": 9,
            "name": "deflate",
        }
        payload = {
            "type": "H5T_STD_I8LE",
            "shape": [1024, 1024],
            "creationProperties": {
                "filters": [gzip_filter],
            },
        }
        dset_uuid = helper.postDataset(self.base_domain, payload)

        # write a horizontal strip of 22s
        req = self.endpoint + "/datasets/" + dset_uuid + "/value" 
        data = [22,] * 1024
        payload = { 'start': [512, 0], 'stop': [513, 1024], 'value': data }
        rsp = requests.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 200)

        # read back the 512,512 element
        req = self.endpoint + "/datasets/" + dset_uuid + "/value"
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

# ----------------------------------------------------------------------

@unittest.skipUnless(config.get("test_on_uploaded_file"), "requires file")
class FileValueTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(FileValueTest, self).__init__(*args, **kwargs)
        self.base_domain = helper.getTestDomainName(self.__class__.__name__)
        helper.setupDomain(self.base_domain)
        self.endpoint = helper.getEndpoint()

    def testGet(self):
        domain = helper.getTestDomain("tall.h5")
        headers = helper.getRequestHeaders(domain=domain)

        # verify domain exists
        req = helper.getEndpoint() + '/'
        rsp = requests.get(req, headers=headers)
        self.assertEqual(
                rsp.status_code,
                200,
                f"Failed to get domain `{domain}`. Is test data setup?")
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
        rsp = requests.get(req, params=params, headers=headers)
        self.assertEqual(rsp.status_code, 400)

# ----------------------------------------------------------------------

if __name__ == '__main__':
    unittest.main()


