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

        # try to read beyond the bounds of the array
        params = {"select": "[2:18]"} # read 6 elements, starting at index 2
        rsp = requests.get(req, params=params, headers=headers)
        self.assertEqual(rsp.status_code, 400)

    def testPut1DDatasetBinary(self):
        # Test PUT value for 1d dataset using binary data
        print("testPut1DDatasetBinary", self.base_domain)

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
        data = { "type": "H5T_STD_I32LE", "shape": 10 }
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
        self.assertEqual(len(data), 40)
        for i in range(10):
            offset = i*4
            self.assertEqual(data[offset+0], 0)
            self.assertEqual(data[offset+1], 0)
            self.assertEqual(data[offset+2], 0)
            self.assertEqual(data[offset+3], 0)
 
        # write to the dset
        # write 0-9 as four-byte little-endian integers
        data = bytearray(4*10)
        for i in range(10):
            data[i*4] = i
        rsp = requests.put(req, data=data, headers=headers_bin_req)
        self.assertEqual(rsp.status_code, 200)
        
        # read back the data
        rsp = requests.get(req, headers=headers_bin_rsp)
        self.assertEqual(rsp.status_code, 200)
        data = rsp.content
        self.assertEqual(len(data), 40)
        for i in range(10):
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
        

    def testPutFixedString(self):
        # Test PUT value for 1d dataset with fixed length string types
        print("testPutFixedString", self.base_domain)

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
                    {'name': 'pressure', 'type': 'H5T_IEEE_F32LE'}) 
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
        payload['creationProperties'] = {'fillValue': 42 }
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

        # write to the etended region 
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
            
 
             
if __name__ == '__main__':
    #setup test files
    
    unittest.main()
    
