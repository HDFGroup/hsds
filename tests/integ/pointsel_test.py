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
import base64
import unittest
import requests
import json
import numpy as np
import helper
 

class PointSelTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(PointSelTest, self).__init__(*args, **kwargs)
        self.base_domain = helper.getTestDomainName(self.__class__.__name__)
        helper.setupDomain(self.base_domain)
        self.endpoint = helper.getEndpoint()
    

    def testPost1DDataset(self):
        
        # Test selecting points in a dataset using POST value
        print("testPost1DDataset", self.base_domain)

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
        data = { "type": "H5T_STD_I32LE", "shape": (100,) }
        data['creationProperties'] = {'layout': {'class': 'H5D_CHUNKED', 'dims': [20,] }}
        
        req = self.endpoint + '/datasets' 
        rsp = requests.post(req, data=json.dumps(data), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)
        dset_id = rspJson["id"]
        self.assertTrue(helper.validateId(dset_id))

        # link new dataset as 'dset1d'
        name = "dset"
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name 
        payload = {"id": dset_id}
        rsp = requests.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        

        # write to the dset
        data = list(range(100))
        data.reverse()   # 99, 98, ..., 0

        payload = { 'value': data }
        req = self.endpoint + "/datasets/" + dset_id + "/value"
        
        rsp = requests.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 200)

        # read back data
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)

        points = [2,3,5,7,11,13,17,19,23,29,31,37,41,43,47,53,59,61,67,71,73,79,83,97,98]
        body = { "points": points }
        # read a selected points
        rsp = requests.post(req, data=json.dumps(body), headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("value" in rspJson)
        ret_value = rspJson["value"]
        self.assertEqual(len(ret_value), len(points))
        expected_result = [97, 96, 94, 92, 88, 86, 82, 80, 76, 70, 68, 62, 58, 56, 52, 46, 40, 38, 32, 28, 26, 20, 16, 2, 1]
        self.assertEqual(ret_value, expected_result)

    def testPost2DDataset(self):
        # Test POST value with selection for 2d dataset
        print("testPost2DDataset", self.base_domain)

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
        data = { "type": "H5T_STD_I32LE", "shape": [20,30] }
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
        
        # make up some data
        arr2d = []
        for i in range(20):
            row = []
            for j in range(30):
                row.append(i*10000+j)
            arr2d.append(row)

        # write some values
        req = self.endpoint + '/datasets/' + dset_id + '/value'
        payload = { 'value': arr2d }
        rsp = requests.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 200)

        # do a point select
        points = []
        for i in range(3):
            for j in range(5):
                pt = [i*5+5,j*5+5]
                points.append(pt)
        body = { "points": points }
        # read a selected points
        rsp = requests.post(req, data=json.dumps(body), headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        expected_result = [50005, 50010, 50015, 50020, 50025, 100005, 100010, 
            100015, 100020, 100025, 150005, 150010, 150015, 150020, 150025]
        self.assertTrue("value" in rspJson)
        values = rspJson["value"]
        self.assertEqual(values, expected_result)


    def testPost1DDatasetBinary(self):
        
        # Test selecting points in a dataset using POST value
        print("testPost1DDatasetBinary", self.base_domain)

        headers = helper.getRequestHeaders(domain=self.base_domain)
        headers_bin_req = helper.getRequestHeaders(domain=self.base_domain)
        headers_bin_req["Content-Type"] = "application/octet-stream"
        headers_bin_reqrsp = helper.getRequestHeaders(domain=self.base_domain)
        headers_bin_reqrsp["accept"] = "application/octet-stream"
        headers_bin_reqrsp["Content-Type"] = "application/octet-stream"
        req = self.endpoint + '/'

        # Get root uuid
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        root_uuid = rspJson["root"]
        helper.validateId(root_uuid)

        # create dataset
        # pass in layout specification so that we can test selection across chunk boundries
        data = { "type": "H5T_STD_I32LE", "shape": (100,) }
        data['creationProperties'] = {'layout': {'class': 'H5D_CHUNKED', 'dims': [20,] }}
        
        req = self.endpoint + '/datasets' 
        rsp = requests.post(req, data=json.dumps(data), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)
        dset_id = rspJson["id"]
        self.assertTrue(helper.validateId(dset_id))

        # link new dataset as 'dset1d'
        name = "dset"
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name 
        payload = {"id": dset_id}
        rsp = requests.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        
        arr = np.zeros((100,), dtype='i4')
        for i in range(100):
            arr[i] = 99 - i
        # write to the dset
        data = arr.tobytes()
         
        req = self.endpoint + "/datasets/" + dset_id + "/value"
        
        rsp = requests.put(req, data=data, headers=headers_bin_req)
        self.assertEqual(rsp.status_code, 200)

        points = [2,3,5,7,11,13,17,19,23,29,31,37,41,43,47,53,59,61,67,71,73,79,83,97,98]
        num_points = len(points)
        arr_points = np.asarray(points, dtype='u8')  # must use unsigned 64-bit int
        data = arr_points.tobytes()
    
        # read selected points
        rsp = requests.post(req, data=data, headers=headers_bin_reqrsp)
        self.assertEqual(rsp.status_code, 200)
        rsp_data = rsp.content
        self.assertEqual(len(rsp_data), num_points*4)
        arr_rsp = np.fromstring(rsp_data, dtype='i4')
        rsp_values = arr_rsp.tolist()
        expected_result = [97, 96, 94, 92, 88, 86, 82, 80, 76, 70, 68, 62, 58, 56, 52, 46, 40, 38, 32, 28, 26, 20, 16, 2, 1]
        self.assertEqual(rsp_values, expected_result)
    

    def testPost2DDatasetBinary(self):
        # Test POST value with selection for 2d dataset
        print("testPost2DDatasetBinary", self.base_domain)

        headers = helper.getRequestHeaders(domain=self.base_domain)
        headers_bin_req = helper.getRequestHeaders(domain=self.base_domain)
        headers_bin_req["Content-Type"] = "application/octet-stream"
        headers_bin_reqrsp = helper.getRequestHeaders(domain=self.base_domain)
        headers_bin_reqrsp["accept"] = "application/octet-stream"
        headers_bin_reqrsp["Content-Type"] = "application/octet-stream"
        
        req = self.endpoint + '/'

        # Get root uuid
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        root_uuid = rspJson["root"]
        helper.validateId(root_uuid)
        
        # create dataset
        # pass in layout specification so that we can test selection across chunk boundries
        data = { "type": "H5T_STD_I32LE", "shape": [20,30] }
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

        arr = np.zeros((20, 30), dtype='i4')
        for i in range(20):
            for j in range(30):
                arr[i,j] = i * 10000 + j
        arr_bytes = arr.tobytes()
           
        # write some values
        req = self.endpoint + '/datasets/' + dset_id + '/value'
        rsp = requests.put(req, data=arr_bytes, headers=headers_bin_req)
        self.assertEqual(rsp.status_code, 200)

        # do a point select
        points = []
        for i in range(3):
            for j in range(5):
                pt = [i*5+5,j*5+5]
                points.append(pt)
        num_points = len(points)
        arr_points = np.asarray(points, dtype='u8')  # must use unsigned 64-bit int
        pt_bytes = arr_points.tobytes()

        # read selected points
        rsp = requests.post(req, data=pt_bytes, headers=headers_bin_reqrsp)
        self.assertEqual(rsp.status_code, 200)
        rsp_data = rsp.content
        self.assertEqual(len(rsp_data), num_points*4)
        arr_rsp = np.fromstring(rsp_data, dtype='i4')
        values = arr_rsp.tolist()
         
        expected_result = [50005, 50010, 50015, 50020, 50025, 100005, 100010, 
            100015, 100020, 100025, 150005, 150010, 150015, 150020, 150025]
         
        self.assertEqual(values, expected_result)

    def testPut1DDataset(self):
        # Test writing using point selection for a 1D dataset
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
        # pass in layout specification so that we can test selection across chunk boundries
        data = { "type": "H5T_STD_I8LE", "shape": (100,) }
        data['creationProperties'] = {'layout': {'class': 'H5D_CHUNKED', 'dims': [20,] }}
        
        req = self.endpoint + "/datasets"
        rsp = requests.post(req, data=json.dumps(data), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)
        dset_id = rspJson["id"]
        self.assertTrue(helper.validateId(dset_id))

        # link new dataset as 'dset1d'
        name = "dset"
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name 
        payload = {"id": dset_id}
        rsp = requests.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        
        # Do a point selection write
        primes = [2,3,5,7,11,13,17,19,23,29,31,37,41,43,47,53,59,61,67,71,73,79,83,89,97]
        value = [1,] * len(primes)  # write 1's at indexes that are prime
        # write 1's to all the prime indexes
        payload = { 'points': primes, 'value': value }
        req = self.endpoint + "/datasets/" + dset_id + "/value"
        rsp = requests.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 200)   

        # read back data
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        # verify the correct elements got set
        value = rspJson["value"]
        for i in range(100):
            if i in primes:
                self.assertEqual(value[i], 1)
            else:
                self.assertEqual(value[i], 0)

        # read back data as one big hyperslab selection
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200) 
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        ret_values = rspJson["value"]
        self.assertEqual(len(ret_values), 100)
        for i in range(100):
            if i in primes:
                self.assertEqual(ret_values[i], 1)
            else:
                self.assertEqual(ret_values[i], 0)

        

    def testPut2DDataset(self):
        # Test writing with point selection for 2d dataset
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
        # pass in layout specification so that we can test selection across chunk boundries
        data = { "type": "H5T_STD_I32LE", "shape": [20,30] }
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

        # make up some points
        points = []
        for i in range(20):
            points.append((i, i))
        value = [1,] * 20

        # write 1's to all the point locations 
        payload = { 'points': points, 'value': value }
        req = self.endpoint + "/datasets/" + dset_id + "/value"
        rsp = requests.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 200) 

        # read back data
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("value" in rspJson)
        # verify the correct elements got set
        value = rspJson["value"]
        #print("value:", value)
        for x in range(20):
            row = value[x]
            for y in range(30):
                if x == y:
                    self.assertEqual(row[y], 1)
                else:
                    self.assertEqual(row[y], 0)
        


    def testPut1DDatasetBinary(self):
        # Test writing using point selection for a 1D dataset
        print("testPut1DDatasetBinary", self.base_domain)

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
        data = { "type": "H5T_STD_I8LE", "shape": (100,) }
        data['creationProperties'] = {'layout': {'class': 'H5D_CHUNKED', 'dims': [20,] }}
        
        req = self.endpoint + "/datasets"
        rsp = requests.post(req, data=json.dumps(data), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)
        dset_id = rspJson["id"]
        self.assertTrue(helper.validateId(dset_id))

        # link new dataset as 'dset1d'
        name = "dset"
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name 
        payload = {"id": dset_id}
        rsp = requests.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        
        # Do a point selection write
        primes = [2,3,5,7,11,13,17,19,23,29,31,37,41,43,47,53,59,61,67,71,73,79,83,89,97]

        # create binary array for the values
        byte_array = bytearray(len(primes))
        for i in range(len(primes)):
            byte_array[i] = 1  
        value_base64 = base64.b64encode(bytes(byte_array))
        value_base64 = value_base64.decode("ascii")
         
        # write 1's to all the prime indexes
        payload = { 'points': primes, 'value_base64': value_base64 }
        req = self.endpoint + "/datasets/" + dset_id + "/value"
        rsp = requests.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 200)   

        # read back data as one big hyperslab selection
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200) 
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        ret_values = rspJson["value"]
        self.assertEqual(len(ret_values), 100)
        for i in range(100):
            if i in primes:
                self.assertEqual(ret_values[i], 1)
            else:
                self.assertEqual(ret_values[i], 0)

    def testPut2DDatasetBinary(self):
        # Test writing with point selection for 2d dataset with binary data
        print("testPut2DDatasetBinary", self.base_domain)

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
        data = { "type": "H5T_STD_I32LE", "shape": [20,30] }
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

        # make up some points
        points = []
        for i in range(20):
            points.append((i, i))
        value = [1,] * 20
        # create a byter array of 20 ints with value 1
        # create binary array for the values
        byte_array = bytearray(20*4)
        for i in range(20):
            byte_array[i*4] = 1  
        value_base64 = base64.b64encode(bytes(byte_array))
        value_base64 = value_base64.decode("ascii")

        # write 1's to all the point locations 
        payload = { 'points': points, 'value_base64': value_base64 }
        req = self.endpoint + "/datasets/" + dset_id + "/value"
        rsp = requests.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 200) 

        # read back data
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("value" in rspJson)
        # verify the correct elements got set
        value = rspJson["value"]
        #print("value:", value)
        for x in range(20):
            row = value[x]
            for y in range(30):
                if x == y:
                    self.assertEqual(row[y], 1)
                else:
                    self.assertEqual(row[y], 0)
             
    
             
if __name__ == '__main__':
    #setup test files
    
    unittest.main()
    
