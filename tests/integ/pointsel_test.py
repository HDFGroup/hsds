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
        print("testPost2DDataset", self.base_domain)

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
         
    
             
if __name__ == '__main__':
    #setup test files
    
    unittest.main()
    
