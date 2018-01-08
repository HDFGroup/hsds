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
 

class VlenTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(VlenTest, self).__init__(*args, **kwargs)
        self.base_domain = helper.getTestDomainName(self.__class__.__name__)
        helper.setupDomain(self.base_domain)
        self.endpoint = helper.getEndpoint()
        
        # main
     
    def testPutVLenInt(self):
        # Test PUT value for 1d attribute with variable length int types
        print("testPutVLenInt", self.base_domain)

        headers = helper.getRequestHeaders(domain=self.base_domain)
        req = self.endpoint + '/'

        # Get root uuid
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        root_uuid = rspJson["root"]
        helper.validateId(root_uuid)

        # create dataset
        vlen_type = {"class": "H5T_VLEN", "base": { "class": "H5T_INTEGER", "base": "H5T_STD_I32LE"}}
        payload = {'type': vlen_type, 'shape': [4,]}
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

        # write values to dataset
        data = [[1,], [1,2], [1,2,3], [1,2,3,4]]
        req = self.endpoint + "/datasets/" + dset_uuid + "/value" 
        payload = { 'value': data }
        rsp = requests.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 200)

        # read values from dataset
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)

         
 
             
if __name__ == '__main__':
    #setup test files
    
    unittest.main()
    
