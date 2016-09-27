##############################################################################
# Copyright by The HDF Group.                                                #
# All rights reserved.                                                       #
#                                                                            #
# This file is part of H5Serv (HDF5 REST Server) Service, Libraries and      #
# Utilities.  The full HDF5 REST Server copyright notice, including          #
# terms governing use, modification, and redistribution, is contained in     #
# the file COPYING, which can be found at the root of the source code        #
# distribution tree.  If you do not have access to this file, you may        #
# request a copy from help@hdfgroup.org.                                     #
##############################################################################
import unittest
import requests
import json
import helper
 

class AttributeTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(AttributeTest, self).__init__(*args, **kwargs)
        self.base_domain = helper.getTestDomainName(self.__class__.__name__)
        print(self.base_domain)
        helper.setupDomain(self.base_domain)
        
        # main

    def testGroupAttr(self):
        print("testGetGroupAttr", self.base_domain)
        headers = helper.getRequestHeaders(domain=self.base_domain)
        req = helper.getEndpoint() + '/'

        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        root_uuid = rspJson["root"]
        helper.validateId(root_uuid)
        print("root_uuid:", root_uuid)
        attr1_name = "attr1"
        attr1_payload = {'type': 'H5T_IEEE_F32LE', 'shape': (1,), 'value': (3.12,)}
        req = helper.getEndpoint() + "/groups/" + root_uuid + "/attributes/" + attr1_name
        rsp = requests.put(req, data=json.dumps(attr1_payload), headers=headers)
        print(rsp)
        self.assertEqual(rsp.status_code, 201)  # create attribute

        attr2_name = "attr2"
        attr2_payload = {'type': 'H5T_STD_I32LE', 'value': 42}
        req = helper.getEndpoint() + "/groups/" + root_uuid + "/attributes/" + attr2_name
        rsp = requests.put(req, data=json.dumps(attr2_payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)  # create attribute

        attr3_name = "attr3"
        data = list(range(10))
        attr3_payload = {'type': 'H5T_STD_I32LE', 'shape': (10,), 'value': data}
        req = helper.getEndpoint() + "/groups/" + root_uuid + "/attributes/" + attr3_name
        rsp = requests.put(req, data=json.dumps(attr3_payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)  # create attribute
    


if __name__ == '__main__':
    #setup test files
    
    unittest.main()
