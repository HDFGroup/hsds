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

    def testListAttr(self):
        print("testGroupAttr", self.base_domain)
        headers = helper.getRequestHeaders(domain=self.base_domain)
        req = helper.getEndpoint() + '/'

        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        root_uuid = rspJson["root"]
        helper.validateId(root_uuid)

        # get group and verify attribute count is 0
        req = helper.getEndpoint() + "/groups/" + root_uuid
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)  
        rspJson = json.loads(rsp.text)
        self.assertEqual(rspJson["attributeCount"], 0)  # no attributes

        attr_count = 10

        for i in range(attr_count):
            # create attr
            attr_name = "attr_{}".format(i)
            attr_payload = {'type': 'H5T_STD_I32LE', 'value': i*2}
            req = helper.getEndpoint() + "/groups/" + root_uuid + "/attributes/" + attr_name
            rsp = requests.put(req, data=json.dumps(attr_payload), headers=headers)
            self.assertEqual(rsp.status_code, 201)  # create attribute

        # get group and verify attribute count is attr_count
        req = helper.getEndpoint() + "/groups/" + root_uuid
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)  
        rspJson = json.loads(rsp.text)
        self.assertEqual(rspJson["attributeCount"], attr_count) 

        # get all the attributes
        req = helper.getEndpoint() + "/groups/" + root_uuid + "/attributes"
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)  
        rspJson = json.loads(rsp.text)
       
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("attributes" in rspJson)
        attributes = rspJson["attributes"]
        self.assertTrue(isinstance(attributes, list))
        self.assertEqual(len(attributes), attr_count)
        for i in range(attr_count):
            attrJson = attributes[i]
            self.assertTrue("name" in attrJson)
            self.assertEqual(attrJson["name"], "attr_{}".format(i))
            self.assertTrue("type" in attrJson)
            self.assertEqual(attrJson["type"], "H5T_STD_I32LE")
            self.assertTrue("shape" in attrJson)
            shapeJson = attrJson["shape"]
            self.assertEqual(shapeJson["class"], "H5S_SCALAR")
            # self.assertTrue("value" not in attrJson)  # TBD - change api to include value?
            self.assertTrue("created" in attrJson)

        # get 3 attributes
        limit = 3
        req = helper.getEndpoint() + "/groups/" + root_uuid + "/attributes?Limit=" + str(limit)
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)  
        rspJson = json.loads(rsp.text)
       
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("attributes" in rspJson)
        attributes = rspJson["attributes"]
        self.assertTrue(isinstance(attributes, list))
        self.assertEqual(len(attributes), limit)

        # get 3 attributes after "attr_5"
        limit = 3
        req = helper.getEndpoint() + "/groups/" + root_uuid + "/attributes?Limit=" + str(limit) + "&Marker=attr_5"
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)  
        rspJson = json.loads(rsp.text)
       
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("attributes" in rspJson)
        attributes = rspJson["attributes"]
        self.assertTrue(isinstance(attributes, list))
        self.assertEqual(len(attributes), limit)
        attrJson = attributes[0]
        self.assertEqual(attrJson["name"], "attr_6")
      

    def testGroupAttr(self):
        print("testGroupAttr", self.base_domain)
        headers = helper.getRequestHeaders(domain=self.base_domain)
        req = helper.getEndpoint() + '/'

        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        root_id = rspJson["root"]

        # create a new group
        req = helper.getEndpoint() + '/groups'
        rsp = requests.post(req, headers=headers)
        self.assertEqual(rsp.status_code, 201) 
        rspJson = json.loads(rsp.text)
        self.assertEqual(rspJson["attributeCount"], 0)   
        grp1_id = rspJson["id"]
        self.assertTrue(helper.validateId(grp1_id))

        # link new group as '/g1'
        headers = helper.getRequestHeaders(domain=self.base_domain)
        req = helper.getEndpoint() + "/groups/" + root_id + "/links/g1" 
        payload = {"id": grp1_id}
        rsp = requests.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)  # created

        # get g1 and verify it has no attributes
        req = helper.getEndpoint() + "/groups/" + grp1_id
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)  
        rspJson = json.loads(rsp.text)
        self.assertEqual(rspJson["attributeCount"], 0)  # no attributes

        # do a GET for attribute "attr" (should return 404)
        attr_name = "attr"
        attr_payload = {'type': 'H5T_STD_I32LE', 'value': 42}
        req = helper.getEndpoint() + "/groups/" + grp1_id + "/attributes/" + attr_name
        rsp = requests.get(req, data=json.dumps(attr_payload), headers=headers)
        self.assertEqual(rsp.status_code, 404)  # not found

        # try adding the attribute as a different user
        headers = helper.getRequestHeaders(domain=self.base_domain, username="test_user2")
        rsp = requests.put(req, data=json.dumps(attr_payload), headers=headers)
        self.assertEqual(rsp.status_code, 403)  # forbidden

        # try adding again with original user, but outside this domain
        another_domain = helper.getParentDomain(self.base_domain)
        headers = helper.getRequestHeaders(domain=another_domain)
        rsp = requests.put(req, data=json.dumps(attr_payload), headers=headers)
        self.assertEqual(rsp.status_code, 400)  # Invalid request

        # try again with original user and proper domain
        headers = helper.getRequestHeaders(domain=self.base_domain)
        rsp = requests.put(req, data=json.dumps(attr_payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)  # created

        # read the attribute we just created
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)  # create attribute
        rspJson = json.loads(rsp.text)
        self.assertTrue("created" in rspJson)
        self.assertTrue("lastModified" in rspJson)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("name" in rspJson)
        self.assertEqual(rspJson["name"], "attr")
        self.assertTrue("shape" in rspJson)
        shape = rspJson["shape"]
        self.assertTrue("class" in shape)
        self.assertEqual(shape["class"], "H5S_SCALAR")

        # get group and verify attribute count is 1
        req = helper.getEndpoint() + "/groups/" + grp1_id
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)  
        rspJson = json.loads(rsp.text)
        self.assertEqual(rspJson["attributeCount"], 1)  # one attribute

        # try creating the attribute again - should return 409
        req = helper.getEndpoint() + "/groups/" + grp1_id + "/attributes/" + attr_name
        rsp = requests.put(req, data=json.dumps(attr_payload), headers=headers)
        self.assertEqual(rsp.status_code, 409)  # conflict

        # delete the attribute
        req = helper.getEndpoint() + "/groups/" + grp1_id + "/attributes/" + attr_name
        rsp = requests.delete(req, data=json.dumps(attr_payload), headers=headers)
        self.assertEqual(rsp.status_code, 200)  # OK


        # get group and verify attribute count is 0
        req = helper.getEndpoint() + "/groups/" + grp1_id
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)  
        rspJson = json.loads(rsp.text)
        self.assertEqual(rspJson["attributeCount"], 0)  # no attributes


    def testPutInvalid(self):
        print("testPutInvalid", self.base_domain)
        headers = helper.getRequestHeaders(domain=self.base_domain)
        req = helper.getEndpoint() + '/'

        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        root_id = rspJson["root"]

        # try creating an attribute with an invalid type
        attr_name = "attr1"
        attr_payload = {'type': 'H5T_FOOBAR', 'value': 42}
        req = helper.getEndpoint() + "/groups/" + root_id + "/attributes/" + attr_name
        rsp = requests.put(req, data=json.dumps(attr_payload), headers=headers)
        self.assertEqual(rsp.status_code, 400)  # invalid request






  

    


if __name__ == '__main__':
    #setup test files
    
    unittest.main()
