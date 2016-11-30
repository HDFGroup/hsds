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
 

class AttributeTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(AttributeTest, self).__init__(*args, **kwargs)
        self.base_domain = helper.getTestDomainName(self.__class__.__name__)
        print(self.base_domain)
        self.endpoint = helper.getEndpoint()
        helper.setupDomain(self.base_domain)
        
        # main

    def testListAttr(self):
        print("testGroupAttr", self.base_domain)
        headers = helper.getRequestHeaders(domain=self.base_domain)
        req = self.endpoint + '/'

        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        root_uuid = rspJson["root"]
        helper.validateId(root_uuid)

        # get group and verify attribute count is 0
        req = self.endpoint + "/groups/" + root_uuid
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)  
        rspJson = json.loads(rsp.text)
        self.assertEqual(rspJson["attributeCount"], 0)  # no attributes

        attr_count = 10

        for i in range(attr_count):
            # create attr
            attr_name = "attr_{}".format(i)
            attr_payload = {'type': 'H5T_STD_I32LE', 'value': i*2}
            req = self.endpoint + "/groups/" + root_uuid + "/attributes/" + attr_name
            rsp = requests.put(req, data=json.dumps(attr_payload), headers=headers)
            self.assertEqual(rsp.status_code, 201)  # create attribute

        # get group and verify attribute count is attr_count
        req = self.endpoint + "/groups/" + root_uuid
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)  
        rspJson = json.loads(rsp.text)
        self.assertEqual(rspJson["attributeCount"], attr_count) 

        # get all the attributes
        req = self.endpoint + "/groups/" + root_uuid + "/attributes"
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
            type_json = attrJson["type"]
            self.assertEqual(type_json["class"], "H5T_INTEGER")
            self.assertEqual(type_json["base"], "H5T_STD_I32LE")
            self.assertTrue("shape" in attrJson)
            shapeJson = attrJson["shape"]
            self.assertEqual(shapeJson["class"], "H5S_SCALAR")
            # self.assertTrue("value" not in attrJson)  # TBD - change api to include value?
            self.assertTrue("created" in attrJson)
            self.assertTrue("href" in attrJson)
            self.assertTrue("value" not in attrJson)

        # get all attributes including data
        req = self.endpoint + "/groups/" + root_uuid + "/attributes?IncludeData=True"
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
            type_json = attrJson["type"]
            self.assertEqual(type_json["class"], "H5T_INTEGER")
            self.assertEqual(type_json["base"], "H5T_STD_I32LE")
            self.assertTrue("shape" in attrJson)
            shapeJson = attrJson["shape"]
            self.assertEqual(shapeJson["class"], "H5S_SCALAR")
            self.assertTrue("created" in attrJson)
            self.assertTrue("href" in attrJson)
            self.assertTrue("value" in attrJson)
            self.assertEqual(attrJson["value"], i*2)

        # get 3 attributes
        limit = 3
        req = self.endpoint + "/groups/" + root_uuid + "/attributes?Limit=" + str(limit)
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
        req = self.endpoint + "/groups/" + root_uuid + "/attributes?Limit=" + str(limit) + "&Marker=attr_5"
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
      

    def testObjAttr(self):
        print("testObjAttr", self.base_domain)
        headers = helper.getRequestHeaders(domain=self.base_domain)
        req = self.endpoint + '/'

        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        root_id = rspJson["root"]

       
        for col_name in ("groups", "datatypes", "datasets"):
            # create a new obj
            req = self.endpoint + '/' + col_name
            data = None
            if col_name != "groups":
                # this will work for datasets or datatypes
                data = { "type": "H5T_IEEE_F32LE" }
            
            rsp = requests.post(req, data=json.dumps(data), headers=headers)
            self.assertEqual(rsp.status_code, 201) 
            rspJson = json.loads(rsp.text)
            self.assertEqual(rspJson["attributeCount"], 0)   
            obj1_id = rspJson["id"]
            self.assertTrue(helper.validateId(obj1_id))

            # link new obj as '/col_name_obj'
            req = self.endpoint + "/groups/" + root_id + "/links/" + col_name + "_obj" 
            payload = {"id": obj1_id}
            rsp = requests.put(req, data=json.dumps(payload), headers=headers)
            self.assertEqual(rsp.status_code, 201)  # created

            # get obj and verify it has no attributes
            req = self.endpoint + '/' + col_name + '/' + obj1_id
            rsp = requests.get(req, headers=headers)
            self.assertEqual(rsp.status_code, 200)  
            rspJson = json.loads(rsp.text)
            self.assertEqual(rspJson["attributeCount"], 0)  # no attributes

            # do a GET for attribute "attr" (should return 404)
            attr_name = "attr"
            attr_payload = {'type': 'H5T_STD_I32LE', 'value': 42}
            req = self.endpoint + '/' + col_name + '/' + obj1_id + "/attributes/" + attr_name
            rsp = requests.get(req, headers=headers)
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

            # get obj and verify attribute count is 1
            req = self.endpoint + '/' + col_name + '/' + obj1_id
            rsp = requests.get(req, headers=headers)
            self.assertEqual(rsp.status_code, 200)  
            rspJson = json.loads(rsp.text)
            self.assertEqual(rspJson["attributeCount"], 1)  # one attribute

            # try creating the attribute again - should return 409
            req = self.endpoint + '/' + col_name + '/' + obj1_id + "/attributes/" + attr_name
            rsp = requests.put(req, data=json.dumps(attr_payload), headers=headers)
            self.assertEqual(rsp.status_code, 409)  # conflict

            # delete the attribute
            rsp = requests.delete(req, headers=headers)
            self.assertEqual(rsp.status_code, 200)  # OK


            # get obj and verify attribute count is 0
            req = self.endpoint + '/' + col_name + '/' + obj1_id
            rsp = requests.get(req, headers=headers)
            self.assertEqual(rsp.status_code, 200)  
            rspJson = json.loads(rsp.text)
            self.assertEqual(rspJson["attributeCount"], 0)  # no attributes

     


    def testPutInvalid(self):
        print("testPutInvalid", self.base_domain)
        headers = helper.getRequestHeaders(domain=self.base_domain)
        req = self.endpoint + '/'

        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        root_id = rspJson["root"]

        # try creating an attribute with an invalid type
        attr_name = "attr1"
        attr_payload = {'type': 'H5T_FOOBAR', 'value': 42}
        req = self.endpoint + "/groups/" + root_id + "/attributes/" + attr_name
        rsp = requests.put(req, data=json.dumps(attr_payload), headers=headers)
        self.assertEqual(rsp.status_code, 400)  # invalid request 


if __name__ == '__main__':
    #setup test files
    
    unittest.main()
