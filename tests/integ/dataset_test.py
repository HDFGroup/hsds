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
import time
import helper

# min/max chunk size - copied from chunkUtil.py
CHUNK_MIN = 512*1024      # Soft lower limit (512k)
CHUNK_MAX = 2*1024*1024   # Hard upper limit (2M) 

class DatasetTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(DatasetTest, self).__init__(*args, **kwargs)
        self.base_domain = helper.getTestDomainName(self.__class__.__name__)
        helper.setupDomain(self.base_domain)
        self.endpoint = helper.getEndpoint()
        
        # main
     
    def testScalarDataset(self):
        # Test creation/deletion of scalar dataset obj

        print("testScalarDataset", self.base_domain)
        headers = helper.getRequestHeaders(domain=self.base_domain)
        req = self.endpoint + '/'

        # Get root uuid
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        root_uuid = rspJson["root"]
        helper.validateId(root_uuid)

        # create a dataset obj
        data = { "type": "H5T_IEEE_F32LE" }
        req = self.endpoint + '/datasets' 
        rsp = requests.post(req, data=json.dumps(data), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)
        self.assertEqual(rspJson["attributeCount"], 0)   
        dset_id = rspJson["id"]
        print("created dset: {}".format(dset_id))
        self.assertTrue(helper.validateId(dset_id))

        # read back the obj
        req = self.endpoint + '/datasets/' + dset_id 
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        for name in ("id", "shape", "hrefs", "layout", "creationProperties", 
            "attributeCount", "created", "lastModified", "root", "domain"):
            self.assertTrue(name in rspJson)
        self.assertEqual(rspJson["id"], dset_id)
        self.assertEqual(rspJson["root"], root_uuid) 
        self.assertEqual(rspJson["domain"], self.base_domain) 
        self.assertEqual(rspJson["attributeCount"], 0)
        shape_json = rspJson["shape"]
        self.assertTrue(shape_json["class"], "H5S_SCALAR")
        self.assertTrue(rspJson["type"], "H5T_IEEE_F32LE")

        # Get the type
        rsp = requests.get(req + "/type", headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("type" in rspJson)
        self.assertTrue(rspJson["type"], "H5T_IEEE_F32LE")
        self.assertTrue("hrefs" in rspJson)
        hrefs = rspJson["hrefs"]
        self.assertEqual(len(hrefs), 3)

        # Get the shape
        rsp = requests.get(req + "/shape", headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("created" in rspJson)
        self.assertTrue("lastModified" in rspJson)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("shape" in rspJson)
        shape_json = rspJson["shape"]
        self.assertTrue(shape_json["class"], "H5S_SCALAR")  
         
        # try get with a different user (who has read permission)
        headers = helper.getRequestHeaders(domain=self.base_domain, username="test_user2")
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertEqual(rspJson["id"], dset_id)

        # try to do a GET with a different domain (should fail)
        another_domain = helper.getParentDomain(self.base_domain)
        headers = helper.getRequestHeaders(domain=another_domain)
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 400)


        # try DELETE with user who doesn't have create permission on this domain
        headers = helper.getRequestHeaders(domain=self.base_domain, username="test_user2")
        rsp = requests.delete(req, headers=headers)
        self.assertEqual(rsp.status_code, 403) # forbidden

        # try to do a DELETE with a different domain (should fail)
        another_domain = helper.getParentDomain(self.base_domain)
        headers = helper.getRequestHeaders(domain=another_domain)
        rsp = requests.delete(req, headers=headers)
        self.assertEqual(rsp.status_code, 400)   
        """
        # delete the dataset
        headers = helper.getRequestHeaders(domain=self.base_domain)
        rsp = requests.delete(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue(rspJson is not None)

        # a get for the dataset should now return 410 (GONE)
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 410)
        """

    def testScalarEmptyDimsDataset(self):
        # Test creation/deletion of scalar dataset obj

        print("testScalarEmptyDimsDataset", self.base_domain)
        headers = helper.getRequestHeaders(domain=self.base_domain)
        req = self.endpoint + '/'

        # Get root uuid
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        root_uuid = rspJson["root"]
        helper.validateId(root_uuid)

        # create a dataset obj
        data = { "type": "H5T_IEEE_F32LE", "shape": [] }
        req = self.endpoint + '/datasets' 
        rsp = requests.post(req, data=json.dumps(data), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)
        self.assertEqual(rspJson["attributeCount"], 0)   
        dset_id = rspJson["id"]
        self.assertTrue(helper.validateId(dset_id))

        # read back the obj
        req = self.endpoint + '/datasets/' + dset_id 
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("id" in rspJson)
        self.assertEqual(rspJson["id"], dset_id)
        self.assertTrue("created" in rspJson)
        self.assertTrue("lastModified" in rspJson)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("attributeCount" in rspJson)
        self.assertEqual(rspJson["attributeCount"], 0)
        self.assertTrue("shape" in rspJson)
        shape_json = rspJson["shape"]
        self.assertTrue(shape_json["class"], "H5S_SCALAR")
        self.assertTrue("type" in rspJson)
        self.assertTrue(rspJson["type"], "H5T_IEEE_F32LE")

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
        dset_uuid = helper.getUUIDByPath(domain, "/g1/g1.1/dset1.1.1")
        self.assertTrue(dset_uuid.startswith("d-"))

        # get the dataset json
        req = helper.getEndpoint() + '/datasets/' + dset_uuid
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        for name in ("id", "shape", "hrefs", "layout", "creationProperties", 
            "attributeCount", "created", "lastModified", "root", "domain"):
            self.assertTrue(name in rspJson)
         
        self.assertEqual(rspJson["id"], dset_uuid) 
        self.assertEqual(rspJson["root"], root_uuid) 
        self.assertEqual(rspJson["domain"], domain) 
        hrefs = rspJson["hrefs"]
        self.assertEqual(len(hrefs), 5)
        self.assertEqual(rspJson["id"], dset_uuid)

        shape = rspJson["shape"]
        for name in ("class", "dims", "maxdims"):
            self.assertTrue(name in shape)
        self.assertEqual(shape["class"], 'H5S_SIMPLE')
        self.assertEqual(shape["dims"], [10,10])
        self.assertEqual(shape["maxdims"], [10,10])

        layout = rspJson["layout"]
        self.assertEqual(layout["class"], 'H5D_CHUNKED')
        self.assertEqual(layout["dims"], [10,10])
         
        type = rspJson["type"]
        for name in ("base", "class"):
            self.assertTrue(name in type)
        self.assertEqual(type["class"], 'H5T_INTEGER')
        self.assertEqual(type["base"], 'H5T_STD_I32BE')

        cpl = rspJson["creationProperties"]
        for name in ("layout", "fillTime"):
            self.assertTrue(name in cpl)

        self.assertEqual(rspJson["attributeCount"], 2)

        now = time.time()
        # the object shouldn't have been just created or updated
        self.assertTrue(rspJson["created"] < now - 60 * 5)
        self.assertTrue(rspJson["lastModified"] < now - 60 * 5)
 

    def testDelete(self):
        # test Delete
        print("testDelete", self.base_domain)
        headers = helper.getRequestHeaders(domain=self.base_domain)

        # get domain
        req = helper.getEndpoint() + '/'
        rsp = requests.get(req, headers=headers)
        rspJson = json.loads(rsp.text)
        self.assertTrue("root" in rspJson)
  
        # create a new dataset
        req = helper.getEndpoint() + '/datasets'  
        rsp = requests.post(req, headers=headers)
        data = { "type": "H5T_IEEE_F32LE" }
        req = self.endpoint + '/datasets' 
        rsp = requests.post(req, data=json.dumps(data), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)
        self.assertEqual(rspJson["attributeCount"], 0)   
        dset_id = rspJson["id"]
        self.assertTrue(helper.validateId(dset_id))
        

        # verify we can do a get on the new dataset
        req = helper.getEndpoint() + '/datasets/' + dset_id
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("id" in rspJson)
        self.assertEqual(rspJson["id"], dset_id)
        

        # try DELETE with user who doesn't have create permission on this domain
        headers = helper.getRequestHeaders(domain=self.base_domain, username="test_user2")
        rsp = requests.delete(req, headers=headers)
        self.assertEqual(rsp.status_code, 403) # forbidden

        # try to do a DELETE with a different domain (should fail)
        another_domain = helper.getParentDomain(self.base_domain)
        headers = helper.getRequestHeaders(domain=another_domain)
        req = helper.getEndpoint() + '/datasets/' + dset_id
        rsp = requests.delete(req, headers=headers)
        self.assertEqual(rsp.status_code, 400)   
        
        # delete the new dataset
        headers = helper.getRequestHeaders(domain=self.base_domain)
        rsp = requests.delete(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue(rspJson is not None)

        # a get for the dataset should now return 410 (GONE)
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 410)

    def testCompound(self):
        # test Dataset with compound type
        print("testCompound", self.base_domain)
        headers = helper.getRequestHeaders(domain=self.base_domain)

        # get domain
        req = helper.getEndpoint() + '/'
        rsp = requests.get(req, headers=headers)
        rspJson = json.loads(rsp.text)
        self.assertTrue("root" in rspJson)
        root_uuid = rspJson["root"]

        fields = ({'name': 'temp', 'type': 'H5T_STD_I32LE'}, 
                    {'name': 'pressure', 'type': 'H5T_IEEE_F32LE'}) 
        datatype = {'class': 'H5T_COMPOUND', 'fields': fields }
        payload = {'type': datatype, 'shape': 10}
        req = self.endpoint + "/datasets"
        rsp = requests.post(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)  # create dataset
        rspJson = json.loads(rsp.text)
        dset_uuid = rspJson['id']
        self.assertTrue(helper.validateId(dset_uuid))
         
        # link the new dataset 
        name = "dset"
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name 
        payload = {"id": dset_uuid}
        rsp = requests.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

    def testCompoundDuplicateMember(self):
        # test Dataset with compound type but field that is repeated
        print("testCompoundDupicateMember", self.base_domain)
        headers = helper.getRequestHeaders(domain=self.base_domain)

        # get domain
        req = helper.getEndpoint() + '/'
        rsp = requests.get(req, headers=headers)
        rspJson = json.loads(rsp.text)
        self.assertTrue("root" in rspJson)
        root_uuid = rspJson["root"]

        fields = ({'name': 'x', 'type': 'H5T_STD_I32LE'}, 
                    {'name': 'x', 'type': 'H5T_IEEE_F32LE'}) 
        datatype = {'class': 'H5T_COMPOUND', 'fields': fields }
        payload = {'type': datatype, 'shape': 10}
        req = self.endpoint + "/datasets"
        rsp = requests.post(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 400)  # Bad Request

    def testPostNullSpace(self):
        # test Dataset with null dataspace type
        print("testNullSpace", self.base_domain)
        headers = helper.getRequestHeaders(domain=self.base_domain)

        # get domain
        req = helper.getEndpoint() + '/'
        rsp = requests.get(req, headers=headers)
        rspJson = json.loads(rsp.text)
        self.assertTrue("root" in rspJson)
        root_uuid = rspJson["root"]

        # pass H5S_NULL for shape 
        payload = {'type': 'H5T_IEEE_F32LE', 'shape': 'H5S_NULL'}
        req = self.endpoint + "/datasets"
        rsp = requests.post(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)  # create dataset
        rspJson = json.loads(rsp.text)
        dset_uuid = rspJson['id']
        self.assertTrue(helper.validateId(dset_uuid))
         
        # link new dataset as 'dset1'
        name = 'dset1'
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name 
        payload = {"id": dset_uuid}
        rsp = requests.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        
        # verify the dataspace is has a null dataspace
        req = self.endpoint + "/datasets/" + dset_uuid
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        shape = rspJson['shape']
        self.assertEqual(shape['class'], 'H5S_NULL')
        # verify type 
        type_json = rspJson["type"]
        self.assertEqual(type_json["class"], 'H5T_FLOAT')
        self.assertEqual(type_json['base'], 'H5T_IEEE_F32LE')

    def testResizableDataset(self):
        # test Dataset with null dataspace type
        print("testResizableDataset", self.base_domain)
        headers = helper.getRequestHeaders(domain=self.base_domain)

        # get domain
        req = helper.getEndpoint() + '/'
        rsp = requests.get(req, headers=headers)
        rspJson = json.loads(rsp.text)
        self.assertTrue("root" in rspJson)
        root_uuid = rspJson["root"]

        # create the dataset 
        req = self.endpoint + "/datasets"
        payload = {'type': 'H5T_IEEE_F32LE', 'shape': 10, 'maxdims': 20}
        payload['creationProperties'] = {'fillValue': 3.12 }
        req = self.endpoint + "/datasets"
        rsp = requests.post(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)  # create dataset
        rspJson = json.loads(rsp.text)
        dset_uuid = rspJson['id']
        self.assertTrue(helper.validateId(dset_uuid))
         
        # link new dataset as 'resizable'
        name = 'resizable'
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name 
        payload = {"id": dset_uuid}
        rsp = requests.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        
        # verify type and shape
        req = helper.getEndpoint() + "/datasets/" + dset_uuid
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        type_json = rspJson['type']
        self.assertEqual(type_json['class'], 'H5T_FLOAT')
        self.assertEqual(type_json['base'], 'H5T_IEEE_F32LE')
        shape = rspJson['shape']
        self.assertEqual(shape['class'], 'H5S_SIMPLE')
        
        self.assertEqual(len(shape['dims']), 1)
        self.assertEqual(shape['dims'][0], 10)  
        self.assertTrue('maxdims' in shape)
        self.assertEqual(shape['maxdims'][0], 20)

        creationProps = rspJson["creationProperties"]
        self.assertEqual(creationProps["fillValue"], 3.12)

        # verify shape using the GET shape request
        req = req + "/shape"
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("type" not in rspJson)
        self.assertTrue("shape" in rspJson)
        shape = rspJson['shape']
        self.assertEqual(shape['class'], 'H5S_SIMPLE') 
        self.assertEqual(len(shape['dims']), 1)
        self.assertEqual(shape['dims'][0], 10)  
        self.assertTrue('maxdims' in shape)
        self.assertEqual(shape['maxdims'][0], 20)

        # resize the dataset to 15 elements
        payload = {"shape": 15}
        rsp = requests.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)

        # verify updated-shape using the GET shape request
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("shape" in rspJson)
        shape = rspJson['shape']
        self.assertEqual(shape['class'], 'H5S_SIMPLE') 
        self.assertEqual(len(shape['dims']), 1)
        self.assertEqual(shape['dims'][0], 15)  # increased to 15  
        self.assertTrue('maxdims' in shape)
        self.assertEqual(shape['maxdims'][0], 20)

    def testResizableUnlimitedDataset(self):
        # test Dataset with unlimited dimension
        print("testResizableUnlimitedDataset", self.base_domain)
        headers = helper.getRequestHeaders(domain=self.base_domain)

        # get domain
        req = helper.getEndpoint() + '/'
        rsp = requests.get(req, headers=headers)
        rspJson = json.loads(rsp.text)
        self.assertTrue("root" in rspJson)
        root_uuid = rspJson["root"]

        # create the dataset 
        req = self.endpoint + "/datasets"
        payload = {'type': 'H5T_IEEE_F32LE', 'shape': [10, 20], 'maxdims': [30, 0]}
        payload['creationProperties'] = {'fillValue': 3.12 }
        req = self.endpoint + "/datasets"
        rsp = requests.post(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)  # create dataset
        rspJson = json.loads(rsp.text)
        dset_uuid = rspJson['id']
        self.assertTrue(helper.validateId(dset_uuid))
         
        # link new dataset as 'resizable'
        name = 'resizable'
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name 
        payload = {"id": dset_uuid}
        rsp = requests.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        
        # verify type and shape
        req = helper.getEndpoint() + "/datasets/" + dset_uuid
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        type_json = rspJson['type']
        self.assertEqual(type_json['class'], 'H5T_FLOAT')
        self.assertEqual(type_json['base'], 'H5T_IEEE_F32LE')
        shape = rspJson['shape']
        self.assertEqual(shape['class'], 'H5S_SIMPLE')
        
        self.assertEqual(len(shape['dims']), 2)
        self.assertEqual(shape['dims'][0], 10) 
        self.assertEqual(shape['dims'][1], 20)  
        self.assertTrue('maxdims' in shape)
        self.assertEqual(shape['maxdims'][0], 30)
        self.assertEqual(shape['maxdims'][1], 0)

        # verify shape using the GET shape request
        req = req + "/shape"
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("type" not in rspJson)
        self.assertTrue("shape" in rspJson)
        shape = rspJson['shape']
        self.assertEqual(shape['class'], 'H5S_SIMPLE') 
        self.assertEqual(len(shape['dims']), 2)
        self.assertEqual(shape['dims'][0], 10)  
        self.assertEqual(shape['dims'][1], 20)  
        self.assertTrue('maxdims' in shape)
        self.assertEqual(len(shape['maxdims']), 2)
        self.assertEqual(shape['maxdims'][0], 30)
        self.assertEqual(shape['maxdims'][1], 0)

        # resize the second dimension  to 500 elements
        payload = {"shape": [10, 500]}

        rsp = requests.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)

        # verify updated-shape using the GET shape request
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("shape" in rspJson)
        shape = rspJson['shape']
        self.assertEqual(shape['class'], 'H5S_SIMPLE') 
        self.assertEqual(len(shape['dims']), 2)
        self.assertEqual(shape['dims'][0], 10)  
        self.assertEqual(shape['dims'][1], 500)  
        self.assertTrue('maxdims' in shape)
        self.assertEqual(len(shape['maxdims']), 2)
        self.assertEqual(shape['maxdims'][0], 30)
        self.assertEqual(shape['maxdims'][1], 0)

    def testCreationPropertiesLayoutDataset(self):
        # test Dataset with creation property list
        print("testCreationPropertiesLayoutDataset", self.base_domain)
        headers = helper.getRequestHeaders(domain=self.base_domain)
        # get domain
        req = helper.getEndpoint() + '/'
        rsp = requests.get(req, headers=headers)
        rspJson = json.loads(rsp.text)
        self.assertTrue("root" in rspJson)
        root_uuid = rspJson["root"]

        # create the dataset 
        req = self.endpoint + "/datasets"
        # Create ~1GB dataset
        payload = {'type': 'H5T_IEEE_F32LE', 'shape': [365, 780, 1024], 'maxdims': [0, 780, 1024]}
        # define a chunk layout with 4 chunks per 'slice'
        # chunk size is 798720 bytes
        payload['creationProperties'] = {'layout': {'class': 'H5D_CHUNKED', 'dims': [1, 390, 512] }}
        req = self.endpoint + "/datasets"
        rsp = requests.post(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)  # create dataset
        rspJson = json.loads(rsp.text)
        dset_uuid = rspJson['id']
        self.assertTrue(helper.validateId(dset_uuid))
         
        # link new dataset as 'chunktest'
        name = 'chunktest'
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name 
        payload = {"id": dset_uuid}
        rsp = requests.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        # verify layout
        req = helper.getEndpoint() + "/datasets/" + dset_uuid
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("layout" in rspJson)
        layout_json = rspJson["layout"]
        self.assertTrue("class" in layout_json)
        self.assertEqual(layout_json["class"], 'H5D_CHUNKED')
        self.assertTrue("dims" in layout_json)
        self.assertEqual(layout_json["dims"], [1, 390, 512])

    
    def testInvalidFillValue(self):
        # test Dataset with simple type and fill value that is incompatible with the type
        print("testInvalidFillValue", self.base_domain)
        headers = helper.getRequestHeaders(domain=self.base_domain)

        # get domain
        req = helper.getEndpoint() + '/'
        rsp = requests.get(req, headers=headers)
        rspJson = json.loads(rsp.text)
        self.assertTrue("root" in rspJson)
        
        fill_value = 'XXXX'  # can't convert to int!
        # create the dataset 
        req = self.endpoint + "/datasets"
        payload = {'type': 'H5T_STD_I32LE', 'shape': 10}
        payload['creationProperties'] = {'fillValue': fill_value }
        req = self.endpoint + "/datasets"
        rsp = requests.post(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 400)  # invalid param

    def testAutoChunk1dDataset(self):
        # test Dataset where chunk layout is set automatically
        print("testAutoChunk1dDataset", self.base_domain)
        headers = helper.getRequestHeaders(domain=self.base_domain)
        # get domain
        req = helper.getEndpoint() + '/'
        rsp = requests.get(req, headers=headers)
        rspJson = json.loads(rsp.text)
        self.assertTrue("root" in rspJson)
        root_uuid = rspJson["root"]

        # create the dataset 
        req = self.endpoint + "/datasets"
        # 50K x 80K dataset
        extent = 1000 * 1000 * 1000
        dims = [extent,]
        fields = (  {'name': 'x', 'type': 'H5T_IEEE_F64LE'}, 
                    {'name': 'y', 'type': 'H5T_IEEE_F64LE'},
                    {'name': 'z', 'type': 'H5T_IEEE_F64LE'}) 
        datatype = {'class': 'H5T_COMPOUND', 'fields': fields }

        payload = {'type': datatype, 'shape': dims }
        # the following should get ignored as too small
        payload['creationProperties'] = {'layout': {'class': 'H5D_CHUNKED', 'dims': [10,] }}
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

        # verify layout
        req = helper.getEndpoint() + "/datasets/" + dset_uuid
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("layout" in rspJson)
        layout_json = rspJson["layout"]
        self.assertTrue("class" in layout_json)
        self.assertEqual(layout_json["class"], 'H5D_CHUNKED')
        self.assertTrue("dims" in layout_json)
        layout = layout_json["dims"]
        self.assertEqual(len(layout), 1)
        self.assertTrue(layout[0] < dims[0])
        chunk_size = layout[0] * 8 * 3  # three 64bit 
        # chunk size should be between chunk min and max
        self.assertTrue(chunk_size >= CHUNK_MIN)
        self.assertTrue(chunk_size <= CHUNK_MAX)
     
    def testAutoChunk2dDataset(self):
        # test Dataset where chunk layout is set automatically
        print("testAutoChunk2dDataset", self.base_domain)
        headers = helper.getRequestHeaders(domain=self.base_domain)
        # get domain
        req = helper.getEndpoint() + '/'
        rsp = requests.get(req, headers=headers)
        rspJson = json.loads(rsp.text)
        self.assertTrue("root" in rspJson)
        root_uuid = rspJson["root"]

        # create the dataset 
        req = self.endpoint + "/datasets"
        # 50K x 80K dataset
        dims = [50000, 80000]
        payload = {'type': 'H5T_IEEE_F32LE', 'shape': dims }
        
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

        # verify layout
        req = helper.getEndpoint() + "/datasets/" + dset_uuid
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("layout" in rspJson)
        layout_json = rspJson["layout"]
        self.assertTrue("class" in layout_json)
        self.assertEqual(layout_json["class"], 'H5D_CHUNKED')
        self.assertTrue("dims" in layout_json)
        layout = layout_json["dims"]
        self.assertEqual(len(layout), 2)
        self.assertTrue(layout[0] < dims[0])
        self.assertTrue(layout[1] < dims[1])
        chunk_size = layout[0] * layout[1] * 4
        # chunk size should be between chunk min and max
        self.assertTrue(chunk_size >= CHUNK_MIN)
        self.assertTrue(chunk_size <= CHUNK_MAX)

    
    def testMinChunkSizeDataset(self):
        # test Dataset where chunk layout is adjusted if provided
        # layout is too small
        print("testMinChunkSizeDataset", self.base_domain)
        headers = helper.getRequestHeaders(domain=self.base_domain)
        # get domain
        req = helper.getEndpoint() + '/'
        rsp = requests.get(req, headers=headers)
        rspJson = json.loads(rsp.text)
        self.assertTrue("root" in rspJson)
        root_uuid = rspJson["root"]

        # create the dataset 
        req = self.endpoint + "/datasets"
        # 50K x 80K dataset
        dims = [50000, 80000]
        payload = {'type': 'H5T_IEEE_F32LE', 'shape': dims }
        # define a chunk layout with lots of small chunks
        payload['creationProperties'] = {'layout': {'class': 'H5D_CHUNKED', 'dims': [10, 10] }}
      
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

        # verify layout
        req = helper.getEndpoint() + "/datasets/" + dset_uuid
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("layout" in rspJson)
        layout_json = rspJson["layout"]
        self.assertTrue("class" in layout_json)
        self.assertEqual(layout_json["class"], 'H5D_CHUNKED')
        self.assertTrue("dims" in layout_json)
        layout = layout_json["dims"]
        self.assertEqual(len(layout), 2)
        self.assertTrue(layout[0] < dims[0])
        self.assertTrue(layout[1] < dims[1])
        chunk_size = layout[0] * layout[1] * 4
        # chunk size should be between chunk min and max
        self.assertTrue(chunk_size >= CHUNK_MIN)
        self.assertTrue(chunk_size <= CHUNK_MAX)


    def testPostWithLink(self):
        print("testPostWithLink", self.base_domain)
        headers = helper.getRequestHeaders(domain=self.base_domain)

        # get domain
        req = helper.getEndpoint() + '/'
        rsp = requests.get(req, headers=headers)
        rspJson = json.loads(rsp.text)
        self.assertTrue("root" in rspJson)
        root_uuid = rspJson["root"]

        # get root group and verify link count is 0
        req = helper.getEndpoint() + '/groups/' + root_uuid
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertEqual(rspJson["linkCount"], 0)
        
        type_vstr = {"charSet": "H5T_CSET_ASCII", 
            "class": "H5T_STRING", 
            "strPad": "H5T_STR_NULLTERM", 
            "length": "H5T_VARIABLE" } 
        payload = {'type': type_vstr, 'shape': 10,
             'link': {'id': root_uuid, 'name': 'linked_dset'} }
        req = self.endpoint + "/datasets"
        rsp = requests.post(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)  # create dataset
        rspJson = json.loads(rsp.text)
        dset_uuid = rspJson['id']
        self.assertTrue(helper.validateId(dset_uuid))

        # get root group and verify link count is 1
        req = helper.getEndpoint() + '/groups/' + root_uuid
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertEqual(rspJson["linkCount"], 1)

        # read the link back and verify
        req = helper.getEndpoint() + "/groups/" + root_uuid + "/links/linked_dset"
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)  # link doesn't exist yet
        rspJson = json.loads(rsp.text)
        self.assertTrue("link" in rspJson)
        link_json = rspJson["link"]
        self.assertEqual(link_json["collection"], "datasets")
        self.assertEqual(link_json["class"], "H5L_TYPE_HARD")
        self.assertEqual(link_json["title"], "linked_dset")
        self.assertEqual(link_json["id"], dset_uuid)

    def testPostCommittedType(self):
        print("testPostCommittedType", self.base_domain)
        headers = helper.getRequestHeaders(domain=self.base_domain)

        # get domain
        req = helper.getEndpoint() + '/'
        rsp = requests.get(req, headers=headers)
        rspJson = json.loads(rsp.text)
        self.assertTrue("root" in rspJson)
        root_uuid = rspJson["root"]
        
        # create the datatype
        payload = {'type': 'H5T_IEEE_F32LE'}
        req = self.endpoint + "/datatypes"
        rsp = requests.post(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)  # create datatype
        rspJson = json.loads(rsp.text)
        dtype_uuid = rspJson['id']
        self.assertTrue(helper.validateId(dtype_uuid))
         
        # link new datatype as 'dtype1'
        name = 'dtype1'
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name 
        payload = {'id': dtype_uuid}
        rsp = requests.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        
        # create the dataset
        payload = {'type': dtype_uuid, 'shape': [10, 10]}
        req = self.endpoint + "/datasets"
        rsp = requests.post(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)  # create dataset
        rspJson = json.loads(rsp.text)
        dset_uuid = rspJson['id']
        self.assertTrue(helper.validateId(dset_uuid))
         
        # link new dataset as 'dset1'
        name = 'dset1'
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name 
        payload = {"id": dset_uuid}
        rsp = requests.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # Fetch the dataset type and verify dtype_uuid
        req = helper.getEndpoint() + "/datasets/" + dset_uuid + "/type"
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("type" in rspJson)
        rsp_type = rspJson["type"]
        self.assertTrue("base" in rsp_type)
        self.assertEqual(rsp_type["base"], 'H5T_IEEE_F32LE')
        self.assertTrue("class" in rsp_type)
        self.assertEqual(rsp_type["class"], 'H5T_FLOAT')
        self.assertTrue("id" in rsp_type)
        self.assertEqual(rsp_type["id"], dtype_uuid)
         
        
             
if __name__ == '__main__':
    #setup test files
    
    unittest.main()
    
