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

        # Get the type
        rsp = requests.get(req + "/type", headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("type" in rspJson)
        self.assertTrue(rspJson["type"], "H5T_IEEE_F32LE")
        self.assertTrue("hrefs" in rspJson)

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
        
        # delete the dataset
        headers = helper.getRequestHeaders(domain=self.base_domain)
        rsp = requests.delete(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue(rspJson is not None)

        # a get for the dataset should now return 410 (GONE)
        rsp = requests.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 410)

    def testDelete(self):
        # test Delete
        print("testDelete", self.base_domain)
        headers = helper.getRequestHeaders(domain=self.base_domain)

        # get domain
        req = helper.getEndpoint() + '/'
        rsp = requests.get(req, headers=headers)
        rspJson = json.loads(rsp.text)
        self.assertTrue("root" in rspJson)
        root_id = rspJson["root"]
  
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
        # test Dataset with null dataspace type
        print("testtestResizableUnlimitedDataset", self.base_domain)
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
        self.assertEqual(shape['maxdims'][1], 'H5S_UNLIMITED')

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
        self.assertEqual(shape['maxdims'][1], 'H5S_UNLIMITED')

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
        self.assertEqual(shape['maxdims'][1], 'H5S_UNLIMITED')

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
        payload = {'type': 'H5T_IEEE_F32LE', 'shape': [100, 200], 'maxdims': [100, 0]}
        payload['creationProperties'] = {'layout': {'class': 'H5D_CHUNKED', 'dims': [10, 10] }}
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
        layout = rspJson["layout"]
        self.assertEqual(layout, [10, 10])

    def testAutoChunkDataset(self):
        # test Dataset where chunk dataset is set automatically
        print("testAutoChunkDataset", self.base_domain)
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
         
        # link new dataset as 'autochunktest'
        name = 'autochunktest'
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
        layout = rspJson["layout"]
        self.assertEqual(len(layout), 2)
        self.assertTrue(layout[0] < dims[0])
        self.assertTrue(layout[1] < dims[1])
         
        

        


    
             
if __name__ == '__main__':
    #setup test files
    
    unittest.main()
    
