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
import json
import time
import numpy as np
import helper
import config

# min/max chunk size - these can be set by config, but
# practially the min config value should be larger than
# CHUNK_MIN and the max config value should less than
# CHUNK_MAX
CHUNK_MIN = 1024  # lower limit  (1024b)
CHUNK_MAX = 50 * 1024 * 1024  # upper limit (50M)


class DatasetTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(DatasetTest, self).__init__(*args, **kwargs)
        self.base_domain = helper.getTestDomainName(self.__class__.__name__)
        helper.setupDomain(self.base_domain, folder=True)
        self.endpoint = helper.getEndpoint()

    def setUp(self):
        self.session = helper.getSession()

    def tearDown(self):
        if self.session:
            self.session.close()

        # main

    def testScalarDataset(self):
        # Test creation/deletion of scalar dataset obj
        domain = self.base_domain + "/testScalarDataset.h5"
        helper.setupDomain(domain)
        print("testScalarDataset", domain)
        headers = helper.getRequestHeaders(domain=domain)
        req = self.endpoint + "/"

        # Get root uuid
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        root_uuid = rspJson["root"]
        helper.validateId(root_uuid)

        # create a dataset obj
        data = {"type": "H5T_IEEE_F32LE"}
        req = self.endpoint + "/datasets"
        rsp = self.session.post(req, data=json.dumps(data), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)
        self.assertEqual(rspJson["attributeCount"], 0)
        dset_id = rspJson["id"]
        self.assertTrue(helper.validateId(dset_id))

        # read back the obj
        req = self.endpoint + "/datasets/" + dset_id
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        for name in (
            "id",
            "shape",
            "hrefs",
            "layout",
            "creationProperties",
            "attributeCount",
            "created",
            "lastModified",
            "root",
            "domain",
        ):
            self.assertTrue(name in rspJson)
        self.assertEqual(rspJson["id"], dset_id)
        self.assertEqual(rspJson["root"], root_uuid)
        self.assertEqual(rspJson["domain"], domain)
        self.assertEqual(rspJson["attributeCount"], 0)
        shape_json = rspJson["shape"]
        self.assertTrue(shape_json["class"], "H5S_SCALAR")
        self.assertTrue(rspJson["type"], "H5T_IEEE_F32LE")

        # Get the type
        rsp = self.session.get(req + "/type", headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("type" in rspJson)
        self.assertTrue(rspJson["type"], "H5T_IEEE_F32LE")
        self.assertTrue("hrefs" in rspJson)
        hrefs = rspJson["hrefs"]
        self.assertEqual(len(hrefs), 3)

        # Get the shape
        rsp = self.session.get(req + "/shape", headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("created" in rspJson)
        self.assertTrue("lastModified" in rspJson)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("shape" in rspJson)
        shape_json = rspJson["shape"]
        self.assertTrue(shape_json["class"], "H5S_SCALAR")

        # try getting verbose info
        params = {"verbose": 1}
        rsp = self.session.get(req, params=params, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        for name in (
            "id",
            "shape",
            "hrefs",
            "layout",
            "creationProperties",
            "attributeCount",
            "created",
            "lastModified",
            "root",
            "domain",
        ):
            self.assertTrue(name in rspJson)
        # self.assertTrue("num_chunks" in rspJson)
        # self.assertTrue("allocated_size" in rspJson)

        # try get with a different user (who has read permission)
        user2_name = config.get('user2_name')
        if user2_name:
            headers = helper.getRequestHeaders(domain=domain, username=user2_name)
            rsp = self.session.get(req, headers=headers)
            if config.get("default_public"):
                self.assertEqual(rsp.status_code, 200)
                rspJson = json.loads(rsp.text)
                self.assertEqual(rspJson["id"], dset_id)
            else:
                self.assertEqual(rsp.status_code, 403)
        else:
            print('user2_name not set')

        # try to do a GET with a different domain (should fail)
        another_domain = self.base_domain + "/testScalarDataset2.h5"
        helper.setupDomain(another_domain)
        print("testScalarDataset2", another_domain)
        headers = helper.getRequestHeaders(domain=another_domain)
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 400)

        # try DELETE with user who doesn't have create permission on this domain
        if user2_name:
            headers = helper.getRequestHeaders(domain=domain, username=user2_name)
            rsp = self.session.delete(req, headers=headers)
            self.assertEqual(rsp.status_code, 403)  # forbidden
        else:
            print("user2_name not set")

        # try to do a DELETE with a different domain (should fail)
        # Test creation/deletion of scalar dataset obj
        headers = helper.getRequestHeaders(domain=another_domain)
        rsp = self.session.delete(req, headers=headers)
        self.assertEqual(rsp.status_code, 400)

        # delete the dataset
        headers = helper.getRequestHeaders(domain=domain)
        rsp = self.session.delete(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue(rspJson is not None)

        # a get for the dataset should now return 410 (GONE)
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 410)

    def testScalarEmptyDimsDataset(self):
        # Test creation/deletion of scalar dataset obj
        domain = self.base_domain + "/testScalarEmptyDimsDataset.h5"
        helper.setupDomain(domain)
        print("testScalarEmptyDimsDataset", domain)
        headers = helper.getRequestHeaders(domain=domain)
        req = self.endpoint + "/"

        # Get root uuid
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        root_uuid = rspJson["root"]
        helper.validateId(root_uuid)

        # create a dataset obj
        data = {"type": "H5T_IEEE_F32LE", "shape": []}
        req = self.endpoint + "/datasets"
        rsp = self.session.post(req, data=json.dumps(data), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)
        self.assertEqual(rspJson["attributeCount"], 0)
        dset_id = rspJson["id"]
        self.assertTrue(helper.validateId(dset_id))

        # read back the obj
        req = self.endpoint + "/datasets/" + dset_id
        rsp = self.session.get(req, headers=headers)
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
        self.assertFalse("dims" in shape_json)
        self.assertTrue("type" in rspJson)
        self.assertTrue(rspJson["type"], "H5T_IEEE_F32LE")

    def testGet(self):
        domain = helper.getTestDomain("tall.h5")
        print("testGetDomain", domain)
        headers = helper.getRequestHeaders(domain=domain)

        # verify domain exists
        req = helper.getEndpoint() + "/"
        rsp = self.session.get(req, headers=headers)
        if rsp.status_code != 200:
            print(
                "WARNING: Failed to get domain: {}. Is test data setup?".format(domain)
            )
            return  # abort rest of test
        domainJson = json.loads(rsp.text)
        root_uuid = domainJson["root"]

        # get the dataset uuid
        dset_uuid = helper.getUUIDByPath(
            domain, "/g1/g1.1/dset1.1.1", session=self.session
        )
        self.assertTrue(dset_uuid.startswith("d-"))

        # get the dataset json
        req = helper.getEndpoint() + "/datasets/" + dset_uuid
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        for name in (
            "id",
            "shape",
            "hrefs",
            "layout",
            "creationProperties",
            "attributeCount",
            "created",
            "lastModified",
            "root",
            "domain",
        ):
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
        self.assertEqual(shape["class"], "H5S_SIMPLE")
        self.assertEqual(shape["dims"], [10, 10])
        self.assertEqual(shape["maxdims"], [10, 10])

        layout = rspJson["layout"]
        self.assertEqual(layout["class"], "H5D_CHUNKED")
        self.assertEqual(layout["dims"], [10, 10])
        self.assertTrue("partition_count" not in layout)

        type = rspJson["type"]
        for name in ("base", "class"):
            self.assertTrue(name in type)
        self.assertEqual(type["class"], "H5T_INTEGER")
        self.assertEqual(type["base"], "H5T_STD_I32BE")

        self.assertEqual(rspJson["attributeCount"], 2)

        # these properties should only be available when verbose is used
        self.assertFalse("num_chunks" in rspJson)
        self.assertFalse("allocated_size" in rspJson)

        # attribute should only be here if include_attrs is used
        self.assertFalse("attributes" in rspJson)

        now = time.time()
        # the object shouldn't have been just created or updated
        self.assertTrue(rspJson["created"] < now - 10)
        self.assertTrue(rspJson["lastModified"] < now - 10)

        # request the dataset path
        req = helper.getEndpoint() + "/datasets/" + dset_uuid
        params = {"getalias": 1}
        rsp = self.session.get(req, params=params, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("alias" in rspJson)
        self.assertEqual(rspJson["alias"], ["/g1/g1.1/dset1.1.1"])

        # request attributes be included
        params = {"include_attrs": 1}
        rsp = self.session.get(req, params=params, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("attributes" in rspJson)
        attrs = rspJson["attributes"]
        self.assertTrue("attr1" in attrs)
        self.assertTrue("attr2" in attrs)

    def testGetByPath(self):
        domain = helper.getTestDomain("tall.h5")
        print("testGetDomain", domain)
        headers = helper.getRequestHeaders(domain=domain)

        # verify domain exists
        req = helper.getEndpoint() + "/"
        rsp = self.session.get(req, headers=headers)
        if rsp.status_code != 200:
            print(f"WARNING: Failed to get domain: {domain}. Is test data setup?")
            return  # abort rest of test
        domainJson = json.loads(rsp.text)
        root_uuid = domainJson["root"]

        # get the dataset at "/g1/g1.1/dset1.1.1"
        h5path = "/g1/g1.1/dset1.1.1"
        req = helper.getEndpoint() + "/datasets/"
        params = {"h5path": h5path}
        rsp = self.session.get(req, headers=headers, params=params)
        self.assertEqual(rsp.status_code, 200)

        rspJson = json.loads(rsp.text)
        for name in (
            "id",
            "shape",
            "hrefs",
            "layout",
            "creationProperties",
            "attributeCount",
            "created",
            "lastModified",
            "root",
            "domain",
        ):
            self.assertTrue(name in rspJson)

        # get the dataset via a relative apth "g1/g1.1/dset1.1.1"
        h5path = "g1/g1.1/dset1.1.1"
        req = helper.getEndpoint() + "/datasets/"
        params = {"h5path": h5path, "grpid": root_uuid}
        rsp = self.session.get(req, headers=headers, params=params)
        self.assertEqual(rsp.status_code, 200)

        rspJson = json.loads(rsp.text)
        for name in (
            "id",
            "shape",
            "hrefs",
            "layout",
            "creationProperties",
            "attributeCount",
            "created",
            "lastModified",
            "root",
            "domain",
        ):
            self.assertTrue(name in rspJson)

        # get the dataset uuid and verify it matches what we got by h5path
        dset_uuid = helper.getUUIDByPath(
            domain, "/g1/g1.1/dset1.1.1", session=self.session
        )
        self.assertTrue(dset_uuid.startswith("d-"))
        self.assertEqual(dset_uuid, rspJson["id"])

        # try a invalid link and verify a 404 is returened
        h5path = "/g1/foobar"
        req = helper.getEndpoint() + "/datasets/"
        params = {"h5path": h5path}
        rsp = self.session.get(req, headers=headers, params=params)
        self.assertEqual(rsp.status_code, 404)

        # try passing a path to a group and verify we get 404
        h5path = "/g1/g1.1"
        req = helper.getEndpoint() + "/datasets/"
        params = {"h5path": h5path}
        rsp = self.session.get(req, headers=headers, params=params)
        self.assertEqual(rsp.status_code, 404)

    def testGetVerbose(self):
        domain = helper.getTestDomain("tall.h5")
        print("testGetDomain", domain)
        headers = helper.getRequestHeaders(domain=domain)

        # verify domain exists
        req = helper.getEndpoint() + "/"
        rsp = self.session.get(req, headers=headers)
        if rsp.status_code != 200:
            print(f"WARNING: Failed to get domain: {domain}. Is test data setup?")
            return  # abort rest of test
        domainJson = json.loads(rsp.text)
        root_uuid = domainJson["root"]
        self.assertTrue(helper.validateId(root_uuid))

        # get the dataset uuid
        dset_uuid = helper.getUUIDByPath(
            domain, "/g1/g1.1/dset1.1.1", session=self.session
        )
        self.assertTrue(dset_uuid.startswith("d-"))

        # get the dataset json
        req = helper.getEndpoint() + "/datasets/" + dset_uuid
        params = {"verbose": 1}
        rsp = self.session.get(req, params=params, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        for name in (
            "id",
            "shape",
            "hrefs",
            "layout",
            "creationProperties",
            "attributeCount",
            "created",
            "lastModified",
            "root",
            "domain",
        ):
            self.assertTrue(name in rspJson)

        # these properties should only be available when verbose is used

        self.assertTrue("num_chunks" in rspJson)
        self.assertTrue("allocated_size" in rspJson)

    def testDelete(self):
        # test Delete
        domain = self.base_domain + "/testDelete.h5"
        helper.setupDomain(domain)
        print("testDelete", domain)
        headers = helper.getRequestHeaders(domain=domain)

        # get domain
        req = helper.getEndpoint() + "/"
        rsp = self.session.get(req, headers=headers)
        rspJson = json.loads(rsp.text)
        self.assertTrue("root" in rspJson)

        # create a new dataset
        req = helper.getEndpoint() + "/datasets"
        rsp = self.session.post(req, headers=headers)
        data = {"type": "H5T_IEEE_F32LE"}
        req = self.endpoint + "/datasets"
        rsp = self.session.post(req, data=json.dumps(data), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)
        self.assertEqual(rspJson["attributeCount"], 0)
        dset_id = rspJson["id"]
        self.assertTrue(helper.validateId(dset_id))

        # verify we can do a get on the new dataset
        req = helper.getEndpoint() + "/datasets/" + dset_id
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("id" in rspJson)
        self.assertEqual(rspJson["id"], dset_id)

        # try DELETE with user who doesn't have create permission on this domain
        user2_name = config.get('user2_name')
        if user2_name:
            headers = helper.getRequestHeaders(domain=domain, username=user2_name)
            rsp = self.session.delete(req, headers=headers)
            self.assertEqual(rsp.status_code, 403)  # forbidden
        else:
            print("test_user2 not set")

        # try to do a DELETE with a different domain (should fail)
        another_domain = helper.getParentDomain(domain)
        headers = helper.getRequestHeaders(domain=another_domain)
        req = helper.getEndpoint() + "/datasets/" + dset_id
        rsp = self.session.delete(req, headers=headers)
        self.assertEqual(rsp.status_code, 400)

        # delete the new dataset
        headers = helper.getRequestHeaders(domain)
        rsp = self.session.delete(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue(rspJson is not None)

        # a get for the dataset should now return 410 (GONE)
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 410)

    def testCompound(self):
        # test Dataset with compound type
        domain = self.base_domain + "/testCompound.h5"
        helper.setupDomain(domain)
        print("testCompound", domain)
        headers = helper.getRequestHeaders(domain=domain)

        # get domain
        req = helper.getEndpoint() + "/"
        rsp = self.session.get(req, headers=headers)
        rspJson = json.loads(rsp.text)
        self.assertTrue("root" in rspJson)
        root_uuid = rspJson["root"]

        fields = (
            {"name": "temp", "type": "H5T_STD_I32LE"},
            {"name": "pressure", "type": "H5T_IEEE_F32LE"},
        )
        datatype = {"class": "H5T_COMPOUND", "fields": fields}
        payload = {"type": datatype, "shape": 10}
        req = self.endpoint + "/datasets"
        rsp = self.session.post(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)  # create dataset
        rspJson = json.loads(rsp.text)
        dset_uuid = rspJson["id"]
        self.assertTrue(helper.validateId(dset_uuid))

        # link the new dataset
        name = "dset"
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
        payload = {"id": dset_uuid}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

    def testCompoundDuplicateMember(self):
        # test Dataset with compound type but field that is repeated
        domain = self.base_domain + "/testCompoundDuplicateMember.h5"
        helper.setupDomain(domain)
        print("testCompoundDupicateMember", domain)
        headers = helper.getRequestHeaders(domain=domain)

        # get domain
        req = helper.getEndpoint() + "/"
        rsp = self.session.get(req, headers=headers)
        rspJson = json.loads(rsp.text)
        self.assertTrue("root" in rspJson)
        root_uuid = rspJson["root"]
        self.assertTrue(helper.validateId(root_uuid))

        fields = (
            {"name": "x", "type": "H5T_STD_I32LE"},
            {"name": "x", "type": "H5T_IEEE_F32LE"},
        )
        datatype = {"class": "H5T_COMPOUND", "fields": fields}
        payload = {"type": datatype, "shape": 10}
        req = self.endpoint + "/datasets"
        rsp = self.session.post(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 400)  # Bad Request

    def testPostNullSpace(self):
        # test Dataset with null dataspace type
        domain = self.base_domain + "/testPostNullSpace.h5"
        helper.setupDomain(domain)

        print("testNullSpace", domain)
        headers = helper.getRequestHeaders(domain=domain)

        # get domain
        req = helper.getEndpoint() + "/"
        rsp = self.session.get(req, headers=headers)
        rspJson = json.loads(rsp.text)
        self.assertTrue("root" in rspJson)
        root_uuid = rspJson["root"]

        # pass H5S_NULL for shape
        payload = {"type": "H5T_IEEE_F32LE", "shape": "H5S_NULL"}
        req = self.endpoint + "/datasets"
        rsp = self.session.post(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)  # create dataset
        rspJson = json.loads(rsp.text)
        dset_uuid = rspJson["id"]
        self.assertTrue(helper.validateId(dset_uuid))

        # link new dataset as 'dset1'
        name = "dset1"
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
        payload = {"id": dset_uuid}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # verify the dataspace is has a null dataspace
        req = self.endpoint + "/datasets/" + dset_uuid
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        shape = rspJson["shape"]
        self.assertEqual(shape["class"], "H5S_NULL")
        # verify type
        type_json = rspJson["type"]
        self.assertEqual(type_json["class"], "H5T_FLOAT")
        self.assertEqual(type_json["base"], "H5T_IEEE_F32LE")

    def testResizableDataset(self):
        # test Dataset with resizable dimension dataspace type
        domain = self.base_domain + "/testResizableDataset.h5"
        helper.setupDomain(domain)
        print("testResizableDataset", domain)
        headers = helper.getRequestHeaders(domain=domain)

        # get domain
        req = helper.getEndpoint() + "/"
        rsp = self.session.get(req, headers=headers)
        rspJson = json.loads(rsp.text)
        self.assertTrue("root" in rspJson)
        root_uuid = rspJson["root"]

        # create the dataset
        req = self.endpoint + "/datasets"
        payload = {"type": "H5T_IEEE_F32LE", "shape": 10, "maxdims": 20}
        payload["creationProperties"] = {"fillValue": 3.12}
        req = self.endpoint + "/datasets"
        rsp = self.session.post(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)  # create dataset
        rspJson = json.loads(rsp.text)
        dset_uuid = rspJson["id"]
        self.assertTrue(helper.validateId(dset_uuid))

        # link new dataset as 'resizable'
        name = "resizable"
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
        payload = {"id": dset_uuid}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # verify type and shape
        req = helper.getEndpoint() + "/datasets/" + dset_uuid
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        type_json = rspJson["type"]
        self.assertEqual(type_json["class"], "H5T_FLOAT")
        self.assertEqual(type_json["base"], "H5T_IEEE_F32LE")
        shape = rspJson["shape"]
        self.assertEqual(shape["class"], "H5S_SIMPLE")

        self.assertEqual(len(shape["dims"]), 1)
        self.assertEqual(shape["dims"][0], 10)
        self.assertTrue("maxdims" in shape)
        self.assertEqual(shape["maxdims"][0], 20)

        creationProps = rspJson["creationProperties"]
        self.assertEqual(creationProps["fillValue"], 3.12)

        # verify shape using the GET shape request
        req = req + "/shape"
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("type" not in rspJson)
        self.assertTrue("shape" in rspJson)
        shape = rspJson["shape"]
        self.assertEqual(shape["class"], "H5S_SIMPLE")
        self.assertEqual(len(shape["dims"]), 1)
        self.assertEqual(shape["dims"][0], 10)
        self.assertTrue("maxdims" in shape)
        self.assertEqual(shape["maxdims"][0], 20)

        # resize the dataset to 15 elements
        payload = {"shape": 15}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)

        # reduce the size to 5 elements
        payload = {"shape": 5}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)

        # verify updated-shape using the GET shape request
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("shape" in rspJson)
        shape = rspJson["shape"]
        self.assertEqual(shape["class"], "H5S_SIMPLE")
        self.assertEqual(len(shape["dims"]), 1)
        self.assertEqual(shape["dims"][0], 15)  # increased to 15
        self.assertTrue("maxdims" in shape)
        self.assertEqual(shape["maxdims"][0], 20)

        # resize the dataset to 25 elements (should fail)
        payload = {"shape": 25}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 409)

    def testResizableUnlimitedDataset(self):
        # test Dataset with unlimited dimension
        domain = self.base_domain + "/testResizableUnlimitedDataset.h5"
        helper.setupDomain(domain)
        print("testResizableUnlimitedDataset", domain)
        headers = helper.getRequestHeaders(domain=domain)

        # get domain
        req = helper.getEndpoint() + "/"
        rsp = self.session.get(req, headers=headers)
        rspJson = json.loads(rsp.text)
        self.assertTrue("root" in rspJson)
        root_uuid = rspJson["root"]

        # create the dataset
        req = self.endpoint + "/datasets"
        payload = {"type": "H5T_IEEE_F32LE", "shape": [10, 20], "maxdims": [30, 0]}
        payload["creationProperties"] = {"fillValue": 3.12}
        req = self.endpoint + "/datasets"
        rsp = self.session.post(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)  # create dataset
        rspJson = json.loads(rsp.text)
        dset_uuid = rspJson["id"]
        self.assertTrue(helper.validateId(dset_uuid))

        # link new dataset as 'resizable'
        name = "resizable"
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
        payload = {"id": dset_uuid}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # verify type and shape
        req = helper.getEndpoint() + "/datasets/" + dset_uuid
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        type_json = rspJson["type"]
        self.assertEqual(type_json["class"], "H5T_FLOAT")
        self.assertEqual(type_json["base"], "H5T_IEEE_F32LE")
        shape = rspJson["shape"]
        self.assertEqual(shape["class"], "H5S_SIMPLE")

        self.assertEqual(len(shape["dims"]), 2)
        self.assertEqual(shape["dims"][0], 10)
        self.assertEqual(shape["dims"][1], 20)
        self.assertTrue("maxdims" in shape)
        self.assertEqual(shape["maxdims"][0], 30)
        self.assertEqual(shape["maxdims"][1], 0)

        # verify shape using the GET shape request
        req = req + "/shape"
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("type" not in rspJson)
        self.assertTrue("shape" in rspJson)
        shape = rspJson["shape"]
        self.assertEqual(shape["class"], "H5S_SIMPLE")
        self.assertEqual(len(shape["dims"]), 2)
        self.assertEqual(shape["dims"][0], 10)
        self.assertEqual(shape["dims"][1], 20)
        self.assertTrue("maxdims" in shape)
        self.assertEqual(len(shape["maxdims"]), 2)
        self.assertEqual(shape["maxdims"][0], 30)
        self.assertEqual(shape["maxdims"][1], 0)

        # resize the second dimension  to 500 elements
        payload = {"shape": [10, 500]}

        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)

        # verify updated-shape using the GET shape request
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("shape" in rspJson)
        shape = rspJson["shape"]
        self.assertEqual(shape["class"], "H5S_SIMPLE")
        self.assertEqual(len(shape["dims"]), 2)
        self.assertEqual(shape["dims"][0], 10)
        self.assertEqual(shape["dims"][1], 500)
        self.assertTrue("maxdims" in shape)
        self.assertEqual(len(shape["maxdims"]), 2)
        self.assertEqual(shape["maxdims"][0], 30)
        self.assertEqual(shape["maxdims"][1], 0)

    def testExtendDataset(self):
        # test extending dataset
        domain = self.base_domain + "/testExtendDataset.h5"
        helper.setupDomain(domain)
        print("testExtendDataset", domain)
        headers = helper.getRequestHeaders(domain=domain)

        # get domain
        req = helper.getEndpoint() + "/"
        rsp = self.session.get(req, headers=headers)
        rspJson = json.loads(rsp.text)
        self.assertTrue("root" in rspJson)
        root_uuid = rspJson["root"]

        # create the dataset
        req = self.endpoint + "/datasets"
        payload = {"type": "H5T_STD_I32LE", "shape": 10, "maxdims": 20}
        req = self.endpoint + "/datasets"
        rsp = self.session.post(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)  # create dataset
        rspJson = json.loads(rsp.text)
        dset_uuid = rspJson["id"]
        self.assertTrue(helper.validateId(dset_uuid))

        # link new dataset as 'extendable'
        name = "extendable"
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
        payload = {"id": dset_uuid}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # verify type and shape
        req = helper.getEndpoint() + "/datasets/" + dset_uuid
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        type_json = rspJson["type"]
        self.assertEqual(type_json["class"], "H5T_INTEGER")
        self.assertEqual(type_json["base"], "H5T_STD_I32LE")
        shape = rspJson["shape"]
        self.assertEqual(shape["class"], "H5S_SIMPLE")

        self.assertEqual(len(shape["dims"]), 1)
        self.assertEqual(shape["dims"][0], 10)
        self.assertTrue("maxdims" in shape)
        self.assertEqual(shape["maxdims"][0], 20)

        # verify shape using the GET shape request
        req = req + "/shape"
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("type" not in rspJson)
        self.assertTrue("shape" in rspJson)
        shape = rspJson["shape"]
        self.assertEqual(shape["class"], "H5S_SIMPLE")
        self.assertEqual(len(shape["dims"]), 1)
        self.assertEqual(shape["dims"][0], 10)
        self.assertTrue("maxdims" in shape)
        self.assertEqual(shape["maxdims"][0], 20)

        # extend the dataset by 5 elements
        payload = {"extend": 5}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)
        self.assertTrue("selection" in rspJson)
        self.assertEqual(rspJson["selection"], "[10:15]")

        # verify updated-shape using the GET shape request
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("shape" in rspJson)
        shape = rspJson["shape"]
        self.assertEqual(shape["class"], "H5S_SIMPLE")
        self.assertEqual(len(shape["dims"]), 1)
        self.assertEqual(shape["dims"][0], 15)  # increased to 15
        self.assertTrue("maxdims" in shape)
        self.assertEqual(shape["maxdims"][0], 20)

        # try extending by 10 elements (should fail)
        payload = {"extend": 10}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 409)

    def testExtend2DDataset(self):
        # test extending dataset with two dimension
        domain = self.base_domain + "/testExtend2DDataset.h5"
        helper.setupDomain(domain)
        print("testExtend2DDataset", domain)
        headers = helper.getRequestHeaders(domain=domain)

        # get domain
        req = helper.getEndpoint() + "/"
        rsp = self.session.get(req, headers=headers)
        rspJson = json.loads(rsp.text)
        self.assertTrue("root" in rspJson)
        root_uuid = rspJson["root"]

        # create the dataset
        req = self.endpoint + "/datasets"
        payload = {"type": "H5T_STD_I32LE", "shape": [10, 20], "maxdims": [0, 0]}
        req = self.endpoint + "/datasets"
        rsp = self.session.post(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)  # create dataset
        rspJson = json.loads(rsp.text)
        dset_uuid = rspJson["id"]
        self.assertTrue(helper.validateId(dset_uuid))

        # link new dataset as 'extendable'
        name = "extendable"
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
        payload = {"id": dset_uuid}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # verify type and shape
        req = helper.getEndpoint() + "/datasets/" + dset_uuid
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        type_json = rspJson["type"]
        self.assertEqual(type_json["class"], "H5T_INTEGER")
        self.assertEqual(type_json["base"], "H5T_STD_I32LE")
        shape = rspJson["shape"]
        self.assertEqual(shape["class"], "H5S_SIMPLE")

        self.assertEqual(len(shape["dims"]), 2)
        self.assertEqual(shape["dims"][0], 10)
        self.assertEqual(shape["dims"][1], 20)
        self.assertTrue("maxdims" in shape)
        self.assertEqual(shape["maxdims"][0], 0)

        # verify shape using the GET shape request
        req = req + "/shape"
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("type" not in rspJson)
        self.assertTrue("shape" in rspJson)
        shape = rspJson["shape"]
        self.assertEqual(shape["class"], "H5S_SIMPLE")
        self.assertEqual(len(shape["dims"]), 2)
        self.assertEqual(shape["dims"][0], 10)
        self.assertTrue("maxdims" in shape)
        self.assertEqual(shape["maxdims"][0], 0)

        # extend the dataset by 5 elements in first dimension
        payload = {"extend": 5, "extend_dim": 0}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)
        self.assertTrue("selection" in rspJson)
        self.assertEqual(rspJson["selection"], "[10:15,:]")

        # verify updated-shape using the GET shape request
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("shape" in rspJson)
        shape = rspJson["shape"]
        self.assertEqual(shape["class"], "H5S_SIMPLE")
        self.assertEqual(len(shape["dims"]), 2)
        self.assertEqual(shape["dims"][0], 15)  # increased to 15
        self.assertEqual(shape["dims"][1], 20)  # still 20

        # extend the dataset by 10 elements in second dimension
        payload = {"extend": 10, "extend_dim": 1}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)
        self.assertTrue("selection" in rspJson)
        self.assertEqual(rspJson["selection"], "[:,20:30]")

        # verify updated-shape using the GET shape request
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("shape" in rspJson)
        shape = rspJson["shape"]
        self.assertEqual(shape["class"], "H5S_SIMPLE")
        self.assertEqual(len(shape["dims"]), 2)
        self.assertEqual(shape["dims"][0], 15)  # increased to 15
        self.assertEqual(shape["dims"][1], 30)  # increased to 30

    def testCreationPropertiesLayoutDataset(self):
        # test Dataset with creation property list
        domain = self.base_domain + "/testCreationPropertiesLayoutDataset.h5"
        helper.setupDomain(domain)

        print("testCreationPropertiesLayoutDataset", domain)
        headers = helper.getRequestHeaders(domain=domain)
        # get domain
        req = helper.getEndpoint() + "/"
        rsp = self.session.get(req, headers=headers)
        rspJson = json.loads(rsp.text)
        self.assertTrue("root" in rspJson)
        root_uuid = rspJson["root"]

        # create the dataset
        req = self.endpoint + "/datasets"
        # Create ~1GB dataset

        payload = {
            "type": "H5T_IEEE_F32LE",
            "shape": [365, 780, 1024],
            "maxdims": [0, 780, 1024],
        }
        # define a chunk layout with 4 chunks per 'slice'
        # chunk size is 798720 bytes
        gzip_filter = {
            "class": "H5Z_FILTER_DEFLATE",
            "id": 1,
            "level": 9,
            "name": "deflate",
        }
        payload["creationProperties"] = {
            "layout": {"class": "H5D_CHUNKED", "dims": [1, 390, 512]},
            "filters": [
                gzip_filter,
            ],
        }
        req = self.endpoint + "/datasets"
        rsp = self.session.post(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)  # create dataset
        rspJson = json.loads(rsp.text)
        dset_uuid = rspJson["id"]
        self.assertTrue(helper.validateId(dset_uuid))

        # link new dataset as 'chunktest'
        name = "chunktest"
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
        payload = {"id": dset_uuid}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        # verify layout
        req = helper.getEndpoint() + "/datasets/" + dset_uuid
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("layout" in rspJson)
        layout_json = rspJson["layout"]
        self.assertTrue("class" in layout_json)
        self.assertEqual(layout_json["class"], "H5D_CHUNKED")
        self.assertTrue("dims" in layout_json)
        self.assertEqual(layout_json["dims"], [1, 390, 1024])
        if config.get("max_chunks_per_folder") > 0:
            self.assertTrue("partition_count" in layout_json)
            self.assertEqual(layout_json["partition_count"], 10)

        # verify compression
        self.assertTrue("creationProperties" in rspJson)
        cpl = rspJson["creationProperties"]
        self.assertTrue("filters") in cpl
        filters = cpl["filters"]
        self.assertEqual(len(filters), 1)
        filter = filters[0]
        self.assertTrue("class") in filter
        self.assertEqual(filter["class"], "H5Z_FILTER_DEFLATE")
        self.assertTrue("level" in filter)
        self.assertEqual(filter["level"], 9)
        self.assertTrue("id" in filter)
        self.assertEqual(filter["id"], 1)

    def testCreationPropertiesContiguousDataset(self):
        # test Dataset with creation property list
        domain = self.base_domain + "/testCreationPropertiesContigousDataset.h5"
        helper.setupDomain(domain)

        print("testCreationPropertiesContiguousDataset", domain)
        headers = helper.getRequestHeaders(domain=domain)
        # get domain
        req = helper.getEndpoint() + "/"
        rsp = self.session.get(req, headers=headers)
        rspJson = json.loads(rsp.text)
        self.assertTrue("root" in rspJson)
        root_uuid = rspJson["root"]

        # create the dataset
        req = self.endpoint + "/datasets"
        # Create ~1GB dataset

        layout = {"class": "H5D_CONTIGUOUS"}
        gzip_filter = {
            "class": "H5Z_FILTER_DEFLATE",
            "id": 1,
            "level": 9,
            "name": "deflate",
        }

        creationProperties = {"layout": layout, "filters": [gzip_filter, ]}

        payload = {"creationProperties": creationProperties,
                   "type": "H5T_IEEE_F32LE",
                   "shape": [10, 20]
                   }

        req = self.endpoint + "/datasets"
        rsp = self.session.post(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)  # create dataset
        rspJson = json.loads(rsp.text)
        dset_uuid = rspJson["id"]
        self.assertTrue(helper.validateId(dset_uuid))

        # link new dataset as 'contiguous_test'
        name = "contiguous_test"
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
        payload = {"id": dset_uuid}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        # verify layout
        req = helper.getEndpoint() + "/datasets/" + dset_uuid
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("layout" in rspJson)
        layout_json = rspJson["layout"]
        self.assertTrue("class" in layout_json)
        self.assertEqual(layout_json["class"], "H5D_CHUNKED")
        self.assertTrue("dims" in layout_json)
        self.assertEqual(layout_json["dims"], [10, 20])
        # verify creation properties are preserved
        self.assertTrue("creationProperties" in rspJson)
        cpl = rspJson["creationProperties"]
        self.assertTrue("layout" in cpl)

    def testCompressionFiltersDataset(self):
        # test Dataset with creation property list
        domain = self.base_domain + "/testCompressionFiltersDataset.h5"
        helper.setupDomain(domain)

        print("testCompressionFiltersDataset", domain)
        headers = helper.getRequestHeaders(domain=domain)
        # get domain
        req = helper.getEndpoint() + "/"
        rsp = self.session.get(req, headers=headers)
        rspJson = json.loads(rsp.text)
        self.assertTrue("root" in rspJson)
        root_uuid = rspJson["root"]

        compressors = rspJson["compressors"]
        self.assertTrue(len(compressors) >= 5)

        for compressor in compressors:

            # create the dataset
            req = self.endpoint + "/datasets"

            payload = {"type": "H5T_IEEE_F32LE", "shape": [40, 80]}
            payload["creationProperties"] = {
                "filters": [
                    compressor,
                ]
            }
            req = self.endpoint + "/datasets"
            rsp = self.session.post(req, data=json.dumps(payload), headers=headers)
            self.assertEqual(rsp.status_code, 201)  # create dataset
            rspJson = json.loads(rsp.text)
            dset_uuid = rspJson["id"]
            self.assertTrue(helper.validateId(dset_uuid))

            # link new dataset
            req = self.endpoint + "/groups/" + root_uuid + "/links/" + compressor
            payload = {"id": dset_uuid}
            rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
            self.assertEqual(rsp.status_code, 201)
            # verify layout
            req = helper.getEndpoint() + "/datasets/" + dset_uuid
            rsp = self.session.get(req, headers=headers)
            self.assertEqual(rsp.status_code, 200)
            rspJson = json.loads(rsp.text)
            self.assertTrue("layout" in rspJson)
            layout_json = rspJson["layout"]
            self.assertTrue("class" in layout_json)
            self.assertEqual(layout_json["class"], "H5D_CHUNKED")

            # verify compression
            self.assertTrue("creationProperties" in rspJson)
            cpl = rspJson["creationProperties"]
            self.assertTrue("filters") in cpl
            filters = cpl["filters"]
            self.assertEqual(len(filters), 1)
            filter = filters[0]
            self.assertTrue(isinstance(filter, dict))
            self.assertTrue("class" in filter)
            self.assertTrue("id" in filter)
            self.assertTrue("name" in filter)
            if compressor == "deflate":
                self.assertTrue(filter["name"] in ("deflate", "gzip"))
            else:
                self.assertEqual(filter["name"], compressor)

    def testCompressionFilterOptionDataset(self):
        # test Dataset with creation property list
        domain = self.base_domain + "/testCompressionFilterOptionDataset.h5"
        helper.setupDomain(domain)

        print("testCompressionFilterOptionDataset", domain)
        headers = helper.getRequestHeaders(domain=domain)
        # get domain
        req = helper.getEndpoint() + "/"
        rsp = self.session.get(req, headers=headers)
        rspJson = json.loads(rsp.text)
        self.assertTrue("root" in rspJson)
        root_uuid = rspJson["root"]

        # create the dataset
        req = self.endpoint + "/datasets"
        compressor = {"class": "H5Z_FILTER_USER", "name": "lz4", "level": 5}

        payload = {"type": "H5T_IEEE_F32LE", "shape": [40, 80]}
        payload["creationProperties"] = {
            "filters": [
                compressor,
            ]
        }
        req = self.endpoint + "/datasets"
        rsp = self.session.post(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)  # create dataset
        rspJson = json.loads(rsp.text)
        dset_uuid = rspJson["id"]
        self.assertTrue(helper.validateId(dset_uuid))

        # link new dataset
        req = self.endpoint + "/groups/" + root_uuid + "/links/dset"
        payload = {"id": dset_uuid}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        # verify layout
        req = helper.getEndpoint() + "/datasets/" + dset_uuid
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("layout" in rspJson)
        layout_json = rspJson["layout"]
        self.assertTrue("class" in layout_json)
        self.assertEqual(layout_json["class"], "H5D_CHUNKED")

        # verify compression
        self.assertTrue("creationProperties" in rspJson)
        cpl = rspJson["creationProperties"]
        self.assertTrue("filters") in cpl
        filters = cpl["filters"]
        self.assertEqual(len(filters), 1)
        filter = filters[0]
        self.assertTrue(isinstance(filter, dict))
        self.assertTrue("class" in filter)
        self.assertEqual(filter["class"], "H5Z_FILTER_USER")
        self.assertTrue("id" in filter)
        self.assertTrue("name" in filter)
        self.assertEqual(filter["name"], "lz4")

    def testInvalidCompressionFilter(self):
        # test invalid compressor fails at dataset creation time
        # test Dataset with creation property list
        domain = self.base_domain + "/testCompressionFilter.h5"
        helper.setupDomain(domain)

        print("testInvalidCompressionFilter", domain)
        headers = helper.getRequestHeaders(domain=domain)
        # get domain
        req = helper.getEndpoint() + "/"
        rsp = self.session.get(req, headers=headers)
        rspJson = json.loads(rsp.text)
        self.assertTrue("root" in rspJson)

        bad_compressors = ("shrink-o-rama", "scaleoffet")
        for compressor_name in bad_compressors:
            # create the dataset
            req = self.endpoint + "/datasets"
            compressor = {
                "class": "H5Z_FILTER_USER",
                "name": compressor_name,
                "level": 5,
            }

            payload = {"type": "H5T_IEEE_F32LE", "shape": [40, 80]}
            payload["creationProperties"] = {
                "filters": [
                    compressor,
                ]
            }
            req = self.endpoint + "/datasets"
            rsp = self.session.post(req, data=json.dumps(payload), headers=headers)
            self.assertEqual(rsp.status_code, 400)  # create dataset

    def testInvalidFillValue(self):
        # test Dataset with simple type and fill value that is incompatible with the type
        domain = self.base_domain + "/testInvalidFillValue.h5"
        helper.setupDomain(domain)
        print("testInvalidFillValue", domain)
        headers = helper.getRequestHeaders(domain=domain)

        # get domain
        req = helper.getEndpoint() + "/"
        rsp = self.session.get(req, headers=headers)
        rspJson = json.loads(rsp.text)
        self.assertTrue("root" in rspJson)

        fill_value = "XXXX"  # can't convert to int!
        # create the dataset
        req = self.endpoint + "/datasets"
        payload = {"type": "H5T_STD_I32LE", "shape": 10}
        payload["creationProperties"] = {"fillValue": fill_value}
        req = self.endpoint + "/datasets"
        rsp = self.session.post(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 400)  # invalid param

    def testNaNFillValue(self):
        # test Dataset with simple type and fill value that is incompatible with the type
        domain = self.base_domain + "/testNaNFillValue.h5"
        helper.setupDomain(domain)
        print("testNaNFillValue", domain)
        headers = helper.getRequestHeaders(domain=domain)

        # get domain
        req = helper.getEndpoint() + "/"
        rsp = self.session.get(req, headers=headers)
        rspJson = json.loads(rsp.text)
        self.assertTrue("root" in rspJson)
        root_uuid = rspJson["root"]

        def get_payload(dset_type, fillValue=None):
            payload = {"type": dset_type, "shape": 10}
            if fillValue is not None:
                payload["creationProperties"] = {"fillValue": fillValue}
            return payload

        # create the dataset
        req = self.endpoint + "/datasets"

        payload = get_payload("H5T_STD_I32LE", fillValue=np.NaN)
        req = self.endpoint + "/datasets"
        rsp = self.session.post(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 400)  # NaN not compatible with integer type

        payload = get_payload("H5T_IEEE_F32LE", fillValue=np.NaN)
        req = self.endpoint + "/datasets"
        rsp = self.session.post(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)  # Dataset created
        rspJson = json.loads(rsp.text)
        dset_uuid = rspJson["id"]

        # link new dataset
        req = self.endpoint + "/groups/" + root_uuid + "/links/dset1"
        payload = {"id": dset_uuid}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # verify creationProperties
        req = helper.getEndpoint() + "/datasets/" + dset_uuid
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("creationProperties" in rspJson)
        creationProps = rspJson["creationProperties"]
        self.assertTrue("fillValue" in creationProps)
        self.assertTrue(np.isnan(creationProps["fillValue"]))

        # get data json returning "nan" for fillValue rather than np.Nan
        # the latter works with the Python JSON package, but is not part
        # of the formal JSON standard
        params = {"ignore_nan": 1}
        rsp = self.session.get(req, headers=headers, params=params)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("creationProperties" in rspJson)
        creationProps = rspJson["creationProperties"]
        self.assertTrue("fillValue" in creationProps)
        self.assertEqual(creationProps["fillValue"], "nan")

        # try creating dataset using "nan" as fillValue (rather than the non JSON compliant nan)
        payload = get_payload("H5T_IEEE_F32LE", fillValue="nan")
        req = self.endpoint + "/datasets"
        rsp = self.session.post(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)  # Dataset created
        rspJson = json.loads(rsp.text)
        dset_uuid = rspJson["id"]

        # link new dataset
        req = self.endpoint + "/groups/" + root_uuid + "/links/dset2"
        payload = {"id": dset_uuid}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # verify creationProperties
        req = helper.getEndpoint() + "/datasets/" + dset_uuid
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("creationProperties" in rspJson)
        creationProps = rspJson["creationProperties"]
        self.assertTrue("fillValue" in creationProps)
        fillValue = creationProps["fillValue"]
        self.assertEqual(fillValue, "nan")

    def testNaNFillValueBase64Encoded(self):
        # test Dataset with simple type and fill value that is incompatible with the type
        domain = self.base_domain + "/testNaNFillValueBase64Encoded.h5"
        helper.setupDomain(domain)
        print("testNaNFillValueBase64Encoded", domain)
        headers = helper.getRequestHeaders(domain=domain)

        # get domain
        req = helper.getEndpoint() + "/"
        rsp = self.session.get(req, headers=headers)
        rspJson = json.loads(rsp.text)
        self.assertTrue("root" in rspJson)
        root_uuid = rspJson["root"]

        def get_payload(dset_type, fillValue=None, encoding=None):
            payload = {"type": dset_type, "shape": 10}
            if fillValue is not None:
                cprops = {"fillValue": fillValue}
                if encoding:
                    cprops["fillValue_encoding"] = encoding
                payload["creationProperties"] = cprops
            return payload

        # create the dataset
        req = self.endpoint + "/datasets"

        payload = get_payload("H5T_IEEE_F32LE", fillValue="AADAfw==", encoding="base64")
        req = self.endpoint + "/datasets"
        rsp = self.session.post(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)  # Dataset created
        rspJson = json.loads(rsp.text)
        dset_uuid = rspJson["id"]

        # link new dataset
        req = self.endpoint + "/groups/" + root_uuid + "/links/dset1"
        payload = {"id": dset_uuid}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # verify creationProperties
        req = helper.getEndpoint() + "/datasets/" + dset_uuid
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("creationProperties" in rspJson)
        creationProps = rspJson["creationProperties"]
        self.assertTrue("fillValue" in creationProps)
        self.assertEqual(creationProps["fillValue"], "AADAfw==")
        self.assertTrue("fillValue_encoding" in creationProps)
        self.assertEqual(creationProps["fillValue_encoding"], "base64")

        # link new dataset
        req = self.endpoint + "/groups/" + root_uuid + "/links/dset2"
        payload = {"id": dset_uuid}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

    def testAutoChunk1dDataset(self):
        # test Dataset where chunk layout is set automatically
        domain = self.base_domain + "/testAutoChunk1dDataset.h5"
        helper.setupDomain(domain)
        print("testAutoChunk1dDataset", domain)
        headers = helper.getRequestHeaders(domain=domain)
        # get domain
        req = helper.getEndpoint() + "/"
        rsp = self.session.get(req, headers=headers)
        rspJson = json.loads(rsp.text)
        self.assertTrue("root" in rspJson)
        root_uuid = rspJson["root"]

        # create the dataset
        req = self.endpoint + "/datasets"
        # 50K x 80K dataset
        extent = 1000 * 1000 * 1000
        dims = [
            extent,
        ]
        fields = (
            {"name": "x", "type": "H5T_IEEE_F64LE"},
            {"name": "y", "type": "H5T_IEEE_F64LE"},
            {"name": "z", "type": "H5T_IEEE_F64LE"},
        )
        datatype = {"class": "H5T_COMPOUND", "fields": fields}

        payload = {"type": datatype, "shape": dims}
        # the following should get ignored as too small
        payload["creationProperties"] = {
            "layout": {
                "class": "H5D_CHUNKED",
                "dims": [
                    10,
                ],
            }
        }
        req = self.endpoint + "/datasets"
        rsp = self.session.post(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)  # create dataset
        rspJson = json.loads(rsp.text)

        dset_uuid = rspJson["id"]
        self.assertTrue(helper.validateId(dset_uuid))

        # link new dataset as 'dset'
        name = "dset"
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
        payload = {"id": dset_uuid}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # verify layout
        req = helper.getEndpoint() + "/datasets/" + dset_uuid
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("layout" in rspJson)
        layout_json = rspJson["layout"]
        self.assertTrue("class" in layout_json)
        self.assertEqual(layout_json["class"], "H5D_CHUNKED")
        self.assertTrue("dims" in layout_json)
        self.assertTrue("partition_count" not in layout_json)
        layout = layout_json["dims"]
        self.assertEqual(len(layout), 1)
        self.assertTrue(layout[0] < dims[0])
        chunk_size = layout[0] * 8 * 3  # three 64bit
        # chunk size should be between chunk min and max
        self.assertTrue(chunk_size >= CHUNK_MIN)
        self.assertTrue(chunk_size <= CHUNK_MAX)

    def testAutoChunk2dDataset(self):
        # test Dataset where chunk layout is set automatically
        domain = self.base_domain + "/testAutoChunk2dDataset.h5"
        helper.setupDomain(domain)
        print("testAutoChunk2dDataset", domain)
        headers = helper.getRequestHeaders(domain=domain)
        # get domain
        req = helper.getEndpoint() + "/"
        rsp = self.session.get(req, headers=headers)
        rspJson = json.loads(rsp.text)
        self.assertTrue("root" in rspJson)
        root_uuid = rspJson["root"]

        # create the dataset
        req = self.endpoint + "/datasets"
        # 50K x 80K dataset
        dims = [50000, 80000]
        payload = {"type": "H5T_IEEE_F32LE", "shape": dims}

        req = self.endpoint + "/datasets"
        rsp = self.session.post(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)  # create dataset
        rspJson = json.loads(rsp.text)

        dset_uuid = rspJson["id"]
        self.assertTrue(helper.validateId(dset_uuid))

        # link new dataset as 'dset'
        name = "dset"
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
        payload = {"id": dset_uuid}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # verify layout
        req = helper.getEndpoint() + "/datasets/" + dset_uuid
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("layout" in rspJson)
        layout_json = rspJson["layout"]
        self.assertTrue("class" in layout_json)
        self.assertEqual(layout_json["class"], "H5D_CHUNKED")
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
        domain = self.base_domain + "/testMinChunkSizeDataset.h5"
        helper.setupDomain(domain)
        print("testMinChunkSizeDataset", domain)
        headers = helper.getRequestHeaders(domain=domain)
        # get domain
        req = helper.getEndpoint() + "/"
        rsp = self.session.get(req, headers=headers)
        rspJson = json.loads(rsp.text)
        self.assertTrue("root" in rspJson)
        root_uuid = rspJson["root"]

        # create the dataset
        req = self.endpoint + "/datasets"
        # 50K x 80K dataset
        dims = [50000, 80000]
        payload = {"type": "H5T_IEEE_F32LE", "shape": dims}
        # define a chunk layout with lots of small chunks
        payload["creationProperties"] = {
            "layout": {"class": "H5D_CHUNKED", "dims": [10, 10]}
        }

        req = self.endpoint + "/datasets"
        rsp = self.session.post(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)  # create dataset
        rspJson = json.loads(rsp.text)
        dset_uuid = rspJson["id"]
        self.assertTrue(helper.validateId(dset_uuid))

        # link new dataset as 'dset'
        name = "dset"
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
        payload = {"id": dset_uuid}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # verify layout
        req = helper.getEndpoint() + "/datasets/" + dset_uuid
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("layout" in rspJson)
        layout_json = rspJson["layout"]
        self.assertTrue("class" in layout_json)
        self.assertEqual(layout_json["class"], "H5D_CHUNKED")
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
        domain = self.base_domain + "/testPostWithLink.h5"
        helper.setupDomain(domain)
        print("testPostWithLink", domain)
        headers = helper.getRequestHeaders(domain=domain)

        # get domain
        req = helper.getEndpoint() + "/"
        rsp = self.session.get(req, headers=headers)
        rspJson = json.loads(rsp.text)
        self.assertTrue("root" in rspJson)
        root_uuid = rspJson["root"]

        # get root group and verify link count is 0
        req = helper.getEndpoint() + "/groups/" + root_uuid
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertEqual(rspJson["linkCount"], 0)

        type_vstr = {
            "charSet": "H5T_CSET_ASCII",
            "class": "H5T_STRING",
            "strPad": "H5T_STR_NULLTERM",
            "length": "H5T_VARIABLE",
        }
        payload = {
            "type": type_vstr,
            "shape": 10,
            "link": {"id": root_uuid, "name": "linked_dset"},
        }
        req = self.endpoint + "/datasets"
        rsp = self.session.post(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)  # create dataset
        rspJson = json.loads(rsp.text)
        dset_uuid = rspJson["id"]
        self.assertTrue(helper.validateId(dset_uuid))

        # get root group and verify link count is 1
        req = helper.getEndpoint() + "/groups/" + root_uuid
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertEqual(rspJson["linkCount"], 1)

        # read the link back and verify
        req = helper.getEndpoint() + "/groups/" + root_uuid + "/links/linked_dset"
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)  # link doesn't exist yet
        rspJson = json.loads(rsp.text)
        self.assertTrue("link" in rspJson)
        link_json = rspJson["link"]
        self.assertEqual(link_json["collection"], "datasets")
        self.assertEqual(link_json["class"], "H5L_TYPE_HARD")
        self.assertEqual(link_json["title"], "linked_dset")
        self.assertEqual(link_json["id"], dset_uuid)

    def testPostCommittedType(self):
        domain = self.base_domain + "/testPostCommittedType.h5"
        helper.setupDomain(domain)
        print("testPostCommittedType", domain)
        headers = helper.getRequestHeaders(domain=domain)

        # get domain
        req = helper.getEndpoint() + "/"
        rsp = self.session.get(req, headers=headers)
        rspJson = json.loads(rsp.text)
        self.assertTrue("root" in rspJson)
        root_uuid = rspJson["root"]

        # create the datatype
        payload = {"type": "H5T_IEEE_F32LE"}
        req = self.endpoint + "/datatypes"
        rsp = self.session.post(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)  # create datatype
        rspJson = json.loads(rsp.text)
        dtype_uuid = rspJson["id"]
        self.assertTrue(helper.validateId(dtype_uuid))

        # link new datatype as 'dtype1'
        name = "dtype1"
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
        payload = {"id": dtype_uuid}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # create the dataset
        payload = {"type": dtype_uuid, "shape": [10, 10]}
        req = self.endpoint + "/datasets"
        rsp = self.session.post(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)  # create dataset
        rspJson = json.loads(rsp.text)
        dset_uuid = rspJson["id"]
        self.assertTrue(helper.validateId(dset_uuid))

        # link new dataset as 'dset1'
        name = "dset1"
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
        payload = {"id": dset_uuid}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # Fetch the dataset type and verify dtype_uuid
        req = helper.getEndpoint() + "/datasets/" + dset_uuid + "/type"
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("type" in rspJson)
        rsp_type = rspJson["type"]
        self.assertTrue("base" in rsp_type)
        self.assertEqual(rsp_type["base"], "H5T_IEEE_F32LE")
        self.assertTrue("class" in rsp_type)
        self.assertEqual(rsp_type["class"], "H5T_FLOAT")
        self.assertTrue("id" in rsp_type)
        self.assertEqual(rsp_type["id"], dtype_uuid)

    def testDatasetwithDomainDelete(self):
        domain = self.base_domain + "/datasetwithdomaindelete.h6"
        print("testDatasetwithDomainDelete:", domain)
        helper.setupDomain(domain)
        headers = helper.getRequestHeaders(domain=domain)

        # get domain
        req = helper.getEndpoint() + "/"
        rsp = self.session.get(req, headers=headers)
        rspJson = json.loads(rsp.text)
        self.assertTrue("root" in rspJson)
        root_uuid = rspJson["root"]

        # get root group and verify link count is 0
        req = helper.getEndpoint() + "/groups/" + root_uuid
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertEqual(rspJson["linkCount"], 0)

        type_vstr = {
            "charSet": "H5T_CSET_ASCII",
            "class": "H5T_STRING",
            "strPad": "H5T_STR_NULLTERM",
            "length": "H5T_VARIABLE",
        }
        payload = {
            "type": type_vstr,
            "shape": 10,
            "link": {"id": root_uuid, "name": "linked_dset"},
        }
        req = self.endpoint + "/datasets"
        rsp = self.session.post(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)  # create dataset
        rspJson = json.loads(rsp.text)
        dset_uuid = rspJson["id"]
        self.assertTrue(helper.validateId(dset_uuid))
        self.assertEqual(root_uuid, rspJson["root"])

        # get root group and verify link count is 1
        req = helper.getEndpoint() + "/groups/" + root_uuid
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertEqual(rspJson["linkCount"], 1)

        # delete the domain (with the orginal user)
        req = helper.getEndpoint() + "/"
        rsp = self.session.delete(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)

        # try getting the domain again
        req = helper.getEndpoint() + "/"
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 410)  # GONE

        # re-create a domain
        rsp = self.session.put(req, headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)
        self.assertTrue("root" in rspJson)
        self.assertTrue(root_uuid != rspJson["root"])
        root_uuid = rspJson["root"]

        # try getting the dataset
        req = self.endpoint + "/datasets/" + dset_uuid
        rsp = self.session.get(req, headers=headers)
        # TODO - this is returning 200 rather than 400
        # to fix: delete domain cache on all SN nodes after domain delete?
        # self.assertEqual(rsp.status_code, 400) # Not Found

        # create a dataset again
        req = self.endpoint + "/datasets"
        payload = {
            "type": type_vstr,
            "shape": 10,
            "link": {"id": root_uuid, "name": "linked_dset"},
        }
        rsp = self.session.post(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)  # create dataset
        rspJson = json.loads(rsp.text)
        dset_uuid = rspJson["id"]
        self.assertTrue(helper.validateId(dset_uuid))
        self.assertEqual(root_uuid, rspJson["root"])

    def testContiguousRefDataset(self):
        # test Dataset where H5D_CONTIGUOUS_REF layout is used
        domain = self.base_domain + "/testContiguousRefDataset.h5"
        helper.setupDomain(domain)
        print("testContiguousRefDataset", domain)
        headers = helper.getRequestHeaders(domain=domain)
        # get domain
        req = helper.getEndpoint() + "/"
        rsp = self.session.get(req, headers=headers)
        rspJson = json.loads(rsp.text)
        self.assertTrue("root" in rspJson)
        root_uuid = rspJson["root"]

        # create the dataset
        req = self.endpoint + "/datasets"
        # 50K x 80K dataset
        dims = [50000, 8000000]
        payload = {"type": "H5T_IEEE_F32LE", "shape": dims}
        file_uri = "s3://a-storage-bucket/some-file.h5"

        offset = 1234
        size = dims[0] * dims[1] * 4  # uncompressed size

        payload["creationProperties"] = {
            "layout": {
                "class": "H5D_CONTIGUOUS_REF",
                "file_uri": file_uri,
                "offset": offset,
                "size": size,
            }
        }
        req = self.endpoint + "/datasets"
        rsp = self.session.post(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)  # create dataset
        rspJson = json.loads(rsp.text)

        dset_uuid = rspJson["id"]
        self.assertTrue(helper.validateId(dset_uuid))

        # link new dataset as 'dset'
        name = "dset"
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
        payload = {"id": dset_uuid}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # verify layout
        req = helper.getEndpoint() + "/datasets/" + dset_uuid
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("layout" in rspJson)
        layout_json = rspJson["layout"]
        self.assertTrue("class" in layout_json)
        self.assertEqual(layout_json["class"], "H5D_CHUNKED")
        self.assertTrue("dims" in layout_json)
        chunk_dims = layout_json["dims"]
        self.assertEqual(len(chunk_dims), 2)
        chunk_size = chunk_dims[0] * chunk_dims[1] * 4
        # chunk size should be between chunk min and max
        self.assertTrue(chunk_size >= CHUNK_MIN)
        self.assertTrue(chunk_size <= CHUNK_MAX)

        # verify cpl
        self.assertTrue("creationProperties" in rspJson)
        cpl = rspJson["creationProperties"]
        self.assertTrue("layout" in cpl)
        cpl_layout = cpl["layout"]
        self.assertTrue("class" in cpl_layout)
        self.assertEqual(cpl_layout["class"], "H5D_CONTIGUOUS_REF")

        self.assertTrue("file_uri" in cpl_layout)
        self.assertEqual(cpl_layout["file_uri"], file_uri)
        self.assertTrue("offset" in cpl_layout)
        self.assertEqual(cpl_layout["offset"], offset)
        self.assertTrue("size" in cpl_layout)
        self.assertEqual(cpl_layout["size"], size)

    def testContiguousRefZeroDimDataset(self):
        # test Dataset where H5D_CONTIGUOUS_REF layout is used
        domain = self.base_domain + "/testContiguousRefZeroDimDataset.h5"
        helper.setupDomain(domain)
        print("testContiguousRefZeroDimDataset", domain)
        headers = helper.getRequestHeaders(domain=domain)
        # get domain
        req = helper.getEndpoint() + "/"
        rsp = self.session.get(req, headers=headers)
        rspJson = json.loads(rsp.text)
        self.assertTrue("root" in rspJson)
        root_uuid = rspJson["root"]

        # create the dataset
        req = self.endpoint + "/datasets"
        # 0 x 10 dataset
        dims = [0, 10]
        payload = {"type": "H5T_STD_I16LE", "shape": dims}
        file_uri = "s3://a-storage-bucket/some-file.h5"

        offset = 1234
        size = dims[0] * dims[1] * 4  # uncompressed size

        payload["creationProperties"] = {
            "layout": {
                "class": "H5D_CONTIGUOUS_REF",
                "file_uri": file_uri,
                "offset": offset,
                "size": size,
            }
        }
        req = self.endpoint + "/datasets"
        rsp = self.session.post(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)  # create dataset
        rspJson = json.loads(rsp.text)

        dset_uuid = rspJson["id"]
        self.assertTrue(helper.validateId(dset_uuid))

        # link new dataset as 'dset'
        name = "dset"
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
        payload = {"id": dset_uuid}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # verify layout
        req = helper.getEndpoint() + "/datasets/" + dset_uuid
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("layout" in rspJson)
        layout_json = rspJson["layout"]
        self.assertTrue("class" in layout_json)
        self.assertEqual(layout_json["class"], "H5D_CHUNKED")
        self.assertTrue("dims" in layout_json)
        chunk_dims = layout_json["dims"]
        self.assertEqual(len(chunk_dims), 2)
        # layout should be same as the dims
        self.assertEqual(chunk_dims[0], dims[0])
        self.assertEqual(chunk_dims[1], dims[1])

        # verify cpl
        self.assertTrue("creationProperties" in rspJson)
        cpl = rspJson["creationProperties"]
        self.assertTrue("layout" in cpl)
        cpl_layout = cpl["layout"]
        self.assertTrue("class" in cpl_layout)
        self.assertEqual(cpl_layout["class"], "H5D_CONTIGUOUS_REF")
        self.assertTrue("file_uri" in cpl_layout)
        self.assertEqual(cpl_layout["file_uri"], file_uri)
        self.assertTrue("offset" in cpl_layout)
        self.assertEqual(cpl_layout["offset"], offset)
        self.assertTrue("size" in cpl_layout)
        self.assertEqual(cpl_layout["size"], size)

    def testChunkedRefDataset(self):
        # test Dataset where H5D_CHUNKED_REF layout is used
        domain = self.base_domain + "/testChunkedRefDataset.h5"
        helper.setupDomain(domain)
        print("testChunkedRefDataset", domain)
        headers = helper.getRequestHeaders(domain=domain)
        # get domain
        req = helper.getEndpoint() + "/"
        rsp = self.session.get(req, headers=headers)
        rspJson = json.loads(rsp.text)
        self.assertTrue("root" in rspJson)
        root_uuid = rspJson["root"]

        # create the dataset
        req = self.endpoint + "/datasets"
        # 2Kx3K dataset
        dims = [2000, 3000]
        # 1000x1000 chunks
        chunk_layout = [1000, 1000]
        chunk_size = chunk_layout[0] * chunk_layout[1] * 2  # uncompressed size
        # make up some chunk locations
        chunks = {}
        chunks["0_0"] = [1234 + 1 * chunk_size, chunk_size]
        chunks["0_1"] = [1234 + 2 * chunk_size, chunk_size]
        chunks["0_2"] = [1234 + 3 * chunk_size, chunk_size]
        chunks["1_0"] = [1234 + 4 * chunk_size, chunk_size]
        chunks["1_1"] = [1234 + 5 * chunk_size, chunk_size]
        chunks["1_2"] = [1234 + 6 * chunk_size, chunk_size]

        file_uri = "s3://a-storage-bucket/some-file.h5"

        layout = {
            "class": "H5D_CHUNKED_REF",
            "file_uri": file_uri,
            "dims": chunk_layout,
            "chunks": chunks,
        }
        payload = {"type": "H5T_STD_I16LE", "shape": dims}
        payload["creationProperties"] = {"layout": layout}

        req = self.endpoint + "/datasets"
        rsp = self.session.post(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)  # create dataset
        rspJson = json.loads(rsp.text)

        dset_uuid = rspJson["id"]
        self.assertTrue(helper.validateId(dset_uuid))

        # link new dataset as 'dset'
        name = "dset"
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
        payload = {"id": dset_uuid}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # verify layout
        req = helper.getEndpoint() + "/datasets/" + dset_uuid
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("layout" in rspJson)
        layout_json = rspJson["layout"]
        self.assertTrue("class" in layout_json)
        self.assertEqual(layout_json["class"], "H5D_CHUNKED")
        self.assertTrue("dims" in layout_json)
        chunk_dims = layout_json["dims"]
        self.assertEqual(len(chunk_dims), 2)
        self.assertTrue("creationProperties" in rspJson)
        cpl = rspJson["creationProperties"]
        self.assertTrue("layout" in cpl)
        cpl_layout = cpl["layout"]
        self.assertTrue("class" in cpl_layout)
        self.assertEqual(cpl_layout["class"], "H5D_CHUNKED_REF")
        self.assertTrue("file_uri" in cpl_layout)
        self.assertEqual(cpl_layout["file_uri"], file_uri)
        self.assertTrue("chunks" in cpl_layout)
        self.assertEqual(cpl_layout["chunks"], chunks)

    def testChunkedRefIndirectDataset(self):
        # test Dataset where H5D_CHUNKED_REF_INDIRECT layout is used
        domain = self.base_domain + "/testChunkedRefIndirectDataset.h5"
        helper.setupDomain(domain)
        print("testChunkedRefIndirectDataset", domain)
        headers = helper.getRequestHeaders(domain=domain)
        # get domain
        req = helper.getEndpoint() + "/"
        rsp = self.session.get(req, headers=headers)
        rspJson = json.loads(rsp.text)
        self.assertTrue("root" in rspJson)
        root_uuid = rspJson["root"]

        # create a dataset to store chunk info
        fields = (
            {"name": "offset", "type": "H5T_STD_I64LE"},
            {"name": "size", "type": "H5T_STD_I32LE"},
        )
        chunkinfo_type = {"class": "H5T_COMPOUND", "fields": fields}
        req = self.endpoint + "/datasets"
        # Store 40 chunk locations
        chunkinfo_dims = [20, 30]
        payload = {"type": chunkinfo_type, "shape": chunkinfo_dims}
        req = self.endpoint + "/datasets"
        rsp = self.session.post(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)  # create dataset
        rspJson = json.loads(rsp.text)
        chunkinfo_uuid = rspJson["id"]
        self.assertTrue(helper.validateId(chunkinfo_uuid))

        # create the primary dataset
        # 20Kx30K dataset
        dims = [20000, 30000]
        # 1000x1000 chunks
        chunk_layout = [1000, 1000]
        file_uri = "s3://a-storage-bucket/some-file.h5"

        layout = {
            "class": "H5D_CHUNKED_REF_INDIRECT",
            "file_uri": file_uri,
            "dims": chunk_layout,
            "chunk_table": chunkinfo_uuid,
        }
        payload = {"type": "H5T_STD_I16LE", "shape": dims}
        payload["creationProperties"] = {"layout": layout}

        req = self.endpoint + "/datasets"
        rsp = self.session.post(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)  # create dataset
        rspJson = json.loads(rsp.text)

        dset_uuid = rspJson["id"]
        self.assertTrue(helper.validateId(dset_uuid))

        # link new dataset as 'dset'
        name = "dset"
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
        payload = {"id": dset_uuid}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # verify layout
        req = helper.getEndpoint() + "/datasets/" + dset_uuid
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("layout" in rspJson)
        layout_json = rspJson["layout"]
        self.assertTrue("class" in layout_json)
        self.assertEqual(layout_json["class"], "H5D_CHUNKED")
        self.assertTrue("chunks" not in layout_json)
        chunk_dims = layout_json["dims"]
        self.assertEqual(len(chunk_dims), 2)

        self.assertTrue("creationProperties" in rspJson)
        cpl = rspJson["creationProperties"]
        self.assertTrue("layout" in cpl)
        cpl_layout = cpl["layout"]
        self.assertTrue("class" in cpl_layout)
        self.assertEqual(cpl_layout["class"], "H5D_CHUNKED_REF_INDIRECT")
        self.assertTrue("file_uri" in cpl_layout)
        self.assertEqual(cpl_layout["file_uri"], file_uri)
        self.assertTrue("chunks" not in cpl_layout)

        self.assertTrue("chunk_table" in cpl_layout)
        self.assertEqual(cpl_layout["chunk_table"], chunkinfo_uuid)

    def testChunkedRefIndirectS3UriDataset(self):
        # test Dataset where H5D_CHUNKED_REF_INDIRECT layout is used with
        # s3uri's stored in the chunk tablee
        domain = self.base_domain + "/testChunkedRefIndirectS3UriDataset.h5"
        helper.setupDomain(domain)
        print("testChunkedRefIndirectS3UriDataset", domain)
        headers = helper.getRequestHeaders(domain=domain)
        # get domain
        req = helper.getEndpoint() + "/"
        rsp = self.session.get(req, headers=headers)
        rspJson = json.loads(rsp.text)
        self.assertTrue("root" in rspJson)
        root_uuid = rspJson["root"]

        # create a dataset to store chunk info
        max_s3_uri_len = 40
        fixed_str_type = {
            "charSet": "H5T_CSET_ASCII",
            "class": "H5T_STRING",
            "length": max_s3_uri_len,
            "strPad": "H5T_STR_NULLPAD",
        }
        fields = (
            {"name": "offset", "type": "H5T_STD_I64LE"},
            {"name": "size", "type": "H5T_STD_I32LE"},
            {"name": "file_uri", "type": fixed_str_type},
        )
        chunkinfo_type = {"class": "H5T_COMPOUND", "fields": fields}
        req = self.endpoint + "/datasets"
        # Store 40 chunk locations
        chunkinfo_dims = [20, 30]
        payload = {"type": chunkinfo_type, "shape": chunkinfo_dims}
        req = self.endpoint + "/datasets"
        rsp = self.session.post(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)  # create dataset
        rspJson = json.loads(rsp.text)
        chunkinfo_uuid = rspJson["id"]
        self.assertTrue(helper.validateId(chunkinfo_uuid))

        # create the primary dataset
        # 20Kx30K dataset
        dims = [20000, 30000]
        # 1000x1000 chunks
        chunk_layout = [1000, 1000]
        file_uri = "s3://a-storage-bucket/some-file.h5"

        layout = {
            "class": "H5D_CHUNKED_REF_INDIRECT",
            "file_uri": file_uri,
            "dims": chunk_layout,
            "chunk_table": chunkinfo_uuid,
        }
        payload = {"type": "H5T_STD_I16LE", "shape": dims}
        payload["creationProperties"] = {"layout": layout}

        req = self.endpoint + "/datasets"
        rsp = self.session.post(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)  # create dataset
        rspJson = json.loads(rsp.text)

        dset_uuid = rspJson["id"]
        self.assertTrue(helper.validateId(dset_uuid))

        # link new dataset as 'dset'
        name = "dset"
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
        payload = {"id": dset_uuid}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # verify layout
        req = helper.getEndpoint() + "/datasets/" + dset_uuid
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("layout" in rspJson)
        layout_json = rspJson["layout"]
        self.assertTrue("class" in layout_json)
        self.assertEqual(layout_json["class"], "H5D_CHUNKED")
        self.assertTrue("chunks" not in layout_json)
        self.assertTrue("dims" in layout_json)
        chunk_dims = layout_json["dims"]
        self.assertEqual(len(chunk_dims), 2)

        self.assertTrue("creationProperties" in rspJson)
        cpl = rspJson["creationProperties"]
        self.assertTrue("layout" in cpl)
        cpl_layout = cpl["layout"]

        self.assertTrue("class" in cpl_layout)
        self.assertEqual(cpl_layout["class"], "H5D_CHUNKED_REF_INDIRECT")
        self.assertTrue("file_uri" in cpl_layout)
        self.assertEqual(cpl_layout["file_uri"], file_uri)
        self.assertTrue("chunk_table" in cpl_layout)
        self.assertEqual(cpl_layout["chunk_table"], chunkinfo_uuid)
        self.assertTrue("chunks" not in cpl)

    def testDatasetChunkPartitioning(self):
        # test Dataset partitioning logic for large datasets
        domain = self.base_domain + "/testDatasetChunkPartitioning.h5"
        helper.setupDomain(domain)
        print("testDatasetChunkPartitioning", domain)
        headers = helper.getRequestHeaders(domain=domain)
        # get domain
        req = helper.getEndpoint() + "/"
        rsp = self.session.get(req, headers=headers)
        rspJson = json.loads(rsp.text)
        self.assertTrue("root" in rspJson)
        root_uuid = rspJson["root"]

        # create the dataset
        req = self.endpoint + "/datasets"
        # 50K x 80K x 90K dataset
        dims = [50000, 80000, 90000]
        payload = {"type": "H5T_IEEE_F32LE", "shape": dims}

        req = self.endpoint + "/datasets"
        rsp = self.session.post(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)  # create dataset
        rspJson = json.loads(rsp.text)

        dset_uuid = rspJson["id"]
        self.assertTrue(helper.validateId(dset_uuid))

        # link new dataset as 'dset'
        name = "dset"
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
        payload = {"id": dset_uuid}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # verify layout
        req = helper.getEndpoint() + "/datasets/" + dset_uuid
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("layout" in rspJson)
        layout_json = rspJson["layout"]
        self.assertTrue("class" in layout_json)
        self.assertEqual(layout_json["class"], "H5D_CHUNKED")
        self.assertTrue("dims" in layout_json)
        if config.get("max_chunks_per_folder") > 0:
            self.assertTrue("partition_count" in layout_json)
            self.assertTrue(
                layout_json["partition_count"] > 1000
            )  # will change if max_chunks_per_folder is updated

        layout = layout_json["dims"]

        self.assertEqual(len(layout), 3)
        self.assertTrue(layout[0] < dims[0])
        self.assertTrue(layout[1] < dims[1])
        self.assertTrue(layout[2] < dims[2])
        chunk_size = layout[0] * layout[1] * layout[2] * 4
        # chunk size should be between chunk min and max
        self.assertTrue(chunk_size >= CHUNK_MIN)
        self.assertTrue(chunk_size <= CHUNK_MAX)

    def testExtendibleDatasetChunkPartitioning(self):
        # test Dataset partitioning logic for large datasets
        domain = self.base_domain + "/testExtendibleDatasetChunkPartitioning.h5"
        helper.setupDomain(domain)
        print("testExtendibleDatasetChunkPartitioning", domain)
        headers = helper.getRequestHeaders(domain=domain)
        # get domain
        req = helper.getEndpoint() + "/"
        rsp = self.session.get(req, headers=headers)
        rspJson = json.loads(rsp.text)
        self.assertTrue("root" in rspJson)
        root_uuid = rspJson["root"]

        # create the dataset
        req = self.endpoint + "/datasets"
        # 50K x 80K x 90K dataset
        dims = [0, 80000, 90000]
        # unlimited extend in dim 0, fixeed in dimension 2, extenbile by 10x in dim 3
        max_dims = [0, 80000, 900000]
        payload = {"type": "H5T_IEEE_F32LE", "shape": dims, "maxdims": max_dims}

        req = self.endpoint + "/datasets"
        rsp = self.session.post(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)  # create dataset
        rspJson = json.loads(rsp.text)

        dset_uuid = rspJson["id"]
        self.assertTrue(helper.validateId(dset_uuid))

        # link new dataset as 'dset'
        name = "dset"
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
        payload = {"id": dset_uuid}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # verify layout
        req = helper.getEndpoint() + "/datasets/" + dset_uuid
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("layout" in rspJson)
        layout_json = rspJson["layout"]
        self.assertTrue("class" in layout_json)
        self.assertEqual(layout_json["class"], "H5D_CHUNKED")
        self.assertTrue("dims" in layout_json)
        if config.get("max_chunks_per_folder") > 0:
            self.assertTrue("partition_count" in layout_json)

        layout = layout_json["dims"]

        self.assertEqual(len(layout), 3)
        chunk_size = layout[0] * layout[1] * layout[2] * 4
        # chunk size should be between chunk min and max
        self.assertTrue(chunk_size >= CHUNK_MIN)
        self.assertTrue(chunk_size <= CHUNK_MAX)

    def testDatasetEmptyChunkExtent(self):
        # Attempting to create 0-extent chunks should respond with Bad Request
        domain = self.base_domain + "/testDatasetEmptyChunkExtent.h5"
        helper.setupDomain(domain)
        print("testDatasetEmptyChunkExtent", domain)
        headers = helper.getRequestHeaders(domain=domain)
        req = helper.getEndpoint() + "/"

        # create the dataset
        req = self.endpoint + "/datasets"

        dims = [1]
        payload = {"type": "H5T_IEEE_F32LE",
                   "shape": dims}

        payload["creationProperties"] = {
            "layout": {"class": "H5D_CHUNKED", "dims": [0]}
        }

        req = self.endpoint + "/datasets"
        rsp = self.session.post(req, data=json.dumps(payload), headers=headers)
        # Should fail with Bad Request due to invalid layout value
        self.assertEqual(rsp.status_code, 400)  # create dataset


if __name__ == "__main__":
    # setup test files

    unittest.main()
