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
import helper
import config


class DatatypeTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(DatatypeTest, self).__init__(*args, **kwargs)
        self.base_domain = helper.getTestDomainName(self.__class__.__name__)
        helper.setupDomain(self.base_domain)
        self.endpoint = helper.getEndpoint()

    def setUp(self):
        self.session = helper.getSession()

    def tearDown(self):
        if self.session:
            self.session.close()

    def testCommittedType(self):
        # Test creation/deletion of datatype obj

        print("testCommittedType", self.base_domain)
        headers = helper.getRequestHeaders(domain=self.base_domain)
        req = self.endpoint + "/"

        # Get root uuid
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        root_uuid = rspJson["root"]
        helper.validateId(root_uuid)

        # create a committed type obj
        data = {"type": "H5T_IEEE_F32LE"}
        req = self.endpoint + "/datatypes"
        rsp = self.session.post(req, data=json.dumps(data), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)
        self.assertEqual(rspJson["attributeCount"], 1)
        ctype_id = rspJson["id"]
        self.assertTrue(helper.validateId(ctype_id))

        # read back the obj
        req = self.endpoint + "/datatypes/" + ctype_id
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("id" in rspJson)
        self.assertEqual(rspJson["id"], ctype_id)
        self.assertTrue("root" in rspJson)
        self.assertEqual(rspJson["root"], root_uuid)
        self.assertTrue("created" in rspJson)
        self.assertTrue("lastModified" in rspJson)
        self.assertTrue("attributeCount" in rspJson)
        self.assertEqual(rspJson["attributeCount"], 0)
        self.assertTrue("type" in rspJson)
        type_json = rspJson["type"]
        self.assertEqual(type_json["class"], "H5T_FLOAT")
        self.assertEqual(type_json["base"], "H5T_IEEE_F32LE")

        # try get with a different user (who has read permission)
        headers = helper.getRequestHeaders(
            domain=self.base_domain, username="test_user2"
        )
        rsp = self.session.get(req, headers=headers)
        if config.get("default_public"):
            self.assertEqual(rsp.status_code, 200)
            rspJson = json.loads(rsp.text)
            self.assertEqual(rspJson["root"], root_uuid)
        else:
            self.assertEqual(rsp.status_code, 403)

        # try to do a GET with a different domain (should fail)
        another_domain = helper.getParentDomain(self.base_domain)
        headers = helper.getRequestHeaders(domain=another_domain)
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 400)

        # try DELETE with user who doesn't have create permission on this domain
        headers = helper.getRequestHeaders(
            domain=self.base_domain, username="test_user2"
        )
        rsp = self.session.delete(req, headers=headers)
        self.assertEqual(rsp.status_code, 403)  # forbidden

        # try to do a DELETE with a different domain (should fail)
        another_domain = helper.getParentDomain(self.base_domain)
        headers = helper.getRequestHeaders(domain=another_domain)
        rsp = self.session.delete(req, headers=headers)
        self.assertEqual(rsp.status_code, 400)

        # delete the datatype
        headers = helper.getRequestHeaders(domain=self.base_domain)
        rsp = self.session.delete(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue(rspJson is not None)

        # a get for the datatype should now return 410 (GONE)
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 410)

    def testPostTypes(self):
        # Test creation with all primitive types

        print("testCommittedType", self.base_domain)
        headers = helper.getRequestHeaders(domain=self.base_domain)
        req = self.endpoint + "/"

        # Get root uuid
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        root_uuid = rspJson["root"]
        helper.validateId(root_uuid)

        # list of types supported
        datatypes = (
            "H5T_STD_I8LE",
            "H5T_STD_U8LE",
            "H5T_STD_I16LE",
            "H5T_STD_U16LE",
            "H5T_STD_I32LE",
            "H5T_STD_U32LE",
            "H5T_STD_I64LE",
            "H5T_STD_U64LE",
            "H5T_IEEE_F32LE",
            "H5T_IEEE_F64LE",
            "H5T_IEEE_F16LE",
        )

        for datatype in datatypes:
            data = {"type": datatype}
            req = self.endpoint + "/datatypes"
            rsp = self.session.post(req, data=json.dumps(data), headers=headers)
            self.assertEqual(rsp.status_code, 201)  # create datatypes
            rspJson = json.loads(rsp.text)
            dtype_uuid = rspJson["id"]
            self.assertTrue(helper.validateId(dtype_uuid))

            # read back the obj
            req = self.endpoint + "/datatypes/" + dtype_uuid
            rsp = self.session.get(req, headers=headers)
            self.assertEqual(rsp.status_code, 200)
            rspJson = json.loads(rsp.text)
            self.assertTrue("id" in rspJson)
            self.assertEqual(rspJson["id"], dtype_uuid)
            self.assertTrue("root" in rspJson)
            self.assertEqual(rspJson["root"], root_uuid)
            self.assertTrue("created" in rspJson)
            self.assertTrue("lastModified" in rspJson)
            self.assertTrue("attributeCount" in rspJson)
            self.assertEqual(rspJson["attributeCount"], 0)
            self.assertTrue("type" in rspJson)
            type_json = rspJson["type"]
            self.assertEqual(type_json["base"], datatype)

            # link new datatype using the type name
            name = datatype
            req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
            data = {"id": dtype_uuid}
            rsp = self.session.put(req, data=json.dumps(data), headers=headers)
            self.assertEqual(rsp.status_code, 201)

            # Try getting the datatype by h5path
            req = self.endpoint + "/datatypes/"
            h5path = "/" + datatype
            params = {"h5path": h5path}
            rsp = self.session.get(req, headers=headers, params=params)
            self.assertEqual(rsp.status_code, 200)
            rspJson = json.loads(rsp.text)
            self.assertEqual(rspJson["id"], dtype_uuid)

            # Try again using relative h5path
            req = self.endpoint + "/datatypes/"
            h5path = datatype
            params = {"h5path": h5path}
            rsp = self.session.get(req, headers=headers, params=params)
            self.assertEqual(rsp.status_code, 400)

            # try using relative h5path and parent group id
            req = self.endpoint + "/datatypes/"
            h5path = datatype
            params = {"h5path": h5path, "grpid": root_uuid}
            rsp = self.session.get(req, headers=headers, params=params)
            self.assertEqual(rsp.status_code, 200)
            rspJson = json.loads(rsp.text)
            self.assertEqual(rspJson["id"], dtype_uuid)

    def testPostCompoundType(self):
        print("testPostCompoundType", self.base_domain)
        headers = helper.getRequestHeaders(domain=self.base_domain)
        req = self.endpoint + "/"

        # Get root uuid
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        root_uuid = rspJson["root"]
        helper.validateId(root_uuid)

        fields = (
            {"name": "temp", "type": "H5T_STD_I32LE"},
            {"name": "pressure", "type": "H5T_IEEE_F32LE"},
        )
        datatype = {"class": "H5T_COMPOUND", "fields": fields}
        payload = {"type": datatype}
        req = self.endpoint + "/datatypes"
        rsp = self.session.post(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)  # create datatype
        rspJson = json.loads(rsp.text)
        dtype_uuid = rspJson["id"]
        self.assertTrue(helper.validateId(dtype_uuid))

        # link the new datatype
        name = "dtype_compound"
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
        payload = {"id": dtype_uuid}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # read back the obj
        req = self.endpoint + "/datatypes/" + dtype_uuid
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("id" in rspJson)
        self.assertEqual(rspJson["id"], dtype_uuid)
        self.assertTrue("root" in rspJson)
        self.assertEqual(rspJson["root"], root_uuid)
        self.assertTrue("created" in rspJson)
        self.assertTrue("lastModified" in rspJson)
        self.assertTrue("attributeCount" in rspJson)
        self.assertEqual(rspJson["attributeCount"], 0)
        self.assertTrue("type" in rspJson)
        type_json = rspJson["type"]
        self.assertTrue("class" in type_json)
        self.assertEqual(type_json["class"], "H5T_COMPOUND")
        self.assertTrue("fields" in type_json)
        fields = type_json["fields"]
        self.assertEqual(len(fields), 2)
        field = fields[0]
        self.assertEqual(field["name"], "temp")
        self.assertEqual(field["type"], "H5T_STD_I32LE")
        field = fields[1]
        self.assertEqual(field["name"], "pressure")
        self.assertEqual(field["type"], "H5T_IEEE_F32LE")

    def testPutAttributeDatatype(self):
        # Test creation/deletion of datatype obj

        print("testPutAttributeDatatype", self.base_domain)
        headers = helper.getRequestHeaders(domain=self.base_domain)
        req = self.endpoint + "/"

        # Get root uuid
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        root_uuid = rspJson["root"]
        helper.validateId(root_uuid)

        # create a committed type obj
        data = {"type": "H5T_IEEE_F32LE"}
        req = self.endpoint + "/datatypes"
        rsp = self.session.post(req, data=json.dumps(data), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)
        self.assertEqual(rspJson["attributeCount"], 0)
        ctype_id = rspJson["id"]
        self.assertTrue(helper.validateId(ctype_id))

        # link the new datatype
        name = "dtype_with_attribute"
        req = self.endpoint + "/groups/" + root_uuid + "/links/" + name
        payload = {"id": ctype_id}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # add an attribute
        attr_name = "attr"
        attr_payload = {"type": "H5T_STD_I32LE", "value": 42}
        req = self.endpoint + "/datatypes/" + ctype_id + "/attributes/" + attr_name
        rsp = self.session.put(req, data=json.dumps(attr_payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)  # created

        # read back the obj
        req = self.endpoint + "/datatypes/" + ctype_id
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("id" in rspJson)
        self.assertEqual(rspJson["id"], ctype_id)
        self.assertFalse("attributes" in rspJson)

        # read back the obj with attributes
        params = {"include_attrs": 1}
        rsp = self.session.get(req, headers=headers, params=params)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("id" in rspJson)
        self.assertEqual(rspJson["id"], ctype_id)
        self.assertTrue("attributes" in rspJson)
        attrs = rspJson["attributes"]
        self.assertTrue("attr" in attrs)

    def testPostWithLink(self):
        # test POST with link
        print("testPutAttributeDatatype", self.base_domain)
        headers = helper.getRequestHeaders(domain=self.base_domain)
        req = self.endpoint + "/"

        # Get root uuid
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        root_uuid = rspJson["root"]
        helper.validateId(root_uuid)

        # get root group and verify link count is 0
        req = helper.getEndpoint() + "/groups/" + root_uuid
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertEqual(rspJson["linkCount"], 0)

        payload = {
            "type": "H5T_IEEE_F64LE",
            "link": {"id": root_uuid, "name": "linked_dtype"},
        }

        req = self.endpoint + "/datatypes"
        # create a new ctype
        rsp = self.session.post(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)
        self.assertEqual(rspJson["attributeCount"], 0)
        dtype_uuid = rspJson["id"]
        self.assertTrue(helper.validateId(dtype_uuid))

        # get root group and verify link count is 1
        req = helper.getEndpoint() + "/groups/" + root_uuid
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertEqual(rspJson["linkCount"], 1)

        # read the link back and verify
        req = helper.getEndpoint() + "/groups/" + root_uuid + "/links/linked_dtype"
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)  # link doesn't exist yet
        rspJson = json.loads(rsp.text)
        self.assertTrue("link" in rspJson)
        link_json = rspJson["link"]
        self.assertEqual(link_json["collection"], "datatypes")
        self.assertEqual(link_json["class"], "H5L_TYPE_HARD")
        self.assertEqual(link_json["title"], "linked_dtype")
        self.assertEqual(link_json["id"], dtype_uuid)

        # request the dataset path
        req = helper.getEndpoint() + "/datatypes/" + dtype_uuid
        params = {"getalias": 1}
        rsp = self.session.get(req, params=params, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("alias" in rspJson)
        self.assertEqual(rspJson["alias"], ["/linked_dtype"])


if __name__ == "__main__":
    # setup test files

    unittest.main()
