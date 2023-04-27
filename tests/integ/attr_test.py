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
import numpy as np
import helper


class AttributeTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(AttributeTest, self).__init__(*args, **kwargs)
        self.base_domain = helper.getTestDomainName(self.__class__.__name__)
        print(self.base_domain)
        self.endpoint = helper.getEndpoint()
        helper.setupDomain(self.base_domain)

    def setUp(self):
        self.session = helper.getSession()

    def tearDown(self):
        if self.session:
            self.session.close()

        # main

    def testListAttr(self):
        print("testGroupAttr", self.base_domain)
        headers = helper.getRequestHeaders(domain=self.base_domain)
        req = self.endpoint + "/"

        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        root_uuid = rspJson["root"]
        helper.validateId(root_uuid)

        # get group and verify attribute count is 0
        req = self.endpoint + "/groups/" + root_uuid
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertEqual(rspJson["attributeCount"], 0)  # no attributes

        attr_count = 10

        for i in range(attr_count):
            # create attr
            attr_name = "attr_{}".format(i)
            attr_payload = {"type": "H5T_STD_I32LE", "value": i * 2}
            req = self.endpoint + "/groups/" + root_uuid + "/attributes/" + attr_name
            rsp = self.session.put(req, data=json.dumps(attr_payload), headers=headers)
            self.assertEqual(rsp.status_code, 201)  # create attribute

        # get group and verify attribute count is attr_count
        req = self.endpoint + "/groups/" + root_uuid
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertEqual(rspJson["attributeCount"], attr_count)

        # get all the attributes
        req = self.endpoint + "/groups/" + root_uuid + "/attributes"
        rsp = self.session.get(req, headers=headers)
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
        params = {"IncludeData": 1}
        rsp = self.session.get(req, headers=headers, params=params)
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
            self.assertEqual(attrJson["value"], i * 2)

        # get 3 attributes
        limit = 3
        req = self.endpoint + "/groups/" + root_uuid + "/attributes?Limit=" + str(limit)
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)

        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("attributes" in rspJson)
        attributes = rspJson["attributes"]
        self.assertTrue(isinstance(attributes, list))
        self.assertEqual(len(attributes), limit)

        # get 3 attributes after "attr_5"
        limit = 3
        req = f"{self.endpoint}/groups/{root_uuid}/attributes"
        params = {"Limit": limit, "Marker": "attr_5"}

        rsp = self.session.get(req, headers=headers, params=params)
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
        req = self.endpoint + "/"

        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        root_id = rspJson["root"]

        for col_name in ("groups", "datatypes", "datasets"):
            # create a new obj
            req = self.endpoint + "/" + col_name
            data = None
            if col_name != "groups":
                # this will work for datasets or datatypes
                data = {"type": "H5T_IEEE_F32LE"}

            rsp = self.session.post(req, data=json.dumps(data), headers=headers)
            self.assertEqual(rsp.status_code, 201)
            rspJson = json.loads(rsp.text)
            self.assertEqual(rspJson["attributeCount"], 0)
            obj1_id = rspJson["id"]
            self.assertTrue(helper.validateId(obj1_id))

            # link new obj as '/col_name_obj'
            req = self.endpoint + "/groups/" + root_id + "/links/" + col_name + "_obj"
            payload = {"id": obj1_id}
            rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
            self.assertEqual(rsp.status_code, 201)  # created

            # get obj and verify it has no attributes
            req = self.endpoint + "/" + col_name + "/" + obj1_id
            rsp = self.session.get(req, headers=headers)
            self.assertEqual(rsp.status_code, 200)
            rspJson = json.loads(rsp.text)
            self.assertEqual(rspJson["attributeCount"], 0)  # no attributes

            # do a GET for attribute "attr" (should return 404)
            attr_name = "attr"
            attr_payload = {"type": "H5T_STD_I32LE", "value": 42}
            req = f"{self.endpoint}/{col_name}/{obj1_id}/attributes/{attr_name}"

            rsp = self.session.get(req, headers=headers)
            self.assertEqual(rsp.status_code, 404)  # not found

            # try adding the attribute as a different user
            headers = helper.getRequestHeaders(
                domain=self.base_domain, username="test_user2"
            )
            rsp = self.session.put(req, data=json.dumps(attr_payload), headers=headers)
            self.assertEqual(rsp.status_code, 403)  # forbidden

            # try adding again with original user, but outside this domain
            another_domain = helper.getParentDomain(self.base_domain)
            headers = helper.getRequestHeaders(domain=another_domain)
            rsp = self.session.put(req, data=json.dumps(attr_payload), headers=headers)
            self.assertEqual(rsp.status_code, 400)  # Invalid request

            # try again with original user and proper domain
            headers = helper.getRequestHeaders(domain=self.base_domain)
            rsp = self.session.put(req, data=json.dumps(attr_payload), headers=headers)
            self.assertEqual(rsp.status_code, 201)  # created

            # read the attribute we just created
            rsp = self.session.get(req, headers=headers)
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
            req = self.endpoint + "/" + col_name + "/" + obj1_id
            rsp = self.session.get(req, headers=headers)
            self.assertEqual(rsp.status_code, 200)
            rspJson = json.loads(rsp.text)
            self.assertEqual(rspJson["attributeCount"], 1)  # one attribute

            # try creating the attribute again - should return 409
            req = f"{self.endpoint}/{col_name}/{obj1_id}/attributes/{attr_name}"

            rsp = self.session.put(req, data=json.dumps(attr_payload), headers=headers)
            self.assertEqual(rsp.status_code, 409)  # conflict

            # delete the attribute
            rsp = self.session.delete(req, headers=headers)
            self.assertEqual(rsp.status_code, 200)  # OK

            # get obj and verify attribute count is 0
            req = self.endpoint + "/" + col_name + "/" + obj1_id
            rsp = self.session.get(req, headers=headers)
            self.assertEqual(rsp.status_code, 200)
            rspJson = json.loads(rsp.text)
            self.assertEqual(rspJson["attributeCount"], 0)  # no attributes

    def testEmptyShapeAttr(self):
        print("testEmptyShapeAttr", self.base_domain)
        headers = helper.getRequestHeaders(domain=self.base_domain)
        req = helper.getEndpoint() + "/"

        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        root_uuid = rspJson["root"]
        helper.validateId(root_uuid)

        attr_name = "attr_empty_shape"
        attr_payload = {"type": "H5T_STD_I32LE", "shape": [], "value": 42}
        req = self.endpoint + "/groups/" + root_uuid + "/attributes/" + attr_name
        rsp = self.session.put(req, headers=headers, data=json.dumps(attr_payload))
        self.assertEqual(rsp.status_code, 201)  # created

        # read back the attribute
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)  # OK
        rspJson = json.loads(rsp.text)
        self.assertTrue("name" in rspJson)
        self.assertEqual(rspJson["name"], "attr_empty_shape")
        self.assertTrue("type" in rspJson)
        attr_type = rspJson["type"]
        self.assertEqual(attr_type["base"], "H5T_STD_I32LE")
        self.assertTrue("hrefs" in rspJson)
        self.assertEqual(len(rspJson["hrefs"]), 3)
        self.assertTrue("value" in rspJson)
        self.assertEqual(rspJson["value"], 42)
        self.assertTrue("shape" in rspJson)
        attr_shape = rspJson["shape"]
        self.assertTrue("class" in attr_shape)
        self.assertEqual(attr_shape["class"], "H5S_SCALAR")

    def testNullShapeAttr(self):
        print("testNullShapeAttr", self.base_domain)
        headers = helper.getRequestHeaders(domain=self.base_domain)
        req = helper.getEndpoint() + "/"

        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        root_uuid = rspJson["root"]
        helper.validateId(root_uuid)

        attr_name = "attr_null_shape"
        attr_payload = {"type": "H5T_STD_I32LE", "shape": "H5S_NULL", "value": 42}
        req = self.endpoint + "/groups/" + root_uuid + "/attributes/" + attr_name
        rsp = self.session.put(req, headers=headers, data=json.dumps(attr_payload))
        self.assertEqual(rsp.status_code, 400)  # can't include data

        # try with invalid json body
        rsp = self.session.put(req, headers=headers, data="foobar_content")
        self.assertEqual(rsp.status_code, 400)  # not json data

        # try again without the data
        attr_payload = {"type": "H5T_STD_I32LE", "shape": "H5S_NULL"}
        req = self.endpoint + "/groups/" + root_uuid + "/attributes/" + attr_name
        rsp = self.session.put(req, headers=headers, data=json.dumps(attr_payload))
        self.assertEqual(rsp.status_code, 201)  # Created

        # read back the attribute
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)  # OK
        rspJson = json.loads(rsp.text)
        self.assertTrue("name" in rspJson)
        self.assertEqual(rspJson["name"], attr_name)
        self.assertTrue("type" in rspJson)
        attr_type = rspJson["type"]
        self.assertEqual(attr_type["base"], "H5T_STD_I32LE")
        self.assertTrue("hrefs" in rspJson)
        self.assertEqual(len(rspJson["hrefs"]), 3)
        self.assertTrue("value" in rspJson)
        self.assertEqual(rspJson["value"], None)
        self.assertTrue("shape" in rspJson)
        attr_shape = rspJson["shape"]
        self.assertTrue("class" in attr_shape)
        self.assertEqual(attr_shape["class"], "H5S_NULL")

        # read value should fail with 400
        rsp = self.session.get(req + "/value", headers=headers)
        self.assertEqual(rsp.status_code, 400)  # Bad Request

    def testNoShapeAttr(self):
        print("testNoShapeAttr", self.base_domain)
        headers = helper.getRequestHeaders(domain=self.base_domain)
        req = helper.getEndpoint() + "/"

        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        root_uuid = rspJson["root"]
        helper.validateId(root_uuid)

        attr_name = "attr_no_shape"
        attr_payload = {"type": "H5T_STD_I32LE"}
        req = self.endpoint + "/groups/" + root_uuid + "/attributes/" + attr_name
        rsp = self.session.put(req, headers=headers, data=json.dumps(attr_payload))
        self.assertEqual(rsp.status_code, 201)  # created

        # read back the attribute
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)  # OK
        rspJson = json.loads(rsp.text)
        self.assertTrue("name" in rspJson)
        self.assertEqual(rspJson["name"], attr_name)
        self.assertTrue("type" in rspJson)
        attr_type = rspJson["type"]
        self.assertEqual(attr_type["base"], "H5T_STD_I32LE")
        self.assertTrue("hrefs" in rspJson)
        self.assertEqual(len(rspJson["hrefs"]), 3)
        self.assertTrue("value" in rspJson)
        self.assertEqual(rspJson["value"], None)
        self.assertTrue("shape" in rspJson)
        attr_shape = rspJson["shape"]
        self.assertTrue("class" in attr_shape)
        self.assertEqual(attr_shape["class"], "H5S_SCALAR")
        self.assertFalse("dims" in attr_shape)

        # read value should return None
        rsp = self.session.get(req + "/value", headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("value" in rspJson)
        self.assertEqual(rspJson["value"], None)

    def testPutFixedString(self):
        # Test PUT value for 1d attribute with fixed length string types
        print("testPutFixedString", self.base_domain)

        headers = helper.getRequestHeaders(domain=self.base_domain)
        req = self.endpoint + "/"

        # Get root uuid
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        root_uuid = rspJson["root"]
        helper.validateId(root_uuid)

        # create attr
        words = ["Parting", "is such", "sweet", "sorrow."]
        fixed_str_type = {
            "charSet": "H5T_CSET_ASCII",
            "class": "H5T_STRING",
            "length": 7,
            "strPad": "H5T_STR_NULLPAD",
        }
        data = {"type": fixed_str_type, "shape": 4, "value": words}
        attr_name = "str_attr"
        req = self.endpoint + "/groups/" + root_uuid + "/attributes/" + attr_name
        rsp = self.session.put(req, data=json.dumps(data), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # read attr
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        self.assertEqual(rspJson["value"], words)
        self.assertTrue("type" in rspJson)
        type_json = rspJson["type"]
        self.assertTrue("class" in type_json)
        self.assertEqual(type_json["class"], "H5T_STRING")
        self.assertTrue("length" in type_json)
        self.assertEqual(type_json["length"], 7)

    def testPutFixedStringNullTerm(self):
        # Test PUT value for 1d attribute with fixed length string/null terminated types
        print("testPutFixedStringNullTerm", self.base_domain)

        headers = helper.getRequestHeaders(domain=self.base_domain)
        req = self.endpoint + "/"

        # Get root uuid
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        root_uuid = rspJson["root"]
        helper.validateId(root_uuid)

        # create attr
        text = "I'm an ASCII null terminated string"
        text_length = len(text) + 1
        fixed_str_type = {
            "charSet": "H5T_CSET_ASCII",
            "class": "H5T_STRING",
            "length": text_length,
            "strPad": "H5T_STR_NULLTERM",
        }
        scalar_shape = {"class": "H5S_SCALAR"}
        data = {"type": fixed_str_type, "shape": scalar_shape, "value": text}
        attr_name = "str_attr"
        req = self.endpoint + "/groups/" + root_uuid + "/attributes/" + attr_name
        rsp = self.session.put(req, data=json.dumps(data), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # read attr
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        self.assertEqual(rspJson["value"], text)
        self.assertTrue("type" in rspJson)
        type_json = rspJson["type"]
        self.assertTrue("class" in type_json)
        self.assertEqual(type_json["class"], "H5T_STRING")
        self.assertTrue("length" in type_json)
        self.assertEqual(type_json["length"], text_length)
        self.assertTrue("strPad" in type_json)
        self.assertEqual(type_json["strPad"], "H5T_STR_NULLTERM")
        self.assertTrue("charSet" in type_json)
        self.assertEqual(type_json["charSet"], "H5T_CSET_ASCII")

    def testPutVLenUTF8String(self):
        # Test PUT value for 1d attribute with fixed length UTF-8 string
        print("testPutFixedUTF8String", self.base_domain)

        headers = helper.getRequestHeaders(domain=self.base_domain)
        req = self.endpoint + "/"

        # Get root uuid
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        root_uuid = rspJson["root"]
        helper.validateId(root_uuid)

        # create attr
        text = "I'm an UTF-8 null terminated string"
        text_length = len(text) + 1
        fixed_str_type = {
            "charSet": "H5T_CSET_UTF8",
            "class": "H5T_STRING",
            "length": text_length,
            "strPad": "H5T_STR_NULLTERM",
        }
        variable_str_type = {
            "charSet": "H5T_CSET_UTF8",
            "class": "H5T_STRING",
            "length": "H5T_VARIABLE",
            "strPad": "H5T_STR_NULLTERM",
        }
        scalar_shape = {"class": "H5S_SCALAR"}
        data = {"type": fixed_str_type, "shape": scalar_shape, "value": text}
        attr_name = "str_attr"
        req = self.endpoint + "/groups/" + root_uuid + "/attributes/" + attr_name
        # Should fail since UTF8 with fixed width is not supported
        rsp = self.session.put(req, data=json.dumps(data), headers=headers)
        self.assertEqual(rsp.status_code, 400)

        data = {"type": variable_str_type, "shape": scalar_shape, "value": text}
        attr_name = "str_attr"
        req = self.endpoint + "/groups/" + root_uuid + "/attributes/" + attr_name
        rsp = self.session.put(req, data=json.dumps(data), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # read attr
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        self.assertEqual(rspJson["value"], text)
        self.assertTrue("type" in rspJson)
        type_json = rspJson["type"]
        self.assertTrue("class" in type_json)
        self.assertEqual(type_json["class"], "H5T_STRING")
        self.assertTrue("length" in type_json)
        self.assertEqual(type_json["length"], "H5T_VARIABLE")
        self.assertTrue("strPad" in type_json)
        self.assertEqual(type_json["strPad"], "H5T_STR_NULLTERM")
        self.assertTrue("charSet" in type_json)
        self.assertEqual(type_json["charSet"], "H5T_CSET_UTF8")

    def testPutVLenString(self):
        # Test PUT value for 1d attribute with variable length string types
        print("testPutVLenString", self.base_domain)

        headers = helper.getRequestHeaders(domain=self.base_domain)
        req = self.endpoint + "/"

        # Get root uuid
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        root_uuid = rspJson["root"]
        helper.validateId(root_uuid)

        # create attr
        words = ["Parting", "is such", "sweet", "sorrow."]
        fixed_str_type = {
            "charSet": "H5T_CSET_ASCII",
            "class": "H5T_STRING",
            "length": "H5T_VARIABLE",
            "strPad": "H5T_STR_NULLTERM",
        }
        data = {"type": fixed_str_type, "shape": 4, "value": words}
        attr_name = "str_attr"
        req = self.endpoint + "/groups/" + root_uuid + "/attributes/" + attr_name
        rsp = self.session.put(req, data=json.dumps(data), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # read attr
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        self.assertEqual(rspJson["value"], words)
        self.assertTrue("type" in rspJson)
        type_json = rspJson["type"]
        self.assertTrue("class" in type_json)
        self.assertEqual(type_json["class"], "H5T_STRING")
        self.assertTrue("length" in type_json)
        self.assertEqual(type_json["length"], "H5T_VARIABLE")

    def testPutVLenInt(self):
        # Test PUT value for 1d attribute with variable length int types
        print("testPutVLenInt", self.base_domain)

        headers = helper.getRequestHeaders(domain=self.base_domain)
        req = self.endpoint + "/"

        # Get root uuid
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        root_uuid = rspJson["root"]
        helper.validateId(root_uuid)

        # create attr
        vlen_type = {
            "class": "H5T_VLEN",
            "base": {"class": "H5T_INTEGER", "base": "H5T_STD_I32LE"},
        }
        value = [
            [
                1,
            ],
            [1, 2],
            [1, 2, 3],
            [1, 2, 3, 4],
        ]
        data = {"type": vlen_type, "shape": 4, "value": value}
        attr_name = "vlen_int_attr"
        req = self.endpoint + "/groups/" + root_uuid + "/attributes/" + attr_name
        rsp = self.session.put(req, data=json.dumps(data), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # read attr
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        self.assertEqual(rspJson["value"], value)
        self.assertTrue("type" in rspJson)
        type_json = rspJson["type"]
        self.assertTrue("class" in type_json)
        self.assertEqual(type_json["class"], "H5T_VLEN")
        self.assertTrue("base" in type_json)
        base_type = type_json["base"]
        self.assertTrue("class" in base_type)
        self.assertEqual(base_type["class"], "H5T_INTEGER")
        self.assertTrue("base" in base_type)
        self.assertEqual(base_type["base"], "H5T_STD_I32LE")
        self.assertTrue("shape" in rspJson)
        shape_json = rspJson["shape"]
        self.assertTrue("class" in shape_json)
        self.assertEqual(shape_json["class"], "H5S_SIMPLE")
        self.assertTrue("dims" in shape_json)
        self.assertEqual(
            shape_json["dims"],
            [
                4,
            ],
        )

    def testPutInvalid(self):
        print("testPutInvalid", self.base_domain)
        headers = helper.getRequestHeaders(domain=self.base_domain)
        req = self.endpoint + "/"

        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        root_id = rspJson["root"]

        # try creating an attribute with an invalid type
        attr_name = "attr1"
        attr_payload = {"type": "H5T_FOOBAR", "value": 42}
        req = self.endpoint + "/groups/" + root_id + "/attributes/" + attr_name
        rsp = self.session.put(req, data=json.dumps(attr_payload), headers=headers)
        self.assertEqual(rsp.status_code, 400)  # invalid request

    def testPutCommittedType(self):
        print("testPutCommittedType", self.base_domain)
        headers = helper.getRequestHeaders(domain=self.base_domain)
        req = self.endpoint + "/"

        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        root_id = rspJson["root"]

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
        req = self.endpoint + "/groups/" + root_id + "/links/" + name
        payload = {"id": dtype_uuid}
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # create the attribute using the type created above
        attr_name = "attr1"
        value = []
        for i in range(10):
            value.append(i * 0.5)
        payload = {"type": dtype_uuid, "shape": 10, "value": value}
        req = self.endpoint + "/groups/" + root_id + "/attributes/" + attr_name
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)  # create attribute

        # read back the attribute and verify the type
        req = self.endpoint + "/groups/" + root_id + "/attributes/attr1"
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("type" in rspJson)
        rsp_type = rspJson["type"]
        self.assertTrue("base" in rsp_type)
        self.assertEqual(rsp_type["base"], "H5T_IEEE_F32LE")
        self.assertTrue("class" in rsp_type)
        self.assertTrue(rsp_type["class"], "H5T_FLOAT")
        self.assertTrue("id" in rsp_type)
        self.assertTrue(rsp_type["id"], dtype_uuid)

    def testPutCompound(self):
        print("testPutCompoundType", self.base_domain)
        headers = helper.getRequestHeaders(domain=self.base_domain)
        req = self.endpoint + "/"

        # Get root uuid
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        root_id = rspJson["root"]
        helper.validateId(root_id)

        fields = (
            {"name": "temp", "type": "H5T_STD_I32LE"},
            {"name": "pressure", "type": "H5T_IEEE_F32LE"},
        )
        datatype = {"class": "H5T_COMPOUND", "fields": fields}
        value = (42, 0.42)

        #
        # create compound scalar attribute
        #
        attr_name = "attr0d"
        payload = {"type": datatype, "value": value}
        req = self.endpoint + "/groups/" + root_id + "/attributes/" + attr_name
        rsp = self.session.put(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)  # create attribute

        # read back the attribute
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        self.assertTrue("type" in rspJson)
        rsp_type = rspJson["type"]
        self.assertTrue("class" in rsp_type)
        self.assertTrue(rsp_type["class"], "H5T_COMPOUND")
        self.assertTrue("fields" in rsp_type)
        rsp_fields = rsp_type["fields"]
        self.assertEqual(len(rsp_fields), 2)
        rsp_field_0 = rsp_fields[0]
        self.assertTrue("type" in rsp_field_0)
        self.assertEqual(rsp_field_0["type"], "H5T_STD_I32LE")
        self.assertTrue("name" in rsp_field_0)
        self.assertEqual(rsp_field_0["name"], "temp")
        rsp_field_1 = rsp_fields[1]
        self.assertTrue("type" in rsp_field_1)
        self.assertEqual(rsp_field_1["type"], "H5T_IEEE_F32LE")
        self.assertTrue("name" in rsp_field_1)
        self.assertEqual(rsp_field_1["name"], "pressure")

        self.assertTrue("shape" in rspJson)
        rsp_shape = rspJson["shape"]
        self.assertTrue("class" in rsp_shape)
        self.assertEqual(rsp_shape["class"], "H5S_SCALAR")

        self.assertTrue("value" in rspJson)
        self.assertEqual(rspJson["value"], [42, 0.42])

    def testPutObjReference(self):
        print("testPutObjReference", self.base_domain)
        headers = helper.getRequestHeaders(domain=self.base_domain)
        req = self.endpoint + "/"

        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        root_id = rspJson["root"]

        # create group "g1"
        payload = {"link": {"id": root_id, "name": "g1"}}
        req = helper.getEndpoint() + "/groups"
        rsp = self.session.post(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)
        g1_id = rspJson["id"]
        self.assertTrue(helper.validateId(g1_id))
        self.assertTrue(g1_id != root_id)

        # create group "g2"
        payload = {"link": {"id": root_id, "name": "g2"}}
        req = helper.getEndpoint() + "/groups"
        rsp = self.session.post(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)
        g2_id = rspJson["id"]
        self.assertTrue(helper.validateId(g1_id))
        self.assertTrue(g1_id != g2_id)

        # create attr of g1 that is a reference to g2
        ref_type = {"class": "H5T_REFERENCE", "base": "H5T_STD_REF_OBJ"}
        attr_name = "g1_ref"
        value = "groups/" + g2_id
        data = {"type": ref_type, "value": value}
        req = self.endpoint + "/groups/" + g1_id + "/attributes/" + attr_name
        rsp = self.session.put(req, data=json.dumps(data), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # read back the attribute and verify the type, space, and value
        req = self.endpoint + "/groups/" + g1_id + "/attributes/" + attr_name
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("type" in rspJson)
        rsp_type = rspJson["type"]
        self.assertTrue("base" in rsp_type)
        self.assertEqual(rsp_type["base"], "H5T_STD_REF_OBJ")
        self.assertTrue("class" in rsp_type)
        self.assertTrue(rsp_type["class"], "H5T_REFERENCE")
        self.assertTrue("shape" in rspJson)
        rsp_shape = rspJson["shape"]
        self.assertTrue("class" in rsp_shape)
        self.assertEqual(rsp_shape["class"], "H5S_SCALAR")
        self.assertTrue("value" in rspJson)
        self.assertEqual(rspJson["value"], value)

    def testPutVlenObjReference(self):
        print("testPutVlenObjReference", self.base_domain)
        headers = helper.getRequestHeaders(domain=self.base_domain)
        req = self.endpoint + "/"

        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        root_id = rspJson["root"]

        # create group "g1"
        payload = {"link": {"id": root_id, "name": "g1"}}
        req = helper.getEndpoint() + "/groups"
        rsp = self.session.post(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)
        g1_id = rspJson["id"]
        self.assertTrue(helper.validateId(g1_id))
        self.assertTrue(g1_id != root_id)

        # create group "g1/g1_1"
        payload = {"link": {"id": g1_id, "name": "g1_1"}}
        req = helper.getEndpoint() + "/groups"
        rsp = self.session.post(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)
        g1_1_id = rspJson["id"]
        self.assertTrue(helper.validateId(g1_1_id))

        # create group "g1/g1_2"
        payload = {"link": {"id": g1_id, "name": "g1_2"}}
        req = helper.getEndpoint() + "/groups"
        rsp = self.session.post(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)
        g1_2_id = rspJson["id"]
        self.assertTrue(helper.validateId(g1_2_id))

        # create group "g1/g1_3"
        payload = {"link": {"id": g1_id, "name": "g1_3"}}
        req = helper.getEndpoint() + "/groups"
        rsp = self.session.post(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)
        g1_3_id = rspJson["id"]
        self.assertTrue(helper.validateId(g1_3_id))

        # create attr of g1 that is a vlen list of obj ref's
        ref_type = {"class": "H5T_REFERENCE", "base": "H5T_STD_REF_OBJ"}
        vlen_type = {"class": "H5T_VLEN", "base": ref_type}
        attr_name = "obj_ref"
        value = [
            [
                g1_1_id,
            ],
            [g1_1_id, g1_2_id],
            [g1_1_id, g1_2_id, g1_3_id],
        ]
        data = {"type": vlen_type, "shape": 3, "value": value}
        req = self.endpoint + "/groups/" + g1_id + "/attributes/" + attr_name
        rsp = self.session.put(req, data=json.dumps(data), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # read back the attribute and verify the type, space, and value
        req = self.endpoint + "/groups/" + g1_id + "/attributes/" + attr_name
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("type" in rspJson)
        rsp_type = rspJson["type"]
        self.assertTrue("class" in rsp_type)
        self.assertEqual(rsp_type["class"], "H5T_VLEN")
        self.assertTrue("base" in rsp_type)
        rsp_type_base = rsp_type["base"]
        self.assertTrue("base" in rsp_type_base)
        self.assertEqual(rsp_type_base["base"], "H5T_STD_REF_OBJ")
        self.assertTrue("class" in rsp_type_base)
        self.assertTrue(rsp_type["class"], "H5T_REFERENCE")
        self.assertTrue("shape" in rspJson)
        rsp_shape = rspJson["shape"]
        self.assertTrue("class" in rsp_shape)
        self.assertEqual(rsp_shape["class"], "H5S_SIMPLE")
        self.assertTrue("dims" in rsp_shape)
        self.assertEqual(
            rsp_shape["dims"],
            [
                3,
            ],
        )
        self.assertTrue("value" in rspJson)
        vlen_values = rspJson["value"]
        self.assertEqual(len(vlen_values), 3)
        for i in range(3):
            vlen_element = vlen_values[i]
            self.assertEqual(len(vlen_element), i + 1)

    def testPutCompoundObjReference(self):
        print("testPutCompoundObjReference", self.base_domain)
        headers = helper.getRequestHeaders(domain=self.base_domain)
        req = self.endpoint + "/"

        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        root_id = rspJson["root"]

        # create group "g1"
        payload = {"link": {"id": root_id, "name": "g1"}}
        req = helper.getEndpoint() + "/groups"
        rsp = self.session.post(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)
        g1_id = rspJson["id"]
        self.assertTrue(helper.validateId(g1_id))
        self.assertTrue(g1_id != root_id)

        # create dataset "dset"
        payload = {"link": {"id": root_id, "name": "dset"}}

        # create the dataset
        req = self.endpoint + "/datasets"

        payload = {
            "type": "H5T_IEEE_F32LE",
            "shape": [5, 8],
            "link": {"id": root_id, "name": "dset"},
        }
        rsp = self.session.post(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)  # create dataset
        rspJson = json.loads(rsp.text)
        dset_id = rspJson["id"]
        self.assertTrue(helper.validateId(dset_id))

        # create attr of g1 that is a reference to g2
        ref_type = {
            "class": "H5T_REFERENCE",
            "base": "H5T_STD_REF_OBJ",
            "charSet": "H5T_CSET_ASCII",
            "length": 38,
        }
        compound_type = {
            "class": "H5T_COMPOUND",
            "fields": [
                {"name": "dataset", "type": ref_type},
                {
                    "name": "dimension",
                    "type": {"class": "H5T_INTEGER", "base": "H5T_STD_I32LE"},
                },
            ],
        }
        attr_name = "dset_ref"
        value = [
            [dset_id, 0],
        ]
        data = {
            "type": compound_type,
            "shape": [
                1,
            ],
            "value": value,
        }
        req = self.endpoint + "/groups/" + g1_id + "/attributes/" + attr_name
        rsp = self.session.put(req, data=json.dumps(data), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # read back the attribute and verify the type, space, and value
        req = self.endpoint + "/groups/" + g1_id + "/attributes/" + attr_name
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("type" in rspJson)
        rsp_type = rspJson["type"]

        self.assertTrue("class" in rsp_type)
        self.assertTrue(rsp_type["class"], "H5T_COMPOUND")
        self.assertTrue("fields" in rsp_type)
        rsp_fields = rsp_type["fields"]
        self.assertEqual(len(rsp_fields), 2)
        rsp_field_0 = rsp_fields[0]
        self.assertTrue("type" in rsp_field_0)
        rsp_field_0_type = rsp_field_0["type"]
        self.assertTrue("class" in rsp_field_0_type)
        self.assertEqual(rsp_field_0_type["class"], "H5T_REFERENCE")
        self.assertTrue("base" in rsp_field_0_type)
        self.assertEqual(rsp_field_0_type["base"], "H5T_STD_REF_OBJ")
        self.assertTrue("charSet" in rsp_field_0_type)
        self.assertEqual(rsp_field_0_type["charSet"], "H5T_CSET_ASCII")
        self.assertTrue("length" in rsp_field_0_type)
        self.assertEqual(rsp_field_0_type["length"], 38)

        rsp_field_1 = rsp_fields[1]
        self.assertTrue("type" in rsp_field_1)
        rsp_field_1_type = rsp_field_1["type"]
        self.assertTrue("class" in rsp_field_1_type)
        self.assertEqual(rsp_field_1_type["class"], "H5T_INTEGER")
        self.assertTrue("base" in rsp_field_1_type)
        self.assertEqual(rsp_field_1_type["base"], "H5T_STD_I32LE")

        self.assertTrue("name" in rsp_field_1)
        self.assertEqual(rsp_field_1["name"], "dimension")

        self.assertTrue("shape" in rspJson)
        rsp_shape = rspJson["shape"]
        self.assertTrue("class" in rsp_shape)
        self.assertEqual(rsp_shape["class"], "H5S_SIMPLE")
        self.assertTrue("dims" in rsp_shape)
        self.assertEqual(
            rsp_shape["dims"],
            [
                1,
            ],
        )
        self.assertTrue("value" in rspJson)
        self.assertEqual(
            rspJson["value"],
            [
                [dset_id, 0],
            ],
        )

    def testPutNoData(self):
        # Test PUT value for 1d attribute without any data provided
        print("testPutNoData", self.base_domain)

        headers = helper.getRequestHeaders(domain=self.base_domain)
        req = self.endpoint + "/"

        # Get root uuid
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        root_uuid = rspJson["root"]
        helper.validateId(root_uuid)

        # create attr
        fixed_str_type = {
            "charSet": "H5T_CSET_ASCII",
            "class": "H5T_STRING",
            "length": 7,
            "strPad": "H5T_STR_NULLPAD",
        }
        data = {"type": fixed_str_type, "shape": 4}
        attr_name = "str_attr"
        req = self.endpoint + "/groups/" + root_uuid + "/attributes/" + attr_name
        rsp = self.session.put(req, data=json.dumps(data), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # read attr
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        self.assertTrue(rspJson["value"] is None)
        self.assertTrue("type" in rspJson)
        type_json = rspJson["type"]
        self.assertTrue("class" in type_json)
        self.assertEqual(type_json["class"], "H5T_STRING")
        self.assertTrue("length" in type_json)
        self.assertEqual(type_json["length"], 7)

        # create attr with 2D float type
        data = {
            "type": {"class": "H5T_FLOAT", "base": "H5T_IEEE_F32LE"},
            "shape": [2, 3],
        }
        attr_name = "float_attr"
        req = self.endpoint + "/groups/" + root_uuid + "/attributes/" + attr_name
        rsp = self.session.put(req, data=json.dumps(data), headers=headers)
        self.assertEqual(rsp.status_code, 201)

    def testPutIntegerArray(self):
        # Test PUT value for 1d attribute with list of integers
        print("testPutIntegerArray", self.base_domain)

        headers = helper.getRequestHeaders(domain=self.base_domain)
        req = self.endpoint + "/"

        # Get root uuid
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        root_uuid = rspJson["root"]
        helper.validateId(root_uuid)

        # create attr
        value = [2, 3, 5, 7, 11, 13]
        data = {"type": "H5T_STD_I32LE", "shape": 6, "value": value}
        attr_name = "int_arr_attr"
        req = self.endpoint + "/groups/" + root_uuid + "/attributes/" + attr_name
        rsp = self.session.put(req, data=json.dumps(data), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # read attr
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        self.assertEqual(rspJson["value"], value)
        self.assertTrue("type" in rspJson)
        type_json = rspJson["type"]
        self.assertTrue("class" in type_json)
        self.assertEqual(type_json["base"], "H5T_STD_I32LE")
        self.assertTrue("shape" in rspJson)
        shape_json = rspJson["shape"]
        self.assertTrue("class" in shape_json)
        self.assertTrue(shape_json["class"], "H5S_SIMPLE")
        self.assertTrue("dims" in shape_json)
        self.assertTrue(shape_json["dims"], [6])

        # try creating an array where the shape doesn't match data values
        data = {"type": "H5T_STD_I32LE", "shape": 5, "value": value}
        attr_name = "badarg_arr_attr"
        req = self.endpoint + "/groups/" + root_uuid + "/attributes/" + attr_name
        rsp = self.session.put(req, data=json.dumps(data), headers=headers)
        self.assertEqual(rsp.status_code, 400)  # Bad request

    def testGetAttributeJsonValue(self):
        # Test GET Attribute value with JSON response
        print("testGetAttributeJsonValue", self.base_domain)

        headers = helper.getRequestHeaders(domain=self.base_domain)
        req = self.endpoint + "/"

        # Get root uuid
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        root_uuid = rspJson["root"]
        helper.validateId(root_uuid)

        # create attr
        value = [2, 3, 5, 7, 11, 13]
        data = {"type": "H5T_STD_I32LE", "shape": 6, "value": value}
        attr_name = "int_arr_attr"
        req = self.endpoint + "/groups/" + root_uuid + "/attributes/" + attr_name
        rsp = self.session.put(req, data=json.dumps(data), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # read attr
        req = f"{self.endpoint}/groups/{root_uuid}/attributes/{attr_name}/value"

        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        self.assertFalse("type" in rspJson)
        self.assertFalse("shape" in rspJson)
        self.assertEqual(rspJson["value"], value)

    def testGetAttributeBinaryValue(self):
        # Test GET Attribute value with binary response
        print("testGetAttributeBinaryValue", self.base_domain)

        headers = helper.getRequestHeaders(domain=self.base_domain)
        headers_bin_rsp = helper.getRequestHeaders(domain=self.base_domain)
        headers_bin_rsp["accept"] = "application/octet-stream"

        req = self.endpoint + "/"

        # Get root uuid
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        root_uuid = rspJson["root"]
        helper.validateId(root_uuid)

        # create attr
        value = [2, 3, 5, 7, 11, 13]
        data = {"type": "H5T_STD_I32LE", "shape": len(value), "value": value}
        attr_name = "int_arr_bin_get_attr"
        req = self.endpoint + "/groups/" + root_uuid + "/attributes/" + attr_name
        rsp = self.session.put(req, data=json.dumps(data), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # read attr
        req = f"{self.endpoint}/groups/{root_uuid}/attributes/{attr_name}/value"

        rsp = self.session.get(req, headers=headers_bin_rsp)
        self.assertEqual(rsp.status_code, 200)
        self.assertEqual(rsp.headers["Content-Type"], "application/octet-stream")
        data = rsp.content
        self.assertEqual(len(data), len(value) * 4)
        for i in range(len(value)):
            offset = i * 4
            self.assertEqual(data[offset + 0], value[i])
            self.assertEqual(data[offset + 1], 0)
            self.assertEqual(data[offset + 2], 0)
            self.assertEqual(data[offset + 3], 0)

    def testPutAttributeBinaryValue(self):
        # Test Put Attribute value with binary response
        print("testGetAttributeBinaryValue", self.base_domain)

        headers = helper.getRequestHeaders(domain=self.base_domain)
        headers_bin_req = helper.getRequestHeaders(domain=self.base_domain)
        headers_bin_req["Content-Type"] = "application/octet-stream"

        req = self.endpoint + "/"

        # Get root uuid
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        root_uuid = rspJson["root"]
        helper.validateId(root_uuid)

        # create attr without any data
        value = [2, 3, 5, 7, 11, 13]
        extent = len(value)
        body = {"type": "H5T_STD_I32LE", "shape": extent}
        attr_name = "int_arr_bin_put_attr"
        req = self.endpoint + "/groups/" + root_uuid + "/attributes/" + attr_name
        rsp = self.session.put(req, data=json.dumps(body), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # read attr - values should be all zeros
        req = f"{self.endpoint}/groups/{root_uuid}/attributes/{attr_name}/value"

        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        self.assertFalse("type" in rspJson)
        self.assertFalse("shape" in rspJson)
        self.assertEqual(rspJson["value"], None)

        # write binary data
        # write values as four-byte little-endian integers
        data = bytearray(4 * extent)
        for i in range(extent):
            data[i * 4] = value[i]
        rsp = self.session.put(req, data=data, headers=headers_bin_req)
        self.assertEqual(rsp.status_code, 200)

        # try writing to few bytes, should fail
        data = bytearray(extent)
        for i in range(extent):
            data[i] = 255
        rsp = self.session.put(req, data=data, headers=headers_bin_req)
        self.assertEqual(rsp.status_code, 400)

        # read attr
        req = f"{self.endpoint}/groups/{root_uuid}/attributes/{attr_name}/value"

        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertTrue("value" in rspJson)
        self.assertFalse("type" in rspJson)
        self.assertFalse("shape" in rspJson)
        self.assertEqual(rspJson["value"], value)

    def testNaNAttributeValue(self):
        # Test GET Attribute value with JSON response that contains NaN data
        print("testNaNAttributeValue", self.base_domain)

        headers = helper.getRequestHeaders(domain=self.base_domain)
        req = self.endpoint + "/"

        # Get root uuid
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        root_uuid = rspJson["root"]
        helper.validateId(root_uuid)

        # create attr
        value = [
            np.NaN,
        ] * 6
        data = {"type": "H5T_IEEE_F32LE", "shape": 6, "value": value}
        attr_name = "nan_arr_attr"
        req = self.endpoint + "/groups/" + root_uuid + "/attributes/" + attr_name
        rsp = self.session.put(req, data=json.dumps(data), headers=headers)
        self.assertEqual(rsp.status_code, 201)

        # get all attributes, then by name, and then by value
        for req_suffix in ("", f"/{attr_name}", f"/{attr_name}/value"):
            for ignore_nan in (False, True):
                req = (
                    self.endpoint + "/groups/" + root_uuid + "/attributes" + req_suffix
                )
                params = {}
                if not req_suffix:
                    # fetch data when getting all attribute
                    params["IncludeData"] = 1
                if ignore_nan:
                    params["ignore_nan"] = 1
                rsp = self.session.get(req, headers=headers, params=params)
                if rsp.status_code == 500 and not ignore_nan:
                    # nan value can generate json encoiding errors
                    continue
                self.assertEqual(rsp.status_code, 200)
                rspJson = json.loads(rsp.text)
                self.assertTrue("hrefs" in rspJson)
                if "attributes" in rspJson:
                    # this is returned for the fetch all attribute req
                    attrs = rspJson["attributes"]
                    self.assertEqual(len(attrs), 1)
                    self.assertTrue("value" in attrs[0])
                    rspValue = attrs[0]["value"]
                else:
                    self.assertTrue("value" in rspJson)
                    rspValue = rspJson["value"]
                self.assertEqual(len(rspValue), 6)
                for i in range(6):
                    if ignore_nan:
                        self.assertTrue(rspValue[i] is None)
                    else:
                        self.assertTrue(np.isnan(rspValue[i]))


if __name__ == "__main__":
    # setup test files

    unittest.main()
