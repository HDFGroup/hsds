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
import time
import json
import uuid

from h5json.objid import createObjId

import helper
import config


class GroupTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(GroupTest, self).__init__(*args, **kwargs)
        self.base_domain = helper.getTestDomainName(self.__class__.__name__)
        helper.setupDomain(self.base_domain)

    def setUp(self):
        self.session = helper.getSession()

    def tearDown(self):
        if self.session:
            self.session.close()

        # main

    def testGetRootGroup(self):
        print("testGetRootGroup", self.base_domain)
        headers = helper.getRequestHeaders(domain=self.base_domain)
        req = helper.getEndpoint() + "/"

        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        root_uuid = rspJson["root"]
        helper.validateId(root_uuid)
        req = helper.getEndpoint() + "/groups/" + root_uuid
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("id" in rspJson)
        group_id = rspJson["id"]
        helper.validateId(group_id)
        self.assertTrue("root" in rspJson)
        root_id = rspJson["root"]
        self.assertEqual(group_id, root_id)
        self.assertTrue("domain" in rspJson)
        self.assertEqual(rspJson["domain"], self.base_domain)
        self.assertTrue("created" in rspJson)
        self.assertTrue("lastModified" in rspJson)
        self.assertTrue("linkCount" in rspJson)
        self.assertTrue("attributeCount" in rspJson)

        # try get with a different user (who has read permission)
        user2_name = config.get("user2_name")
        if user2_name:
            headers = helper.getRequestHeaders(
                domain=self.base_domain, username=user2_name
            )
            rsp = self.session.get(req, headers=headers)
            if config.get("default_public"):
                self.assertEqual(rsp.status_code, 200)
                rspJson = json.loads(rsp.text)
                self.assertEqual(rspJson["root"], root_uuid)
            else:
                self.assertEqual(rsp.status_code, 403)
        else:
            print("user2_name not set")

        # try to do a GET with a different domain (should fail)
        another_domain = helper.getParentDomain(self.base_domain)
        headers = helper.getRequestHeaders(domain=another_domain)
        req = helper.getEndpoint() + "/groups/" + root_uuid
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 400)

    def testGet(self):
        domain = helper.getTestDomain("tall.h5")

        headers = helper.getRequestHeaders(domain=domain)

        # verify domain exists
        req = helper.getEndpoint() + "/"
        rsp = self.session.get(req, headers=headers)
        if rsp.status_code != 200:
            msg = f"WARNING: Failed to get domain: {domain}. Is test data setup?"
            print(msg)

            return  # abort rest of test

        rspJson = json.loads(rsp.text)

        grp_uuid = root_uuid = rspJson["root"]
        self.assertTrue(grp_uuid.startswith("g-"))

        # get the group json
        req = helper.getEndpoint() + "/groups/" + grp_uuid
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        for name in (
            "id",
            "hrefs",
            "attributeCount",
            "linkCount",
            "domain",
            "root",
            "created",
            "lastModified",
        ):
            self.assertTrue(name in rspJson)

        self.assertEqual(rspJson["id"], grp_uuid)

        hrefs = rspJson["hrefs"]
        self.assertEqual(len(hrefs), 5)
        self.assertEqual(rspJson["id"], grp_uuid)
        self.assertEqual(rspJson["attributeCount"], 2)
        self.assertEqual(rspJson["linkCount"], 2)
        self.assertEqual(rspJson["root"], root_uuid)
        self.assertEqual(rspJson["domain"], domain)
        # attribute should only be here if include_attrs is used
        self.assertFalse("attributes" in rspJson)
        # links should onnly be here if include_links is used
        self.assertFalse("links" in rspJson)
        now = time.time()
        # the object shouldn't have been just created or updated
        self.assertTrue(rspJson["created"] < now - 10)
        self.assertTrue(rspJson["lastModified"] < now - 10)

        # request the group path
        req = helper.getEndpoint() + "/groups/" + grp_uuid
        params = {"getalias": 1}
        rsp = self.session.get(req, params=params, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("alias" in rspJson)
        self.assertEqual(rspJson["alias"], ["/"])

        # do a get including the links
        params = {"include_links": 1}
        rsp = self.session.get(req, params=params, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("links" in rspJson)
        links = rspJson["links"]
        self.assertTrue("g1" in links)
        self.assertTrue("g2" in links)

        # do a get including attributes
        params = {"include_attrs": 1}
        rsp = self.session.get(req, params=params, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("attributes" in rspJson)
        attrs = rspJson["attributes"]
        self.assertTrue("attr1" in attrs)
        self.assertTrue("attr2" in attrs)

        # verify trying to read this group from a different domain fails
        headers = helper.getRequestHeaders(domain=self.base_domain)
        req = helper.getEndpoint() + "/groups/" + grp_uuid
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 400)

    def testGetInvalidUUID(self):
        print("testGetInvalidUUID", self.base_domain)
        headers = helper.getRequestHeaders(domain=self.base_domain)
        req = helper.getEndpoint() + "/"
        invalid_uuid = "foobar"
        req = helper.getEndpoint() + "/groups/" + invalid_uuid
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 400)
        bad_uuid = "g-" + str(uuid.uuid1())
        req = helper.getEndpoint() + "/groups/" + bad_uuid
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 404)

    def testPost(self):
        # test POST group
        print("testPost", self.base_domain)
        endpoint = helper.getEndpoint()
        headers = helper.getRequestHeaders(domain=self.base_domain)
        req = endpoint + "/groups"

        # create a new group
        rsp = self.session.post(req, headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)
        self.assertEqual(rspJson["linkCount"], 0)
        self.assertEqual(rspJson["attributeCount"], 0)
        group_id = rspJson["id"]
        self.assertTrue(helper.validateId(group_id))

        # verify we can do a get on the new group
        req = endpoint + "/groups/" + group_id
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("id" in rspJson)
        self.assertEqual(rspJson["id"], group_id)
        self.assertTrue("root" in rspJson)
        self.assertTrue(rspJson["root"] != group_id)
        self.assertTrue("domain" in rspJson)
        self.assertEqual(rspJson["domain"], self.base_domain)

        # try getting the path of the group
        params = {"getalias": 1}
        rsp = self.session.get(req, params=params, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("alias" in rspJson)
        self.assertEqual(rspJson["alias"], [])

        # try POST with user who doesn't have create permission on this domain
        test_user2 = config.get("user2_name")  # some tests will be skipped if not set
        if not test_user2:
            print("user2_name not set")
            return

        headers = helper.getRequestHeaders(
            domain=self.base_domain, username="test_user2"
        )
        req = endpoint + "/groups"
        rsp = self.session.post(req, headers=headers)
        self.assertEqual(rsp.status_code, 403)  # forbidden

    def testPostWithId(self):
        # test POST group with a client-generated id
        print("testPostWithId", self.base_domain)
        endpoint = helper.getEndpoint()
        headers = helper.getRequestHeaders(domain=self.base_domain)
        req = endpoint + "/groups"

        # get root id
        req = helper.getEndpoint() + "/"
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        root_uuid = rspJson["root"]
        helper.validateId(root_uuid)

        # create a group id
        grp_id = createObjId("groups", root_id=root_uuid)

        # create a new group using the grp_id
        payload = {"id": grp_id}
        req = helper.getEndpoint() + "/groups"
        rsp = self.session.post(req, data=json.dumps(payload), headers=headers)

        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)
        print("rspJson:", rspJson)
        self.assertEqual(rspJson["linkCount"], 0)
        self.assertEqual(rspJson["attributeCount"], 0)
        self.assertEqual(grp_id, rspJson["id"])

        # try sending the same request again
        rsp = self.session.post(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 400)  # bad request

    def testPostWithLink(self):
        # test POST with link creation
        print("testPostWithLink", self.base_domain)
        headers = helper.getRequestHeaders(domain=self.base_domain)

        # get root id
        req = helper.getEndpoint() + "/"
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        root_uuid = rspJson["root"]
        helper.validateId(root_uuid)

        # delete the domain
        rsp = self.session.delete(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)

        # try getting the domain
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 410)

        # try re-creating a domain
        rsp = self.session.put(req, headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)
        new_root_id = rspJson["root"]
        self.assertTrue(new_root_id != root_uuid)

        root_uuid = new_root_id

        # get root group and verify link count is 0
        req = helper.getEndpoint() + "/groups/" + root_uuid
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertEqual(rspJson["linkCount"], 0)

        # create new group
        payload = {"link": {"id": root_uuid, "name": "linked_group"}}
        req = helper.getEndpoint() + "/groups"
        rsp = self.session.post(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)
        self.assertEqual(rspJson["linkCount"], 0)
        self.assertEqual(rspJson["attributeCount"], 0)
        new_group_id = rspJson["id"]
        self.assertTrue(helper.validateId(rspJson["id"]))
        self.assertTrue(new_group_id != root_uuid)

        # get root group and verify link count is 1
        req = helper.getEndpoint() + "/groups/" + root_uuid
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertEqual(rspJson["linkCount"], 1)

        # read the link back and verify
        req = helper.getEndpoint() + "/groups/" + root_uuid + "/links/linked_group"
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("link" in rspJson)
        link_json = rspJson["link"]
        self.assertEqual(link_json["collection"], "groups")
        self.assertEqual(link_json["class"], "H5L_TYPE_HARD")
        self.assertEqual(link_json["title"], "linked_group")
        self.assertEqual(link_json["id"], new_group_id)

        # try getting the path of the group
        req = helper.getEndpoint() + "/groups/" + new_group_id
        params = {"getalias": 1}
        rsp = self.session.get(req, params=params, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("alias" in rspJson)
        self.assertEqual(rspJson["alias"], ["/linked_group",])

    def testPostIdWithLink(self):
        # test POST with link creation
        print("testPostIdWithLink", self.base_domain)
        headers = helper.getRequestHeaders(domain=self.base_domain)

        # get root id
        req = helper.getEndpoint() + "/"
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        root_uuid = rspJson["root"]
        helper.validateId(root_uuid)

        # create a group id
        grp_id = createObjId("groups", root_id=root_uuid)

        # create new group
        payload = {"id": grp_id, "link": {"id": root_uuid, "name": "linked_group"}}
        req = helper.getEndpoint() + "/groups"
        rsp = self.session.post(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)
        self.assertEqual(rspJson["linkCount"], 0)
        self.assertEqual(rspJson["attributeCount"], 0)
        self.assertEqual(grp_id, rspJson["id"])

    def testPostWithAttributes(self):
        # test POST with attribute initialization
        print("testPostWithAttributes", self.base_domain)
        headers = helper.getRequestHeaders(domain=self.base_domain)

        # get root id
        req = helper.getEndpoint() + "/"
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        root_uuid = rspJson["root"]
        helper.validateId(root_uuid)

        # setup some attributes to include
        attr_count = 4
        attributes = {}
        extent = 10
        for i in range(attr_count):
            value = [i * 10 + j for j in range(extent)]
            data = {"type": "H5T_STD_I32LE", "shape": extent, "value": value}
            attr_name = f"attr{i + 1:04d}"
            attributes[attr_name] = data

        # create new group
        payload = {"attributes": attributes, "link": {"id": root_uuid, "name": "linked_group"}}
        req = helper.getEndpoint() + "/groups"
        rsp = self.session.post(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)
        self.assertEqual(rspJson["linkCount"], 0)
        self.assertEqual(rspJson["attributeCount"], attr_count)
        grp_id = rspJson["id"]
        self.assertTrue(helper.validateId(grp_id))

        # fetch the attributes, check count
        req = f"{helper.getEndpoint()}/groups/{grp_id}/attributes"
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("hrefs" in rspJson)
        self.assertFalse("type" in rspJson)
        self.assertFalse("shape" in rspJson)
        self.assertTrue("attributes") in rspJson
        self.assertEqual(len(rspJson["attributes"]), attr_count)

    def testPostWithPath(self):
        # test POST with implicit parent group creation
        print("testPostWithPath", self.base_domain)
        headers = helper.getRequestHeaders(domain=self.base_domain)

        # get root id
        req = helper.getEndpoint() + "/"
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

        # create new group with link path: /g1
        payload = {"h5path": "g1"}
        req = helper.getEndpoint() + "/groups"
        rsp = self.session.post(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)
        self.assertEqual(rspJson["linkCount"], 0)
        self.assertEqual(rspJson["attributeCount"], 0)
        new_group_id = rspJson["id"]
        self.assertTrue(helper.validateId(rspJson["id"]))
        self.assertTrue(new_group_id != root_uuid)

        # get root group and verify link count is 1
        req = helper.getEndpoint() + "/groups/" + root_uuid
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertEqual(rspJson["linkCount"], 1)

        # get the group at "g1"
        req = helper.getEndpoint() + "/groups/"
        params = {"h5path": "/g1"}
        rsp = self.session.get(req, headers=headers, params=params)
        self.assertEqual(rsp.status_code, 200)

        # try creating new group with link path: /g2/g2.1
        payload = {"h5path": "g2/g2.1"}
        req = helper.getEndpoint() + "/groups"
        rsp = self.session.post(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 404)  # g2 not found

        # try again with implicit creation set
        params = {"implicit": 1}
        rsp = self.session.post(req, data=json.dumps(payload), params=params, headers=headers)
        self.assertEqual(rsp.status_code, 201)  # g2 and g2.1 created
        rspJson = json.loads(rsp.text)
        self.assertEqual(rspJson["linkCount"], 0)
        self.assertEqual(rspJson["attributeCount"], 0)
        new_group_id = rspJson["id"]
        self.assertTrue(helper.validateId(rspJson["id"]))
        self.assertTrue(new_group_id != root_uuid)

        # get root group and verify link count is 2
        req = helper.getEndpoint() + "/groups/" + root_uuid
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertEqual(rspJson["linkCount"], 2)

        # get the group at "/g2"
        req = helper.getEndpoint() + "/groups/"
        params = {"h5path": "/g2"}
        rsp = self.session.get(req, headers=headers, params=params)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertEqual(rspJson["linkCount"], 1)  # group g2.1

        # get the group at "/g2/g2.1"
        req = helper.getEndpoint() + "/groups/"
        params = {"h5path": "/g2/g2.1"}
        rsp = self.session.get(req, headers=headers, params=params)
        self.assertEqual(rsp.status_code, 200)

        # try creating new group with link path: /g2/g2.2
        payload = {"h5path": "g2/g2.2"}
        req = helper.getEndpoint() + "/groups"
        rsp = self.session.post(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)  # g2.2 created
        rspJson = json.loads(rsp.text)
        self.assertEqual(rspJson["linkCount"], 0)
        self.assertEqual(rspJson["attributeCount"], 0)
        new_group_id = rspJson["id"]
        self.assertTrue(helper.validateId(new_group_id))
        self.assertTrue(new_group_id.startswith("g-"))
        self.assertTrue(new_group_id != root_uuid)

        # get root group and verify link count is still 2
        req = helper.getEndpoint() + "/groups/" + root_uuid
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertEqual(rspJson["linkCount"], 2)

        # get the group at "/g2"
        req = helper.getEndpoint() + "/groups/"
        params = {"h5path": "/g2"}
        rsp = self.session.get(req, headers=headers, params=params)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertEqual(rspJson["linkCount"], 2)  # groups g2.1 and g2.2

        # get the group at "/g2/g2.1"
        req = helper.getEndpoint() + "/groups/"
        params = {"h5path": "/g2/g2.1"}
        rsp = self.session.get(req, headers=headers, params=params)
        self.assertEqual(rsp.status_code, 200)

    def testPostIdWithPath(self):
        # test POST with implicit parent group creation
        print("testPostIdWithPath", self.base_domain)
        headers = helper.getRequestHeaders(domain=self.base_domain)

        # get root id
        req = helper.getEndpoint() + "/"
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

        # create new group with link path: /g1
        g1_id = createObjId("groups", root_id=root_uuid)
        payload = {"id": g1_id, "h5path": "g1"}
        req = helper.getEndpoint() + "/groups"
        rsp = self.session.post(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)
        self.assertEqual(rspJson["linkCount"], 0)
        self.assertEqual(rspJson["attributeCount"], 0)
        self.assertEqual(rspJson["id"], g1_id)

        # get root group and verify link count is 1
        req = helper.getEndpoint() + "/groups/" + root_uuid
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertEqual(rspJson["linkCount"], 1)

        # get the group at "g1"
        req = helper.getEndpoint() + "/groups/"
        params = {"h5path": "/g1"}
        rsp = self.session.get(req, headers=headers, params=params)
        self.assertEqual(rsp.status_code, 200)

        # try creating new group with link path: /g2/g2.1
        g21_id = createObjId("groups", root_id=root_uuid)
        payload = {"id": g21_id, "h5path": "g2/g2.1"}
        req = helper.getEndpoint() + "/groups"
        rsp = self.session.post(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 404)  # g2 not found

        # try again with implicit creation set
        params = {"implicit": 1}
        rsp = self.session.post(req, data=json.dumps(payload), params=params, headers=headers)
        self.assertEqual(rsp.status_code, 201)  # g2 and g2.1 created
        rspJson = json.loads(rsp.text)
        self.assertEqual(rspJson["linkCount"], 0)
        self.assertEqual(rspJson["attributeCount"], 0)
        self.assertEqual(rspJson["id"], g21_id)

        # get root group and verify link count is 2
        req = helper.getEndpoint() + "/groups/" + root_uuid
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertEqual(rspJson["linkCount"], 2)

        # get the group at "/g2"
        req = helper.getEndpoint() + "/groups/"
        params = {"h5path": "/g2"}
        rsp = self.session.get(req, headers=headers, params=params)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertEqual(rspJson["linkCount"], 1)  # group g2.1

        # get the group at "/g2/g2.1"
        req = helper.getEndpoint() + "/groups/"
        params = {"h5path": "/g2/g2.1"}
        rsp = self.session.get(req, headers=headers, params=params)
        self.assertEqual(rsp.status_code, 200)

    def testPostWithCreationProps(self):
        # test POST group with creation properties
        print("testPostWithCreationProps", self.base_domain)
        endpoint = helper.getEndpoint()
        headers = helper.getRequestHeaders(domain=self.base_domain)
        req = endpoint + "/groups"

        # create a new group
        creation_props = {"CreateOrder": True, "rdcc_nbytes": 1024}
        payload = {"creationProperties": creation_props}
        rsp = self.session.post(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)
        self.assertEqual(rspJson["linkCount"], 0)
        self.assertEqual(rspJson["attributeCount"], 0)
        group_id = rspJson["id"]
        self.assertTrue(helper.validateId(group_id))

        # verify we can do a get on the new group
        req = endpoint + "/groups/" + group_id
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("id" in rspJson)
        self.assertEqual(rspJson["id"], group_id)
        self.assertTrue("root" in rspJson)
        self.assertTrue(rspJson["root"] != group_id)
        self.assertTrue("domain" in rspJson)
        self.assertTrue("creationProperties" in rspJson)
        cprops = rspJson["creationProperties"]
        for k in ("CreateOrder", "rdcc_nbytes"):
            self.assertTrue(k in cprops)
            self.assertEqual(cprops[k], creation_props[k])
        self.assertEqual(rspJson["domain"], self.base_domain)

        # try getting the path of the group
        params = {"getalias": 1}
        rsp = self.session.get(req, params=params, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("alias" in rspJson)
        self.assertEqual(rspJson["alias"], [])

    def testDelete(self):
        # test Delete
        print("testDelete", self.base_domain)
        headers = helper.getRequestHeaders(domain=self.base_domain)

        # get domain
        req = helper.getEndpoint() + "/"
        rsp = self.session.get(req, headers=headers)
        rspJson = json.loads(rsp.text)
        self.assertTrue("root" in rspJson)
        root_id = rspJson["root"]

        req = helper.getEndpoint() + "/groups"

        # create a new group
        rsp = self.session.post(req, headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)
        self.assertTrue("id" in rspJson)
        group_id = rspJson["id"]
        self.assertTrue(helper.validateId(group_id))

        # verify we can do a get on the new group
        req = helper.getEndpoint() + "/groups/" + group_id
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("id" in rspJson)
        self.assertEqual(rspJson["id"], group_id)
        self.assertTrue("root" in rspJson)
        self.assertTrue(rspJson["root"] != group_id)
        self.assertTrue("domain" in rspJson)
        self.assertEqual(rspJson["domain"], self.base_domain)

        # try DELETE with user who doesn't have create permission on this domain
        test_user2 = config.get("user2_name")  # some tests will be skipped if not set
        if test_user2:
            headers = helper.getRequestHeaders(domain=self.base_domain, username="test_user2")
            rsp = self.session.delete(req, headers=headers)
            self.assertEqual(rsp.status_code, 403)  # forbidden
        else:
            print("user2_name not set")

        # try to do a DELETE with a different domain (should fail)
        another_domain = helper.getParentDomain(self.base_domain)
        headers = helper.getRequestHeaders(domain=another_domain)
        req = helper.getEndpoint() + "/groups/" + group_id
        rsp = self.session.delete(req, headers=headers)
        self.assertEqual(rsp.status_code, 400)

        # delete the new group
        headers = helper.getRequestHeaders(domain=self.base_domain)
        rsp = self.session.delete(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue(rspJson is not None)

        # a get for the group should now return 410 (GONE)
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 410)

        # try deleting the root group
        req = helper.getEndpoint() + "/groups/" + root_id
        rsp = self.session.delete(req, headers=headers)
        self.assertEqual(rsp.status_code, 403)  # Forbidden

    def testGetByPath(self):
        domain = helper.getTestDomain("tall.h5")
        print("testGetByPath", domain)
        headers = helper.getRequestHeaders(domain=domain)

        # verify domain exists
        req = helper.getEndpoint() + "/"
        rsp = self.session.get(req, headers=headers)
        if rsp.status_code != 200:
            print(f"WARNING: Failed to get domain: {domain}. Is test data setup?")
            return  # abort rest of test

        rspJson = json.loads(rsp.text)
        root_uuid = rspJson["root"]
        helper.validateId(root_uuid)

        # get the group at "/g1/g1.1"
        h5path = "/g1/g1.1"
        req = helper.getEndpoint() + "/groups/"
        params = {"h5path": h5path}
        rsp = self.session.get(req, headers=headers, params=params)
        self.assertEqual(rsp.status_code, 200)

        rspJson = json.loads(rsp.text)
        for name in (
            "id",
            "hrefs",
            "attributeCount",
            "linkCount",
            "domain",
            "root",
            "created",
            "lastModified",
        ):
            self.assertTrue(name in rspJson)

        # verify we get the same id when following the path via service calls
        g11id = helper.getUUIDByPath(domain, "/g1/g1.1", session=self.session)
        self.assertEqual(g11id, rspJson["id"])

        # Try with a trailing slash
        h5path = "/g1/g1.1/"
        req = helper.getEndpoint() + "/groups/"
        params = {"h5path": h5path}
        rsp = self.session.get(req, headers=headers, params=params)
        self.assertEqual(rsp.status_code, 200)

        rspJson = json.loads(rsp.text)
        self.assertEqual(g11id, rspJson["id"])

        # try relative h5path
        g1id = helper.getUUIDByPath(domain, "/g1/", session=self.session)
        h5path = "./g1.1"
        req = helper.getEndpoint() + "/groups/" + g1id
        params = {"h5path": h5path}
        rsp = self.session.get(req, headers=headers, params=params)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertEqual(g11id, rspJson["id"])

        # try a invalid link and verify a 404 is returened
        h5path = "/g1/foobar"
        req = helper.getEndpoint() + "/groups/"
        params = {"h5path": h5path}
        rsp = self.session.get(req, headers=headers, params=params)
        self.assertEqual(rsp.status_code, 404)

        # try passing a path to a dataset and verify we get 404
        h5path = "/g1/g1.1/dset1.1.1"
        req = helper.getEndpoint() + "/groups/"
        params = {"h5path": h5path}
        rsp = self.session.get(req, headers=headers, params=params)
        self.assertEqual(rsp.status_code, 404)

        # try getting the path of the group
        req = helper.getEndpoint() + "/groups/" + g11id
        params = {"getalias": 1}
        rsp = self.session.get(req, params=params, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("alias" in rspJson)
        self.assertEqual(rspJson["alias"], ["/g1/g1.1", ])


if __name__ == "__main__":
    # setup test files

    unittest.main()
