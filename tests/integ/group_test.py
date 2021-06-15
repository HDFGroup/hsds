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
        req = helper.getEndpoint() + '/'

        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        root_uuid = rspJson["root"]
        helper.validateId(root_uuid)
        req = helper.getEndpoint() + '/groups/' + root_uuid
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
        #self.assertEqual(rspJson["domain"], self.base_domain) #TBD
        self.assertTrue("created" in rspJson)
        self.assertTrue("lastModified" in rspJson)
        self.assertTrue("linkCount" in rspJson)
        self.assertTrue("attributeCount" in rspJson)

        # try get with a different user (who has read permission)
        headers = helper.getRequestHeaders(domain=self.base_domain, username="test_user2")
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
        req = helper.getEndpoint() + '/groups/' + root_uuid
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 400)

    def testGet(self):
        domain = helper.getTestDomain("tall.h5")

        headers = helper.getRequestHeaders(domain=domain)

        # verify domain exists
        req = helper.getEndpoint() + '/'
        rsp = self.session.get(req, headers=headers)
        if rsp.status_code != 200:
            print("WARNING: Failed to get domain: {}. Is test data setup?".format(domain))
            return  # abort rest of test

        rspJson = json.loads(rsp.text)

        grp_uuid = root_uuid = rspJson["root"]
        self.assertTrue(grp_uuid.startswith("g-"))

        # get the group json
        req = helper.getEndpoint() + '/groups/' + grp_uuid
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        for name in ("id", "hrefs", "attributeCount", "linkCount",
            "domain", "root", "created", "lastModified"):
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
        req = helper.getEndpoint() + '/groups/' + grp_uuid
        params = {"getalias": 1}
        rsp = self.session.get(req, params=params, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("alias" in rspJson)
        self.assertEqual(rspJson["alias"], ['/'])

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
        req = helper.getEndpoint() + '/groups/' + grp_uuid
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 400)

    def testGetInvalidUUID(self):
        print("testGetInvalidUUID", self.base_domain)
        headers = helper.getRequestHeaders(domain=self.base_domain)
        req = helper.getEndpoint() + '/'
        invalid_uuid = "foobar"
        req = helper.getEndpoint() + "/groups/" + invalid_uuid
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 400)

        import uuid
        bad_uuid = "g-" + str(uuid.uuid1())
        req = helper.getEndpoint() + "/groups/" + bad_uuid
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 404)

    def testPost(self):
        # test POST group
        print("testPost", self.base_domain)
        endpoint = helper.getEndpoint()
        headers = helper.getRequestHeaders(domain=self.base_domain)
        req = endpoint + '/groups'

        # create a new group
        rsp = self.session.post(req, headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)
        self.assertEqual(rspJson["linkCount"], 0)
        self.assertEqual(rspJson["attributeCount"], 0)
        group_id = rspJson["id"]
        self.assertTrue(helper.validateId(group_id))

        # verify we can do a get on the new group
        req = endpoint + '/groups/' + group_id
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("id" in rspJson)
        self.assertEqual(rspJson["id"], group_id)
        self.assertTrue("root" in rspJson)
        self.assertTrue(rspJson["root"] != group_id)
        self.assertTrue("domain" in rspJson)
        #self.assertEqual(rspJson["domain"], domain) # TBD

        # try getting the path of the group
        params = {"getalias": 1}
        rsp = self.session.get(req, params=params, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("alias" in rspJson)
        self.assertEqual(rspJson["alias"], [])


        # try POST with user who doesn't have create permission on this domain
        headers = helper.getRequestHeaders(domain=self.base_domain, username="test_user2")
        req = endpoint + '/groups'
        rsp = self.session.post(req, headers=headers)
        self.assertEqual(rsp.status_code, 403) # forbidden

    def testPostWithLink(self):
        # test PUT_root
        print("testPostWithLink", self.base_domain)
        headers = helper.getRequestHeaders(domain=self.base_domain)

        # get root id
        req = helper.getEndpoint() + '/'
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
        req = helper.getEndpoint() + '/groups/' + root_uuid
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertEqual(rspJson["linkCount"], 0)

        # create new group
        payload = { 'link': { 'id': root_uuid, 'name': 'linked_group' } }
        req = helper.getEndpoint() + "/groups"
        rsp = self.session.post(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)
        self.assertEqual(rspJson["linkCount"], 0)
        self.assertEqual(rspJson["attributeCount"], 0)
        new_group_id = rspJson["id"]
        self.assertTrue(helper.validateId(rspJson["id"]) )
        self.assertTrue(new_group_id != root_uuid)

        # get root group and verify link count is 1
        req = helper.getEndpoint() + '/groups/' + root_uuid
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertEqual(rspJson["linkCount"], 1)

        # read the link back and verify
        req = helper.getEndpoint() + "/groups/" + root_uuid + "/links/linked_group"
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)  # link doesn't exist yet
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
        self.assertEqual(rspJson["alias"], ['/linked_group',])

    def testDelete(self):
        # test Delete
        print("testDelete", self.base_domain)
        headers = helper.getRequestHeaders(domain=self.base_domain)

        # get domain
        req = helper.getEndpoint() + '/'
        rsp = self.session.get(req, headers=headers)
        rspJson = json.loads(rsp.text)
        self.assertTrue("root" in rspJson)
        root_id = rspJson["root"]

        req = helper.getEndpoint() + '/groups'

        # create a new group
        rsp = self.session.post(req, headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)
        self.assertTrue("id" in rspJson)
        group_id = rspJson["id"]
        self.assertTrue(helper.validateId(group_id))

        # verify we can do a get on the new group
        req = helper.getEndpoint() + '/groups/' + group_id
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rspJson = json.loads(rsp.text)
        self.assertTrue("id" in rspJson)
        self.assertEqual(rspJson["id"], group_id)
        self.assertTrue("root" in rspJson)
        self.assertTrue(rspJson["root"] != group_id)
        self.assertTrue("domain" in rspJson)
        #self.assertEqual(rspJson["domain"], self.base_domain)  #TBD

        # try DELETE with user who doesn't have create permission on this domain
        headers = helper.getRequestHeaders(domain=self.base_domain, username="test_user2")
        rsp = self.session.delete(req, headers=headers)
        self.assertEqual(rsp.status_code, 403) # forbidden

        # try to do a DELETE with a different domain (should fail)
        another_domain = helper.getParentDomain(self.base_domain)
        headers = helper.getRequestHeaders(domain=another_domain)
        req = helper.getEndpoint() + '/groups/' + group_id
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
        req = helper.getEndpoint() + '/groups/' + root_id
        rsp = self.session.delete(req, headers=headers)
        self.assertEqual(rsp.status_code, 403)  # Forbidden

    def testGetByPath(self):
        domain = helper.getTestDomain("tall.h5")
        print("testGetByPath", domain)
        headers = helper.getRequestHeaders(domain=domain)

        # verify domain exists
        req = helper.getEndpoint() + '/'
        rsp = self.session.get(req, headers=headers)
        if rsp.status_code != 200:
            print("WARNING: Failed to get domain: {}. Is test data setup?".format(domain))
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
        for name in ("id", "hrefs", "attributeCount", "linkCount",
            "domain", "root", "created", "lastModified"):
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
        self.assertEqual(rspJson["alias"], ['/g1/g1.1',])

if __name__ == '__main__':
    #setup test files

    unittest.main()
