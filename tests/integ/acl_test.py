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
import config
import helper

acl_keys = ("create", "read", "update", "delete", "readACL", "updateACL")


class AclTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(AclTest, self).__init__(*args, **kwargs)
        self.base_domain = helper.getTestDomainName(self.__class__.__name__)
        helper.setupDomain(self.base_domain)

    def setUp(self):
        self.session = helper.getSession()

    def tearDown(self):
        if self.session:
            self.session.close()

        # main

    def testGetAcl(self):
        print("testGetAcl", self.base_domain)
        headers = helper.getRequestHeaders(domain=self.base_domain)

        # there should be an ACL for "test_user1" who has ability to do any action on the domain
        username = config.get("user_name")
        req = helper.getEndpoint() + "/acls/" + username
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        self.assertEqual(rsp.headers["content-type"], "application/json; charset=utf-8")
        rsp_json = json.loads(rsp.text)
        self.assertTrue("acl" in rsp_json)
        self.assertTrue("hrefs" in rsp_json)
        acl = rsp_json["acl"]
        self.assertEqual(len(acl.keys()), len(acl_keys) + 1)
        self.assertEqual(acl["userName"], username)
        for k in acl_keys:
            self.assertTrue(k in acl)
            self.assertTrue(acl[k])

        # get the default ACL.  Only 'read' should be true if it exists
        req = helper.getEndpoint() + "/acls/default"
        rsp = self.session.get(req, headers=headers)
        if config.get("default_public"):
            self.assertEqual(rsp.status_code, 200)
            self.assertEqual(
                rsp.headers["content-type"], "application/json; charset=utf-8"
            )
            rsp_json = json.loads(rsp.text)
            self.assertTrue("acl" in rsp_json)
            self.assertTrue("hrefs" in rsp_json)
            acl = rsp_json["acl"]
            self.assertEqual(len(acl.keys()), len(acl_keys) + 1)
            for k in acl_keys:
                self.assertTrue(k in acl)
                if k == "read":
                    self.assertEqual(acl[k], True)
                else:
                    self.assertEqual(acl[k], False)
        else:
            self.assertTrue(rsp.status_code == 404)

        # get the root id
        req = helper.getEndpoint() + "/"
        rsp = self.session.get(req, headers=headers)
        rspJson = json.loads(rsp.text)
        self.assertTrue("root" in rspJson)

        # try getting the ACL for a random user, should return 404
        req = helper.getEndpoint() + "/acls/joebob"
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 404)

        # try fetching an ACL from a user who doesn't have readACL permissions
        req = helper.getEndpoint() + "/acls/" + username
        user2name = config.get("user2_name")
        if user2name:
            headers = helper.getRequestHeaders(domain=self.base_domain, username=user2name)
            rsp = self.session.get(req, headers=headers)
            self.assertEqual(rsp.status_code, 403)  # forbidden
        else:
            print("user2_name not set")

    def testGetAcls(self):
        print("testGetAcls", self.base_domain)
        headers = helper.getRequestHeaders(domain=self.base_domain)
        if config.get("default_public"):
            expected_acl_count = 2
        else:
            expected_acl_count = 1

        # there should be an ACL for "default" with read-only access and
        #  "test_user1" who has ability to do any action on the domain
        username = config.get("user_name")
        req = helper.getEndpoint() + "/acls"
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        self.assertEqual(rsp.headers["content-type"], "application/json; charset=utf-8")
        rsp_json = json.loads(rsp.text)
        self.assertTrue("acls" in rsp_json)
        self.assertTrue("hrefs" in rsp_json)
        acls = rsp_json["acls"]

        self.assertEqual(len(acls), expected_acl_count)

        for acl in acls:
            self.assertEqual(len(acl.keys()), len(acl_keys) + 1)
            self.assertTrue("userName" in acl)
            userName = acl["userName"]
            self.assertTrue(userName in ("default", username))
            if userName == "default":
                if expected_acl_count == 1:
                    # should just have the owner acl
                    self.assertTrue(False)
                for k in acl.keys():
                    if k == "userName":
                        continue
                    if k not in acl_keys:
                        self.assertTrue(False)
                    if k == "read":
                        self.assertEqual(acl[k], True)
                    else:
                        self.assertEqual(acl[k], False)
            else:
                for k in acl.keys():
                    if k == "userName":
                        continue
                    if k not in acl_keys:
                        self.assertTrue(False)
                    self.assertEqual(acl[k], True)

        # get root uuid
        req = helper.getEndpoint() + "/"
        rsp = self.session.get(req, headers=headers)
        rspJson = json.loads(rsp.text)
        self.assertTrue("root" in rspJson)
        root_uuid = rspJson["root"]

        # create a dataset
        payload = {
            "type": "H5T_STD_I32LE",
            "shape": 10,
            "link": {"id": root_uuid, "name": "dset"},
        }
        req = helper.getEndpoint() + "/datasets"
        rsp = self.session.post(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)  # create dataset
        rspJson = json.loads(rsp.text)
        dset_uuid = rspJson["id"]
        self.assertTrue(helper.validateId(dset_uuid))

        # create a committed type
        payload = {"type": "H5T_IEEE_F64LE", "link": {"id": root_uuid, "name": "dtype"}}

        req = helper.getEndpoint() + "/datatypes"
        # create a new ctype
        rsp = self.session.post(req, data=json.dumps(payload), headers=headers)
        self.assertEqual(rsp.status_code, 201)
        rspJson = json.loads(rsp.text)
        self.assertEqual(rspJson["attributeCount"], 0)
        dtype_uuid = rspJson["id"]
        self.assertTrue(helper.validateId(dtype_uuid))

        # try fetching ACLs from a user who doesn't have readACL permissions
        req = helper.getEndpoint() + "/acls"
        user2name = config.get("user2_name")
        if user2name:
            headers = helper.getRequestHeaders(domain=self.base_domain, username=user2name)
            rsp = self.session.get(req, headers=headers)
            self.assertEqual(rsp.status_code, 403)  # forbidden
        else:
            print("user2_name not set")

    def testPutAcl(self):
        print("testPutAcl", self.base_domain)
        headers = helper.getRequestHeaders(domain=self.base_domain)

        # create an ACL for "test_user2" with read and update access
        user2name = config.get("user2_name")
        if not user2name:
            print("user2_name not set")
            return
        req = helper.getEndpoint() + "/acls/" + user2name
        perm = {"read": True, "update": True}

        rsp = self.session.put(req, headers=headers, data=json.dumps(perm))
        self.assertEqual(rsp.status_code, 201)

        # fetch the acl and verify it has been updated
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rsp_json = json.loads(rsp.text)
        self.assertTrue("acl" in rsp_json)
        self.assertTrue("hrefs" in rsp_json)
        acl = rsp_json["acl"]
        self.assertEqual(
            len(acl.keys()), len(acl_keys) + 2
        )  # acl_keys + "domain" + "username"

        for k in acl_keys:
            self.assertTrue(k in acl)
            if k in ("read", "update"):
                self.assertEqual(acl[k], True)
            else:
                self.assertEqual(acl[k], False)

        # The ACL should be fetchable by test_user2...
        req = helper.getEndpoint() + "/acls/" + user2name
        headers = helper.getRequestHeaders(domain=self.base_domain, username=user2name)
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)  # ok

        # The default ACL should be fetchable by test_user2 as well...
        if config.get("default_public"):
            req = helper.getEndpoint() + "/acls/default"
            headers = helper.getRequestHeaders(
                domain=self.base_domain, username=user2name
            )
            rsp = self.session.get(req, headers=headers)
            self.assertEqual(rsp.status_code, 200)  # ok

        # test_user2 shouldn't be able to read test_user1's ACL
        username = config.get("user_name")
        req = helper.getEndpoint() + "/acls/" + username
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 403)  # Forbidden

    def testPutAclPermissionCheck(self):
        """
        Regression test for a fixed security gap: PUT /acls/{username}
        used to perform no ACL/permission check at all, so any
        authenticated user - even one with no permissions whatsoever on
        the domain - could grant or revoke any permission (including
        updateACL, i.e. ownership-equivalent access) for any user on the
        domain. PUT_ACL (hsds/domain_sn.py) now calls aclCheck with the
        "updateACL" action before forwarding the write to the DN, same as
        every other ACL/domain-mutating operation.
        """
        print("testPutAclPermissionCheck", self.base_domain)

        user2name = config.get("user2_name")
        if not user2name:
            print("user2_name not set")
            return

        # test_user2 has no ACL entry at all on this domain (only the
        # owner, test_user1, does by default) - so test_user2 should not
        # be able to grant/revoke ANY permission for ANY user here,
        # including a third, unrelated user ("joebob") who isn't even
        # test_user2 themselves.
        headers = helper.getRequestHeaders(domain=self.base_domain, username=user2name)

        req = helper.getEndpoint() + "/acls/joebob"
        perm = {
            "create": True,
            "read": True,
            "update": True,
            "delete": True,
            "readACL": True,
            "updateACL": True,
        }
        rsp = self.session.put(req, headers=headers, data=json.dumps(perm))
        # test_user2 lacks updateACL permission on this domain, so this
        # must be rejected
        self.assertEqual(rsp.status_code, 403)

    def testGroupAcl(self):
        print("testPutAcl", self.base_domain)
        headers = helper.getRequestHeaders(domain=self.base_domain)

        test_group = config.get("test_group")
        if not test_group:
            print("test_group config not set, skipping testGroupAcl")
            return
        group_acl = (
            "g:" + test_group
        )  # "g:" prefix is used to distinguish user from group names
        username = config.get("user_name")
        user2name = config.get("user2_name")

        # create an ACL for with read and update access for the test_group
        req = helper.getEndpoint() + "/acls/" + group_acl
        perm = {"read": True, "update": True}

        rsp = self.session.put(req, headers=headers, data=json.dumps(perm))
        self.assertEqual(rsp.status_code, 201)

        # fetch the acl and verify it has been updated
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        rsp_json = json.loads(rsp.text)
        self.assertTrue("acl" in rsp_json)
        self.assertTrue("hrefs" in rsp_json)
        acl = rsp_json["acl"]
        self.assertEqual(
            len(acl.keys()), len(acl_keys) + 2
        )  # acl_keys + "domain" + "username"

        for k in acl_keys:
            self.assertTrue(k in acl)
            if k in ("read", "update"):
                self.assertEqual(acl[k], True)
            else:
                self.assertEqual(acl[k], False)

        # The ACL should not be fetchable by test_user2...
        req = helper.getEndpoint() + "/acls/" + user2name
        headers = helper.getRequestHeaders(domain=self.base_domain, username=user2name)
        rsp = self.session.get(req, headers=headers)
        self.assertTrue(rsp.status_code in (403, 404))  # forbidden or not found

        # The default ACL should be fetchable by test_user2 as well...
        if config.get("default_public"):
            req = helper.getEndpoint() + "/acls/default"
            headers = helper.getRequestHeaders(
                domain=self.base_domain, username=user2name
            )
            rsp = self.session.get(req, headers=headers)
            self.assertEqual(rsp.status_code, 200)  # ok
        else:
            self.assertTrue(rsp.status_code in (403, 404))  # forbidden or not found

        # test_user2 shouldn't be able to read test_user1's ACL
        username = config.get("user_name")
        req = helper.getEndpoint() + "/acls/" + username
        rsp = self.session.get(req, headers=headers)
        self.assertEqual(rsp.status_code, 403)  # Forbidden

    def _privateDomain(self, name):
        """Create an admin-owned domain beside the test domain that test_user1 can't read.
        Returns (domain, root_id), or (None, None) if the setup doesn't allow it."""
        admin = config.get("admin_username")
        if not admin or config.get("default_public"):
            print("admin_username not set or default_public on, skipping")
            return None, None
        admin_pw = config.get("admin_password")
        folder = helper.getParentDomain(self.base_domain)
        domain = folder + "/" + helper.getRandomName() + "_" + name
        req = helper.getEndpoint() + "/"
        admin_headers = helper.getRequestHeaders(domain=domain, username=admin, password=admin_pw)
        rsp = self.session.put(req, headers=admin_headers)
        self.assertEqual(rsp.status_code, 201)
        self.addCleanup(self.session.delete, req, headers=admin_headers)
        root_id = rsp.json()["root"]
        # domains inherit the folder owner's acl; take test_user1's away
        acl_req = helper.getEndpoint() + "/acls/" + config.get("user_name")
        no_access = {k: False for k in acl_keys}
        rsp = self.session.put(acl_req, data=json.dumps(no_access), headers=admin_headers)
        self.assertEqual(rsp.status_code, 201)
        attr_req = helper.getEndpoint() + "/groups/" + root_id + "/attributes/secret"
        payload = {"type": "H5T_STD_I32LE", "value": 424242}
        rsp = self.session.put(attr_req, data=json.dumps(payload), headers=admin_headers)
        self.assertEqual(rsp.status_code, 201)
        rsp = self.session.get(req, headers=helper.getRequestHeaders(domain=domain))
        self.assertEqual(rsp.status_code, 403)  # test_user1 can't read it
        return domain, root_id

    def testParentIdOutsideDomain(self):
        # parent_id must belong to the domain whose access was checked
        print("testParentIdOutsideDomain", self.base_domain)
        _, victim_root = self._privateDomain("parent_id_victim.h5")
        if not victim_root:
            return
        req = helper.getEndpoint() + "/"
        headers = helper.getRequestHeaders(domain=self.base_domain)
        params = {"h5path": ".", "parent_id": victim_root, "include_attrs": 1}
        rsp = self.session.get(req, params=params, headers=headers)
        self.assertEqual(rsp.status_code, 400)
        self.assertNotIn("424242", rsp.text)

        body = {"h5paths": ["."]}
        rsp = self.session.post(req, params=params, data=json.dumps(body), headers=headers)
        self.assertEqual(rsp.status_code, 400)
        self.assertNotIn("424242", rsp.text)

        # anonymous, against the (readable) folder holding the domains
        folder = helper.getParentDomain(self.base_domain)
        rsp = self.session.get(req, params={"domain": folder, **params})
        self.assertIn(rsp.status_code, (400, 401, 403))
        self.assertNotIn("424242", rsp.text)

    def testExternalLinkToUnreadableDomain(self):
        # following an external link checks read access on the target domain
        print("testExternalLinkToUnreadableDomain", self.base_domain)
        victim, _ = self._privateDomain("external_link_victim.h5")
        if not victim:
            return
        headers = helper.getRequestHeaders(domain=self.base_domain)
        req = helper.getEndpoint() + "/"
        root_id = self.session.get(req, headers=headers).json()["root"]
        link_req = helper.getEndpoint() + "/groups/" + root_id + "/links/ext_private"
        rsp = self.session.put(link_req, data=json.dumps({"h5path": "/", "file": victim}),
                               headers=headers)
        self.assertEqual(rsp.status_code, 201)

        params = {"h5path": "/ext_private", "follow_external_links": 1, "include_attrs": 1}
        rsp = self.session.get(req, params=params, headers=headers)
        self.assertEqual(rsp.status_code, 403)
        self.assertNotIn("424242", rsp.text)

        body = {"h5paths": ["/ext_private"]}
        rsp = self.session.post(req, params=params, data=json.dumps(body), headers=headers)
        self.assertEqual(rsp.status_code, 403)
        self.assertNotIn("424242", rsp.text)

    def testListingHidesUnreadable(self):
        # GET /domains lists, and queries, only domains the caller can read
        print("testListingHidesUnreadable", self.base_domain)
        victim, victim_root = self._privateDomain("listing_victim.h5")
        if not victim:
            return
        folder = helper.getParentDomain(self.base_domain)
        req = helper.getEndpoint() + "/domains"
        headers = helper.getRequestHeaders()
        admin_headers = helper.getRequestHeaders(username=config.get("admin_username"),
                                                 password=config.get("admin_password"))

        params = {"domain": folder + "/", "verbose": 1}
        rsp = self.session.get(req, params=params, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        names = [d["name"] for d in rsp.json()["domains"]]
        self.assertIn(self.base_domain, names)
        self.assertNotIn(victim, names)
        self.assertNotIn(victim_root, rsp.text)
        rsp = self.session.get(req, params=params, headers=admin_headers)
        self.assertIn(victim, [d["name"] for d in rsp.json()["domains"]])

        # an attribute query mustn't reveal anything about unreadable domains
        params = {"domain": folder + "/", "query": "secret > 1"}
        rsp = self.session.get(req, params=params, headers=headers)
        self.assertEqual(rsp.status_code, 200)
        self.assertNotIn(victim, [d["name"] for d in rsp.json()["domains"]])

        # a folder without read access can't be listed at all
        private_folder = folder + "/" + helper.getRandomName() + "_private_folder"
        folder_headers = helper.getRequestHeaders(domain=private_folder,
                                                  username=config.get("admin_username"),
                                                  password=config.get("admin_password"))
        root_req = helper.getEndpoint() + "/"
        rsp = self.session.put(root_req, data=json.dumps({"folder": True}), headers=folder_headers)
        self.assertEqual(rsp.status_code, 201)
        self.addCleanup(self.session.delete, root_req, headers=folder_headers)
        for acl_user in ("default", config.get("user_name")):
            acl_req = helper.getEndpoint() + "/acls/" + acl_user
            rsp = self.session.put(acl_req, data=json.dumps({"read": False}),
                                   headers=folder_headers)
            self.assertEqual(rsp.status_code, 201)
        rsp = self.session.get(req, params={"domain": private_folder + "/"}, headers=headers)
        self.assertEqual(rsp.status_code, 403)


if __name__ == "__main__":
    # setup test files

    unittest.main()
